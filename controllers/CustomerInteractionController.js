const db = require("../config/db");
const { chat } = require("../config/openai");
const { orderProducts } = require("../categoryHandlers/ORD/CREATE");
const { modifyOrder } = require("../categoryHandlers/ORD/MODIFY");
const { buildOrderReviewMessage } = require("../categoryHandlers/ORD/REVIEW");
const {
  getPromptFromDB,
  buildClassifierContextHeader,
  buildOpenQuestionsContext,
} = require("../repositories/prompt");
const {
  fetchOpenQuestions,
  fetchRecentClosedQuestions,
} = require("../utilities/openQuestions");
const { normalizeOutboundMessage } = require("../utilities/normalize");
const {
  getActiveOrder,
  getOrderItems,
  buildActiveOrderSignals,
} = require("../utilities/orders");
const { detectIsEnglish } = require("../utilities/lang");
const CLASSIFIER_PROMPT_CAT = "initial";
const CLASSIFIER_PROMPT_SUB = "initial-classification";

async function wasSentBefore(customer_id, shop_id, message) {
  const DEDUP_WINDOW_SECONDS = 30;
  const [recentSame] = await db.query(
    `
      SELECT id
      FROM chat
      WHERE customer_id = ?
        AND shop_id = ?
        AND sender = 'customer'
        AND message = ?
        AND created_at >= (NOW() - INTERVAL ? SECOND)
      ORDER BY id DESC
      LIMIT 1
      `,
    [customer_id, shop_id, message, DEDUP_WINDOW_SECONDS]
  );

  if (recentSame.length) {
    console.log("[DEDUP] skipping logically duplicate message", {
      customer_id,
      shop_id,
      message,
    });
    return true;
  }

  return false;
}

function isValidCategorySub(category, subcategory) {
  const ALLOWED = {
    ORD: new Set(["CREATE", "MODIFY", "REVIEW", "CHECKOUT", "CANCEL"]),
    ARR: new Set(["ADDRESS", "PARKING", "ACCESS", "PUBLIC_TRAN"]),
    HRS: new Set(["REGULAR_DAYS", "UNUSUAL"]), // normalize ל-UPPERCASE
    INV: new Set(["AVAIL", "SUBSTITUTE", "SUGGEST"]),
    PRM: new Set(["CURRENT"]),
    SHP: new Set(["ZONES", "SLOTS", "COST", "TRACK", "INSTRUCTIONS"]),
    BUS: new Set(["CONTACT", "BRANCHES", "RETURNS", "POLICY", "PAYMENT"]),
  };

  if (!category || !subcategory) return false;
  const cat = String(category).toUpperCase().trim();
  const sub = String(subcategory).toUpperCase().trim();
  return !!(ALLOWED[cat] && ALLOWED[cat].has(sub));
}

async function ensureCustomer(shop_id, phone) {
  const [rows] = await db.query(
    `SELECT id FROM customer WHERE shop_id = ? AND phone = ? LIMIT 1`,
    [shop_id, phone]
  );
  if (rows.length) return rows[0].id;

  const [ins] = await db.query(
    `INSERT INTO customer (name, shop_id, phone, email)
     VALUES (?, ?, ?, NULL)`,
    [phone, shop_id, phone]
  );
  return ins.insertId;
}

async function getHistory(customer_id, shop_id, maxMsgs = 7) {
  const [rows] = await db.query(
    `SELECT sender, status, message, created_at
       FROM chat
      WHERE customer_id = ? 
        AND shop_id = ?
        AND created_at >= (NOW() - INTERVAL 48 HOUR)
      ORDER BY created_at DESC, id DESC
      LIMIT 50`,
    [customer_id, shop_id]
  );

  const pickedDesc = [];
  for (const r of rows) {
    if (r.status === "close") break;
    const content = (r.message || "").trim();
    if (!content) continue;
    const role = r.sender === "customer" ? "user" : "assistant";
    pickedDesc.push({ role, content });
    if (pickedDesc.length >= maxMsgs) break;
  }

  return pickedDesc.reverse();
}

async function saveChat({ customer_id, shop_id, sender, status, message }) {
  const [ins] = await db.query(
    `INSERT INTO chat (customer_id, shop_id, message, sender, status)
     VALUES (?, ?, ?, ?, ?)`,
    [customer_id, shop_id, message || "", sender, status]
  );
  return ins.insertId;
}

function parseModelMessage(raw) {
  if (!raw || typeof raw !== "string") return { type: "raw", text: "" };

  const parts = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  // 0, <question>
  if (parts[0] === "0") {
    const rest = raw.slice(raw.indexOf(",") + 1).trim();
    return { type: "clarify", text: rest };
  }

  if (parts[0] === "1") {
    let category = "";
    let subcategory = "";

    if (parts.length >= 3) {
      category = (parts[1] || "").toUpperCase().trim();
      const subRaw = (parts[2] || "").trim();
      subcategory = subRaw.split(".").pop().toUpperCase().trim();
    } else if (parts.length === 2) {
      const m = parts[1].match(/^([A-Za-z]+)\s*[\.\-\/]\s*([A-Za-z_.]+)$/);
      if (m) {
        category = m[1].toUpperCase().trim();
        subcategory = m[2].toUpperCase().split(".").pop().trim();
      } else {
        category = parts[1].toUpperCase().trim();
        subcategory = "";
      }
    }

    return { type: "classified", category, subcategory };
  }

  return { type: "raw", text: raw };
}

module.exports = {
  async processMessage(message, phone_number, shop_id) {
    const customer_id = await ensureCustomer(shop_id, phone_number);

    const wasSent = await wasSentBefore(customer_id, shop_id, message);
    if (wasSent) {
      return { skipSend: true };
    }

    const activeOrder = await getActiveOrder(customer_id, shop_id);
    const order_id = activeOrder ? activeOrder.id : null;
    const items = activeOrder ? await getOrderItems(activeOrder.id) : [];
    const sig = buildActiveOrderSignals(activeOrder, items);
    const openQs = await fetchOpenQuestions(customer_id, shop_id, 7);
    const closedQs = await fetchRecentClosedQuestions(customer_id, shop_id, 5);

    let systemPromptBase = await getPromptFromDB(
      CLASSIFIER_PROMPT_CAT,
      CLASSIFIER_PROMPT_SUB
    );
    const contextHeader = buildClassifierContextHeader({ sig });
    const openQuestionsCtx = buildOpenQuestionsContext({ openQs, closedQs });
    const systemPrompt = [
      systemPromptBase,
      "",
      "=== STRUCTURED CONTEXT ===",
      contextHeader,
      openQuestionsCtx,
    ].join("\n");

    let history = await getHistory(customer_id, shop_id, 7);
    console.log("history:", history);
    const answer = await chat({ message, history, systemPrompt });
    const replyText =
      typeof answer === "string"
        ? answer
        : answer && typeof answer.message === "string"
        ? answer.message
        : "";

    const parsed = parseModelMessage(replyText);

    if (parsed.type === "clarify") {
      await saveChat({
        customer_id,
        shop_id,
        sender: "customer",
        status: "unclassified",
        message,
      });

      //bot's question
      await saveChat({
        customer_id,
        shop_id,
        sender: "bot",
        status: "unclassified",
        message: parsed.text,
      });
      return parsed.text || "לא התקבלה תשובה מהמודל.";
    } else if (parsed.type === "classified") {
      const { category, subcategory } = parsed;
      console.log(
        `[classification] category=${category}, subcategory=${subcategory}`
      );

      if (!isValidCategorySub(category, subcategory)) {
        const apology =
          "מצטערים, לא הצלחנו להבין את הבקשה. נשמח אם תנוסח שוב בקצרה";
        await saveChat({
          customer_id,
          shop_id,
          sender: "customer",
          status: "unclassified",
          message,
        });

        await saveChat({
          customer_id,
          shop_id,
          sender: "bot",
          status: "unclassified",
          message: apology,
        });
        return apology;
      }

      await saveChat({
        customer_id,
        shop_id,
        sender: "customer",
        status: "classified",
        message,
      });

      let botPayload = "";
      if (category == "ORD") {
        if (subcategory == "CREATE") {
          botPayload = await orderProducts({
            message,
            customer_id,
            shop_id,
            history,
            openQuestions: openQs,
            recentClosed: closedQs,
          });
        } else if (subcategory == "MODIFY") {
          botPayload = await modifyOrder({
            message,
            customer_id,
            shop_id,
            order_id,
            activeOrder,
            items,
            history,
            openQuestions: openQs,
            recentClosed: closedQs,
          });
        } else if (subcategory == "REVIEW") {
          const isEnglish = detectIsEnglish(message);
          botPayload = buildOrderReviewMessage(activeOrder, items, isEnglish);
        } else if (subcategory == "CHECKOUT" || subcategory == "CANCEL") {
          // placeholder עד שימומש
          const isEnglish = detectIsEnglish(message);
          botPayload = isEnglish
            ? "At the moment you can only create, modify and review orders. Checkout and cancel will be available soon."
            : "כרגע יש לנו תמיכה רק ביצירת, עריכת וצפייה בהזמנות. סיום וביטול הזמנה יתווספו בהמשך.";
        }

        const botText = normalizeOutboundMessage(botPayload);
        await saveChat({
          customer_id,
          shop_id,
          sender: "bot",
          status: "classified",
          message: botText,
        });

        return botPayload;
      }

      //temporary until the case is handled
      await saveChat({
        customer_id,
        shop_id,
        sender: "server",
        status: "close",
        message: "",
      });
      return "כרגע יש לנו תמיכה רק ביצירת, עריכת וצפייה בהזמנות אבל זיהינו שהכוונה שלך היא לא אחת מאלו";
    } else {
      //if not classified and not clarify
      await saveChat({
        customer_id,
        shop_id,
        sender: "customer",
        status: "unclassified",
        message,
      });
      await saveChat({
        customer_id,
        shop_id,
        sender: "bot",
        status: "unclassified",
        message: replyText || "",
      });
      return replyText || "לא התקבלה תשובה מהמודל.";
    }
  },

  async handleMessage(req, res, next) {
    const { message, phone_number, shop_id } = req.body;
    if (!message || typeof message !== "string" || !phone_number || !shop_id) {
      return res.status(400).json({
        success: false,
        message: "message, phone_number and shop_id are required",
      });
    }

    try {
      const responseMessage = await module.exports.processMessage(
        message,
        phone_number,
        shop_id
      );

      const messageText = normalizeOutboundMessage(responseMessage);
      res.json({ success: true, message: messageText });
    } catch (error) {
      console.error(error);
      next(error);
    }
  },
};

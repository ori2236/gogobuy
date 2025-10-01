const db = require("../config/db");
const { chat } = require("../config/openai");
const { searchProducts } = require("../categoryHandlers/ORD-CREATE");
const { getPromptFromDB } = require("../repositories/prompt");

const DEFAULT_PROMPT_CAT = "defaultSystemPrompt";
const DEFAULT_PROMPT_SUB = "defaultSystemPrompt";
const CLASSIFIER_PROMPT_CAT = "initial";
const CLASSIFIER_PROMPT_SUB = "initial-classification";

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

async function getLastChatStatus(customer_id, shop_id) {
  const [rows] = await db.query(
    `SELECT status
       FROM chat
      WHERE customer_id = ? AND shop_id = ?
      ORDER BY created_at DESC, id DESC
      LIMIT 1`,
    [customer_id, shop_id]
  );
  return rows?.[0]?.status || null;
}

async function getUnclassifiedHistory(customer_id, shop_id) {
  const [rows] = await db.query(
    `SELECT sender, status, message
       FROM chat
      WHERE customer_id = ? AND shop_id = ?
      ORDER BY created_at DESC, id DESC
      LIMIT 20`,
    [customer_id, shop_id]
  );

  const chunk = [];
  for (const r of rows) {
    if (r.status !== "unclassified") break;
    chunk.push(r);
  }
  chunk.reverse();

  const history = [];
  for (const r of chunk) {
    const content = (r.message || "").trim();
    if (!content) continue;
    if (r.sender === "customer") history.push({ role: "user", content });
    else if (r.sender === "bot") history.push({ role: "assistant", content });
  }
  return history;
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
  const parts = raw.split(",").map((s) => s.trim());
  if (parts[0] === "0") {
    const rest = raw.slice(raw.indexOf(",") + 1).trim();
    return { type: "clarify", text: rest };
  }
  if (parts[0] === "1") {
    const category = parts[1] || "";
    const subRaw = (parts[2] || "").trim();
    const subcategory = subRaw.split(".").pop().trim();
    return { type: "classified", category, subcategory };
  }
  return { type: "raw", text: raw };
}

function normalizeOutboundMessage(payload) {
  if (!payload) return "";
  if (typeof payload === "string") return payload.trim();

  if (payload && typeof payload.reply === "string") {
    return payload.reply.trim();
  }
  if (payload && typeof payload.message === "string") {
    return payload.message.trim();
  }
  if (payload && payload.message && typeof payload.message.reply === "string") {
    return payload.message.reply.trim();
  }

  try {
    return JSON.stringify(payload);
  } catch {
    return String(payload);
  }
}

module.exports = {
  async processMessage(message, phone_number, shop_id) {
    const customer_id = await ensureCustomer(shop_id, phone_number);
    const lastStatus = await getLastChatStatus(customer_id, shop_id);

    let systemPrompt;
    let history = [];
    if (!lastStatus || lastStatus === "close") {
      systemPrompt = await getPromptFromDB(
        CLASSIFIER_PROMPT_CAT,
        CLASSIFIER_PROMPT_SUB
      );
    } else if (lastStatus === "unclassified") {
      systemPrompt = await getPromptFromDB(
        CLASSIFIER_PROMPT_CAT,
        CLASSIFIER_PROMPT_SUB
      );
      history = await getUnclassifiedHistory(customer_id, shop_id);
    } else {
      //temporary, need handle cases here
      systemPrompt = await getPromptFromDB(
        DEFAULT_PROMPT_CAT,
        DEFAULT_PROMPT_SUB
      );
    }

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

      await saveChat({
        customer_id,
        shop_id,
        sender: "customer",
        status: "classified",
        message,
      });

      if (category == "ORD") {
        if (subcategory == "CREATE") {
          const resp = await searchProducts({ message, customer_id, shop_id });
          const botText = normalizeOutboundMessage(resp);
          await saveChat({
            customer_id,
            shop_id,
            sender: "bot",
            status: "classified",
            message: botText,
          });
          return resp;
        }
      }
      //temporary until the case is handled
      await saveChat({
        customer_id,
        shop_id,
        sender: "server",
        status: "close",
        message: "",
      });
      return "תודה";
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

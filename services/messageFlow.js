const {
  routeByCategory,
  isValidCategorySub,
} = require("../categoryHandlers/router");
const { classifyIncoming } = require("../services/classifier");
const { wasSentBefore, saveChat } = require("../repositories/chat");
const { ensureCustomer } = require("../repositories/customer");
const {
  fetchOpenQuestions,
  fetchRecentClosedQuestions,
  buildOpenQuestionsContextForPrompt,
} = require("../utilities/openQuestions");
const { normalizeOutboundMessage } = require("../utilities/normalize");
const {
  getActiveOrder,
  getOrderItems,
  buildActiveOrderSignals,
} = require("../utilities/orders");
const { detectIsEnglish } = require("../utilities/lang");
const { checkIfToCancelOrder } = require("../categoryHandlers/ORD/CANCEL");
const { checkIfToCheckoutOrder } = require("../categoryHandlers/ORD/CHECKOUT");
const { answerOrderStatus } = require("../categoryHandlers/ORD/STATUS");
const { sendWhatsAppMarkAsRead } = require("../utilities/whatsapp");
const { startSlowProgression } = require("./sendProgressionMessage");
const { handleSuggestionReply } = require("./orderSuggestions");
const { handleFulfillmentReply } = require("./fulfillment");
const {
  handleCheckoutNudgeReply,
  attachCheckoutNudgeIfNeeded,
} = require("../utilities/checkoutNudge");
const db = require("../config/db");

const DEFAULT_MAX_PER_PRODUCT = 10;
let maxPerProductSchemaReadyPromise = null;

async function ensureMaxPerProductSchema() {
  if (!maxPerProductSchemaReadyPromise) {
    maxPerProductSchemaReadyPromise = (async () => {
      const [rows] = await db.query(
        `
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'shop'
          AND COLUMN_NAME = 'max_order_quantity_per_product'
        LIMIT 1
        `,
      );
      if (!rows.length) {
        await db.query(
          `ALTER TABLE shop ADD COLUMN max_order_quantity_per_product INT UNSIGNED NOT NULL DEFAULT 10`,
        );
      }
    })().catch((err) => {
      maxPerProductSchemaReadyPromise = null;
      throw err;
    });
  }
  return maxPerProductSchemaReadyPromise;
}

async function getMaxPerProductForShop(shop_id) {
  try {
    await ensureMaxPerProductSchema();
    const [[row]] = await db.query(
      `SELECT max_order_quantity_per_product FROM shop WHERE id = ? LIMIT 1`,
      [shop_id],
    );
    const n = Number(row?.max_order_quantity_per_product);
    return Number.isFinite(n) && n >= DEFAULT_MAX_PER_PRODUCT
      ? Math.floor(n)
      : DEFAULT_MAX_PER_PRODUCT;
  } catch (err) {
    console.error("[messageFlow.getMaxPerProductForShop]", err?.message || err);
    return DEFAULT_MAX_PER_PRODUCT;
  }
}

function isOrderStatusQuestion(message) {
  const raw = String(message || "").trim().toLowerCase();
  if (!raw) return false;

  const asksCartContent = /(מה\s+יש\s+(בסל|בעגלה|בהזמנה)|תראה\s+לי\s+(את\s+)?(הסל|העגלה)|סיכום\s+הזמנה|cart|basket|order summary)/i.test(raw);
  if (asksCartContent) return false;

  return /(מה\s+עם\s+ההזמנה|איפה\s+ההזמנה|סטטוס\s+הזמנה|מה\s+קורה\s+עם\s+ההזמנה|המשלוח\s+יצא|שליח\s+בדרך|ההזמנה\s+מוכנה|מתי\s+מגיע|מתי\s+מוכן|נשלחה|נאספה|status|tracking|where\s+is\s+my\s+order|is\s+my\s+order\s+ready|delivery\s+sent)/i.test(raw);
}


function isWrongOpenOrderClarification(text) {
  const raw = String(text || "").toLowerCase();
  if (!raw) return false;
  return (
    raw.includes("יש כבר הזמנה פתוחה") ||
    raw.includes("כבר יש הזמנה פתוחה") ||
    raw.includes("already have an open order") ||
    raw.includes("there is already an open order")
  );
}

function buildNoActiveOrderReply(message) {
  const isEnglish = detectIsEnglish(message);
  if (isEnglish) {
    return [
      "There is no open order right now.",
      "If the previous order was cancelled automatically, its products were returned to stock.",
      "To start a new order, just write the products you would like to buy.",
    ].join("\n");
  }

  return [
    "אין לך הזמנה פתוחה כרגע.",
    "אם הכוונה להזמנה הקודמת — היא כבר בוטלה אוטומטית והמוצרים חזרו למלאי.",
    "כדי להתחיל הזמנה חדשה, פשוט כתוב את המוצרים שתרצה להזמין.",
  ].join("\n");
}

async function saveAndReturnUnclassifiedReply({ customer_id, shop_id, message, reply }) {
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
    message: reply,
  });

  return reply;
}

async function processMessage(
  message,
  phone_number,
  shop_id,
  waMessageId = "",
  receivedAt = Date.now(),
  businessPhoneNumberId = "",
) {
  const maxPerProduct = await getMaxPerProductForShop(shop_id);
  const customer_id = await ensureCustomer(shop_id, phone_number);

  const wasSent = await wasSentBefore(customer_id, shop_id, message);
  if (wasSent) {
    return { skipSend: true };
  }

  if (waMessageId) {
    setTimeout(() => {
      sendWhatsAppMarkAsRead(waMessageId, businessPhoneNumberId).catch((e) =>
        console.error("[wa markAsRead]", e?.response?.data || e),
      );
    }, 800);
  }

  const activeOrder = await getActiveOrder(customer_id, shop_id);
  const order_id = activeOrder ? activeOrder.id : null;
  const items = activeOrder ? await getOrderItems(activeOrder.id) : [];
  const sig = buildActiveOrderSignals(activeOrder, items);
  const openQs = await fetchOpenQuestions(customer_id, shop_id, 20);

  const fulfillmentReply = await handleFulfillmentReply({
    message,
    customer_id,
    shop_id,
    activeOrder,
    openQs,
    saveChat,
    maxPerProduct,
  });

  if (fulfillmentReply) return fulfillmentReply;

  const checkoutReply = await checkIfToCheckoutOrder({
    activeOrder,
    message,
    customer_id,
    shop_id,
    saveChat,
  });

  if (checkoutReply) return checkoutReply;

  const cancelReply = await checkIfToCancelOrder({
    activeOrder,
    message,
    customer_id,
    shop_id,
    saveChat,
  });

  if (cancelReply) return cancelReply;

  const checkoutNudgeReply = await handleCheckoutNudgeReply({
    message,
    customer_id,
    shop_id,
    activeOrder,
    openQs,
    saveChat,
  });

  if (checkoutNudgeReply) return checkoutNudgeReply;

  const suggestionReply = await handleSuggestionReply({
    message,
    customer_id,
    shop_id,
    activeOrder,
    openQs,
    maxPerProduct,
  });

  if (suggestionReply) {
    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message,
    });

    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: suggestionReply,
    });

    return suggestionReply;
  }

  if (isOrderStatusQuestion(message)) {
    const statusReply = await answerOrderStatus({
      message,
      customer_id,
      shop_id,
      isEnglish: detectIsEnglish(message),
    });

    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message,
    });

    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: statusReply,
    });

    return statusReply;
  }

  const closedQs = await fetchRecentClosedQuestions(customer_id, shop_id, 5);

  const { parsed, replyText, history } = await classifyIncoming({
    message,
    customer_id,
    shop_id,
    sig,
    openQs,
    closedQs,
  });

  if (parsed.type === "clarify") {
    let clarifyText = parsed.text || "לא התקבלה תשובה מהמודל.";

    // Defensive state guard: the classifier prompt contains a clarification that is valid
    // only when ACTIVE_ORDER_EXISTS=true. In vague follow-ups after an automatic expiry,
    // the model may still output that text. Never tell the customer there is an open
    // order when the DB says there is no active order.
    if (!activeOrder && isWrongOpenOrderClarification(clarifyText)) {
      clarifyText = buildNoActiveOrderReply(message);
    }

    return saveAndReturnUnclassifiedReply({
      customer_id,
      shop_id,
      message,
      reply: clarifyText,
    });
  } else if (parsed.type === "classified") {
    const { category, subcategory } = parsed;
    console.log(
      `[classification] category=${category}, subcategory=${subcategory}`,
    );

    if (!activeOrder && category === "ORD" && subcategory === "MODIFY") {
      return saveAndReturnUnclassifiedReply({
        customer_id,
        shop_id,
        message,
        reply: buildNoActiveOrderReply(message),
      });
    }

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

    const openQsCtx = buildOpenQuestionsContextForPrompt(openQs);
    const isEnglish = detectIsEnglish(message);
    const ctx = {
      message,
      customer_id,
      shop_id,
      history,
      openQsCtx,
      activeOrder,
      order_id,
      items,
      isEnglish,
      maxPerProduct,
    };

    const stopProgression = startSlowProgression({
      category,
      subcategory,
      isEnglish,
      phone_number,
      waMessageId,
      receivedAt,
      typingAtMs: 2000,
      progressEveryMs: 8000,
      businessPhoneNumberId,
    });

    let botPayload = null;

    try {
      botPayload = await routeByCategory(category, subcategory, ctx);
    } catch (err) {
      console.error("[routeByCategory error]", err);
    } finally {
      stopProgression();
    }

    if (botPayload == null) {
      await saveChat({
        customer_id,
        shop_id,
        sender: "server",
        status: "close",
        message: "",
      });

      const fallback = "כרגע אין לנו תמיכה בבקשות מסוג זה";
      return fallback;
    }

    botPayload = await attachCheckoutNudgeIfNeeded({
      botPayload,
      category,
      subcategory,
      customer_id,
      shop_id,
      isEnglish,
    });

    const botText = normalizeOutboundMessage(botPayload);
    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: botText,
    });

    if (botPayload && Array.isArray(botPayload.followUpMessages)) {
      for (const followUp of botPayload.followUpMessages) {
        const msg = typeof followUp === "string" ? followUp.trim() : "";
        if (!msg) continue;
        await saveChat({
          customer_id,
          shop_id,
          sender: "bot",
          status: "classified",
          message: msg,
        });
      }
    }

    return botPayload;
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
}

module.exports = {
  processMessage,
};

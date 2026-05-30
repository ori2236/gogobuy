const db = require("../config/db");
const { saveChat } = require("../repositories/chat");
const { sendWhatsAppText } = require("./whatsapp");
const { saveOpenQuestions, closeQuestionsByIds, fetchOpenQuestions } = require("./openQuestions");
const { detectIsEnglish } = require("./lang");
const { getActiveOrder, getOrderItems } = require("./orders");
const { askToCheckoutOrder } = require("../categoryHandlers/ORD/CHECKOUT");

const CHECKOUT_NUDGE_TYPE = "CHECKOUT_NUDGE";
const CHECKOUT_NUDGE_NAME = "checkout_nudge";
const MIN_ITEMS_FOR_AUTO_NUDGE = 3;
const MIN_EDITS_BETWEEN_MODIFY_NUDGES = 2;

const REPLY_CONFIG = {
  positive: [
    "כן",
    "כן תודה",
    "כן בבקשה",
    "כן שלח",
    "כן תשלח",
    "כן תשלחי",
    "שלח",
    "שלחי",
    "שלח הזמנה",
    "שלחי הזמנה",
    "תשלח",
    "תשלחי",
    "תשלח הזמנה",
    "תשלחי הזמנה",
    "אפשר לשלוח",
    "אפשר לסיים",
    "סיימתי",
    "סיים",
    "סיים הזמנה",
    "סיימי הזמנה",
    "יאללה",
    "יאללה שלח",
    "קדימה",
    "קדימה שלח",
    "בטח",
    "ברור",
    "סגור",
    "סבבה",
    "מאשר",
    "מאשרת",
    "אשר",
    "אישור",
    "תכין",
    "תכיני",
    "תכין לשליחה",
    "תכיני לשליחה",
    "yes",
    "y",
    "yes please",
    "please send",
    "send",
    "send order",
    "send the order",
    "submit",
    "submit order",
    "checkout",
    "check out",
    "finish",
    "finish order",
    "done",
    "i'm done",
    "im done",
    "go ahead",
    "proceed",
    "confirm",
    "ok",
    "okay",
    "sure",
  ],
  negative: [
    "לא",
    "לא תודה",
    "לא עכשיו",
    "עדיין לא",
    "עוד לא",
    "רגע",
    "חכה",
    "חכי",
    "אני ממשיך",
    "ממשיך להזמין",
    "אמשיך להזמין",
    "no",
    "n",
    "no thanks",
    "not now",
    "not yet",
    "wait",
    "keep shopping",
    "continue shopping",
  ],
};

const NUDGE_VARIANTS = {
  he: [
    "🛒 רוצה שאכין את ההזמנה לשליחה לחנות?\n✅ כתוב \"שלח הזמנה\", או פשוט המשך להוסיף מוצרים.",
    "🛒 נראה שההזמנה כבר מתקדמת יפה. להכין אותה לשליחה לחנות?\n✅ אפשר לכתוב \"שלח הזמנה\", או להמשיך לערוך חופשי.",
    "🧾 תרצה שאעביר אותך לאישור סופי של ההזמנה?\n✅ כתוב \"שלח הזמנה\" כשאתה מוכן, או המשך להוסיף מוצרים.",
    "✅ אם סיימת לבחור מוצרים, אוכל להכין את ההזמנה לשליחה לחנות.\n🛒 כתוב \"שלח הזמנה\", או פשוט המשך להזמין.",
    "🧾 רוצה להתקדם לסיום ההזמנה?\n✅ כתוב \"שלח הזמנה\" ואעביר אותך לאישור סופי, או המשך להוסיף מוצרים.",
  ],
  en: [
    "🛒 Would you like me to prepare this order to be sent to the store?\n✅ Write \"send order\", or just keep adding products.",
    "🛒 Your order is coming together. Should I prepare it for sending to the store?\n✅ Write \"send order\", or keep editing freely.",
    "🧾 Ready to move to final order confirmation?\n✅ Write \"send order\" when you are ready, or keep adding products.",
    "✅ If you’re done choosing products, I can prepare the order for the store.\n🛒 Write \"send order\", or just continue shopping.",
    "🧾 Would you like to continue to checkout?\n✅ Write \"send order\" and I’ll move you to final confirmation, or keep adding products.",
  ],
};

function safeJsonParse(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function normalizeText(message) {
  return String(message || "")
    .trim()
    .toLowerCase()
    .replace(/[.!?؟,;:()\[\]{}"'״׳`]+/g, "")
    .replace(/\s+/g, " ");
}

function hasPhrase(message, phrases) {
  const norm = normalizeText(message);
  if (!norm) return false;
  return phrases.some((p) => {
    const phrase = normalizeText(p);
    return phrase && (norm === phrase || norm.includes(phrase));
  });
}

function isPositiveCheckoutReply(message) {
  return hasPhrase(message, REPLY_CONFIG.positive);
}

function isExplicitCheckoutIntent(message) {
  return hasPhrase(message, [
    "שלח הזמנה",
    "שלחי הזמנה",
    "תשלח הזמנה",
    "תשלחי הזמנה",
    "אפשר לשלוח",
    "אפשר לסיים",
    "סיימתי",
    "סיים הזמנה",
    "סיימי הזמנה",
    "תכין לשליחה",
    "תכיני לשליחה",
    "send order",
    "send the order",
    "submit order",
    "checkout",
    "check out",
    "finish order",
    "go ahead",
    "proceed",
  ]);
}

function isNegativeCheckoutReply(message) {
  return hasPhrase(message, REPLY_CONFIG.negative);
}

function getQuestionType(q) {
  const options = safeJsonParse(q && q.option_set);
  return options && options.type ? String(options.type) : "";
}

function isCheckoutNudgeQuestion(q) {
  return getQuestionType(q) === CHECKOUT_NUDGE_TYPE;
}

function countBlockingOpenQuestions(openQs = []) {
  return (openQs || []).filter((q) => !isCheckoutNudgeQuestion(q)).length;
}

function getLatestCheckoutNudge(openQs = [], orderId = null) {
  return (
    (openQs || [])
      .filter((q) => isCheckoutNudgeQuestion(q))
      .filter((q) => !orderId || Number(q.order_id) === Number(orderId))
      .sort((a, b) => Number(b.id) - Number(a.id))[0] || null
  );
}

async function closeOpenCheckoutNudges({ customer_id, shop_id, order_id }) {
  const params = [Number(customer_id), Number(shop_id)];
  let sql = `
    SELECT id, option_set
      FROM chat_open_question
     WHERE customer_id = ?
       AND shop_id = ?
       AND status = 'open'
  `;

  if (order_id) {
    sql += ` AND order_id = ?`;
    params.push(Number(order_id));
  }

  const [rows] = await db.query(sql, params);
  const ids = (rows || [])
    .filter((q) => isCheckoutNudgeQuestion(q))
    .map((q) => Number(q.id))
    .filter(Boolean);

  if (ids.length) await closeQuestionsByIds(ids);
  return ids.length;
}

function pickNudgeText({ isEnglish, lastVariant }) {
  const lang = isEnglish ? "en" : "he";
  const variants = NUDGE_VARIANTS[lang];
  const indices = variants.map((_, i) => i).filter((i) => i !== Number(lastVariant));
  const pool = indices.length ? indices : variants.map((_, i) => i);
  const idx = pool[Math.floor(Math.random() * pool.length)];
  return { text: variants[idx], variant: idx };
}

async function getCheckoutNudgeState(orderId, shopId) {
  const [[row]] = await db.query(
    `SELECT checkout_nudge_count,
            checkout_nudge_last_at,
            checkout_nudge_edits_since,
            checkout_nudge_last_variant
       FROM orders
      WHERE id = ? AND shop_id = ?
      LIMIT 1`,
    [Number(orderId), Number(shopId)],
  );

  return {
    count: Number(row?.checkout_nudge_count || 0),
    lastAt: row?.checkout_nudge_last_at || null,
    editsSince: Number(row?.checkout_nudge_edits_since || 0),
    lastVariant:
      row?.checkout_nudge_last_variant === null || row?.checkout_nudge_last_variant === undefined
        ? null
        : Number(row.checkout_nudge_last_variant),
  };
}

async function markCheckoutNudgeShown({ order_id, shop_id, variant }) {
  await db.query(
    `UPDATE orders
        SET checkout_nudge_count = COALESCE(checkout_nudge_count, 0) + 1,
            checkout_nudge_last_at = NOW(6),
            checkout_nudge_edits_since = 0,
            checkout_nudge_last_variant = ?
      WHERE id = ? AND shop_id = ?`,
    [String(variant), Number(order_id), Number(shop_id)],
  );
}

async function recordCheckoutRelevantEdit({ order_id, shop_id, customer_id }) {
  if (!order_id) return;

  await closeOpenCheckoutNudges({ customer_id, shop_id, order_id });

  await db.query(
    `UPDATE orders
        SET checkout_nudge_edits_since = COALESCE(checkout_nudge_edits_since, 0) + 1
      WHERE id = ? AND shop_id = ? AND status = 'pending'`,
    [Number(order_id), Number(shop_id)],
  );
}

async function createCheckoutNudgeOpenQuestion({
  customer_id,
  shop_id,
  order_id,
  questionText,
  source,
  variant,
}) {
  await closeOpenCheckoutNudges({ customer_id, shop_id, order_id });

  await saveOpenQuestions({
    customer_id,
    shop_id,
    order_id,
    questions: [
      {
        name: CHECKOUT_NUDGE_NAME,
        question: questionText,
        options: {
          type: CHECKOUT_NUDGE_TYPE,
          source,
          variant,
          reply_config: REPLY_CONFIG,
        },
      },
    ],
  });
}

function appendFollowUpMessage(botPayload, followUp) {
  if (!followUp) return botPayload;

  if (botPayload && typeof botPayload === "object" && !Array.isArray(botPayload)) {
    return {
      ...botPayload,
      followUpMessages: [
        ...(Array.isArray(botPayload.followUpMessages) ? botPayload.followUpMessages : []),
        followUp,
      ],
    };
  }

  return {
    message: typeof botPayload === "string" ? botPayload : String(botPayload || ""),
    followUpMessages: [followUp],
  };
}

function attachDeferredNudge(botPayload, nudgeContext) {
  if (!nudgeContext) return botPayload;

  if (botPayload && typeof botPayload === "object" && !Array.isArray(botPayload)) {
    return {
      ...botPayload,
      deferredCheckoutNudge: nudgeContext,
    };
  }

  return {
    message: typeof botPayload === "string" ? botPayload : String(botPayload || ""),
    deferredCheckoutNudge: nudgeContext,
  };
}

async function materializeNudgeContext({
  customer_id,
  shop_id,
  order_id,
  questionText,
  source,
  variant,
}) {
  const [[order]] = await db.query(
    `SELECT id, status
       FROM orders
      WHERE id = ? AND shop_id = ? AND customer_id = ?
      LIMIT 1`,
    [Number(order_id), Number(shop_id), Number(customer_id)],
  );

  if (!order || String(order.status) !== "pending") return null;

  const openQs = await fetchOpenQuestions(customer_id, shop_id, 20);
  if (String(source).toUpperCase() !== "REVIEW" && countBlockingOpenQuestions(openQs) > 0) {
    return null;
  }

  await createCheckoutNudgeOpenQuestion({
    customer_id,
    shop_id,
    order_id,
    questionText,
    source,
    variant,
  });

  await markCheckoutNudgeShown({
    order_id,
    shop_id,
    variant,
  });

  return questionText;
}

async function prepareCheckoutNudgeForAction({
  category,
  subcategory,
  customer_id,
  shop_id,
  isEnglish,
}) {
  if (String(category).toUpperCase() !== "ORD") return null;

  const action = String(subcategory || "").toUpperCase();
  if (!["CREATE", "MODIFY", "REVIEW"].includes(action)) return null;

  const order = await getActiveOrder(customer_id, shop_id);
  if (!order || String(order.status) !== "pending") return null;

  const items = await getOrderItems(order.id);
  const itemCount = Array.isArray(items) ? items.length : 0;
  if (itemCount < 1) return null;

  const openQs = await fetchOpenQuestions(customer_id, shop_id, 20);
  const blockingOpenQuestions = countBlockingOpenQuestions(openQs);

  if (action === "CREATE" || action === "MODIFY") {
    await recordCheckoutRelevantEdit({
      order_id: order.id,
      shop_id,
      customer_id,
    });
  }

  if (action === "REVIEW") {
    // A review is the clearest moment to ask, even if the user had asked before.
  } else if (action === "CREATE") {
    if (itemCount < MIN_ITEMS_FOR_AUTO_NUDGE) return null;
    if (blockingOpenQuestions > 0) return null;
  } else if (action === "MODIFY") {
    if (itemCount < MIN_ITEMS_FOR_AUTO_NUDGE) return null;
    if (blockingOpenQuestions > 0) return null;

    const stateAfterEdit = await getCheckoutNudgeState(order.id, shop_id);
    if (stateAfterEdit.editsSince < MIN_EDITS_BETWEEN_MODIFY_NUDGES) return null;
  }

  const state = await getCheckoutNudgeState(order.id, shop_id);
  const picked = pickNudgeText({ isEnglish, lastVariant: state.lastVariant });

  return {
    customer_id,
    shop_id,
    order_id: order.id,
    questionText: picked.text,
    source: action,
    variant: picked.variant,
    isEnglish: Boolean(isEnglish),
  };
}

async function attachCheckoutNudgeIfNeeded({
  botPayload,
  category,
  subcategory,
  customer_id,
  shop_id,
  isEnglish,
}) {
  const nudgeContext = await prepareCheckoutNudgeForAction({
    category,
    subcategory,
    customer_id,
    shop_id,
    isEnglish,
  });

  if (!nudgeContext) return botPayload;

  const action = String(subcategory || "").toUpperCase();

  if (action === "REVIEW") {
    const text = await materializeNudgeContext(nudgeContext);
    return text ? appendFollowUpMessage(botPayload, text) : botPayload;
  }

  const existingFollowUps =
    botPayload &&
    typeof botPayload === "object" &&
    Array.isArray(botPayload.followUpMessages)
      ? botPayload.followUpMessages
          .map((x) => (typeof x === "string" ? x.trim() : ""))
          .filter(Boolean)
      : [];

  // Bundle/promo follow-ups are also open questions. Do not ask for checkout in the same turn.
  if (existingFollowUps.length > 0) return botPayload;

  // CREATE/MODIFY nudges are deferred to the webhook, after async product recommendations finish.
  return attachDeferredNudge(botPayload, nudgeContext);
}

async function sendDeferredCheckoutNudge({
  checkoutNudge,
  phone_number,
  businessPhoneNumberId = null,
}) {
  if (!checkoutNudge || !phone_number) return false;

  try {
    const text = await materializeNudgeContext(checkoutNudge);
    if (!text) return false;

    await sendWhatsAppText(phone_number, text, businessPhoneNumberId);
    await saveChat({
      customer_id: checkoutNudge.customer_id,
      shop_id: checkoutNudge.shop_id,
      sender: "bot",
      status: "classified",
      message: text,
    });

    return true;
  } catch (err) {
    console.error("[checkout nudge deferred send]", err?.response?.data || err);
    return false;
  }
}

async function handleCheckoutNudgeReply({
  message,
  customer_id,
  shop_id,
  activeOrder,
  openQs,
  saveChat: saveChatFn,
}) {
  if (!activeOrder || String(activeOrder.status) !== "pending") return null;
  if (!Array.isArray(openQs) || !openQs.length) return null;

  const q = getLatestCheckoutNudge(openQs, activeOrder.id);
  if (!q) return null;

  const latestOpenQuestion =
    [...openQs]
      .filter((x) => String(x.status || "open") === "open")
      .sort((a, b) => Number(b.id) - Number(a.id))[0] || null;
  const checkoutQuestionIsLatest =
    latestOpenQuestion && Number(latestOpenQuestion.id) === Number(q.id);
  const explicitCheckout = isExplicitCheckoutIntent(message);

  if (!checkoutQuestionIsLatest && !explicitCheckout) return null;

  const isEnglish = detectIsEnglish(message || q.question_text || "");
  const persistChat = saveChatFn || saveChat;

  if (isPositiveCheckoutReply(message) || explicitCheckout) {
    await closeQuestionsByIds([q.id]);

    await persistChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message,
    });

    const botText = await askToCheckoutOrder(activeOrder, isEnglish, customer_id, shop_id);

    await persistChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: botText,
    });

    return botText;
  }

  if (checkoutQuestionIsLatest && isNegativeCheckoutReply(message)) {
    await closeQuestionsByIds([q.id]);

    await persistChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message,
    });

    const botText = isEnglish
      ? "No problem, keep editing the order. When you’re ready, write \"send order\"."
      : "אין בעיה, אפשר להמשיך לערוך את ההזמנה. כשתרצה, כתוב \"שלח הזמנה\".";

    await persistChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: botText,
    });

    return botText;
  }

  return null;
}

module.exports = {
  CHECKOUT_NUDGE_TYPE,
  attachCheckoutNudgeIfNeeded,
  handleCheckoutNudgeReply,
  isCheckoutNudgeQuestion,
  sendDeferredCheckoutNudge,
};

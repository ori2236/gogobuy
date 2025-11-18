const db = require("../config/db");

async function getPromptFromDB(category, subCategory) {
  const [rows] = await db.query(
    `SELECT prompt
       FROM prompt
      WHERE category = ? AND sub_category = ?
      ORDER BY id DESC
      LIMIT 1`,
    [category, subCategory]
  );
  return rows?.[0]?.prompt || null;
}

function oneLine(str) {
  return String(str || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 800);
}

function buildClassifierContextHeader({ sig }) {
  return [
    "CONTEXT SIGNALS",
    `- ACTIVE_ORDER_EXISTS = ${sig.ACTIVE_ORDER_EXISTS}`,
    `- ACTIVE_ORDER_SUMMARY = ${oneLine(sig.ACTIVE_ORDER_SUMMARY)}`,

    "",
    "CLASSIFICATION BIAS",
    "- If ACTIVE_ORDER_EXISTS=true and the message indicates add/remove/change quantity/replace → classify as: 1, ORD, ORD.MODIFY.",
    '- If ACTIVE_ORDER_EXISTS=true but the user explicitly says "new order"/"start a new cart"/"סל חדש" → classify as: 1, ORD, ORD.CREATE.',
    "- If ACTIVE_ORDER_EXISTS=false and the message is about adding items → classify as: 1, ORD, ORD.CREATE.",
    '- "what\'s in my cart"/"מה יש בסל" → 1, ORD, ORD.REVIEW.',
    '- "finish/pay"/"לתשלום"/"סיימתי" → 1, ORD, ORD.CHECKOUT.',
    '- "cancel the order"/"בטל הזמנה" → 1, ORD, ORD.CANCEL.',
    "",
  ].join("\n");
}

function buildOpenQuestionsContext({ openQs = [], closedQs = [] }) {
  const toLite = (q) => ({
    id: q.id,
    order_id: q.order_id,
    product_name: q.product_name,
    question_text: q.question_text,
    options: (() => {
      try {
        return q.option_set ? JSON.parse(q.option_set) : null;
      } catch {
        return null;
      }
    })(),
    asked_at: q.asked_at,
  });
  const openLite = openQs.map(toLite);
  const closedLite = closedQs.map(toLite);

  return [
    "OPEN QUESTIONS CONTEXT",
    `- OPEN_QUESTIONS_COUNT = ${openLite.length}`,
    `- CLOSED_QUESTIONS_RECENT_COUNT = ${closedLite.length}`,
    "- OPEN_QUESTIONS (JSON, last 48h):",
    JSON.stringify(openLite).slice(0, 3000),
    "- CLOSED_QUESTIONS_RECENT (JSON, last 48h):",
    JSON.stringify(closedLite).slice(0, 2000),
    "",
    "HINT",
    "- With open questions, the next customer message is LIKELY an answer → bias towards ORD.MODIFY if it refers to items/alternatives/quantities.",
    "",
  ].join("\n");
}


module.exports = {
  getPromptFromDB,
  buildClassifierContextHeader,
  buildOpenQuestionsContext,
};

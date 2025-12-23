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
    "",
  ].join("\n");
}

function buildOpenQuestionsContext({ openQs = [], closedQs = [] }) {
  const toLite = (q) => ({
    id: q.id,
    product_name: q.product_name,
    question_text: q.question_text,
    options: (() => {
      try {
        return q.option_set ? JSON.parse(q.option_set) : null;
      } catch {
        return null;
      }
    })(),
  });
  const openLite = openQs.map(toLite);
  const closedLite = closedQs.map(toLite);

  return [
    "`=== STRUCTURED CONTEXT ===",
    "- OPEN_QUESTIONS (last 48h):",
    JSON.stringify(openLite).slice(0, 3000),
    "- CLOSED_QUESTIONS_RECENT (last 48h):",
    JSON.stringify(closedLite).slice(0, 2000),
  ].join("\n");
}


module.exports = {
  getPromptFromDB,
  buildClassifierContextHeader,
  buildOpenQuestionsContext,
};

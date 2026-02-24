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

function buildClassifierContextHeader({ sig }) {
  const lines = [];
  lines.push(`ACTIVE_ORDER_EXISTS=${sig.ACTIVE_ORDER_EXISTS ? 1 : 0}`);

  if (sig.ACTIVE_ORDER_EXISTS) {
    if (
      Array.isArray(sig.ACTIVE_ORDER_EXAMPLES) &&
      sig.ACTIVE_ORDER_EXAMPLES.length
    ) {
      lines.push(
        `ACTIVE_ORDER_EXAMPLES=${sig.ACTIVE_ORDER_EXAMPLES.join("|")}`
      );
    }
    lines.push(
      `BIAS=Prefer ORD.MODIFY for add/remove/qty/replace unless user explicitly says "new order"/"סל חדש"`
    );
  }
  return lines.join("\n");
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

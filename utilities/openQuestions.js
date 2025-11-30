const db = require("../config/db");

async function saveOpenQuestions({
  customer_id,
  shop_id,
  order_id = null,
  questions,
}) {
  if (!Array.isArray(questions) || !questions.length) return;

  const rows = [];
  for (const q of questions) {
    if (!q) continue;

    let name = null;
    let questionText = null;
    let optionsArr = null;

    if (typeof q === "string") {
      questionText = q.trim() || null;
    } else if (typeof q === "object") {
      name = (typeof q.name === "string" && q.name.trim()) || null;
      questionText =
        (typeof q.question === "string" && q.question.trim()) || null;

      if (Array.isArray(q.options)) {
        optionsArr = q.options.map((s) => String(s).trim()).filter(Boolean);
        if (!optionsArr.length) optionsArr = null;
      }
    }

    if (!questionText) continue;

    rows.push([
      customer_id,
      shop_id,
      order_id,
      name || "כללי",
      questionText,
      optionsArr ? JSON.stringify(optionsArr) : null, // נשמר ל-option_set
    ]);
  }

  if (!rows.length) return;

  const placeholders = rows.map(() => "(?, ?, ?, ?, ?, ?, 'open')").join(", ");
  const sql = `
    INSERT INTO chat_open_question
      (customer_id, shop_id, order_id, product_name, question_text, option_set, status)
    VALUES ${placeholders}`;
  const params = rows.flat();
  await db.query(sql, params);
}

async function getOpenQuestions(
  customer_id,
  shop_id,
  { hours = 48, limit = 7 } = {}
) {
  const [rows] = await db.query(
    `SELECT id, order_id, product_name, question_text, option_set, asked_at
       FROM chat_open_question
      WHERE customer_id = ? 
        AND shop_id = ?
        AND status = 'open'
        AND asked_at >= (NOW() - INTERVAL ? HOUR)
      ORDER BY asked_at DESC, id DESC
      LIMIT ?`,
    [customer_id, shop_id, hours, limit]
  );

  return (rows || []).map((r) => ({
    ...r,
    options: (() => {
      if (!r.option_set) return null;
      try {
        return JSON.parse(r.option_set);
      } catch {
        return null;
      }
    })(),
  }));
}

async function fetchOpenQuestions(customer_id, shop_id, limit = 7) {
  const [rows] = await db.query(
    `SELECT id, order_id, product_name, question_text, status, asked_at, option_set
       FROM chat_open_question
      WHERE customer_id = ? AND shop_id = ?
        AND status = 'open'
        AND asked_at >= (NOW() - INTERVAL 48 HOUR)
      ORDER BY asked_at DESC, id DESC
      LIMIT ?`,
    [customer_id, shop_id, Number(limit)]
  );
  return rows || [];
}

async function fetchRecentClosedQuestions(customer_id, shop_id, limit = 7) {
  const [rows] = await db.query(
    `SELECT id, order_id, product_name, question_text, status, asked_at, option_set
       FROM chat_open_question
      WHERE customer_id = ? AND shop_id = ?
        AND status = 'close'
        AND asked_at >= (NOW() - INTERVAL 48 HOUR)
      ORDER BY asked_at DESC, id DESC
      LIMIT ?`,
    [customer_id, shop_id, Number(limit)]
  );
  return rows || [];
}

async function closeQuestionsByIds(ids = []) {
  if (!Array.isArray(ids) || !ids.length) return 0;
  const [res] = await db.query(
    `UPDATE chat_open_question
        SET status = 'close'
      WHERE id IN (${ids.map(() => "?").join(",")})`,
    ids
  );
  return res?.affectedRows || 0;
}

async function deleteQuestionsByIds(ids = []) {
  if (!Array.isArray(ids) || !ids.length) return 0;
  const [res] = await db.query(
    `DELETE FROM chat_open_question
      WHERE id IN (${ids.map(() => "?").join(",")})`,
    ids
  );
  return res?.affectedRows || 0;
}

function buildOpenQuestionsContextForPrompt(
  openQuestions = []
) {
  const lite = (qs) =>
    qs.map((q) => ({
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
    }));
  return [
    "OPEN QUESTIONS CONTEXT",
    `- OPEN_QUESTIONS_COUNT = ${openQuestions.length}`,
    "- OPEN_QUESTIONS (JSON):",
    JSON.stringify(lite(openQuestions)).slice(0, 3000),
    "",
    "HINT",
    "- If user message is short like 'כן/פוזילי/קח ברילה', it likely answers one of the open questions.",
    "- Prefer classifying as order modification when relevant.",
    "",
  ].join("\n");
}

module.exports = {
  saveOpenQuestions,
  getOpenQuestions,
  fetchOpenQuestions,
  fetchRecentClosedQuestions,
  closeQuestionsByIds,
  deleteQuestionsByIds,
  buildOpenQuestionsContextForPrompt,
};

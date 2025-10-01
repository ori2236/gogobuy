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

module.exports = { getPromptFromDB };

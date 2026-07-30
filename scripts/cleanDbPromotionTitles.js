require("dotenv").config();
const db = require("../config/db");

function cleanPromotionTitle(title) {
  if (!title) return "";
  return String(title)
    .replace(/\s*\d+\s*ב-?\s*\d+(\.\d+)?\s*$/gi, "")
    .trim();
}

async function run() {
  console.log("=== Cleaning Existing Promotion Titles in Database ===");

  const [groupRows] = await db.query(`SELECT id, title FROM product_group_promotion`);
  let updatedGroups = 0;

  for (const row of groupRows) {
    const cleaned = cleanPromotionTitle(row.title);
    if (cleaned !== row.title) {
      await db.query(`UPDATE product_group_promotion SET title = ? WHERE id = ?`, [cleaned, row.id]);
      console.log(`Group #${row.id}: "${row.title}" => "${cleaned}"`);
      updatedGroups++;
    }
  }

  const [cartRows] = await db.query(`SELECT id, title FROM cart_promotion_rule`);
  let updatedCartRules = 0;

  for (const row of cartRows) {
    const cleaned = cleanPromotionTitle(row.title);
    if (cleaned !== row.title) {
      await db.query(`UPDATE cart_promotion_rule SET title = ? WHERE id = ?`, [cleaned, row.id]);
      console.log(`Cart Rule #${row.id}: "${row.title}" => "${cleaned}"`);
      updatedCartRules++;
    }
  }

  console.log(`\nDone! Cleaned ${updatedGroups} group promotion titles and ${updatedCartRules} cart promotion titles.`);
  process.exit(0);
}

run().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});

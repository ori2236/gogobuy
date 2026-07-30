require("dotenv").config();
const db = require("../config/db");

async function run() {
  console.log("====================================================");
  console.log("🔍 Inspecting Promotion Group Emojis");
  console.log("====================================================\n");

  const [groups] = await db.query(`SELECT id, title, emoji FROM product_group_promotion WHERE shop_id = 2`);
  for (const g of groups) {
    console.log(`• Group #${g.id} | "${g.title}" | Emoji: ${g.emoji || "NULL (Computed dynamically)"}`);
  }

  const [cartRules] = await db.query(`SELECT id, title, rule_type FROM cart_promotion_rule WHERE shop_id = 2`);
  console.log("\n--- Cart Promotion Rules ---");
  for (const c of cartRules) {
    console.log(`• Cart Rule #${c.id} | "${c.title}" | Type: ${c.rule_type}`);
  }

  process.exit(0);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

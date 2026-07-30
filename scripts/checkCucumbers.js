require("dotenv").config();
const db = require("../config/db");

async function main() {
  console.log("====================================================");
  console.log("🥒 Checking Cucumbers & Produce Products across DB");
  console.log("====================================================\n");

  const [cucumbers] = await db.query(
    `SELECT id, shop_id, name, emoji FROM product WHERE name LIKE '%מלפפון%' ORDER BY shop_id, id`
  );

  console.log(`Found ${cucumbers.length} cucumber products:\n`);
  for (const c of cucumbers) {
    console.log(`ID #${c.id} (Shop ${c.shop_id}) | "${c.name}" | Emoji: ${c.emoji}`);
  }

  process.exit(0);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

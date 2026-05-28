require("dotenv").config({ quiet: true });

const db = require("../config/db");
const { rebuildTokenWeightsForShop } = require("../services/buildTokenWeights");

async function getAllShopIds() {
  const [rows] = await db.query(`
    SELECT DISTINCT shop_id
    FROM product
    WHERE shop_id IS NOT NULL
    ORDER BY shop_id
  `);

  return rows.map((row) => Number(row.shop_id)).filter(Boolean);
}

async function main() {
  const shopIds = await getAllShopIds();

  if (!shopIds.length) {
    console.log("[token-weights] no shops with products found");
    return;
  }

  console.log(`[token-weights] rebuilding all shops: ${shopIds.join(", ")}`);

  for (const shopId of shopIds) {
    console.log(`[token-weights] rebuilding shop_id=${shopId}`);
    await rebuildTokenWeightsForShop(shopId);
    console.log(`[token-weights] done shop_id=${shopId}`);
  }

  console.log("[token-weights] all done");
}

main()
  .catch((err) => {
    console.error("[token-weights] failed:", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await db.end();
    } catch {}
  });

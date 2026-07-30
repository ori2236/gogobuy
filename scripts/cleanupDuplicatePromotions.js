require("dotenv").config({ quiet: true });

const db = require("../config/db");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function boolArg(name, fallback = false) {
  const val = argValue(name, fallback);
  if (val === true || val === false) return val;
  const str = String(val).trim().toLowerCase();
  return ["1", "true", "yes", "y", "confirm"].includes(str);
}

const SHOP_ID = Number(argValue("--shopId", process.env.PROMO_SYNC_SHOP_ID || 2));
const CONFIRM = boolArg("--confirm", false);
const DRY_RUN = !CONFIRM || boolArg("--dryRun", false);

function cleanTitle(title) {
  if (!title) return "";
  return String(title)
    .replace(/\s*\d+\s*ב-?\s*\d+(\.\d+)?\s*(₪|ש"ח)?\s*$/gi, "")
    .replace(/\s*\d+\s*ב-?\s*\d+\s*$/gi, "")
    .replace(/["'״]/g, "")
    .trim()
    .toLowerCase();
}

async function main() {
  console.log(`====================================================`);
  console.log(`🧹 Cleaning Duplicate Active Group Promotions`);
  console.log(`====================================================`);
  console.log(`Mode:           ${DRY_RUN ? "🔍 DRY-RUN (Preview Only)" : "⚡ CONFIRM (Deactivating Duplicates)"}`);
  console.log(`Target Shop ID: ${SHOP_ID}`);
  console.log(`====================================================\n`);

  const [groups] = await db.query(
    `SELECT id, title, is_active, created_at FROM product_group_promotion WHERE shop_id = ? ORDER BY id DESC`,
    [SHOP_ID]
  );

  console.log(`Found ${groups.length} total group promotions for shop ID ${SHOP_ID}`);

  // Group by cleaned title
  const titleGroups = new Map();
  for (const g of groups) {
    const key = cleanTitle(g.title);
    if (!key) continue;
    if (!titleGroups.has(key)) titleGroups.set(key, []);
    titleGroups.get(key).push(g);
  }

  let totalDeactivated = 0;

  for (const [cleanName, promoList] of titleGroups.entries()) {
    const activePromos = promoList.filter((p) => p.is_active === 1);
    if (activePromos.length > 1) {
      console.log(`\n⚠️  Found ${activePromos.length} duplicate active group promos for: "${cleanName}"`);
      
      // Keep the newest one (highest ID), deactivate older ones
      const keep = activePromos[0]; // highest ID because ORDER BY id DESC
      const deactivateList = activePromos.slice(1);

      console.log(`   ✅ KEEPING ACTIVE: Group Promo #${keep.id} - "${keep.title}"`);
      for (const d of deactivateList) {
        console.log(`   ⛔ DEACTIVATING DUPLICATE: Group Promo #${d.id} - "${d.title}"`);
        totalDeactivated++;

        if (!DRY_RUN) {
          await db.query(`UPDATE product_group_promotion SET is_active = 0 WHERE id = ?`, [d.id]);
        }
      }
    }
  }

  console.log(`\n====================================================`);
  console.log(`Summary: ${totalDeactivated} duplicate group promotions ${DRY_RUN ? "would be deactivated" : "deactivated"}`);
  console.log(`====================================================\n`);

  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

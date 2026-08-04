require("dotenv").config({ quiet: true });

const db = require("../config/db");
const { cleanProductPromotionTitle } = require("../services/promotionImportIdentity");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

const SHOP_ID = Number(argValue("--shopId", 2));
const CONFIRM = Boolean(argValue("--confirm", false));

async function loadChanges(conn) {
  const [rows] = await conn.query(
    `
    SELECT id, title, bundle_buy_qty, bundle_pay_price
    FROM product_group_promotion
    WHERE shop_id = ?
    ORDER BY id
    `,
    [SHOP_ID],
  );

  return rows
    .map((row) => ({
      id: Number(row.id),
      before: String(row.title || ""),
      after: cleanProductPromotionTitle(row.title, row.bundle_buy_qty, row.bundle_pay_price),
      bundle_buy_qty: Number(row.bundle_buy_qty),
      bundle_pay_price: Number(row.bundle_pay_price),
    }))
    .filter((row) => row.after && row.after !== row.before);
}

async function main() {
  if (!Number.isInteger(SHOP_ID) || SHOP_ID <= 0) throw new Error("shopId must be a positive integer");

  const conn = await db.getConnection();
  try {
    const changes = await loadChanges(conn);

    if (!CONFIRM) {
      console.log(JSON.stringify({
        mode: "dry_run",
        shop_id: SHOP_ID,
        titles_to_update: changes.length,
        changes,
        note: "No DB changes were made. Add --confirm to update these titles.",
      }, null, 2));
      return;
    }

    await conn.beginTransaction();
    for (const change of changes) {
      const [result] = await conn.query(
        `UPDATE product_group_promotion SET title = ?, updated_at = NOW() WHERE id = ? AND shop_id = ? AND title = ?`,
        [change.after, change.id, SHOP_ID, change.before],
      );
      if (Number(result.affectedRows) !== 1) {
        throw new Error(`Promotion group #${change.id} changed while cleaning titles`);
      }
    }
    await conn.commit();

    console.log(JSON.stringify({
      mode: "applied",
      shop_id: SHOP_ID,
      updated_titles: changes.length,
      changes,
    }, null, 2));
  } catch (error) {
    try { await conn.rollback(); } catch (_) {}
    throw error;
  } finally {
    conn.release();
    await db.end();
  }
}

main().catch((error) => {
  console.error("[clean-db-promotion-titles] Error:", error.message || error);
  process.exit(1);
});

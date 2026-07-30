require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");
const { ensureProductGroupPromotionColumns } = require("../services/productGroupPromotions");

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
const DEFAULT_PAYLOAD_FILE = path.join(__dirname, "..", "data", `promotions_prod_payload_shop${SHOP_ID}.json`);
const PAYLOAD_FILE = path.resolve(argValue("--payload", DEFAULT_PAYLOAD_FILE));
const CONFIRM = boolArg("--confirm", false);
const DRY_RUN = !CONFIRM || boolArg("--dryRun", false);
const DEACTIVATE_UNMATCHED = boolArg("--deactivateUnmatched", false);

const DEACTIVATE_IDS_RAW = argValue("--deactivateIds", "");
const DEACTIVATE_IDS = String(DEACTIVATE_IDS_RAW || "")
  .split(",")
  .map((x) => Number(x.trim()))
  .filter((x) => Number.isInteger(x) && x > 0);

const REPORTS_DIR = path.join(__dirname, "..", "reports");

function pad2(v) {
  return String(v).padStart(2, "0");
}

function formatSqlValue(colName, val) {
  if (val === null || val === undefined) return null;
  if (
    ["start_at", "end_at", "created_at", "updated_at"].includes(colName) ||
    (typeof val === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(val))
  ) {
    const d = new Date(val);
    if (!isNaN(d.getTime())) {
      const year = d.getFullYear();
      const month = pad2(d.getMonth() + 1);
      const day = pad2(d.getDate());
      const hours = pad2(d.getHours());
      const mins = pad2(d.getMinutes());
      const secs = pad2(d.getSeconds());
      return `${year}-${month}-${day} ${hours}:${mins}:${secs}`;
    }
  }
  return val;
}

function stamp() {
  const d = new Date();
  return `${d.getFullYear()}${pad2(d.getMonth() + 1)}${pad2(d.getDate())}_${pad2(d.getHours())}${pad2(d.getMinutes())}${pad2(d.getSeconds())}`;
}

function qid(id) {
  return `\`${id}\``;
}

async function tableExists(conn, tableName) {
  const [rows] = await conn.query(
    `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
    [tableName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function getTableColumns(conn, tableName) {
  const [rows] = await conn.query(
    `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
    [tableName]
  );
  return (rows || []).map((r) => r.COLUMN_NAME);
}

// Product cache for fast resolution
const PRODUCT_RESOLUTION_CACHE = new Map();

async function resolveProductId(conn, shopId, lookup, report) {
  if (!lookup) return null;
  const cacheKey = `${shopId}:${lookup.id || ""}:${lookup.barcode || ""}:${lookup.chain_product_key || ""}:${lookup.name || ""}`;
  if (PRODUCT_RESOLUTION_CACHE.has(cacheKey)) {
    return PRODUCT_RESOLUTION_CACHE.get(cacheKey);
  }

  // 1. Match by chain_product_key
  if (lookup.chain_product_key) {
    const [rows] = await conn.query(
      `SELECT id FROM product WHERE shop_id = ? AND chain_product_key = ? LIMIT 1`,
      [shopId, lookup.chain_product_key]
    );
    if (rows?.[0]) {
      const pid = Number(rows[0].id);
      PRODUCT_RESOLUTION_CACHE.set(cacheKey, pid);
      return pid;
    }
  }

  // 2. Match by barcode
  if (lookup.barcode) {
    const [rows] = await conn.query(
      `SELECT id FROM product WHERE shop_id = ? AND barcode = ? LIMIT 1`,
      [shopId, lookup.barcode]
    );
    if (rows?.[0]) {
      const pid = Number(rows[0].id);
      PRODUCT_RESOLUTION_CACHE.set(cacheKey, pid);
      return pid;
    }
  }

  // 3. Match by exact name
  if (lookup.name) {
    const [rows] = await conn.query(
      `SELECT id FROM product WHERE shop_id = ? AND name = ? LIMIT 1`,
      [shopId, lookup.name]
    );
    if (rows?.[0]) {
      const pid = Number(rows[0].id);
      PRODUCT_RESOLUTION_CACHE.set(cacheKey, pid);
      return pid;
    }
  }

  // Not found
  report.missing_products.push({
    lookup,
    reason: "Product not found in target DB by key, barcode, or name",
  });
  PRODUCT_RESOLUTION_CACHE.set(cacheKey, null);
  return null;
}

async function main() {
  console.log(`====================================================`);
  console.log(`🚀 Production Promotions Sync Tool`);
  console.log(`====================================================`);
  console.log(`Mode:                 ${DRY_RUN ? "🔍 DRY-RUN (Preview Only - No DB changes)" : "⚡ CONFIRM (Live DB Updates)"}`);
  console.log(`Target Shop ID:       ${SHOP_ID}`);
  console.log(`Payload File:         ${PAYLOAD_FILE}`);
  console.log(`Deactivate Unmatched: ${DEACTIVATE_UNMATCHED ? "YES" : "NO"}`);
  if (DEACTIVATE_IDS.length) {
    console.log(`Deactivate Specific IDs: ${DEACTIVATE_IDS.join(", ")}`);
  }
  console.log(`====================================================\n`);

  if (!fs.existsSync(PAYLOAD_FILE)) {
    throw new Error(`Payload file does not exist at: ${PAYLOAD_FILE}`);
  }

  const payloadData = JSON.parse(fs.readFileSync(PAYLOAD_FILE, "utf8"));
  console.log(`📦 Loaded Payload exported at: ${payloadData.metadata?.exported_at}`);

  // Ensure DB schema helpers
  await ensureCartPromotionSchema(db);
  await ensureProductGroupPromotionColumns(db);

  const report = {
    timestamp: new Date().toISOString(),
    shop_id: SHOP_ID,
    dry_run: DRY_RUN,
    product_group_promotions: { inserted: [], updated: [], skipped: [] },
    cart_promotion_rules: { inserted: [], updated: [], skipped: [] },
    product_promotions: { inserted: [], updated: [], skipped: [] },
    unmatched_prod_promotions: [],
    deactivated_promotions: [],
    missing_products: [],
  };

  const conn = await db.getConnection();
  await conn.beginTransaction();

  try {
    // ----------------------------------------------------
    // 1. SYNC PRODUCT GROUP PROMOTIONS
    // ----------------------------------------------------
    if (await tableExists(conn, "product_group_promotion")) {
      const groupCols = await getTableColumns(conn, "product_group_promotion");
      const groupColSet = new Set(groupCols);
      const matchedGroupIds = new Set();

      const itemsInPayload = payloadData.product_group_promotions || [];
      console.log(`\n🏷️  Processing ${itemsInPayload.length} Product Group Promotions...`);

      for (const entry of itemsInPayload) {
        const g = entry.group;
        const items = entry.items || [];

        // Find existing match by external_id or title
        let existing = null;
        if (g.external_id) {
          const [rows] = await conn.query(
            `SELECT * FROM product_group_promotion WHERE shop_id = ? AND external_id = ? LIMIT 1`,
            [SHOP_ID, g.external_id]
          );
          existing = rows?.[0] || null;
        }
        if (!existing && g.title) {
          const [rows] = await conn.query(
            `SELECT * FROM product_group_promotion WHERE shop_id = ? AND title = ? LIMIT 1`,
            [SHOP_ID, g.title]
          );
          existing = rows?.[0] || null;
        }

        let groupId = existing ? Number(existing.id) : null;

        // Build data columns
        const rowData = {};
        for (const col of groupCols) {
          if (["id", "created_at", "updated_at"].includes(col)) continue;
          if (col === "shop_id") rowData[col] = SHOP_ID;
          else if (Object.prototype.hasOwnProperty.call(g, col)) rowData[col] = formatSqlValue(col, g[col]);
        }

        if (existing) {
          matchedGroupIds.add(groupId);
          report.product_group_promotions.updated.push({ id: groupId, title: g.title });
          console.log(`   ✏️  [UPDATE] Group Promo #${groupId}: "${g.title}"`);

          if (!DRY_RUN) {
            const setClause = Object.keys(rowData).map((c) => `${qid(c)} = ?`).join(", ");
            const setValues = Object.values(rowData);
            await conn.query(
              `UPDATE product_group_promotion SET ${setClause} WHERE id = ?`,
              [...setValues, groupId]
            );
          }
        } else {
          report.product_group_promotions.inserted.push({ title: g.title });
          console.log(`   ➕ [INSERT] Group Promo: "${g.title}"`);

          if (!DRY_RUN) {
            const cols = Object.keys(rowData);
            const vals = Object.values(rowData);
            const sql = `INSERT INTO product_group_promotion (${cols.map(qid).join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`;
            const [res] = await conn.query(sql, vals);
            groupId = Number(res.insertId);
            matchedGroupIds.add(groupId);
          }
        }

        // Sync items for this group
        if (groupId && !DRY_RUN && (await tableExists(conn, "product_group_promotion_item"))) {
          // Clear existing items for group
          await conn.query(`DELETE FROM product_group_promotion_item WHERE group_promotion_id = ?`, [groupId]);

          for (const item of items) {
            const prodId = await resolveProductId(conn, SHOP_ID, item._product_lookup, report);
            if (prodId) {
              await conn.query(
                `INSERT INTO product_group_promotion_item (group_promotion_id, product_id, shop_id) VALUES (?, ?, ?)`,
                [groupId, prodId, SHOP_ID]
              );
            }
          }
        }
      }

      // Check for unmatched Group Promos in Prod DB
      const [allProdGroups] = await conn.query(
        `SELECT id, title, is_active FROM product_group_promotion WHERE shop_id = ?`,
        [SHOP_ID]
      );
      for (const pg of allProdGroups || []) {
        const pid = Number(pg.id);
        if (!matchedGroupIds.has(pid)) {
          report.unmatched_prod_promotions.push({
            type: "product_group_promotion",
            id: pid,
            title: pg.title,
            is_active: pg.is_active,
          });
        }
      }
    }

    // ----------------------------------------------------
    // 2. SYNC CART PROMOTION RULES
    // ----------------------------------------------------
    if (await tableExists(conn, "cart_promotion_rule")) {
      const cartCols = await getTableColumns(conn, "cart_promotion_rule");
      const matchedCartIds = new Set();

      const itemsInPayload = payloadData.cart_promotion_rules || [];
      console.log(`\n🛒 Processing ${itemsInPayload.length} Cart Promotion Rules...`);

      for (const entry of itemsInPayload) {
        const r = entry.rule;
        const rewardLookup = entry._reward_product_lookup;

        let rewardProdId = null;
        if (rewardLookup) {
          rewardProdId = await resolveProductId(conn, SHOP_ID, rewardLookup, report);
        }

        let existing = null;
        if (r.source && r.external_reward_id) {
          const [rows] = await conn.query(
            `SELECT * FROM cart_promotion_rule WHERE shop_id = ? AND source = ? AND external_reward_id = ? LIMIT 1`,
            [SHOP_ID, r.source, r.external_reward_id]
          );
          existing = rows?.[0] || null;
        }
        if (!existing && r.title && r.rule_type) {
          const [rows] = await conn.query(
            `SELECT * FROM cart_promotion_rule WHERE shop_id = ? AND rule_type = ? AND title = ? LIMIT 1`,
            [SHOP_ID, r.rule_type, r.title]
          );
          existing = rows?.[0] || null;
        }

        const rowData = {};
        for (const col of cartCols) {
          if (["id", "created_at", "updated_at"].includes(col)) continue;
          if (col === "shop_id") rowData[col] = SHOP_ID;
          else if (col === "reward_product_id") rowData[col] = rewardProdId;
          else if (Object.prototype.hasOwnProperty.call(r, col)) rowData[col] = formatSqlValue(col, r[col]);
        }

        if (existing) {
          const cid = Number(existing.id);
          matchedCartIds.add(cid);
          report.cart_promotion_rules.updated.push({ id: cid, title: r.title });
          console.log(`   ✏️  [UPDATE] Cart Rule #${cid}: "${r.title}"`);

          if (!DRY_RUN) {
            const setClause = Object.keys(rowData).map((c) => `${qid(c)} = ?`).join(", ");
            const setValues = Object.values(rowData);
            await conn.query(`UPDATE cart_promotion_rule SET ${setClause} WHERE id = ?`, [...setValues, cid]);
          }
        } else {
          report.cart_promotion_rules.inserted.push({ title: r.title });
          console.log(`   ➕ [INSERT] Cart Rule: "${r.title}"`);

          if (!DRY_RUN) {
            const cols = Object.keys(rowData);
            const vals = Object.values(rowData);
            const sql = `INSERT INTO cart_promotion_rule (${cols.map(qid).join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`;
            const [res] = await conn.query(sql, vals);
            matchedCartIds.add(Number(res.insertId));
          }
        }
      }

      // Check for unmatched Cart Rules in Prod DB
      const [allProdCartRules] = await conn.query(
        `SELECT id, title, is_active FROM cart_promotion_rule WHERE shop_id = ?`,
        [SHOP_ID]
      );
      for (const cr of allProdCartRules || []) {
        const cid = Number(cr.id);
        if (!matchedCartIds.has(cid)) {
          report.unmatched_prod_promotions.push({
            type: "cart_promotion_rule",
            id: cid,
            title: cr.title,
            is_active: cr.is_active,
          });
        }
      }
    }

    // ----------------------------------------------------
    // 3. SYNC STANDARD PRODUCT PROMOTIONS
    // ----------------------------------------------------
    if (await tableExists(conn, "promotion")) {
      const promoCols = await getTableColumns(conn, "promotion");
      const matchedPromoIds = new Set();

      const itemsInPayload = payloadData.product_promotions || [];
      console.log(`\n🏷️  Processing ${itemsInPayload.length} Product Promotions...`);

      for (const entry of itemsInPayload) {
        const p = entry.promotion;
        const prodLookup = entry._product_lookup;

        const targetProdId = await resolveProductId(conn, SHOP_ID, prodLookup, report);
        if (!targetProdId) {
          report.product_promotions.skipped.push({ title: p.title || p.description, reason: "Product resolution failed" });
          continue;
        }

        let existing = null;
        if (p.title) {
          const [rows] = await conn.query(
            `SELECT * FROM promotion WHERE shop_id = ? AND product_id = ? AND title = ? LIMIT 1`,
            [SHOP_ID, targetProdId, p.title]
          );
          existing = rows?.[0] || null;
        } else {
          const [rows] = await conn.query(
            `SELECT * FROM promotion WHERE shop_id = ? AND product_id = ? LIMIT 1`,
            [SHOP_ID, targetProdId]
          );
          existing = rows?.[0] || null;
        }

        const rowData = {};
        for (const col of promoCols) {
          if (["id", "created_at", "updated_at"].includes(col)) continue;
          if (col === "shop_id") rowData[col] = SHOP_ID;
          else if (col === "product_id") rowData[col] = targetProdId;
          else if (Object.prototype.hasOwnProperty.call(p, col)) rowData[col] = formatSqlValue(col, p[col]);
        }

        if (existing) {
          const pid = Number(existing.id);
          matchedPromoIds.add(pid);
          report.product_promotions.updated.push({ id: pid, title: p.title || p.description });
          console.log(`   ✏️  [UPDATE] Product Promo #${pid}: "${p.title || p.description}"`);

          if (!DRY_RUN) {
            const setClause = Object.keys(rowData).map((c) => `${qid(c)} = ?`).join(", ");
            const setValues = Object.values(rowData);
            await conn.query(`UPDATE promotion SET ${setClause} WHERE id = ?`, [...setValues, pid]);
          }
        } else {
          report.product_promotions.inserted.push({ title: p.title || p.description });
          console.log(`   ➕ [INSERT] Product Promo: "${p.title || p.description}"`);

          if (!DRY_RUN) {
            const cols = Object.keys(rowData);
            const vals = Object.values(rowData);
            const sql = `INSERT INTO promotion (${cols.map(qid).join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`;
            const [res] = await conn.query(sql, vals);
            matchedPromoIds.add(Number(res.insertId));
          }
        }
      }

      // Check for unmatched Product Promos in Prod DB
      const titleCol = promoCols.includes("title") ? "title" : promoCols.includes("description") ? "description" : promoCols.includes("name") ? "name" : "id AS title";
      const hasActiveCol = promoCols.includes("is_active");
      const activeSelect = hasActiveCol ? ", is_active" : ", 1 AS is_active";
      const [allProdPromos] = await conn.query(
        `SELECT id, ${titleCol} ${activeSelect} FROM promotion WHERE shop_id = ?`,
        [SHOP_ID]
      );
      for (const pr of allProdPromos || []) {
        const pid = Number(pr.id);
        if (!matchedPromoIds.has(pid)) {
          report.unmatched_prod_promotions.push({
            type: "promotion",
            id: pid,
            title: pr.title,
            is_active: pr.is_active,
          });
        }
      }
    }

    // ----------------------------------------------------
    // 4. HANDLE DEACTIVATION FOR UNMATCHED PROMOTIONS
    // ----------------------------------------------------
    if (DEACTIVATE_UNMATCHED || DEACTIVATE_IDS.length > 0) {
      console.log(`\n⏸️  Handling Soft Deactivations...`);
      for (const un of report.unmatched_prod_promotions) {
        const shouldDeactivate =
          DEACTIVATE_UNMATCHED || DEACTIVATE_IDS.includes(un.id);

        if (shouldDeactivate && un.is_active) {
          report.deactivated_promotions.push(un);
          console.log(`   ⛔ [DEACTIVATE] ${un.type} #${un.id}: "${un.title}"`);

          if (!DRY_RUN) {
            if (await hasColumn(conn, un.type, "is_active")) {
              await conn.query(`UPDATE ${qid(un.type)} SET is_active = 0 WHERE id = ?`, [un.id]);
            }
          }
        }
      }
    }

    if (DRY_RUN) {
      await conn.rollback();
      console.log(`\n🔍 DRY-RUN Complete. All changes were rolled back.`);
    } else {
      await conn.commit();
      console.log(`\n⚡ CONFIRM Complete. Live DB transaction committed successfully!`);
    }
  } catch (err) {
    await conn.rollback();
    console.error(`\n❌ Error during promotion sync execution:`, err);
    throw err;
  } finally {
    conn.release();
  }

  // ----------------------------------------------------
  // REPORT AUDIT LOGGING
  // ----------------------------------------------------
  if (!fs.existsSync(REPORTS_DIR)) {
    fs.mkdirSync(REPORTS_DIR, { recursive: true });
  }
  const reportPath = path.join(REPORTS_DIR, `promotions_sync_report_${stamp()}.json`);
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");

  console.log(`\n====================================================`);
  console.log(`📊 SYNC REPORT SUMMARY`);
  console.log(`====================================================`);
  console.log(`Product Group Promos: Inserted: ${report.product_group_promotions.inserted.length}, Updated: ${report.product_group_promotions.updated.length}`);
  console.log(`Cart Rules:           Inserted: ${report.cart_promotion_rules.inserted.length}, Updated: ${report.cart_promotion_rules.updated.length}`);
  console.log(`Product Promos:       Inserted: ${report.product_promotions.inserted.length}, Updated: ${report.product_promotions.updated.length}`);
  console.log(`Unmatched Prod Promos: ${report.unmatched_prod_promotions.length} (Listed in report)`);
  if (report.unmatched_prod_promotions.length > 0) {
    console.log(`⚠️  NOTICE: Found ${report.unmatched_prod_promotions.length} promotions in Prod DB that are NOT in the local export!`);
    console.log(`   To deactivate them, re-run with --deactivateUnmatched or --deactivateIds=ID1,ID2`);
  }
  console.log(`Report File Saved:    ${reportPath}`);
  console.log(`====================================================\n`);

  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal sync error:", err);
  process.exit(1);
});

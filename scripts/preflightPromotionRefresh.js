require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

const SOURCE_SHOP_ID = Number(argValue("--sourceShopId", 2));
const TARGET_SHOP_ID = Number(argValue("--targetShopId", 3));
const DATA_FILE = path.resolve(argValue(
  "--data",
  path.join(__dirname, "..", "data", "shop2_promotions_2026_07_28.json"),
));

const PROMO_TABLES = [
  "promotion",
  "cart_promotion_rule",
  "product_group_promotion",
  "product_group_promotion_item",
  "order_promotion_application",
  "order_product_group_promotion_application",
];

const ACTIVE_ORDER_STATUSES = [
  "pending",
  "checkout_pending",
  "confirmed",
  "preparing",
  "ready",
  "delivering",
  "cancel_pending",
];

function qid(value) {
  if (!/^[a-zA-Z0-9_]+$/.test(String(value || ""))) throw new Error(`Unsafe identifier: ${value}`);
  return `\`${value}\``;
}

async function tableExists(conn, tableName) {
  const [rows] = await conn.query(
    `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
    [tableName],
  );
  return Boolean(rows?.length);
}

async function hasColumn(conn, tableName, columnName) {
  const [rows] = await conn.query(
    `SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1`,
    [tableName, columnName],
  );
  return Boolean(rows?.length);
}

async function countByShop(conn, tableName, shopId) {
  if (!(await tableExists(conn, tableName))) return null;
  if (!(await hasColumn(conn, tableName, "shop_id"))) return null;
  const [[row]] = await conn.query(`SELECT COUNT(*) AS count FROM ${qid(tableName)} WHERE shop_id = ?`, [shopId]);
  return Number(row?.count || 0);
}

async function promoCounts(conn, shopId) {
  const result = {};
  for (const tableName of PROMO_TABLES) result[tableName] = await countByShop(conn, tableName, shopId);
  return result;
}

async function activeOrderStats(conn, shopId) {
  if (!(await tableExists(conn, "orders"))) return null;
  const placeholders = ACTIVE_ORDER_STATUSES.map(() => "?").join(", ");
  const params = [shopId, ...ACTIVE_ORDER_STATUSES];
  const [[ordersRow]] = await conn.query(
    `SELECT COUNT(*) AS count FROM orders WHERE shop_id = ? AND status IN (${placeholders})`,
    params,
  );

  let linkedItems = 0;
  if (await tableExists(conn, "order_item")) {
    const predicates = [];
    if (await hasColumn(conn, "order_item", "promo_id")) predicates.push("oi.promo_id IS NOT NULL");
    if (await hasColumn(conn, "order_item", "cart_promotion_rule_id")) predicates.push("oi.cart_promotion_rule_id IS NOT NULL");
    if (await hasColumn(conn, "order_item", "is_gift")) predicates.push("oi.is_gift = 1");
    if (predicates.length) {
      const [[row]] = await conn.query(
        `
        SELECT COUNT(*) AS count
        FROM order_item oi
        JOIN orders o ON o.id = oi.order_id
        WHERE o.shop_id = ?
          AND o.status IN (${placeholders})
          AND (${predicates.join(" OR ")})
        `,
        params,
      );
      linkedItems = Number(row?.count || 0);
    }
  }

  return { active_orders: Number(ordersRow?.count || 0), active_order_items_with_promotion_links: linkedItems };
}

async function referencedProductIds(conn, shopId) {
  const ids = new Set();
  if (await tableExists(conn, "promotion")) {
    const [rows] = await conn.query(`SELECT DISTINCT product_id FROM promotion WHERE shop_id = ?`, [shopId]);
    for (const row of rows || []) if (row.product_id) ids.add(Number(row.product_id));
  }
  if (await tableExists(conn, "cart_promotion_rule")) {
    const [rows] = await conn.query(`SELECT DISTINCT reward_product_id FROM cart_promotion_rule WHERE shop_id = ? AND reward_product_id IS NOT NULL`, [shopId]);
    for (const row of rows || []) if (row.reward_product_id) ids.add(Number(row.reward_product_id));
  }
  if (await tableExists(conn, "product_group_promotion_item")) {
    const [rows] = await conn.query(`SELECT DISTINCT product_id FROM product_group_promotion_item WHERE shop_id = ?`, [shopId]);
    for (const row of rows || []) if (row.product_id) ids.add(Number(row.product_id));
  }
  return [...ids].sort((a, b) => a - b);
}

async function productTransferCoverage(conn, sourceShopId, targetShopId) {
  const ids = await referencedProductIds(conn, sourceShopId);
  if (!ids.length) return { referenced_products: 0, matched: 0, missing: 0, match_methods: {}, missing_products: [] };

  const [sourceRows] = await conn.query(
    `SELECT id, name, barcode, chain_product_key FROM product WHERE shop_id = ? AND id IN (${ids.map(() => "?").join(",")})`,
    [sourceShopId, ...ids],
  );
  const sourceById = new Map((sourceRows || []).map((row) => [Number(row.id), row]));
  const [targetRows] = await conn.query(
    `SELECT id, name, barcode, chain_product_key FROM product WHERE shop_id = ? ORDER BY id ASC`,
    [targetShopId],
  );

  const indexes = {};
  for (const column of ["chain_product_key", "barcode", "name"]) {
    const map = new Map();
    for (const row of targetRows || []) {
      const value = row[column];
      if (value !== null && value !== undefined && String(value).trim() !== "" && !map.has(String(value))) {
        map.set(String(value), row);
      }
    }
    indexes[column] = map;
  }

  const matchMethods = {};
  const missingProducts = [];
  let matched = 0;
  for (const id of ids) {
    const source = sourceById.get(id);
    if (!source) {
      missingProducts.push({ source_product_id: id, reason: "source_product_not_found" });
      continue;
    }
    let method = null;
    for (const column of ["chain_product_key", "barcode", "name"]) {
      const value = source[column];
      if (value !== null && value !== undefined && String(value).trim() !== "" && indexes[column].has(String(value))) {
        method = column;
        break;
      }
    }
    if (method) {
      matched += 1;
      matchMethods[method] = (matchMethods[method] || 0) + 1;
    } else {
      missingProducts.push({
        source_product_id: id,
        name: source.name,
        barcode: source.barcode || null,
        chain_product_key: source.chain_product_key || null,
      });
    }
  }

  return {
    referenced_products: ids.length,
    matched,
    missing: missingProducts.length,
    match_methods: matchMethods,
    missing_products: missingProducts,
  };
}

function excelSummary(filePath) {
  if (!fs.existsSync(filePath)) throw new Error(`Data file not found: ${filePath}`);
  const rows = JSON.parse(fs.readFileSync(filePath, "utf8"));
  const byType = {};
  let active = 0;
  let expired = 0;
  const now = Date.now();
  for (const row of rows) {
    byType[row.type || "unknown"] = (byType[row.type || "unknown"] || 0) + 1;
    if (String(row.active || "").trim() === "כן") active += 1;
    if (row.end_date) {
      const end = new Date(`${row.end_date}T23:59:59`);
      if (Number.isFinite(end.getTime()) && end.getTime() < now) expired += 1;
    }
  }
  return { total: rows.length, active, expired, by_type: byType };
}

async function main() {
  if (!Number.isInteger(SOURCE_SHOP_ID) || SOURCE_SHOP_ID <= 0) throw new Error(`Invalid source shop: ${SOURCE_SHOP_ID}`);
  if (!Number.isInteger(TARGET_SHOP_ID) || TARGET_SHOP_ID <= 0) throw new Error(`Invalid target shop: ${TARGET_SHOP_ID}`);
  if (SOURCE_SHOP_ID === TARGET_SHOP_ID) throw new Error("Source and target shops must be different");

  const conn = await db.getConnection();
  try {
    const [shops] = await conn.query(`SELECT id, name FROM shop WHERE id IN (?, ?) ORDER BY id`, [SOURCE_SHOP_ID, TARGET_SHOP_ID]);
    const shopById = new Map((shops || []).map((row) => [Number(row.id), row.name]));
    const report = {
      mode: "read_only_preflight",
      generated_at: new Date().toISOString(),
      source_shop: { id: SOURCE_SHOP_ID, name: shopById.get(SOURCE_SHOP_ID) || null },
      target_shop: { id: TARGET_SHOP_ID, name: shopById.get(TARGET_SHOP_ID) || null },
      new_excel: excelSummary(DATA_FILE),
      source_counts: await promoCounts(conn, SOURCE_SHOP_ID),
      target_counts: await promoCounts(conn, TARGET_SHOP_ID),
      source_active_orders: await activeOrderStats(conn, SOURCE_SHOP_ID),
      target_active_orders: await activeOrderStats(conn, TARGET_SHOP_ID),
      product_transfer_coverage: await productTransferCoverage(conn, SOURCE_SHOP_ID, TARGET_SHOP_ID),
      warnings: [],
    };

    const sourceLogical = ["promotion", "cart_promotion_rule", "product_group_promotion"]
      .reduce((sum, key) => sum + Number(report.source_counts[key] || 0), 0);
    const targetLogical = ["promotion", "cart_promotion_rule", "product_group_promotion"]
      .reduce((sum, key) => sum + Number(report.target_counts[key] || 0), 0);

    if (!report.source_shop.name || !report.target_shop.name) report.warnings.push("One or both shop IDs do not exist");
    if (sourceLogical === 0) report.warnings.push("Source shop has no promotions. Do not run the transfer publish command");
    if (targetLogical > 0) report.warnings.push("Target shop already has promotions. The replace-target command will delete them");
    if (report.product_transfer_coverage.missing > 0) {
      report.warnings.push(`${report.product_transfer_coverage.missing} referenced products are missing in the target shop. The transfer script will create them with stock 0 unless --noCreateMissingProducts is used`);
    }
    if (Number(report.source_active_orders?.active_order_items_with_promotion_links || 0) > 0) {
      report.warnings.push("Source shop has active order items linked to promotions. Publishing is blocked unless explicitly overridden");
    }
    if (Number(report.target_active_orders?.active_order_items_with_promotion_links || 0) > 0) {
      report.warnings.push("Target shop has active order items linked to promotions. Replacing target promotions is blocked unless explicitly overridden");
    }

    console.log(JSON.stringify(report, null, 2));
  } finally {
    conn.release();
  }
}

main()
  .catch((err) => {
    console.error("[preflight-promotion-refresh]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try { await db.end(); } catch (_) {}
  });

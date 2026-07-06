require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");
const { ensureProductGroupPromotionColumns } = require("../services/productGroupPromotions");

const REPORTS_DIR = path.join(__dirname, "..", "reports");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function boolArg(name, fallback = false) {
  const value = argValue(name, fallback);
  if (value === true || value === false) return value;
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(text)) return true;
  if (["0", "false", "no", "n", "off"].includes(text)) return false;
  return Boolean(value);
}

const SOURCE_SHOP_ID = Number(argValue("--sourceShopId", process.env.PROMO_TRANSFER_SOURCE_SHOP_ID || 2));
const TARGET_SHOP_ID = Number(argValue("--targetShopId", process.env.PROMO_TRANSFER_TARGET_SHOP_ID || 4));
const CONFIRM = boolArg("--confirm", false);
const DRY_RUN = !CONFIRM || boolArg("--dryRun", false);
const REPLACE_TARGET = boolArg("--replaceTarget", false);
const CLEAR_SOURCE = boolArg("--clearSource", false);
const CREATE_MISSING_PRODUCTS = !boolArg("--noCreateMissingProducts", false);
const BACKUP_BEFORE_CHANGE = boolArg("--backup", false);

const PROMOTION_TABLES = [
  "promotion",
  "cart_promotion_rule",
  "product_group_promotion",
  "product_group_promotion_item",
  "order_promotion_application",
  "order_product_group_promotion_application",
];

const PRODUCT_MATCH_COLUMNS = ["chain_product_key", "barcode", "name"];

function pad2(value) {
  return String(value).padStart(2, "0");
}

function stamp() {
  const d = new Date();
  return [
    d.getFullYear(),
    pad2(d.getMonth() + 1),
    pad2(d.getDate()),
    "_",
    pad2(d.getHours()),
    pad2(d.getMinutes()),
    pad2(d.getSeconds()),
  ].join("");
}

function qid(identifier) {
  if (!/^[a-zA-Z0-9_]+$/.test(String(identifier || ""))) {
    throw new Error(`Unsafe SQL identifier: ${identifier}`);
  }
  return `\`${identifier}\``;
}

function hasUsefulValue(value) {
  return value !== null && value !== undefined && String(value).trim() !== "";
}

function asNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

const TABLE_EXISTS_CACHE = new Map();
const COLUMNS_CACHE = new Map();

async function tableExists(conn, tableName) {
  if (TABLE_EXISTS_CACHE.has(tableName)) return TABLE_EXISTS_CACHE.get(tableName);
  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
    LIMIT 1
    `,
    [tableName],
  );
  const exists = Array.isArray(rows) && rows.length > 0;
  TABLE_EXISTS_CACHE.set(tableName, exists);
  return exists;
}

async function getTableColumns(conn, tableName) {
  if (COLUMNS_CACHE.has(tableName)) return COLUMNS_CACHE.get(tableName);
  const [rows] = await conn.query(
    `
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION ASC
    `,
    [tableName],
  );
  const columns = (rows || []).map((row) => row.COLUMN_NAME);
  COLUMNS_CACHE.set(tableName, columns);
  return columns;
}

async function hasColumn(conn, tableName, columnName) {
  const columns = await getTableColumns(conn, tableName);
  return columns.includes(columnName);
}

async function countShopRows(conn, tableName, shopId) {
  if (!(await tableExists(conn, tableName))) return 0;
  if (!(await hasColumn(conn, tableName, "shop_id"))) return 0;
  const [[row]] = await conn.query(`SELECT COUNT(*) AS count FROM ${qid(tableName)} WHERE shop_id = ?`, [shopId]);
  return Number(row?.count || 0);
}

async function backupTable(conn, tableName, backupName, shopId) {
  if (!(await tableExists(conn, tableName))) return false;
  if (!(await hasColumn(conn, tableName, "shop_id"))) return false;
  await conn.query(`CREATE TABLE ${qid(backupName)} AS SELECT * FROM ${qid(tableName)} WHERE shop_id = ?`, [shopId]);
  return true;
}

const BACKUP_TABLE_CODES = {
  promotion: "promo",
  cart_promotion_rule: "cart",
  product_group_promotion: "grp",
  product_group_promotion_item: "grpi",
  order_promotion_application: "opa",
  order_product_group_promotion_application: "ogpa",
};

function backupLabelCode(label) {
  const text = String(label || "").toLowerCase();
  if (text.includes("source")) return "src";
  if (text.includes("target")) return "tgt";
  return text.replace(/[^a-zA-Z0-9_]/g, "_").slice(0, 8) || "x";
}

function buildBackupName(label, tableName, shopId, suffix) {
  const tableCode = BACKUP_TABLE_CODES[tableName] || tableName.replace(/[^a-zA-Z0-9_]/g, "_").slice(0, 12);
  const name = `bak_pt_${backupLabelCode(label)}_${tableCode}_s${shopId}_${suffix}`;
  if (name.length > 64) {
    throw new Error(`Generated backup table name is too long (${name.length}): ${name}`);
  }
  return name;
}

async function backupPromotionTables(conn, shopId, label, report) {
  const suffix = stamp();
  for (const tableName of PROMOTION_TABLES) {
    const backupName = buildBackupName(label, tableName, shopId, suffix);
    if (await backupTable(conn, tableName, backupName, shopId)) {
      report.backup_tables.push(backupName);
    }
  }
}

async function insertRowFromExisting(conn, tableName, row, { overrides = {}, skipColumns = [] } = {}) {
  const tableColumns = await getTableColumns(conn, tableName);
  const tableColumnSet = new Set(tableColumns);
  const skip = new Set(skipColumns);

  const columns = [];
  const values = [];
  for (const column of tableColumns) {
    if (skip.has(column)) continue;
    if (Object.prototype.hasOwnProperty.call(overrides, column)) {
      columns.push(column);
      values.push(overrides[column]);
      continue;
    }
    if (Object.prototype.hasOwnProperty.call(row, column)) {
      columns.push(column);
      values.push(row[column]);
    }
  }

  for (const column of Object.keys(overrides)) {
    if (!tableColumnSet.has(column) || columns.includes(column) || skip.has(column)) continue;
    columns.push(column);
    values.push(overrides[column]);
  }

  if (!columns.length) throw new Error(`No insertable columns for table ${tableName}`);

  const sql = `INSERT INTO ${qid(tableName)} (${columns.map(qid).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
  const [result] = await conn.query(sql, values);
  return result;
}

async function findTargetProduct(conn, targetShopId, sourceProduct, report) {
  for (const column of PRODUCT_MATCH_COLUMNS) {
    if (!Object.prototype.hasOwnProperty.call(sourceProduct, column)) continue;
    const value = sourceProduct[column];
    if (!hasUsefulValue(value)) continue;

    const [rows] = await conn.query(
      `SELECT id, name FROM product WHERE shop_id = ? AND ${qid(column)} = ? ORDER BY id ASC LIMIT 1`,
      [targetShopId, value],
    );
    const hit = rows?.[0];
    if (hit) {
      report.product_matches.push({
        source_product_id: Number(sourceProduct.id),
        target_product_id: Number(hit.id),
        method: column,
        value,
      });
      return hit;
    }
  }
  return null;
}

async function loadSourceProduct(conn, sourceShopId, productId) {
  const [rows] = await conn.query(`SELECT * FROM product WHERE shop_id = ? AND id = ? LIMIT 1`, [sourceShopId, productId]);
  return rows?.[0] || null;
}

async function createMissingProduct(conn, targetShopId, sourceProduct, report) {
  const overrides = { shop_id: targetShopId, stock_amount: 0 };
  const result = await insertRowFromExisting(conn, "product", sourceProduct, {
    overrides,
    skipColumns: ["id", "created_at", "updated_at"],
  });
  const targetProductId = Number(result.insertId);
  report.created_products.push({
    source_product_id: Number(sourceProduct.id),
    target_product_id: targetProductId,
    name: sourceProduct.name,
    barcode: sourceProduct.barcode || null,
    chain_product_key: sourceProduct.chain_product_key || null,
    stock_amount: 0,
  });
  return { id: targetProductId, name: sourceProduct.name };
}

async function resolveTargetProductId(conn, sourceShopId, targetShopId, sourceProductId, cache, report) {
  const productId = Number(sourceProductId);
  if (!Number.isInteger(productId) || productId <= 0) return null;
  if (cache.has(productId)) return cache.get(productId);

  const sourceProduct = await loadSourceProduct(conn, sourceShopId, productId);
  if (!sourceProduct) {
    report.skipped.push({ reason: "source_product_not_found", source_product_id: productId });
    cache.set(productId, null);
    return null;
  }

  const found = await findTargetProduct(conn, targetShopId, sourceProduct, report);
  if (found) {
    const targetId = Number(found.id);
    cache.set(productId, targetId);
    return targetId;
  }

  const missing = {
    source_product_id: productId,
    name: sourceProduct.name,
    barcode: sourceProduct.barcode || null,
    chain_product_key: sourceProduct.chain_product_key || null,
  };
  report.missing_products.push(missing);

  if (DRY_RUN) {
    cache.set(productId, null);
    return null;
  }

  if (!CREATE_MISSING_PRODUCTS) {
    throw new Error(`Product ${productId} (${sourceProduct.name}) was not found in shop ${targetShopId}`);
  }

  const created = await createMissingProduct(conn, targetShopId, sourceProduct, report);
  const targetId = Number(created.id);
  cache.set(productId, targetId);
  return targetId;
}

async function clearOrderItemPromotionLinks(conn, shopId) {
  if (!(await tableExists(conn, "orders")) || !(await tableExists(conn, "order_item"))) return 0;

  const updates = [];
  const predicates = [];
  if (await hasColumn(conn, "order_item", "promo_id")) {
    updates.push("oi.promo_id = NULL");
    predicates.push("oi.promo_id IS NOT NULL");
  }
  if (await hasColumn(conn, "order_item", "cart_promotion_rule_id")) {
    updates.push("oi.cart_promotion_rule_id = NULL");
    predicates.push("oi.cart_promotion_rule_id IS NOT NULL");
  }
  if (await hasColumn(conn, "order_item", "is_gift")) {
    updates.push("oi.is_gift = 0");
    predicates.push("oi.is_gift = 1");
  }

  if (!updates.length) return 0;
  const [result] = await conn.query(
    `
    UPDATE order_item oi
    JOIN orders o ON o.id = oi.order_id
    SET ${updates.join(", ")}
    WHERE o.shop_id = ?
      AND (${predicates.join(" OR ")})
    `,
    [shopId],
  );
  return Number(result?.affectedRows || 0);
}

async function clearShopPromotions(conn, shopId) {
  const deleted = {};
  deleted.order_item_links = await clearOrderItemPromotionLinks(conn, shopId);

  for (const tableName of [
    "order_product_group_promotion_application",
    "order_promotion_application",
    "product_group_promotion_item",
    "product_group_promotion",
    "cart_promotion_rule",
    "promotion",
  ]) {
    deleted[tableName] = await countShopRows(conn, tableName, shopId);
    if (deleted[tableName] > 0 || (await tableExists(conn, tableName))) {
      if (await hasColumn(conn, tableName, "shop_id")) {
        await conn.query(`DELETE FROM ${qid(tableName)} WHERE shop_id = ?`, [shopId]);
      }
    }
  }

  return deleted;
}

async function copyProductPromotions(conn, sourceShopId, targetShopId, productCache, report) {
  if (!(await tableExists(conn, "promotion"))) return;
  const [rows] = await conn.query(`SELECT * FROM promotion WHERE shop_id = ? ORDER BY id ASC`, [sourceShopId]);
  report.source_counts.promotion = rows.length;

  for (const row of rows || []) {
    const targetProductId = await resolveTargetProductId(conn, sourceShopId, targetShopId, row.product_id, productCache, report);
    const itemReport = {
      source_promotion_id: Number(row.id),
      source_product_id: Number(row.product_id),
      target_product_id: targetProductId,
      kind: row.kind,
      description: row.description || null,
    };

    if (!targetProductId && !DRY_RUN) {
      report.skipped.push({ ...itemReport, reason: "target_product_not_resolved" });
      continue;
    }

    report.planned.product_promotions += 1;
    if (DRY_RUN) continue;

    const result = await insertRowFromExisting(conn, "promotion", row, {
      overrides: { shop_id: targetShopId, product_id: targetProductId },
      skipColumns: ["id", "created_at", "updated_at"],
    });
    report.copied.product_promotions += 1;
    report.inserted_ids.promotion.push(Number(result.insertId));
  }
}

async function copyCartPromotionRules(conn, sourceShopId, targetShopId, productCache, report) {
  if (!(await tableExists(conn, "cart_promotion_rule"))) return;
  const [rows] = await conn.query(`SELECT * FROM cart_promotion_rule WHERE shop_id = ? ORDER BY id ASC`, [sourceShopId]);
  report.source_counts.cart_promotion_rule = rows.length;

  for (const row of rows || []) {
    let targetRewardProductId = null;
    if (row.reward_product_id) {
      targetRewardProductId = await resolveTargetProductId(conn, sourceShopId, targetShopId, row.reward_product_id, productCache, report);
      if (!targetRewardProductId && !DRY_RUN) {
        report.skipped.push({
          reason: "cart_rule_reward_product_not_resolved",
          source_cart_rule_id: Number(row.id),
          source_reward_product_id: Number(row.reward_product_id),
        });
        continue;
      }
    }

    report.planned.cart_rules += 1;
    if (DRY_RUN) continue;

    const result = await insertRowFromExisting(conn, "cart_promotion_rule", row, {
      overrides: { shop_id: targetShopId, reward_product_id: targetRewardProductId },
      skipColumns: ["id", "created_at", "updated_at"],
    });
    report.copied.cart_rules += 1;
    report.inserted_ids.cart_promotion_rule.push(Number(result.insertId));
  }
}

async function copyProductGroupPromotions(conn, sourceShopId, targetShopId, productCache, report) {
  if (!(await tableExists(conn, "product_group_promotion"))) return;
  if (!(await tableExists(conn, "product_group_promotion_item"))) return;

  const [groups] = await conn.query(`SELECT * FROM product_group_promotion WHERE shop_id = ? ORDER BY id ASC`, [sourceShopId]);
  report.source_counts.product_group_promotion = groups.length;

  for (const group of groups || []) {
    report.planned.group_promotions += 1;
    let newGroupId = null;

    if (!DRY_RUN) {
      const result = await insertRowFromExisting(conn, "product_group_promotion", group, {
        overrides: { shop_id: targetShopId },
        skipColumns: ["id", "created_at", "updated_at"],
      });
      newGroupId = Number(result.insertId);
      report.copied.group_promotions += 1;
      report.inserted_ids.product_group_promotion.push(newGroupId);
    }

    const [items] = await conn.query(
      `SELECT * FROM product_group_promotion_item WHERE shop_id = ? AND group_promotion_id = ? ORDER BY id ASC`,
      [sourceShopId, group.id],
    );
    report.source_counts.product_group_promotion_item += items.length;

    for (const item of items || []) {
      const targetProductId = await resolveTargetProductId(conn, sourceShopId, targetShopId, item.product_id, productCache, report);
      if (!targetProductId && !DRY_RUN) {
        report.skipped.push({
          reason: "group_item_product_not_resolved",
          source_group_promotion_id: Number(group.id),
          source_group_item_id: Number(item.id),
          source_product_id: Number(item.product_id),
        });
        continue;
      }

      report.planned.group_items += 1;
      if (DRY_RUN) continue;

      await insertRowFromExisting(conn, "product_group_promotion_item", item, {
        overrides: {
          group_promotion_id: newGroupId,
          shop_id: targetShopId,
          product_id: targetProductId,
        },
        skipColumns: ["id", "created_at"],
      });
      report.copied.group_items += 1;
    }
  }
}

async function countAllPromotionRows(conn, shopId) {
  const counts = {};
  for (const tableName of ["promotion", "cart_promotion_rule", "product_group_promotion", "product_group_promotion_item"]) {
    counts[tableName] = await countShopRows(conn, tableName, shopId);
  }
  return counts;
}

function writeReport(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const file = path.join(REPORTS_DIR, `shop_promotions_transfer_report_${stamp()}.json`);
  fs.writeFileSync(file, JSON.stringify(report, null, 2), "utf8");
  return file;
}

function printableSummary(report) {
  return {
    mode: report.mode,
    source_shop_id: report.source_shop_id,
    target_shop_id: report.target_shop_id,
    replace_target: report.replace_target,
    clear_source: report.clear_source,
    create_missing_products: report.create_missing_products,
    before_counts: report.before_counts,
    planned: report.planned,
    copied: report.copied,
    missing_products_to_create: report.missing_products.length,
    created_products: report.created_products.length,
    skipped: report.skipped.length,
    deleted_target: report.deleted_target,
    deleted_source: report.deleted_source,
    after_counts: report.after_counts,
    backup_tables: report.backup_tables,
    report_file: report.report_file,
  };
}

async function main() {
  if (!Number.isInteger(SOURCE_SHOP_ID) || SOURCE_SHOP_ID <= 0) throw new Error(`Invalid --sourceShopId: ${SOURCE_SHOP_ID}`);
  if (!Number.isInteger(TARGET_SHOP_ID) || TARGET_SHOP_ID <= 0) throw new Error(`Invalid --targetShopId: ${TARGET_SHOP_ID}`);
  if (SOURCE_SHOP_ID === TARGET_SHOP_ID) throw new Error("sourceShopId and targetShopId must be different");

  await ensureCartPromotionSchema();
  await ensureProductGroupPromotionColumns();

  const conn = await db.getConnection();
  const report = {
    mode: DRY_RUN ? "dryRun" : "confirm",
    source_shop_id: SOURCE_SHOP_ID,
    target_shop_id: TARGET_SHOP_ID,
    replace_target: REPLACE_TARGET,
    clear_source: CLEAR_SOURCE,
    create_missing_products: CREATE_MISSING_PRODUCTS,
    backup_before_change: BACKUP_BEFORE_CHANGE,
    generated_at: new Date().toISOString(),
    source_counts: {
      promotion: 0,
      cart_promotion_rule: 0,
      product_group_promotion: 0,
      product_group_promotion_item: 0,
    },
    before_counts: {},
    after_counts: {},
    planned: { product_promotions: 0, cart_rules: 0, group_promotions: 0, group_items: 0 },
    copied: { product_promotions: 0, cart_rules: 0, group_promotions: 0, group_items: 0 },
    product_matches: [],
    missing_products: [],
    created_products: [],
    skipped: [],
    inserted_ids: { promotion: [], cart_promotion_rule: [], product_group_promotion: [] },
    deleted_target: {},
    deleted_source: {},
    backup_tables: [],
  };

  try {
    report.before_counts.source = await countAllPromotionRows(conn, SOURCE_SHOP_ID);
    report.before_counts.target = await countAllPromotionRows(conn, TARGET_SHOP_ID);

    const productCache = new Map();

    if (!DRY_RUN) {
      if (BACKUP_BEFORE_CHANGE) {
        await backupPromotionTables(conn, SOURCE_SHOP_ID, "source", report);
        await backupPromotionTables(conn, TARGET_SHOP_ID, "target", report);
      }
      await conn.beginTransaction();
    }

    try {
      if (REPLACE_TARGET) {
        if (DRY_RUN) {
          report.deleted_target = await countAllPromotionRows(conn, TARGET_SHOP_ID);
        } else {
          report.deleted_target = await clearShopPromotions(conn, TARGET_SHOP_ID);
        }
      }

      await copyProductPromotions(conn, SOURCE_SHOP_ID, TARGET_SHOP_ID, productCache, report);
      await copyCartPromotionRules(conn, SOURCE_SHOP_ID, TARGET_SHOP_ID, productCache, report);
      await copyProductGroupPromotions(conn, SOURCE_SHOP_ID, TARGET_SHOP_ID, productCache, report);

      if (CLEAR_SOURCE) {
        if (DRY_RUN) {
          report.deleted_source = await countAllPromotionRows(conn, SOURCE_SHOP_ID);
        } else {
          report.deleted_source = await clearShopPromotions(conn, SOURCE_SHOP_ID);
        }
      }

      if (!DRY_RUN) await conn.commit();
    } catch (err) {
      if (!DRY_RUN) await conn.rollback();
      throw err;
    }

    report.after_counts.source = await countAllPromotionRows(conn, SOURCE_SHOP_ID);
    report.after_counts.target = await countAllPromotionRows(conn, TARGET_SHOP_ID);
    const reportFile = writeReport(report);
    report.report_file = reportFile;

    console.log(JSON.stringify(printableSummary(report), null, 2));
    if (DRY_RUN) {
      console.log("\nDry run only. Run again with --confirm to actually copy/delete rows.");
    }
  } finally {
    conn.release();
  }
}

main()
  .catch((err) => {
    console.error("[transfer-shop-promotions]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });

/*
  Import a cleaned supermarket inventory CSV into product_import_staging.

  This script is intentionally safe:
  - It does NOT touch the product table.
  - It skips rows without a valid price or with a negative price.
  - It skips rows without a valid/non-negative stock amount.
  - If the same product name appears more than once, it keeps only the row with the LOWER stock_amount.
  - Imported rows start with status='raw' and translation_status='pending'.
  - Imported rows are marked with shop_id=2 by default, because some existing product_import_staging schemas require shop_id.

  Usage from project root:
    node scripts/importCleanProductsToStaging.js --batchId=leshem_2026_06_14
    node scripts/importCleanProductsToStaging.js --batchId=leshem_2026_06_14 --csv=data/leshem_products_2026_06_14_clean.csv
    node scripts/importCleanProductsToStaging.js --batchId=leshem_2026_06_14 --shopId=2
    node scripts/importCleanProductsToStaging.js --batchId=leshem_2026_06_14 --dryRun
*/

require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");

function getArg(name, defaultValue = null) {
  const prefix = `--${name}=`;
  const arg = process.argv.find((x) => x.startsWith(prefix));
  if (!arg) return defaultValue;
  return arg.slice(prefix.length);
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function normalizeName(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function parseNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const text = String(value).replace(/^\uFEFF/, "").replace(/,/g, "").trim();
  if (!text) return null;
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
}

function toPositiveInt(value, defaultValue) {
  const n = Number(value);
  if (!Number.isInteger(n) || n <= 0) return defaultValue;
  return n;
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];

    if (inQuotes) {
      if (ch === '"' && next === '"') {
        field += '"';
        i += 1;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (ch === "\r") {
      // Ignore CR. LF will close the row.
    } else {
      field += ch;
    }
  }

  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter((r) => r.some((cell) => String(cell || "").trim() !== ""));
}

function rowsToObjects(rows) {
  if (!rows.length) return [];
  const headers = rows[0].map((h) => String(h || "").replace(/^\uFEFF/, "").trim());
  return rows.slice(1).map((row, idx) => {
    const obj = { __csvLine: idx + 2 };
    headers.forEach((header, colIdx) => {
      obj[header] = row[colIdx] ?? "";
    });
    return obj;
  });
}

async function hasColumn(tableName, columnName, conn = db) {
  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?
    LIMIT 1
    `,
    [tableName, columnName],
  );
  return rows.length > 0;
}

async function ensureColumn(tableName, columnName, columnSql, conn = db) {
  if (!(await hasColumn(tableName, columnName, conn))) {
    await conn.query(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnSql}`);
  }
}

async function ensureStagingSchema(conn = db) {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS product_import_staging (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      shop_id INT UNSIGNED NOT NULL DEFAULT 2,
      import_batch_id VARCHAR(100) NOT NULL,
      source_file VARCHAR(255) DEFAULT NULL,
      source_row INT DEFAULT NULL,
      warehouse VARCHAR(50) DEFAULT NULL,
      warehouse_name VARCHAR(120) DEFAULT NULL,
      barcode VARCHAR(80) DEFAULT NULL,
      name VARCHAR(255) NOT NULL,
      price DECIMAL(10,2) NOT NULL,
      stock_amount DECIMAL(10,3) NOT NULL DEFAULT 0,
      ai_subcategory_id INT UNSIGNED DEFAULT NULL,
      ai_category VARCHAR(100) DEFAULT NULL,
      ai_sub_category VARCHAR(100) DEFAULT NULL,
      ai_confidence DECIMAL(5,4) DEFAULT NULL,
      ai_reason VARCHAR(255) DEFAULT NULL,
      ai_raw_json JSON DEFAULT NULL,
      display_name_en VARCHAR(255) DEFAULT NULL,
      translation_status VARCHAR(32) NOT NULL DEFAULT 'pending',
      translation_confidence DECIMAL(5,4) DEFAULT NULL,
      translation_reason VARCHAR(255) DEFAULT NULL,
      translation_raw_json JSON DEFAULT NULL,
      status VARCHAR(32) NOT NULL DEFAULT 'raw',
      review_note VARCHAR(255) DEFAULT NULL,
      published_product_id INT UNSIGNED DEFAULT NULL,
      created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
      PRIMARY KEY (id),
      UNIQUE KEY uniq_product_import_batch_row (import_batch_id, source_row),
      KEY idx_product_import_shop_batch (shop_id, import_batch_id),
      KEY idx_product_import_batch_status (import_batch_id, status),
      KEY idx_product_import_batch_translation (import_batch_id, translation_status),
      KEY idx_product_import_subcategory (ai_subcategory_id),
      KEY idx_product_import_name (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  // If the table already existed from an older version, add any columns the AI scripts need.
  await ensureColumn("product_import_staging", "shop_id", "INT UNSIGNED NOT NULL DEFAULT 2", conn);
  await ensureColumn("product_import_staging", "source_file", "VARCHAR(255) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "source_row", "INT DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "warehouse", "VARCHAR(50) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "warehouse_name", "VARCHAR(120) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "barcode", "VARCHAR(80) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "name", "VARCHAR(255) NOT NULL", conn);
  await ensureColumn("product_import_staging", "price", "DECIMAL(10,2) NOT NULL", conn);
  await ensureColumn("product_import_staging", "stock_amount", "DECIMAL(10,3) NOT NULL DEFAULT 0", conn);
  await ensureColumn("product_import_staging", "ai_subcategory_id", "INT UNSIGNED DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "ai_category", "VARCHAR(100) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "ai_sub_category", "VARCHAR(100) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "ai_confidence", "DECIMAL(5,4) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "ai_reason", "VARCHAR(255) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "ai_raw_json", "JSON DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "display_name_en", "VARCHAR(255) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "translation_status", "VARCHAR(32) NOT NULL DEFAULT 'pending'", conn);
  await ensureColumn("product_import_staging", "translation_confidence", "DECIMAL(5,4) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "translation_reason", "VARCHAR(255) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "translation_raw_json", "JSON DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "status", "VARCHAR(32) NOT NULL DEFAULT 'raw'", conn);
  await ensureColumn("product_import_staging", "review_note", "VARCHAR(255) DEFAULT NULL", conn);
  await ensureColumn("product_import_staging", "published_product_id", "INT UNSIGNED DEFAULT NULL", conn);
}

function cleanRows(rawRows) {
  const byName = new Map();
  const skipped = [];
  let duplicateRows = 0;

  for (const row of rawRows) {
    const sourceRow = Math.trunc(parseNumber(row.source_row) || parseNumber(row["source_row"]) || row.__csvLine);
    const name = normalizeName(row.name || row["שם פריט"]);
    const price = parseNumber(row.price || row["מחיר מחירון לתאריך: 18/03/2026"] || row["מחיר מחירון"]);
    const stockAmount = parseNumber(row.stock_amount || row["כמות"]);

    if (!name) {
      skipped.push({ sourceRow, reason: "missing_name" });
      continue;
    }
    if (price === null || price < 0) {
      skipped.push({ sourceRow, name, reason: "invalid_price" });
      continue;
    }
    if (stockAmount === null || stockAmount < 0) {
      skipped.push({ sourceRow, name, reason: "invalid_stock_amount" });
      continue;
    }

    const item = {
      source_row: sourceRow,
      warehouse: String(row.warehouse || row["מחסן"] || "").trim() || null,
      warehouse_name: String(row.warehouse_name || row["שם מחסן"] || "").trim() || null,
      barcode: String(row.barcode || row["ברקוד"] || "").trim() || null,
      name,
      price,
      stock_amount: stockAmount,
    };

    const existing = byName.get(name);
    if (!existing) {
      byName.set(name, item);
      continue;
    }

    duplicateRows += 1;
    if (
      item.stock_amount < existing.stock_amount ||
      (item.stock_amount === existing.stock_amount && item.source_row < existing.source_row)
    ) {
      byName.set(name, item);
    }
  }

  const cleaned = Array.from(byName.values()).sort((a, b) => a.source_row - b.source_row);
  return { cleaned, skipped, duplicateRows };
}

async function insertRows({ batchId, sourceFile, shopId, cleaned, dryRun }) {
  if (dryRun) return;

  const safeShopId = toPositiveInt(shopId, 2);

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();
    await ensureStagingSchema(conn);

    await conn.query(`DELETE FROM product_import_staging WHERE import_batch_id = ?`, [batchId]);

    const chunkSize = 500;
    for (let i = 0; i < cleaned.length; i += chunkSize) {
      const chunk = cleaned.slice(i, i + chunkSize);
      const values = chunk.map((row) => [
        safeShopId,
        batchId,
        sourceFile,
        row.source_row,
        row.warehouse,
        row.warehouse_name,
        row.barcode,
        row.name,
        row.price,
        row.stock_amount,
        "raw",
        "pending",
      ]);

      await conn.query(
        `
        INSERT INTO product_import_staging
          (shop_id, import_batch_id, source_file, source_row, warehouse, warehouse_name, barcode, name, price, stock_amount, status, translation_status)
        VALUES ?
        `,
        [values],
      );
    }

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

async function printSummary(batchId) {
  const [rows] = await db.query(
    `
    SELECT status, translation_status, COUNT(*) AS rows_count
    FROM product_import_staging
    WHERE import_batch_id = ?
    GROUP BY status, translation_status
    ORDER BY status, translation_status
    `,
    [batchId],
  );
  console.table(rows);
}

async function main() {
  const batchId = getArg("batchId", "leshem_2026_06_14");
  const csvArg = getArg("csv", "data/leshem_products_2026_06_14_clean.csv");
  const shopId = toPositiveInt(getArg("shopId", "2"), 2);
  const dryRun = hasFlag("dryRun");

  const csvPath = path.resolve(process.cwd(), csvArg);
  if (!fs.existsSync(csvPath)) {
    throw new Error(`CSV file not found: ${csvPath}`);
  }

  const text = fs.readFileSync(csvPath, "utf8");
  const objects = rowsToObjects(parseCsv(text));
  const { cleaned, skipped, duplicateRows } = cleanRows(objects);

  console.log(`[import-staging] batchId=${batchId}`);
  console.log(`[import-staging] csv=${csvPath}`);
  console.log(`[import-staging] shopId=${shopId}`);
  console.log(`[import-staging] source rows=${objects.length}`);
  console.log(`[import-staging] skipped invalid=${skipped.length}`);
  console.log(`[import-staging] duplicate rows removed=${duplicateRows}`);
  console.log(`[import-staging] rows to stage=${cleaned.length}`);

  if (skipped.length) {
    console.log("[import-staging] first skipped rows:");
    console.table(skipped.slice(0, 20));
  }

  await insertRows({
    batchId,
    sourceFile: path.basename(csvPath),
    shopId,
    cleaned,
    dryRun,
  });

  if (dryRun) {
    console.log("[import-staging] dryRun=true, DB was not changed");
  } else {
    await printSummary(batchId);
    console.log("[import-staging] done");
  }
}

main()
  .catch((err) => {
    console.error("[import-staging] failed:", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await db.end();
    } catch {}
  });

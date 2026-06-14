/*
  Replace shop 2 inventory from product_import_staging after AI classification/translation.

  This script performs the requested operation safely in one transaction:
  1. Copy stock_amount from sourceShopId=2 to targetShopId=3 by exact product name.
  2. Ensure archiveShopId=6 exists in the shop table, based on source shop 2.
  3. Move all current products from publishShopId=2 to archiveShopId=6 instead of deleting them.
  4. Insert the staged products into publishShopId=2.
  5. Set is_default=0 for all inserted products.
  6. Set category/sub_category from product_subcategory/product_category.
  7. Set emoji from product_subcategory.emoji.
  8. Rebuild token weights for publishShopId and archiveShopId.

  By default it requires every staged row to be ready:
  - status IN ('classified', 'approved')
  - translation_status='translated'
  - ai_subcategory_id points to an existing subcategory
  - display_name_en exists

  Usage from project root:
    node scripts/replaceShopInventoryFromStaging.js --batchId=leshem_2026_06_14 --dryRun
    node scripts/replaceShopInventoryFromStaging.js --batchId=leshem_2026_06_14 --confirm

  Optional args:
    --sourceShopId=2 --targetShopId=3 --publishShopId=2 --archiveShopId=6

  Optional emergency mode, not recommended:
    node scripts/replaceShopInventoryFromStaging.js --batchId=leshem_2026_06_14 --confirm --allowMissingAi
*/

require("dotenv").config({ quiet: true });

const db = require("../config/db");
const { rebuildTokenWeightsForShop } = require("../services/buildTokenWeights");
const { ensureProductDefaultSchemaNow } = require("../utilities/productDefaultSchema");

const PRODUCT_EMOJI_COLUMN_SQL = "VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL";

function getArg(name, defaultValue = null) {
  const prefix = `--${name}=`;
  const arg = process.argv.find((x) => x.startsWith(prefix));
  if (!arg) return defaultValue;
  return arg.slice(prefix.length);
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function toPositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function quoteIdentifier(name) {
  const text = String(name || "");
  if (!/^[A-Za-z0-9_]+$/.test(text)) {
    throw new Error(`Unsafe SQL identifier: ${text}`);
  }
  return `\`${text}\``;
}

async function hasTable(tableName, conn = db) {
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
  return rows.length > 0;
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

async function ensureProductEmojiSchema(conn = db) {
  if (!(await hasTable("product_subcategory", conn))) {
    throw new Error("Missing table product_subcategory. Run/create the category taxonomy before publishing.");
  }

  if (!(await hasColumn("product", "emoji", conn))) {
    await conn.query(`ALTER TABLE product ADD COLUMN emoji ${PRODUCT_EMOJI_COLUMN_SQL} AFTER image`);
  } else {
    await conn.query(`ALTER TABLE product MODIFY COLUMN emoji ${PRODUCT_EMOJI_COLUMN_SQL}`);
  }

  if (!(await hasColumn("product_subcategory", "emoji", conn))) {
    await conn.query(`ALTER TABLE product_subcategory ADD COLUMN emoji ${PRODUCT_EMOJI_COLUMN_SQL} AFTER name`);
  } else {
    await conn.query(`ALTER TABLE product_subcategory MODIFY COLUMN emoji ${PRODUCT_EMOJI_COLUMN_SQL}`);
  }
}


async function getTableColumns(tableName, conn = db) {
  const [rows] = await conn.query(
    `
    SELECT COLUMN_NAME, EXTRA, IS_NULLABLE, COLUMN_DEFAULT, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    `,
    [tableName],
  );

  return rows.map((row) => ({
    name: row.COLUMN_NAME,
    extra: String(row.EXTRA || "").toLowerCase(),
    nullable: String(row.IS_NULLABLE || "").toUpperCase() === "YES",
    defaultValue: row.COLUMN_DEFAULT,
    dataType: String(row.DATA_TYPE || "").toLowerCase(),
    maxLength: row.CHARACTER_MAXIMUM_LENGTH ? Number(row.CHARACTER_MAXIMUM_LENGTH) : null,
  }));
}

async function getShopColumns(conn = db) {
  return getTableColumns("shop", conn);
}

function sqlStringLiteral(value) {
  return `'${String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "''")}'`;
}

function safeGeneratedEmail(prefix, id) {
  const ts = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  return `${prefix}-${id}-${ts}@gogobuy.local`;
}

async function createArchiveOwnerForShop({ conn, sourceShopId, archiveShopId }) {
  if (!(await hasTable("owner", conn)) || !(await hasColumn("shop", "owner_id", conn))) {
    return null;
  }

  const [[sourceOwner]] = await conn.query(
    `
    SELECT o.id
    FROM shop s
    JOIN owner o ON o.id = s.owner_id
    WHERE s.id = ?
    LIMIT 1
    `,
    [sourceShopId],
  );

  if (!sourceOwner) {
    return null;
  }

  const ownerColumns = (await getTableColumns("owner", conn))
    .filter((col) => !col.extra.includes("generated") && !col.extra.includes("auto_increment"));

  const insertColumns = ownerColumns.map((col) => col.name);
  const archiveEmail = safeGeneratedEmail("archive-owner-shop", archiveShopId);

  const selectExpressions = ownerColumns.map((col) => {
    const columnName = col.name;
    if (columnName === "name") return "LEFT(CONCAT(`name`, ' - ארכיון מלאי'), 100) AS `name`";
    if (columnName === "email") return `${sqlStringLiteral(archiveEmail)} AS \`email\``;
    if (columnName === "shop_id") return "NULL AS `shop_id`";
    if (["created_at", "updated_at"].includes(columnName)) return `CURRENT_TIMESTAMP AS \`${columnName}\``;
    return `\`${columnName}\``;
  });

  const [result] = await conn.query(
    `
    INSERT INTO owner (${insertColumns.map(quoteIdentifier).join(", ")})
    SELECT ${selectExpressions.join(", ")}
    FROM owner
    WHERE id = ?
    LIMIT 1
    `,
    [sourceOwner.id],
  );

  if (!result?.insertId) {
    throw new Error(`Failed to create archive owner for archive shop ${archiveShopId}.`);
  }

  return Number(result.insertId);
}

async function ensureArchiveShop({ conn, sourceShopId, archiveShopId }) {
  if (sourceShopId === archiveShopId) {
    throw new Error("archiveShopId must be different from sourceShopId.");
  }

  const [[existingArchive]] = await conn.query(`SELECT id FROM shop WHERE id = ? LIMIT 1`, [archiveShopId]);
  if (existingArchive) {
    return { created: false, shopId: archiveShopId };
  }

  const [[sourceShop]] = await conn.query(`SELECT id FROM shop WHERE id = ? LIMIT 1`, [sourceShopId]);
  if (!sourceShop) {
    throw new Error(`Cannot create archive shop ${archiveShopId}: source shop ${sourceShopId} does not exist.`);
  }

  const columns = (await getShopColumns(conn)).filter((col) => !col.extra.includes("generated"));
  if (!columns.some((col) => col.name === "id")) {
    throw new Error("shop.id column was not found.");
  }

  const archiveOwnerId = await createArchiveOwnerForShop({ conn, sourceShopId, archiveShopId });
  const archiveEmail = safeGeneratedEmail("archive-shop", archiveShopId);

  const insertColumns = columns.map((col) => col.name);
  const selectExpressions = columns.map((col) => {
    const columnName = col.name;
    if (columnName === "id") return "? AS `id`";
    if (columnName === "owner_id" && archiveOwnerId) return "? AS `owner_id`";
    if (columnName === "name") return "LEFT(CONCAT(`name`, ' - ארכיון מלאי'), 150) AS `name`";
    if (columnName === "email") return col.nullable ? "NULL AS `email`" : `${sqlStringLiteral(archiveEmail)} AS \`email\``;
    if (columnName === "whatsapp_phone") return col.nullable ? "NULL AS `whatsapp_phone`" : "`whatsapp_phone`";
    if (columnName === "phone") return col.nullable ? "NULL AS `phone`" : "`phone`";
    if (["created_at", "updated_at"].includes(columnName)) return `CURRENT_TIMESTAMP AS \`${columnName}\``;
    return `\`${columnName}\``;
  });

  const queryParams = archiveOwnerId ? [archiveShopId, archiveOwnerId, sourceShopId] : [archiveShopId, sourceShopId];
  const [result] = await conn.query(
    `
    INSERT INTO shop (${insertColumns.map(quoteIdentifier).join(", ")})
    SELECT ${selectExpressions.join(", ")}
    FROM shop
    WHERE id = ?
    LIMIT 1
    `,
    queryParams,
  );

  if (!result?.affectedRows) {
    throw new Error(`Failed to create archive shop ${archiveShopId}.`);
  }

  return { created: true, shopId: archiveShopId };
}

async function requireStagingSchema(conn = db) {
  if (!(await hasTable("product_import_staging", conn))) {
    throw new Error("Missing table product_import_staging. Run scripts/importCleanProductsToStaging.js first.");
  }

  const requiredColumns = [
    "import_batch_id",
    "name",
    "price",
    "stock_amount",
    "status",
    "translation_status",
    "display_name_en",
    "ai_subcategory_id",
  ];

  for (const column of requiredColumns) {
    if (!(await hasColumn("product_import_staging", column, conn))) {
      throw new Error(`Missing product_import_staging.${column}. Run the updated import script first.`);
    }
  }
}

async function loadStagingSummary(batchId, conn = db) {
  const [[summary]] = await conn.query(
    `
    SELECT
      COUNT(*) AS total_rows,
      SUM(CASE WHEN price IS NULL OR price < 0 THEN 1 ELSE 0 END) AS invalid_price,
      SUM(CASE WHEN stock_amount IS NULL OR stock_amount < 0 THEN 1 ELSE 0 END) AS invalid_stock,
      SUM(CASE WHEN status IN ('classified', 'approved') THEN 1 ELSE 0 END) AS classified_rows,
      SUM(CASE WHEN translation_status = 'translated' THEN 1 ELSE 0 END) AS translated_rows,
      SUM(CASE WHEN ps.id IS NOT NULL THEN 1 ELSE 0 END) AS taxonomy_ready_rows,
      SUM(CASE WHEN display_name_en IS NOT NULL AND TRIM(display_name_en) <> '' THEN 1 ELSE 0 END) AS english_ready_rows,
      SUM(
        CASE
          WHEN status IN ('classified', 'approved')
           AND translation_status = 'translated'
           AND ps.id IS NOT NULL
           AND display_name_en IS NOT NULL
           AND TRIM(display_name_en) <> ''
          THEN 1 ELSE 0
        END
      ) AS ready_rows
    FROM product_import_staging s
    LEFT JOIN product_subcategory ps ON ps.id = s.ai_subcategory_id
    WHERE s.import_batch_id = ?
    `,
    [batchId],
  );

  return Object.fromEntries(
    Object.entries(summary || {}).map(([key, value]) => [key, Number(value || 0)]),
  );
}

async function loadRowsForPublish({ batchId, allowMissingAi, conn = db }) {
  const whereReady = allowMissingAi
    ? "1 = 1"
    : `s.status IN ('classified', 'approved')
       AND s.translation_status = 'translated'
       AND ps.id IS NOT NULL
       AND pc.id IS NOT NULL
       AND s.display_name_en IS NOT NULL
       AND TRIM(s.display_name_en) <> ''`;

  const [rows] = await conn.query(
    `
    SELECT
      s.id AS staging_id,
      s.name,
      s.display_name_en,
      s.price,
      s.stock_amount,
      pc.name AS category,
      ps.name AS sub_category,
      ps.emoji AS emoji
    FROM product_import_staging s
    LEFT JOIN product_subcategory ps ON ps.id = s.ai_subcategory_id
    LEFT JOIN product_category pc ON pc.id = ps.category_id
    WHERE s.import_batch_id = ?
      AND s.name IS NOT NULL
      AND TRIM(s.name) <> ''
      AND s.price IS NOT NULL
      AND s.price >= 0
      AND s.stock_amount IS NOT NULL
      AND s.stock_amount >= 0
      AND ${whereReady}
    ORDER BY s.source_row ASC, s.id ASC
    `,
    [batchId],
  );

  return rows;
}

async function createBackupTables({ conn, targetShopId, publishShopId, archiveShopId, batchId }) {
  const suffix = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  const safeBatch = String(batchId || "batch").replace(/[^A-Za-z0-9_]/g, "_").slice(0, 40);

  const productBackup = `backup_product_shop${publishShopId}_${safeBatch}_${suffix}`;
  const archiveProductBackup = `backup_product_shop${archiveShopId}_${safeBatch}_${suffix}`;
  const stockBackup = `backup_product_stock_shop${targetShopId}_${safeBatch}_${suffix}`;
  const stagingBackup = `backup_product_staging_${safeBatch}_${suffix}`;

  await conn.query(
    `CREATE TABLE ${quoteIdentifier(productBackup)} AS SELECT * FROM product WHERE shop_id = ?`,
    [publishShopId],
  );

  await conn.query(
    `CREATE TABLE ${quoteIdentifier(archiveProductBackup)} AS SELECT * FROM product WHERE shop_id = ?`,
    [archiveShopId],
  );

  await conn.query(
    `CREATE TABLE ${quoteIdentifier(stockBackup)} AS SELECT id, shop_id, name, stock_amount, updated_at FROM product WHERE shop_id = ?`,
    [targetShopId],
  );

  await conn.query(
    `CREATE TABLE ${quoteIdentifier(stagingBackup)} AS SELECT * FROM product_import_staging WHERE import_batch_id = ?`,
    [batchId],
  );

  return { productBackup, archiveProductBackup, stockBackup, stagingBackup };
}

async function copyStockByName({ conn, sourceShopId, targetShopId }) {
  const [result] = await conn.query(
    `
    UPDATE product p3
    JOIN (
      SELECT MIN(name) AS name, MIN(stock_amount) AS stock_amount
      FROM product
      WHERE shop_id = ?
      GROUP BY name COLLATE utf8mb4_general_ci
    ) p2 ON p3.name COLLATE utf8mb4_general_ci = p2.name COLLATE utf8mb4_general_ci
    SET p3.stock_amount = p2.stock_amount,
        p3.updated_at = CURRENT_TIMESTAMP
    WHERE p3.shop_id = ?
    `,
    [sourceShopId, targetShopId],
  );

  return Number(result?.affectedRows || 0);
}

async function movePublishShopProductsToArchive({ conn, publishShopId, archiveShopId }) {
  await conn.query(`DELETE FROM product_token_weight WHERE shop_id IN (?, ?)`, [publishShopId, archiveShopId]);

  let promotionsMoved = 0;
  if (await hasTable("promotion", conn)) {
    const [promoResult] = await conn.query(
      `
      UPDATE promotion pr
      LEFT JOIN product p ON p.id = pr.product_id
      SET pr.shop_id = ?
      WHERE pr.shop_id = ? OR p.shop_id = ?
      `,
      [archiveShopId, publishShopId, publishShopId],
    );
    promotionsMoved = Number(promoResult?.affectedRows || 0);
  }

  const [productResult] = await conn.query(
    `
    UPDATE product
    SET shop_id = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE shop_id = ?
    `,
    [archiveShopId, publishShopId],
  );

  return {
    promotionsMoved,
    productsMoved: Number(productResult?.affectedRows || 0),
  };
}

async function insertProducts({ conn, publishShopId, rows }) {
  const chunkSize = 500;
  let inserted = 0;

  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const values = chunk.map((row) => [
      publishShopId,
      String(row.name || "").trim(),
      row.display_name_en ? String(row.display_name_en).trim() : null,
      Number(row.price),
      Number(row.stock_amount),
      0,
      row.category || null,
      row.sub_category || null,
      row.emoji || null,
    ]);

    const [result] = await conn.query(
      `
      INSERT INTO product
        (shop_id, name, display_name_en, price, stock_amount, is_default, category, sub_category, emoji)
      VALUES ?
      `,
      [values],
    );

    inserted += Number(result?.affectedRows || 0);
  }

  return inserted;
}

async function main() {
  const batchId = getArg("batchId", "leshem_2026_06_14");
  const sourceShopId = toPositiveInt(getArg("sourceShopId", "2"), 2);
  const targetShopId = toPositiveInt(getArg("targetShopId", "3"), 3);
  const publishShopId = toPositiveInt(getArg("publishShopId", "2"), 2);
  const archiveShopId = toPositiveInt(getArg("archiveShopId", "6"), 6);
  const dryRun = hasFlag("dryRun");
  const confirm = hasFlag("confirm");
  const allowMissingAi = hasFlag("allowMissingAi");
  const skipBackup = hasFlag("skipBackup");

  console.log(`[replace-inventory] batchId=${batchId}`);
  console.log(`[replace-inventory] copy stock: shop ${sourceShopId} -> shop ${targetShopId}`);
  console.log(`[replace-inventory] move old shop ${publishShopId} products to archive shop ${archiveShopId}`);
  console.log(`[replace-inventory] insert new products into shop ${publishShopId}`);
  console.log(`[replace-inventory] allowMissingAi=${allowMissingAi}`);

  const conn = await db.getConnection();

  try {
    await requireStagingSchema(conn);
    await ensureProductDefaultSchemaNow(conn);
    await ensureProductEmojiSchema(conn);

    const summary = await loadStagingSummary(batchId, conn);
    const rowsForPublish = await loadRowsForPublish({ batchId, allowMissingAi, conn });

    console.table(summary);
    console.log(`[replace-inventory] rows selected for product insert=${rowsForPublish.length}`);

    if (!summary.total_rows) {
      throw new Error(`No staging rows found for batchId=${batchId}`);
    }

    if (!allowMissingAi && rowsForPublish.length !== summary.total_rows) {
      throw new Error(
        `Not all staging rows are ready. Ready=${rowsForPublish.length}, total=${summary.total_rows}. Run classify + translate again, or use --allowMissingAi if you intentionally want NULL category/display_name_en.`,
      );
    }

    if (!rowsForPublish.length) {
      throw new Error("No rows are ready to publish.");
    }

    if (dryRun) {
      console.log("[replace-inventory] dryRun=true, DB was not changed");
      return;
    }

    if (!confirm) {
      throw new Error("Missing --confirm. This script updates shop 3 stock, moves old shop 2 products to shop 6, and inserts new shop 2 products.");
    }

    // CREATE TABLE causes an implicit commit in MySQL, so backups are created
    // before opening the transaction that performs the destructive changes.
    const backups = skipBackup
      ? null
      : await createBackupTables({ conn, targetShopId, publishShopId, archiveShopId, batchId });

    if (backups) {
      console.log("[replace-inventory] backup tables created:", backups);
    }

    await conn.beginTransaction();

    const stockRowsUpdated = await copyStockByName({ conn, sourceShopId, targetShopId });
    const archiveShop = await ensureArchiveShop({ conn, sourceShopId, archiveShopId });
    const moveSummary = await movePublishShopProductsToArchive({ conn, publishShopId, archiveShopId });
    const inserted = await insertProducts({ conn, publishShopId, rows: rowsForPublish });

    await conn.commit();

    console.log(`[replace-inventory] shop ${targetShopId} stock rows updated=${stockRowsUpdated}`);
    console.log(`[replace-inventory] archive shop ${archiveShopId} created=${archiveShop.created}`);
    console.log(`[replace-inventory] promotions moved to shop ${archiveShopId}=${moveSummary.promotionsMoved}`);
    console.log(`[replace-inventory] old shop ${publishShopId} products moved to shop ${archiveShopId}=${moveSummary.productsMoved}`);
    console.log(`[replace-inventory] new shop ${publishShopId} products inserted=${inserted}`);

    console.log(`[replace-inventory] rebuilding token weights for shop_id=${publishShopId}`);
    await rebuildTokenWeightsForShop(publishShopId);
    console.log(`[replace-inventory] rebuilding token weights for archive shop_id=${archiveShopId}`);
    await rebuildTokenWeightsForShop(archiveShopId);
    console.log("[replace-inventory] done");
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    throw err;
  } finally {
    conn.release();
    try {
      await db.end();
    } catch {}
  }
}

main().catch((err) => {
  console.error("[replace-inventory] failed:", err);
  process.exitCode = 1;
});

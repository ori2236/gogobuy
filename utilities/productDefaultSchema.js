const db = require("../config/db");

const PRODUCT_DEFAULT_COLUMN_SQL = "TINYINT(1) NOT NULL DEFAULT 0";

let productDefaultSchemaReadyPromise = null;

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

async function hasIndex(tableName, indexName, conn = db) {
  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
      AND INDEX_NAME = ?
    LIMIT 1
    `,
    [tableName, indexName],
  );
  return rows.length > 0;
}

async function ensureProductDefaultSchemaNow(conn = db) {
  if (!(await hasColumn("product", "is_default", conn))) {
    await conn.query(
      `ALTER TABLE product ADD COLUMN is_default ${PRODUCT_DEFAULT_COLUMN_SQL} AFTER stock_amount`,
    );
  } else {
    await conn.query(`UPDATE product SET is_default = 0 WHERE is_default IS NULL`);
    await conn.query(
      `ALTER TABLE product MODIFY COLUMN is_default ${PRODUCT_DEFAULT_COLUMN_SQL}`,
    );
  }

  if (!(await hasIndex("product", "idx_product_shop_default", conn))) {
    await conn.query(
      `CREATE INDEX idx_product_shop_default ON product (shop_id, is_default, updated_at, id)`,
    );
  }
}

async function ensureProductDefaultSchema(conn = db) {
  if (conn !== db) {
    await ensureProductDefaultSchemaNow(conn);
    return;
  }

  if (!productDefaultSchemaReadyPromise) {
    productDefaultSchemaReadyPromise = ensureProductDefaultSchemaNow(db).catch((err) => {
      productDefaultSchemaReadyPromise = null;
      throw err;
    });
  }

  return productDefaultSchemaReadyPromise;
}

function toBool(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const v = value.trim().toLowerCase();
    if (["1", "true", "yes", "y", "כן"].includes(v)) return true;
    if (["0", "false", "no", "n", "לא"].includes(v)) return false;
  }
  return fallback;
}

module.exports = {
  ensureProductDefaultSchema,
  ensureProductDefaultSchemaNow,
  toBool,
};

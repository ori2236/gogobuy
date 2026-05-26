const db = require("../config/db");

const SHOP_EXTRA_COLUMNS = {
  google_maps_url: "VARCHAR(512) DEFAULT NULL",
  whatsapp_phone: "VARCHAR(32) DEFAULT NULL",
  supports_delivery: "TINYINT(1) NOT NULL DEFAULT 0",
  supports_pickup: "TINYINT(1) NOT NULL DEFAULT 1",
  kashrut: "VARCHAR(120) DEFAULT NULL",
  about: "TEXT DEFAULT NULL",
  min_order_amount: "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
  delivery_fee: "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
  cart_empty_reminder_minutes: "INT UNSIGNED NOT NULL DEFAULT 0",
  stock_release_after_inactive_minutes: "INT UNSIGNED NOT NULL DEFAULT 0",
};

let schemaReadyPromise = null;

async function hasColumn(columnName) {
  const [rows] = await db.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'shop'
      AND COLUMN_NAME = ?
    LIMIT 1
    `,
    [columnName],
  );
  return rows.length > 0;
}

async function ensureShopInfoSchema() {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      for (const [column, definition] of Object.entries(SHOP_EXTRA_COLUMNS)) {
        if (!(await hasColumn(column))) {
          await db.query(`ALTER TABLE shop ADD COLUMN ${column} ${definition}`);
        }
      }

      await db.query(`
        CREATE TABLE IF NOT EXISTS shop_regular_hours (
          shop_id INT UNSIGNED NOT NULL,
          day_of_week TINYINT UNSIGNED NOT NULL,
          is_closed TINYINT(1) NOT NULL DEFAULT 0,
          open_time TIME DEFAULT NULL,
          close_time TIME DEFAULT NULL,
          note VARCHAR(255) DEFAULT NULL,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (shop_id, day_of_week),
          KEY idx_shop_regular_hours_shop (shop_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);

      await db.query(`
        CREATE TABLE IF NOT EXISTS shop_special_hours (
          id INT UNSIGNED NOT NULL AUTO_INCREMENT,
          shop_id INT UNSIGNED NOT NULL,
          special_date DATE NOT NULL,
          label VARCHAR(120) DEFAULT NULL,
          is_closed TINYINT(1) NOT NULL DEFAULT 0,
          open_time TIME DEFAULT NULL,
          close_time TIME DEFAULT NULL,
          note VARCHAR(255) DEFAULT NULL,
          created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uniq_shop_special_hours_date (shop_id, special_date),
          KEY idx_shop_special_hours_shop_date (shop_id, special_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);
    })().catch((err) => {
      schemaReadyPromise = null;
      throw err;
    });
  }

  return schemaReadyPromise;
}

async function getShopInfo(shop_id) {
  await ensureShopInfoSchema();

  const [[row]] = await db.query(
    `
    SELECT
      s.id AS shop_id,
      s.name,
      s.address,
      s.google_maps_url,
      s.phone,
      s.whatsapp_phone,
      s.email,
      s.supports_delivery,
      s.supports_pickup,
      s.kashrut,
      s.about,
      s.min_order_amount,
      s.delivery_fee,
      s.cart_empty_reminder_minutes,
      s.stock_release_after_inactive_minutes
    FROM shop s
    WHERE s.id = ?
    LIMIT 1
    `,
    [shop_id],
  );

  return row || null;
}

async function getRegularHours(shop_id) {
  await ensureShopInfoSchema();

  const [rows] = await db.query(
    `
    SELECT
      day_of_week,
      is_closed,
      TIME_FORMAT(open_time, '%H:%i') AS open_time,
      TIME_FORMAT(close_time, '%H:%i') AS close_time,
      note
    FROM shop_regular_hours
    WHERE shop_id = ?
    ORDER BY day_of_week ASC
    `,
    [shop_id],
  );

  return rows || [];
}

async function getSpecialHours(shop_id) {
  await ensureShopInfoSchema();

  const [rows] = await db.query(
    `
    SELECT
      special_date,
      is_closed,
      TIME_FORMAT(open_time, '%H:%i') AS open_time,
      TIME_FORMAT(close_time, '%H:%i') AS close_time,
      label,
      note
    FROM shop_special_hours
    WHERE shop_id = ?
    ORDER BY special_date ASC
    `,
    [shop_id],
  );

  return rows || [];
}

function buildGeneralInfoContext({ info, regularHours, specialHours, now }) {
  return [
    `NOW_ISRAEL=${now.isoDate}`,
    `NOW_ISRAEL_WEEKDAY=${now.weekday}`,
    `SHOP_INFO=${JSON.stringify(info || {})}`,
    `REGULAR_HOURS=${JSON.stringify(regularHours || [])}`,
    `SPECIAL_HOURS=${JSON.stringify(specialHours || [])}`,
  ].join("\n");
}

module.exports = {
  getShopInfo,
  getRegularHours,
  getSpecialHours,
  buildGeneralInfoContext,
  ensureShopInfoSchema,
};

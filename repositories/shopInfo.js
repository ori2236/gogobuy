const db = require("../config/db");
const { buildDeliveryTimingMessage, calculateDeliveryTiming } = require("../utilities/deliveryTiming");

const SHOP_EXTRA_COLUMNS = {
  google_maps_url: "VARCHAR(512) DEFAULT NULL",
  whatsapp_phone: "VARCHAR(32) DEFAULT NULL",
  supports_delivery: "TINYINT(1) NOT NULL DEFAULT 0",
  supports_pickup: "TINYINT(1) NOT NULL DEFAULT 1",
  kashrut: "VARCHAR(120) DEFAULT NULL",
  about: "TEXT DEFAULT NULL",
  min_order_amount: "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
  min_delivery_order_amount: "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
  min_pickup_order_amount: "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
  delivery_fee: "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
  cart_empty_reminder_minutes: "INT UNSIGNED NOT NULL DEFAULT 5",
  stock_release_after_inactive_minutes: "INT UNSIGNED NOT NULL DEFAULT 30",
  max_order_quantity_per_product: "INT UNSIGNED NOT NULL DEFAULT 10",
  order_same_day_cutoff_time: "TIME NOT NULL DEFAULT '15:00:00'",
  delivery_arrival_start_time: "TIME DEFAULT NULL",
  delivery_arrival_end_time: "TIME DEFAULT NULL",
};

let schemaReadyPromise = null;

async function getShopColumnInfo(columnName) {
  const [rows] = await db.query(
    `
    SELECT IS_NULLABLE, COLUMN_DEFAULT, DATA_TYPE, COLUMN_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'shop'
      AND COLUMN_NAME = ?
    LIMIT 1
    `,
    [columnName],
  );
  return rows[0] || null;
}

async function hasColumn(columnName) {
  return Boolean(await getShopColumnInfo(columnName));
}

async function ensureShopInfoSchema() {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      const addedColumns = new Set();
      for (const [column, definition] of Object.entries(SHOP_EXTRA_COLUMNS)) {
        if (!(await hasColumn(column))) {
          await db.query(`ALTER TABLE shop ADD COLUMN ${column} ${definition}`);
          addedColumns.add(column);
        }
      }

      if (addedColumns.has("min_delivery_order_amount") && (await hasColumn("min_order_amount"))) {
        await db.query(
          `UPDATE shop SET min_delivery_order_amount = COALESCE(NULLIF(min_order_amount, 0), min_delivery_order_amount)`,
        );
      }

      if (addedColumns.has("min_pickup_order_amount") && (await hasColumn("min_order_amount"))) {
        await db.query(
          `UPDATE shop SET min_pickup_order_amount = COALESCE(NULLIF(min_order_amount, 0), min_pickup_order_amount)`,
        );
      }

      const cutoffColumn = await getShopColumnInfo("order_same_day_cutoff_time");
      if (cutoffColumn) {
        await db.query(
          `UPDATE shop SET order_same_day_cutoff_time = '15:00:00' WHERE order_same_day_cutoff_time IS NULL`,
        );

        const defaultValue = String(cutoffColumn.COLUMN_DEFAULT || "");
        if (cutoffColumn.IS_NULLABLE === "YES" || defaultValue !== "15:00:00") {
          await db.query(
            `ALTER TABLE shop MODIFY COLUMN order_same_day_cutoff_time TIME NOT NULL DEFAULT '15:00:00'`,
          );
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
      s.min_delivery_order_amount,
      s.min_pickup_order_amount,
      s.delivery_fee,
      s.cart_empty_reminder_minutes,
      s.stock_release_after_inactive_minutes,
      s.max_order_quantity_per_product,
      TIME_FORMAT(s.order_same_day_cutoff_time, '%H:%i') AS order_same_day_cutoff_time,
      TIME_FORMAT(s.delivery_arrival_start_time, '%H:%i') AS delivery_arrival_start_time,
      TIME_FORMAT(s.delivery_arrival_end_time, '%H:%i') AS delivery_arrival_end_time
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
  const deliveryTimingHebrew = buildDeliveryTimingMessage({
    shop: info,
    isEnglish: false,
    includeCutoff: true,
  });
  const deliveryTimingEnglish = buildDeliveryTimingMessage({
    shop: info,
    isEnglish: true,
    includeCutoff: true,
  });
  const deliveryTiming = calculateDeliveryTiming({ shop: info });

  return [
    `NOW_ISRAEL=${now.isoDate}`,
    `NOW_ISRAEL_WEEKDAY=${now.weekday}`,
    `SHOP_INFO=${JSON.stringify(info || {})}`,
    `DELIVERY_TIMING=${JSON.stringify({
      cutoff_time: info?.order_same_day_cutoff_time || "15:00",
      arrival_start_time: info?.delivery_arrival_start_time || null,
      arrival_end_time: info?.delivery_arrival_end_time || null,
      no_delivery_days: ["Friday", "Saturday"],
      expected_delivery_date_if_confirmed_now: deliveryTiming.expectedDate,
      expected_delivery_date_text_he: deliveryTiming.expectedDateText,
      expected_arrival_window: deliveryTiming.arrivalWindowText || null,
      same_day_available_if_confirmed_now: deliveryTiming.isToday,
      message_he: deliveryTimingHebrew.text || null,
      message_en: deliveryTimingEnglish.text || null,
    })}`,
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

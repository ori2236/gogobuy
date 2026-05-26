const db = require("../config/db");
const { parseShopId } = require("../utilities/dashboardUtils");

const SHOP_EXTRA_COLUMNS = {
  google_maps_url: "VARCHAR(512) DEFAULT NULL",
  whatsapp_phone: "VARCHAR(32) DEFAULT NULL",
  supports_delivery: "TINYINT(1) NOT NULL DEFAULT 0",
  supports_pickup: "TINYINT(1) NOT NULL DEFAULT 1",
  kashrut: "VARCHAR(120) DEFAULT NULL",
  about: "TEXT DEFAULT NULL",
};

const DAYS = [0, 1, 2, 3, 4, 5, 6];

function cleanText(value, max = 1000) {
  const s = String(value ?? "").trim();
  if (!s) return null;
  return s.slice(0, max);
}

function cleanRequiredText(value, fieldName, max = 255) {
  const s = String(value ?? "").trim();
  if (!s) {
    const err = new Error(`${fieldName} is required`);
    err.status = 400;
    throw err;
  }
  return s.slice(0, max);
}

function toBool(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") return ["1", "true", "yes", "כן", "on"].includes(value.toLowerCase());
  return false;
}

function cleanTime(value) {
  const s = String(value ?? "").trim();
  if (!s) return null;
  const m = s.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (!m) return null;
  const h = Number(m[1]);
  const min = Number(m[2]);
  if (!Number.isInteger(h) || !Number.isInteger(min) || h < 0 || h > 23 || min < 0 || min > 59) return null;
  return `${String(h).padStart(2, "0")}:${String(min).padStart(2, "0")}:00`;
}

function cleanDate(value) {
  const s = String(value ?? "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  return s;
}

async function hasColumn(conn, tableName, columnName) {
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

async function ensureShopSettingsSchema(conn) {
  for (const [column, definition] of Object.entries(SHOP_EXTRA_COLUMNS)) {
    if (!(await hasColumn(conn, "shop", column))) {
      await conn.query(`ALTER TABLE shop ADD COLUMN ${column} ${definition}`);
    }
  }

  await conn.query(`
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

  await conn.query(`
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
}

function normalizeRegularHours(rows) {
  const byDay = new Map(rows.map((r) => [Number(r.day_of_week), r]));
  return DAYS.map((day) => {
    const r = byDay.get(day) || {};
    return {
      day_of_week: day,
      is_closed: Boolean(r.is_closed),
      open_time: r.open_time || "",
      close_time: r.close_time || "",
      note: r.note || "",
    };
  });
}

function normalizeRegularPayload(input) {
  const rows = Array.isArray(input) ? input : [];
  const byDay = new Map();

  for (const raw of rows) {
    const day = Number(raw?.day_of_week);
    if (!DAYS.includes(day)) continue;
    const isClosed = toBool(raw?.is_closed);
    byDay.set(day, {
      day_of_week: day,
      is_closed: isClosed ? 1 : 0,
      open_time: isClosed ? null : cleanTime(raw?.open_time),
      close_time: isClosed ? null : cleanTime(raw?.close_time),
      note: cleanText(raw?.note, 255),
    });
  }

  return DAYS.map((day) =>
    byDay.get(day) || {
      day_of_week: day,
      is_closed: 1,
      open_time: null,
      close_time: null,
      note: null,
    },
  );
}

function normalizeSpecialPayload(input) {
  const rows = Array.isArray(input) ? input : [];
  const byDate = new Map();

  for (const raw of rows) {
    const specialDate = cleanDate(raw?.special_date);
    if (!specialDate) continue;
    const isClosed = toBool(raw?.is_closed);
    byDate.set(specialDate, {
      special_date: specialDate,
      label: cleanText(raw?.label, 120),
      is_closed: isClosed ? 1 : 0,
      open_time: isClosed ? null : cleanTime(raw?.open_time),
      close_time: isClosed ? null : cleanTime(raw?.close_time),
      note: cleanText(raw?.note, 255),
    });
  }

  return Array.from(byDate.values()).sort((a, b) => a.special_date.localeCompare(b.special_date));
}

function shopIdForRequest(req) {
  return req.dashboardUser?.shop_id ? Number(req.dashboardUser.shop_id) : parseShopId(req);
}

exports.getBusinessSettings = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = shopIdForRequest(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    await ensureShopSettingsSchema(conn);

    const [[info]] = await conn.query(
      `
      SELECT
        id AS shop_id,
        name,
        address,
        google_maps_url,
        phone,
        whatsapp_phone,
        email,
        supports_delivery,
        supports_pickup,
        kashrut,
        about
      FROM shop
      WHERE id = ?
      LIMIT 1
      `,
      [shopId],
    );

    if (!info) {
      return res.status(404).json({ ok: false, message: "Shop not found" });
    }

    const [regularRows] = await conn.query(
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
      [shopId],
    );

    const [specialRows] = await conn.query(
      `
      SELECT
        id,
        DATE_FORMAT(special_date, '%Y-%m-%d') AS special_date,
        label,
        is_closed,
        TIME_FORMAT(open_time, '%H:%i') AS open_time,
        TIME_FORMAT(close_time, '%H:%i') AS close_time,
        note
      FROM shop_special_hours
      WHERE shop_id = ?
      ORDER BY special_date ASC
      `,
      [shopId],
    );

    return res.json({
      ok: true,
      info: {
        ...info,
        supports_delivery: Boolean(info.supports_delivery),
        supports_pickup: Boolean(info.supports_pickup),
      },
      regular_hours: normalizeRegularHours(regularRows),
      special_hours: specialRows.map((r) => ({
        ...r,
        is_closed: Boolean(r.is_closed),
        open_time: r.open_time || "",
        close_time: r.close_time || "",
        note: r.note || "",
        label: r.label || "",
      })),
    });
  } catch (err) {
    console.error("[dashboardSettings.getBusinessSettings]", err);
    return res.status(err.status || 500).json({ ok: false, message: err.status ? err.message : "Server error" });
  } finally {
    conn.release();
  }
};

exports.updateBusinessSettings = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = shopIdForRequest(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const payload = req.body || {};
    const info = payload.info || payload;
    const regularHours = normalizeRegularPayload(payload.regular_hours);
    const specialHours = normalizeSpecialPayload(payload.special_hours);

    const name = cleanRequiredText(info.name, "Shop name", 150);
    const address = cleanRequiredText(info.address, "Address", 255);

    await ensureShopSettingsSchema(conn);
    await conn.beginTransaction();

    await conn.query(
      `
      UPDATE shop
      SET
        name = ?,
        address = ?,
        google_maps_url = ?,
        phone = ?,
        whatsapp_phone = ?,
        email = ?,
        supports_delivery = ?,
        supports_pickup = ?,
        kashrut = ?,
        about = ?
      WHERE id = ?
      `,
      [
        name,
        address,
        cleanText(info.google_maps_url, 512),
        cleanText(info.phone, 32),
        cleanText(info.whatsapp_phone, 32),
        cleanText(info.email, 255),
        toBool(info.supports_delivery) ? 1 : 0,
        toBool(info.supports_pickup) ? 1 : 0,
        cleanText(info.kashrut, 120),
        cleanText(info.about, 4000),
        shopId,
      ],
    );

    for (const r of regularHours) {
      await conn.query(
        `
        INSERT INTO shop_regular_hours
          (shop_id, day_of_week, is_closed, open_time, close_time, note)
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          is_closed = VALUES(is_closed),
          open_time = VALUES(open_time),
          close_time = VALUES(close_time),
          note = VALUES(note)
        `,
        [shopId, r.day_of_week, r.is_closed, r.open_time, r.close_time, r.note],
      );
    }

    await conn.query("DELETE FROM shop_special_hours WHERE shop_id = ?", [shopId]);
    for (const r of specialHours) {
      await conn.query(
        `
        INSERT INTO shop_special_hours
          (shop_id, special_date, label, is_closed, open_time, close_time, note)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        `,
        [shopId, r.special_date, r.label, r.is_closed, r.open_time, r.close_time, r.note],
      );
    }

    await conn.commit();
    return res.json({ ok: true });
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    console.error("[dashboardSettings.updateBusinessSettings]", err);
    return res.status(err.status || 500).json({ ok: false, message: err.status ? err.message : "Server error" });
  } finally {
    conn.release();
  }
};

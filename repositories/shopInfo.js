const db = require("../config/db");

async function getShopInfo(shop_id) {
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
      s.about
    FROM shop s
    WHERE s.id = ?
    LIMIT 1
    `,
    [shop_id],
  );

  return row || null;
}

async function getRegularHours(shop_id) {
  const [rows] = await db.query(
    `
    SELECT
      day_of_week,
      is_closed,
      open_time,
      close_time,
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
  const [rows] = await db.query(
    `
    SELECT
      special_date,
      is_closed,
      open_time,
      close_time,
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
};

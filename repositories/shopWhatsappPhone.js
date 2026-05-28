const db = require("../config/db");

async function getShopByIncomingPhoneId(phoneNumberId) {
  const cleanPhoneNumberId = String(phoneNumberId || "").trim();

  if (!cleanPhoneNumberId) return null;

  const [rows] = await db.query(
    `
    SELECT
      swp.shop_id,
      swp.phone_number_id,
      swp.display_phone_number,
      swp.label,
      s.chain_id,
      s.name AS shop_name
    FROM shop_whatsapp_phone swp
    JOIN shop s ON s.id = swp.shop_id
    WHERE swp.phone_number_id = ?
      AND swp.is_active = 1
    LIMIT 1
    `,
    [cleanPhoneNumberId],
  );

  return rows[0] || null;
}

async function getShopById(shopId) {
  const id = Number(shopId);
  if (!Number.isInteger(id) || id <= 0) return null;

  const [rows] = await db.query(
    `
    SELECT
      s.id AS shop_id,
      s.chain_id,
      s.name AS shop_name
    FROM shop s
    WHERE s.id = ?
    LIMIT 1
    `,
    [id],
  );

  return rows[0] || null;
}

module.exports = {
  getShopByIncomingPhoneId,
  getShopById,
};

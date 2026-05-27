const db = require("../config/db");

async function getShopByIncomingPhoneId(phoneNumberId) {
  const cleanPhoneNumberId = String(phoneNumberId || "").trim();

  if (!cleanPhoneNumberId) return null;

  const [rows] = await db.query(
    `
    SELECT
      shop_id,
      phone_number_id,
      display_phone_number,
      label
    FROM shop_whatsapp_phone
    WHERE phone_number_id = ?
      AND is_active = 1
    LIMIT 1
    `,
    [cleanPhoneNumberId],
  );

  return rows[0] || null;
}

module.exports = {
  getShopByIncomingPhoneId,
};

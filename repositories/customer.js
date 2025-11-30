const db = require("../config/db");

async function ensureCustomer(shop_id, phone) {
  const [rows] = await db.query(
    `SELECT id FROM customer WHERE shop_id = ? AND phone = ? LIMIT 1`,
    [shop_id, phone]
  );
  if (rows.length) return rows[0].id;

  const [ins] = await db.query(
    `INSERT INTO customer (name, shop_id, phone, email)
     VALUES (?, ?, ?, NULL)`,
    [phone, shop_id, phone]
  );
  return ins.insertId;
}

module.exports = {
  ensureCustomer,
};

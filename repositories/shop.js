const db = require("../config/db");

function normalizeShopId(value) {
  const shopId = Number(value);
  if (!Number.isInteger(shopId) || shopId <= 0) return null;
  return shopId;
}

async function getShopById(shop_id) {
  const shopId = normalizeShopId(shop_id);
  if (!shopId) return null;

  const [rows] = await db.query(
    `SELECT id, chain_id, owner_id, name, address, phone, email
       FROM shop
      WHERE id = ?
      LIMIT 1`,
    [shopId],
  );

  return rows[0] || null;
}

async function requireValidShop(shop_id) {
  const shop = await getShopById(shop_id);

  if (!shop) {
    const err = new Error(`Invalid shop_id: ${shop_id}`);
    err.statusCode = 400;
    err.publicMessage = "Invalid shop_id";
    throw err;
  }

  if (!shop.chain_id) {
    const err = new Error(`shop_id=${shop.id} has no chain_id`);
    err.statusCode = 500;
    err.publicMessage = "Shop is missing chain configuration";
    throw err;
  }

  return shop;
}

module.exports = {
  normalizeShopId,
  getShopById,
  requireValidShop,
};

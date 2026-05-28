const db = require("../config/db");

async function getShopChainId(shop_id) {
  const shopId = Number(shop_id);

  if (!Number.isFinite(shopId) || shopId <= 0) {
    throw new Error(`[ensureCustomer] Invalid shop_id: ${shop_id}`);
  }

  const [rows] = await db.query(
    `SELECT chain_id FROM shop WHERE id = ? LIMIT 1`,
    [shopId],
  );

  const chainId = rows[0]?.chain_id ? Number(rows[0].chain_id) : null;

  if (!chainId) {
    throw new Error(
      `[ensureCustomer] shop_id=${shopId} was not found or has no chain_id`,
    );
  }

  return chainId;
}

async function findCustomerByChainAndPhone(chain_id, phone) {
  const [rows] = await db.query(
    `SELECT id FROM customer WHERE chain_id = ? AND phone = ? LIMIT 1`,
    [chain_id, phone],
  );

  return rows[0]?.id || null;
}

async function ensureCustomer(shop_id, phone) {
  const shopId = Number(shop_id);
  const cleanPhone = String(phone || "").trim();

  if (!cleanPhone) {
    throw new Error("[ensureCustomer] Missing customer phone");
  }

  const chainId = await getShopChainId(shopId);

  const existingCustomerId = await findCustomerByChainAndPhone(
    chainId,
    cleanPhone,
  );

  if (existingCustomerId) return existingCustomerId;

  try {
    const [ins] = await db.query(
      `INSERT INTO customer (name, shop_id, chain_id, phone, email)
       VALUES (?, ?, ?, ?, NULL)`,
      [cleanPhone, shopId, chainId, cleanPhone],
    );

    return ins.insertId;
  } catch (err) {
    // Handles rare race condition: two messages from the same new customer arrive together.
    if (err?.code === "ER_DUP_ENTRY") {
      const customerId = await findCustomerByChainAndPhone(
        chainId,
        cleanPhone,
      );

      if (customerId) return customerId;
    }

    throw err;
  }
}

module.exports = {
  ensureCustomer,
};

const db = require("../config/db");

async function wasSentBefore(customer_id, shop_id, message) {
  const DEDUP_WINDOW_SECONDS = 30;
  const [recentSame] = await db.query(
    `
      SELECT id
      FROM chat
      WHERE customer_id = ?
        AND shop_id = ?
        AND sender = 'customer'
        AND message = ?
        AND created_at >= (NOW() - INTERVAL ? SECOND)
      ORDER BY id DESC
      LIMIT 1
      `,
    [customer_id, shop_id, message, DEDUP_WINDOW_SECONDS]
  );

  if (recentSame.length) {
    console.log("[DEDUP] skipping logically duplicate message", {
      customer_id,
      shop_id,
      message,
    });
    return true;
  }

  return false;
}

async function getHistory(customer_id, shop_id, maxMsgs = 5) {
  const [rows] = await db.query(
    `SELECT sender, status, message, created_at
       FROM chat
      WHERE customer_id = ? 
        AND shop_id = ?
        AND created_at >= (NOW() - INTERVAL 48 HOUR)
      ORDER BY created_at DESC, id DESC
      LIMIT ?`,
    [customer_id, shop_id, maxMsgs]
  );

  const pickedDesc = [];
  for (const r of rows) {
    if (r.status === "close") break;
    const content = (r.message || "").trim();
    if (!content) continue;
    const role = r.sender === "customer" ? "user" : "assistant";
    pickedDesc.push({ role, content });
    if (pickedDesc.length >= maxMsgs) break;
  }

  return pickedDesc.reverse();
}

async function saveChat({ customer_id, shop_id, sender, status, message }) {
  const [ins] = await db.query(
    `INSERT INTO chat (customer_id, shop_id, message, sender, status)
     VALUES (?, ?, ?, ?, ?)`,
    [customer_id, shop_id, message || "", sender, status]
  );
  return ins.insertId;
}

module.exports = {
  wasSentBefore,
  getHistory,
  saveChat,
};

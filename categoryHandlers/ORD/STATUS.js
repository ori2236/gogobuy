const db = require("../../config/db");
const { detectIsEnglish } = require("../../utilities/lang");

function isDelivery(order) {
  return String(order?.fulfillment_method || "").toLowerCase() === "delivery";
}

function statusText(order, isEnglish) {
  const status = String(order?.status || "").toLowerCase();
  const delivery = isDelivery(order);

  if (isEnglish) {
    const map = {
      pending: "open and waiting for final confirmation",
      checkout_pending: "waiting for your final confirmation",
      confirmed: "waiting to be picked",
      preparing: "being picked now",
      ready: delivery ? "ready and waiting to be sent" : "ready for pickup",
      delivering: "sent — the courier is on the way",
      completed: delivery ? "delivered" : "picked up",
      cancel_pending: "waiting for cancel confirmation",
    };
    return map[status] || status;
  }

  const map = {
    pending: "פתוחה וממתינה לאישור סופי",
    checkout_pending: "ממתינה לאישור הסופי שלך",
    confirmed: "ממתינה לליקוט",
    preparing: "בליקוט",
    ready: delivery ? "מוכנה ומחכה להישלח" : "מוכנה לאיסוף",
    delivering: "נשלחה — השליח בדרך אליך",
    completed: delivery ? "נמסרה" : "נאספה",
    cancel_pending: "ממתינה לאישור ביטול",
  };
  return map[status] || status;
}

function methodText(order, isEnglish) {
  const method = String(order?.fulfillment_method || "").toLowerCase();
  if (method === "delivery") return isEnglish ? "Home delivery" : "משלוח עד הבית";
  if (method === "pickup") return isEnglish ? "Store pickup" : "איסוף עצמי מהחנות";
  return "";
}

async function getRelevantCustomerIds(customer_id, shop_id) {
  const [[customer]] = await db.query(
    `SELECT id, phone, chain_id FROM customer WHERE id = ? LIMIT 1`,
    [customer_id],
  );

  if (!customer?.phone) return [Number(customer_id)];

  const params = [customer.phone];
  let chainFilter = "";
  if (customer.chain_id) {
    chainFilter = " AND chain_id = ?";
    params.push(customer.chain_id);
  }

  const [rows] = await db.query(
    `SELECT id FROM customer WHERE phone = ?${chainFilter}`,
    params,
  );

  const ids = (rows || []).map((r) => Number(r.id)).filter(Boolean);
  if (!ids.includes(Number(customer_id))) ids.push(Number(customer_id));
  return [...new Set(ids)];
}

async function getRecentCustomerOrders(customer_id, shop_id, hours = 24) {
  const ids = await getRelevantCustomerIds(customer_id, shop_id);
  if (!ids.length) return [];

  const placeholders = ids.map(() => "?").join(",");
  const [rows] = await db.query(
    `
    SELECT
      id,
      status,
      price,
      fulfillment_method,
      delivery_address,
      delivery_fee,
      created_at,
      updated_at
    FROM orders
    WHERE customer_id IN (${placeholders})
      AND shop_id = ?
      AND status IN ('pending','checkout_pending','confirmed','preparing','ready','delivering','completed','cancel_pending')
      AND (
        created_at >= (NOW() - INTERVAL ? HOUR)
        OR updated_at >= (NOW() - INTERVAL ? HOUR)
      )
    ORDER BY created_at DESC, id DESC
    LIMIT 20
    `,
    [...ids, shop_id, Number(hours), Number(hours)],
  );

  return rows || [];
}

function formatOrderLine(order, isEnglish) {
  const id = Number(order.id);
  const status = statusText(order, isEnglish);
  const method = methodText(order, isEnglish);
  const total = Number(order.price || 0).toFixed(2);

  if (isEnglish) {
    const parts = [`#${id} — ${status}`];
    if (method) parts.push(method);
    parts.push(`₪${total}`);
    if (isDelivery(order) && order.delivery_address) parts.push(`address: ${order.delivery_address}`);
    return `• ${parts.join(" — ")}`;
  }

  const parts = [`#${id} — ${status}`];
  if (method) parts.push(method);
  parts.push(`₪${total}`);
  if (isDelivery(order) && order.delivery_address) parts.push(`כתובת: ${order.delivery_address}`);
  return `• ${parts.join(" — ")}`;
}

async function answerOrderStatus({ message, customer_id, shop_id, isEnglish }) {
  const english = typeof isEnglish === "boolean" ? isEnglish : detectIsEnglish(message);
  const orders = await getRecentCustomerOrders(customer_id, shop_id, 24);

  if (!orders.length) {
    return english
      ? "I couldn't find any orders from the last 24 hours. Would you like to start a new order?"
      : "לא מצאתי הזמנות שלך מה־24 שעות האחרונות. תרצה לפתוח הזמנה חדשה?";
  }

  const header = english
    ? "Here are your orders from the last 24 hours:"
    : "אלה ההזמנות שלך מה־24 שעות האחרונות:";

  return [header, "", ...orders.map((order) => formatOrderLine(order, english))].join("\n");
}

module.exports = {
  answerOrderStatus,
  getRecentCustomerOrders,
};

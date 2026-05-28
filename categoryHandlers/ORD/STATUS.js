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

async function getLatestCustomerOrder(customer_id, shop_id) {
  const [[row]] = await db.query(
    `
    SELECT
      id,
      status,
      price,
      fulfillment_method,
      delivery_address,
      delivery_fee,
      delivery_notes,
      created_at,
      updated_at
    FROM orders
    WHERE customer_id = ?
      AND shop_id = ?
      AND status IN ('pending','checkout_pending','confirmed','preparing','ready','delivering','completed','cancel_pending')
    ORDER BY updated_at DESC, id DESC
    LIMIT 1
    `,
    [customer_id, shop_id],
  );
  return row || null;
}

async function answerOrderStatus({ message, customer_id, shop_id, isEnglish }) {
  const english = typeof isEnglish === "boolean" ? isEnglish : detectIsEnglish(message);
  const order = await getLatestCustomerOrder(customer_id, shop_id);

  if (!order) {
    return english
      ? "I couldn't find an order for you right now. Would you like to start a new order?"
      : "לא מצאתי עבורך הזמנה כרגע. תרצה לפתוח הזמנה חדשה?";
  }

  const lines = [];
  if (english) {
    lines.push(`Order #${order.id} status: ${statusText(order, true)}.`);
    const method = methodText(order, true);
    if (method) lines.push(`Receiving method: ${method}.`);
    if (isDelivery(order) && order.delivery_address) lines.push(`Delivery address: ${order.delivery_address}.`);
    if (order.price !== null && order.price !== undefined) lines.push(`Total: ₪${Number(order.price || 0).toFixed(2)}.`);
  } else {
    lines.push(`סטטוס הזמנה #${order.id}: ${statusText(order, false)}.`);
    const method = methodText(order, false);
    if (method) lines.push(`אופן קבלה: ${method}.`);
    if (isDelivery(order) && order.delivery_address) lines.push(`כתובת למשלוח: ${order.delivery_address}.`);
    if (order.price !== null && order.price !== undefined) lines.push(`סה״כ לתשלום: ₪${Number(order.price || 0).toFixed(2)}.`);
  }

  return lines.join("\n");
}

module.exports = {
  answerOrderStatus,
  getLatestCustomerOrder,
};

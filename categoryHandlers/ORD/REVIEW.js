const {
  formatMoney,
  formatQuantity,
  addMoney,
  mulMoney,
} = require("../../utilities/decimal");

function buildOrderReviewMessage(order, items, isEnglish = false) {
  // no open order
  if (!order) {
    return isEnglish ? "There is no open order.\nwould you like to open a new one?" :
     "אין לך כרגע הזמנה פתוחה,\n תרצה לפתוח הזמנה חדשה?";
  }

  // empty order
  if (!items || !items.length) {
    return isEnglish
      ? `Your order is currently empty (Order: #${order.id}).`
      : `ההזמנה שלך כרגע ריקה (הזמנה מספר: #${order.id}).`;
  }

  // order with items
  let subtotal = 0;
  const lines = [];

  if (isEnglish) {
    lines.push("Items currently in your order:");
  } else {
    lines.push("המוצרים שכעת בהזמנה:");
  }

  for (const item of items) {
    const qty = formatQuantity(item.amount);
    const lineTotal = Number(item.price || 0);

    const unitPrice =
      Number(item.amount) > 0 ? lineTotal / Number(item.amount) : lineTotal;

    subtotal = addMoney(subtotal, lineTotal);

    const displayName = isEnglish
      ? (item.display_name_en && item.display_name_en.trim()) || item.name
      : item.name;

    lines.push(
      isEnglish
        ? `* ${displayName} × ${qty} - ₪${formatMoney(
            lineTotal
          )} (₪${formatMoney(unitPrice)} each)`
        : `* ${displayName} × ${qty} - ₪${formatMoney(
            lineTotal
          )} (₪${formatMoney(unitPrice)} ליח')`
    );
  }


  lines.push("");

  if (isEnglish) {
    lines.push(`Order: #${order.id}`);
    lines.push(`Subtotal: ₪${formatMoney(subtotal)}`);
  } else {
    lines.push(`מספר הזמנה: #${order.id}`);
    lines.push(`סה״כ ביניים: *₪${formatMoney(subtotal)}*`);
  }

  return lines.join("\n");
}

module.exports = {
  buildOrderReviewMessage,
};
const {
  formatMoney,
  formatQuantity,
  addMoney,
} = require("../../utilities/decimal");
const { saveOpenQuestions } = require("../../utilities/openQuestions");

async function orderReview(order, items, isEnglish, customer_id, shop_id) {
  // no open order
  if (!order) {
    const botPayload = isEnglish
      ? "You don't have any open orders at the moment. Would you like to start a new order?"
      : "אין לך הזמנה פתוחה כרגע. תרצה לפתוח הזמנה חדשה?";

    const question = botPayload.split(". ")[1];

    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id: null,
      questions: [
        {
          name: null,
          question,
          options: isEnglish ? ["yes", "no"] : ["כן", "לא"],
        },
      ],
    });

    return botPayload;
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

    const isWeight = item.sold_by_weight === 1 || item.sold_by_weight === true;

    const unitsRaw = Number(item.requested_units);
    const units = Number.isFinite(unitsRaw) && unitsRaw > 0 ? unitsRaw : null;

    if (!isWeight) {
      lines.push(
        isEnglish
          ? `* ${displayName} × ${qty} - ₪${formatMoney(
              lineTotal
            )} (₪${formatMoney(unitPrice)} each)`
          : `* ${displayName} × ${qty} - ₪${formatMoney(
              lineTotal
            )} (₪${formatMoney(unitPrice)} ליח')`
      );
    } else {
      if (units) {
        lines.push(
          isEnglish
            ? `* ${displayName} × ${qty} - ₪${formatMoney(
                lineTotal
              )} (₪${formatMoney(
                unitPrice
              )} per kg, approx price for ${units} units)`
            : `* ${displayName} × ${qty} - ₪${formatMoney(
                lineTotal
              )} (₪${formatMoney(
                unitPrice
              )} לק"ג, מחיר משוערך ל${units} יחידות)`
        );
      } else {
        lines.push(
          isEnglish
            ? `* ${displayName} × ${qty} - ₪${formatMoney(
                lineTotal
              )} (₪${formatMoney(unitPrice)} per kg)`
            : `* ${displayName} × ${qty} - ₪${formatMoney(
                lineTotal
              )} (₪${formatMoney(unitPrice)} לק"ג)`
        );
      }
    }
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
  orderReview,
};

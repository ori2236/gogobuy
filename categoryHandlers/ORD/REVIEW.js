const { addMoney, roundTo } = require("../../utilities/decimal");
const { saveOpenQuestions } = require("../../utilities/openQuestions");
const { formatOrderStatus } = require("../../utilities/orders");
const { buildItemsBlock } = require("../../utilities/messageBuilders");

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

  const itemsForView = (items || []).map((it) => {
    const displayName = isEnglish
      ? (it.display_name_en && it.display_name_en.trim()) || it.name
      : it.name;

    const isWeight = it.sold_by_weight === 1 || it.sold_by_weight === true;

    const unitsRaw = Number(it.requested_units);
    const units = Number.isFinite(unitsRaw) && unitsRaw > 0 ? unitsRaw : null;

    const promoId = it.promo_id ? Number(it.promo_id) : null;

    return {
      name: displayName,
      amount: Number(it.amount),

      // unit price (from product)
      price: Number(it.unit_price),

      // line total (stored in order_item.price)
      line_total: Number(it.price),

      promo_id: promoId,
      promo: promoId
        ? {
            kind: it.promo_kind,
            percent_off: it.percent_off,
            amount_off: it.amount_off,
            fixed_price: it.fixed_price,
            bundle_buy_qty: it.bundle_buy_qty,
            bundle_pay_price: it.bundle_pay_price,
          }
        : null,

      ...(isWeight ? { sold_by_weight: true } : {}),
      ...(isWeight && units ? { units } : {}),
    };
  });

  const itemsBlock = buildItemsBlock({
    items: itemsForView,
    isEnglish,
    mode: "review",
  });

  let subtotal = 0;
  for (const it of itemsForView) {
    subtotal = addMoney(subtotal, Number(it.line_total || 0));
  }

  let totalNoPromos = 0;
  for (const it of itemsForView || []) {
    const unit = Number(it.price);
    const qty = Number(it.amount);
    if (!Number.isFinite(unit) || !Number.isFinite(qty)) continue;
    totalNoPromos = addMoney(totalNoPromos, roundTo(unit * qty, 2));
  }

  const totalWithPromos = Number(subtotal || 0);
  const savings = roundTo(totalNoPromos - totalWithPromos, 2);
  const hasSavings = Number.isFinite(savings) && savings >= 0.01;

  const statusText = formatOrderStatus(order.status, isEnglish);

  const headerBlock = isEnglish
    ? [
        `Order: #${order.id}`,
        `Status: ${statusText}`,
        hasSavings
          ? `Subtotal: *₪${totalWithPromos.toFixed(
              2,
            )}* instead of ₪${totalNoPromos.toFixed(2)}`
          : `Subtotal: *₪${totalWithPromos.toFixed(2)}*`,
      ].join("\n")
    : [
        `מספר הזמנה: #${order.id}`,
        `סטטוס: ${statusText}`,
        hasSavings
          ? `סה״כ ביניים: *₪${totalWithPromos.toFixed(
              2,
            )}* במקום ₪${totalNoPromos.toFixed(2)}`
          : `סה״כ ביניים: *₪${totalWithPromos.toFixed(2)}*`,
      ].join("\n");

  return [itemsBlock, " ", headerBlock].filter(Boolean).join("\n");
}

module.exports = { orderReview };

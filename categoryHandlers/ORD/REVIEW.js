const { saveOpenQuestions } = require("../../utilities/openQuestions");
const { buildOrderSummaryMessage } = require("../../utilities/orderSummaryMessage");

async function orderReview(order, items, isEnglish, customer_id, shop_id) {
  // no open order
  if (!order) {
    const botPayload = isEnglish
      ? "You don't have any open orders at the moment. Would you like to start a new order?"
      : "אין לך הזמנה פתוחה כרגע. תרצה לפתוח הזמנה חדשה?";

    const question = isEnglish
      ? "Would you like to start a new order?"
      : "תרצה לפתוח הזמנה חדשה?";

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
      emoji: it.emoji,

      // unit price (from product)
      price: Number(it.unit_price),
      unit_price: Number(it.unit_price),

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

  return buildOrderSummaryMessage({
    orderId: order.id,
    status: order.status,
    items: itemsForView,
    isEnglish,
    totalWithPromos: order.price,
    fulfillmentMethod: order.fulfillment_method,
    deliveryAddress: order.delivery_address,
    deliveryFee: order.delivery_fee,
    deliveryNotes: order.delivery_notes,
  });
}

module.exports = { orderReview };

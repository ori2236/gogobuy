const db = require("../../config/db");
const {
  saveOpenQuestions,
  deleteOpenQuestionsByOrderId,
} = require("../../utilities/openQuestions");
const { detectIsEnglish } = require("../../utilities/lang");
const {
  prepareFulfillmentBeforeCheckout,
  moveOrderToCheckoutPending,
  getOrderForCheckout,
} = require("../../services/fulfillment");
const { buildDeliveryTimingMessage } = require("../../utilities/deliveryTiming");

function parseCheckoutConfirmation(msg) {
  const raw = String(msg || "").trim();
  const m = raw.match(/^#?\s*(\d+)(?:(?:\s+|\s*[-–—:,.]\s*)(.+))?$/s);
  if (!m) return { orderId: null, note: null };

  const note = String(m[2] || "")
    .trim()
    .replace(/^[-–—:,.]+\s*/, "")
    .trim()
    .slice(0, 1000);

  return {
    orderId: Number(m[1]),
    note: note || null,
  };
}

async function getShopDeliveryTimingInfo(shop_id) {
  const [[shop]] = await db.query(
    `SELECT id, order_same_day_cutoff_time, delivery_arrival_start_time, delivery_arrival_end_time
       FROM shop
      WHERE id = ?
      LIMIT 1`,
    [shop_id],
  );
  return shop || null;
}

function buildCheckoutInstruction({ orderId, isEnglish }) {
  if (isEnglish) {
    return [
      `To confirm your order (#${orderId}), reply with:`,
      String(orderId),
      "",
      "If you’d like to add a note for the picker, write it after the number.",
      `For example: ${orderId} Please choose ripe bananas`,
    ].join("\n");
  }

  return [
    `כדי לסיים את ההזמנה שלך (#${orderId}), השב עם:`,
    String(orderId),
    "",
    "אם תרצה להוסיף הערה למלקט, אפשר לכתוב אותה אחרי המספר.",
    `לדוגמה: ${orderId} בלי שקיות בבקשה`,
  ].join("\n");
}

async function checkIfToCheckoutOrder({
  activeOrder,
  message,
  customer_id,
  shop_id,
  saveChat,
}) {
  if (!activeOrder || activeOrder.status !== "checkout_pending") return null;

  await saveChat({
    customer_id,
    shop_id,
    sender: "customer",
    status: "classified",
    message,
  });

  const isEnglish = detectIsEnglish(message);
  const checkoutConfirmation = parseCheckoutConfirmation(message);
  const sentOrderId = checkoutConfirmation.orderId;
  const customerNoteToPicker = checkoutConfirmation.note;
  const isConfirm = Number(sentOrderId) === Number(activeOrder.id);

  let botText;

  if (isConfirm) {
    const sets = [
      "status = 'confirmed'",
      "prev_status = NULL",
      "updated_at = NOW()",
    ];
    const params = [];

    if (customerNoteToPicker) {
      sets.push("customer_note_to_picker = ?");
      params.push(customerNoteToPicker);
    }

    params.push(activeOrder.id, shop_id);

    const [res] = await db.query(
      `UPDATE orders
         SET ${sets.join(", ")}
       WHERE id = ? AND shop_id = ? AND status = 'checkout_pending'`,
      params
    );

    if (res.affectedRows === 0) {
      botText = isEnglish
        ? "⚠️ There was a problem confirming your order. Please try again or contact the shop."
        : "⚠️ הייתה בעיה באישור ההזמנה. אפשר לנסות שוב או ליצור קשר עם החנות.";
    } else {
      await deleteOpenQuestionsByOrderId(activeOrder.id, shop_id);

      const noteSuffix = customerNoteToPicker
        ? isEnglish
          ? "\n📝 I also saved your note for the picker."
          : "\n📝 שמרתי גם את ההערה שלך למלקט."
        : "";

      let confirmedOrder = await getOrderForCheckout(activeOrder.id, shop_id);
      let deliveryTimingText = "";
      if (String(confirmedOrder?.fulfillment_method || "") === "delivery") {
        const shopDeliveryTiming = await getShopDeliveryTimingInfo(shop_id);
        const deliveryTiming = buildDeliveryTimingMessage({
          shop: shopDeliveryTiming,
          isEnglish,
          includeCutoff: false,
        });
        if (deliveryTiming?.timing?.hasArrivalWindow) {
          await db.query(
            `UPDATE orders
                SET delivery_expected_date = ?,
                    delivery_expected_start_time = ?,
                    delivery_expected_end_time = ?
              WHERE id = ? AND shop_id = ?`,
            [
              deliveryTiming.timing.expectedDate,
              deliveryTiming.timing.arrivalStart,
              deliveryTiming.timing.arrivalEnd,
              activeOrder.id,
              shop_id,
            ],
          );
          confirmedOrder = await getOrderForCheckout(activeOrder.id, shop_id);
        }
        deliveryTimingText = deliveryTiming?.text ? `
${deliveryTiming.text}` : "";
      }

      const fulfillmentLine = (() => {
        if (!confirmedOrder) return "";
        if (String(confirmedOrder.fulfillment_method || "") === "delivery") {
          return isEnglish
            ? `
Total including delivery: ₪${Number(confirmedOrder.price || 0).toFixed(2)}. Delivery address: ${confirmedOrder.delivery_address || "saved address"}.${deliveryTimingText}`
            : `
סה״כ כולל משלוח: ₪${Number(confirmedOrder.price || 0).toFixed(2)}. כתובת למשלוח: ${confirmedOrder.delivery_address || "הכתובת השמורה"}.${deliveryTimingText}`;
        }
        if (String(confirmedOrder.fulfillment_method || "") === "pickup") {
          return isEnglish
            ? `
Pickup total: ₪${Number(confirmedOrder.price || 0).toFixed(2)}.`
            : `
סה״כ לתשלום באיסוף עצמי: ₪${Number(confirmedOrder.price || 0).toFixed(2)}.`;
        }
        return "";
      })();

      botText = isEnglish
        ? `✅ Your order (#${activeOrder.id}) has been confirmed and sent to the shop.${noteSuffix}${fulfillmentLine}`
        : `✅ ההזמנה שלך (#${activeOrder.id}) אושרה ונשלחה לחנות.${noteSuffix}${fulfillmentLine}`;
    }
  } else {
    await db.query(
      `UPDATE orders
         SET status = COALESCE(prev_status, 'pending'),
             prev_status = NULL,
             updated_at = NOW()
       WHERE id = ? AND shop_id = ? AND status = 'checkout_pending'`,
      [activeOrder.id, shop_id]
    );

    botText = isEnglish
      ? `ℹ️ Your order (#${activeOrder.id}) was not sent. Anything else you'd like to do?`
      : `ℹ️ ההזמנה שלך (#${activeOrder.id}) לא נשלחה. יש דבר נוסף שתרצה לעשות?`;
  }

  await saveChat({
    customer_id,
    shop_id,
    sender: "bot",
    status: "classified",
    message: botText,
  });

  return botText;
}

async function askToCheckoutOrder(
  activeOrder,
  isEnglish,
  customer_id,
  shop_id,
  maxPerProduct = null
) {
  if (!activeOrder) {
    const botPayload = isEnglish
      ? "🛒 You don't have any open orders at the moment. Would you like to start a new order?"
      : "🛒 אין לך הזמנה פתוחה כרגע. תרצה לפתוח הזמנה חדשה?";

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

  if (activeOrder.status === "confirmed") {
    return isEnglish
      ? `✅ Your order (#${activeOrder.id}) is already confirmed.`
      : `✅ ההזמנה שלך (#${activeOrder.id}) כבר מאושרת.`;
  }

  const fulfillmentQuestion = await prepareFulfillmentBeforeCheckout({
    activeOrder,
    isEnglish,
    customer_id,
    shop_id,
  });

  if (fulfillmentQuestion) return fulfillmentQuestion;

  return moveOrderToCheckoutPending({
    order_id: activeOrder.id,
    shop_id,
    isEnglish,
    customer_id,
    maxPerProduct,
  });
}

module.exports = {
  askToCheckoutOrder,
  checkIfToCheckoutOrder,
};

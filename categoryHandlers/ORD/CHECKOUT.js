const db = require("../../config/db");
const {
  saveOpenQuestions,
  deleteOpenQuestionsByOrderId,
} = require("../../utilities/openQuestions");
const { detectIsEnglish } = require("../../utilities/lang");

function parseOrderIdOnly(msg) {
  const m = String(msg || "")
    .trim()
    .match(/^#?\s*(\d+)\s*$/);
  return m ? Number(m[1]) : null;
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
  const sentOrderId = parseOrderIdOnly(message);
  const isConfirm = Number(sentOrderId) === Number(activeOrder.id);

  let botText;

  if (isConfirm) {
    const [res] = await db.query(
      `UPDATE orders
         SET status = 'confirmed',
             prev_status = NULL,
             updated_at = NOW()
       WHERE id = ? AND shop_id = ? AND status = 'checkout_pending'`,
      [activeOrder.id, shop_id]
    );

    if (res.affectedRows === 0) {
      botText = isEnglish
        ? "There was a problem confirming your order. Please try again or contact the shop."
        : "הייתה בעיה באישור ההזמנה. אפשר לנסות שוב או ליצור קשר עם החנות.";
    } else {
      await deleteOpenQuestionsByOrderId(activeOrder.id, shop_id);

      botText = isEnglish
        ? `Your order (#${activeOrder.id}) has been confirmed and sent to the shop.`
        : `ההזמנה שלך (#${activeOrder.id}) אושרה ונשלחה לחנות.`;
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
      ? `Your order (#${activeOrder.id}) was not checked out. Anything else you'd like to do?`
      : `ההזמנה שלך (#${activeOrder.id}) לא נשלחה. יש דבר נוסף שתרצה לעשות?`;
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
  shop_id
) {
  if (!activeOrder) {
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

  if (activeOrder.status === "confirmed") {
    return isEnglish
      ? `Your order (#${activeOrder.id}) is already confirmed.`
      : `ההזמנה שלך (#${activeOrder.id}) כבר מאושרת.`;
  }

  const [res] = await db.query(
    `UPDATE orders
        SET prev_status = status,
            status = 'checkout_pending',
            updated_at = NOW()
      WHERE id = ? AND shop_id = ? AND status = 'pending'`,
    [activeOrder.id, shop_id]
  );

  if (res.affectedRows === 0) {
    return isEnglish
      ? `Order (#${activeOrder.id}) can't be checked out at this stage.`
      : `אי אפשר לסיים את ההזמנה (#${activeOrder.id}) בשלב הזה.`;
  }

  return isEnglish
    ? `To checkout your order (#${activeOrder.id}), reply with the order number only: ${activeOrder.id}`
    : `כדי לסיים את ההזמנה שלך (#${activeOrder.id}), השב עם מספר ההזמנה בלבד: ${activeOrder.id}`;
}

module.exports = {
  askToCheckoutOrder,
  checkIfToCheckoutOrder,
};

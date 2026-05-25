const db = require("../../config/db");
const {
  saveOpenQuestions,
  deleteOpenQuestionsByOrderId,
} = require("../../utilities/openQuestions");
const { detectIsEnglish } = require("../../utilities/lang");

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
        ? "There was a problem confirming your order. Please try again or contact the shop."
        : "הייתה בעיה באישור ההזמנה. אפשר לנסות שוב או ליצור קשר עם החנות.";
    } else {
      await deleteOpenQuestionsByOrderId(activeOrder.id, shop_id);

      const noteSuffix = customerNoteToPicker
        ? isEnglish
          ? "\nI also saved your note for the picker."
          : "\nשמרתי גם את ההערה שלך למלקט."
        : "";

      botText = isEnglish
        ? `Your order (#${activeOrder.id}) has been confirmed and sent to the shop.${noteSuffix}`
        : `ההזמנה שלך (#${activeOrder.id}) אושרה ונשלחה לחנות.${noteSuffix}`;
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

  return buildCheckoutInstruction({
    orderId: activeOrder.id,
    isEnglish,
  });
}

module.exports = {
  askToCheckoutOrder,
  checkIfToCheckoutOrder,
};

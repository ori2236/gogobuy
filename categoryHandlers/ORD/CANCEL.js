const db = require("../../config/db");
const { saveOpenQuestions } = require("../../utilities/openQuestions");

async function askToCancelOrder(activeOrder, isEnglish, customer_id, shop_id) {
  let botPayload;
  if (!activeOrder) {
    botPayload = isEnglish
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
  } else {
    //change status to "canceled"
    await db.query(
      `UPDATE orders
          SET status = 'canceled', updated_at = NOW()
        WHERE id = ?`,
      [activeOrder.id]
    );

    botPayload = isEnglish
      ? `To cancel your order (#${activeOrder.id}), reply with the word "cancel". Any other message will keep your order open.`
      : `כדי לבטל את ההזמנה שלך (#${activeOrder.id}), כתוב רק את המילה "ביטול". כל הודעה אחרת תשאיר את ההזמנה פתוחה.`;
  }

  return botPayload;
}

module.exports = {
  askToCancelOrder,
};

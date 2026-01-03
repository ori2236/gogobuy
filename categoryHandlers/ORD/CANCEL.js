const db = require("../../config/db");
const { saveOpenQuestions } = require("../../utilities/openQuestions");
const { detectIsEnglish } = require("../../utilities/lang");
const {
  deleteOpenQuestionsByOrderId,
} = require("../../utilities/openQuestions");

async function cancelOrderAndRestoreStock(order_id, shop_id) {
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const [[ord]] = await conn.query(
      `SELECT id, shop_id, status
         FROM orders
        WHERE id = ? FOR UPDATE`,
      [order_id]
    );

    if (!ord) {
      await conn.rollback();
      return false;
    }

    const orderShopId = Number(ord.shop_id);
    if (shop_id && Number(shop_id) !== orderShopId) {
      await conn.rollback();
      return false;
    }

    if (ord.status !== "cancel_pending") {
      await conn.rollback();
      return false;
    }


    const [items] = await conn.query(
      `SELECT product_id, amount
         FROM order_item
        WHERE order_id = ?
        FOR UPDATE`,
      [order_id]
    );

    if (items.length) {
      const ids = items.map((i) => Number(i.product_id));
      const placeholders = ids.map(() => "?").join(",");

      await conn.query(
        `SELECT id
           FROM product
          WHERE id IN (${placeholders}) AND shop_id = ?
          FOR UPDATE`,
        [...ids, orderShopId]
      );

      for (const it of items) {
        await conn.query(
          `UPDATE product
              SET stock_amount = COALESCE(stock_amount,0) + ?
            WHERE id = ? AND shop_id = ?`,
          [Number(it.amount), Number(it.product_id), orderShopId]
        );
      }
    }

    await deleteOpenQuestionsByOrderId(order_id, orderShopId, conn);

    await conn.query(`DELETE FROM order_item WHERE order_id = ?`, [order_id]);
    await conn.query(`DELETE FROM orders WHERE id = ?`, [order_id]);

    await conn.commit();
    return true;
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

function parseOrderIdOnly(msg) {
  const m = String(msg || "")
    .trim()
    .match(/^#?\s*(\d+)\s*$/);
  return m ? Number(m[1]) : null;
}

async function checkIfToCancelOrder({
  activeOrder,
  message,
  customer_id,
  shop_id,
  saveChat,
}) {
  if (!activeOrder || activeOrder.status !== "cancel_pending") return null;

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
    const result = await cancelOrderAndRestoreStock(activeOrder.id, shop_id);

    if (!result) {
      botText = isEnglish
        ? "There was a problem cancelling your order. Please try again or contact the shop."
        : "הייתה בעיה בביטול ההזמנה. אפשר לנסות שוב או ליצור קשר עם החנות.";
    } else {
      botText = isEnglish
        ? `Your order (#${activeOrder.id}) has been cancelled.`
        : `ההזמנה שלך (#${activeOrder.id}) בוטלה.`;
    }
  } else {
    //not canceling
    await db.query(
      `UPDATE orders
         SET status = 'pending', updated_at = NOW()
       WHERE id = ? AND status = 'cancel_pending'`,
      [activeOrder.id]
    );

    botText = isEnglish
      ? `Your order (#${activeOrder.id}) was not cancelled. Anything else you'd like to do?`
      : `ההזמנה שלך (#${activeOrder.id}) לא בוטלה. יש דבר נוסף שתרצה לעשות?`;
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
    return botPayload;
  }
  //change status to "cancel_pending"
  const [res] = await db.query(
    `UPDATE orders
      SET status='cancel_pending', updated_at=NOW()
    WHERE id=? AND status='pending'`,
    [activeOrder.id]
  );

  if (res.affectedRows === 0) {
    return isEnglish
      ? `Order (#${activeOrder.id}) can't be cancelled at this stage.`
      : `אי אפשר לבטל את ההזמנה (#${activeOrder.id}) בשלב הזה.`;
  }

  return isEnglish
    ? `To cancel your order (#${activeOrder.id}), reply with the order number only: ${activeOrder.id}`
    : `כדי לבטל את ההזמנה שלך (#${activeOrder.id}), השב עם מספר ההזמנה בלבד: ${activeOrder.id}`;
}

module.exports = {
  askToCancelOrder,
  checkIfToCancelOrder,
};

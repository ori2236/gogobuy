const express = require("express");
const router = express.Router();
const db = require("../config/db");

const WHATSAPP_PHONE_ID = process.env.WHATSAPP_PHONE_ID;

router.delete("/:verifyTokenSent", async (req, res) => {
  const verifyTokenSent = req.params.verifyTokenSent;
  if (verifyTokenSent !== WHATSAPP_PHONE_ID) {
    return res.status(400).send(`wrong token\n+${WHATSAPP_PHONE_ID}`);
  }

  let conn;
  try {
    conn = await db.getConnection();
    await conn.beginTransaction();

    const [updateRes] = await conn.query(
      `
      UPDATE product p
      JOIN (
        SELECT product_id, SUM(amount) AS qty_to_restore
        FROM order_item
        WHERE id > 0
        GROUP BY product_id
      ) oi ON oi.product_id = p.id
      SET p.stock_amount = COALESCE(p.stock_amount, 0) + COALESCE(oi.qty_to_restore, 0)
      `
    );

    const [delOrderItemsRes] = await conn.query(
      `DELETE FROM order_item WHERE id > 0`
    );
    const [delOrdersRes] = await conn.query(`DELETE FROM orders WHERE id > 0`);
    const [delChatRes] = await conn.query(`DELETE FROM chat WHERE id > 6`);
    const [delOpenQsRes] = await conn.query(
      `DELETE FROM chat_open_question WHERE id > 0`
    );

    await conn.commit();

    console.log("DB-RESET Finished successfully:");
    console.log(
      `  Restored stock for ${
        updateRes.affectedRows || 0
      } products (via UPDATE JOIN)`
    );
    console.log(
      `  Deleted ${delOrderItemsRes.affectedRows || 0} rows from order_item`
    );
    console.log(`  Deleted ${delOrdersRes.affectedRows || 0} rows from orders`);
    console.log(
      `  Deleted ${delChatRes.affectedRows || 0} rows from chat (id > 6)`
    );
    console.log(
      `  Deleted ${delOpenQsRes.affectedRows || 0} rows from chat_open_question`
    );

    return res.json({
      ok: true,
      message: "DB reset finished successfully",
    });
  } catch (e) {
    if (conn) {
      try {
        await conn.rollback();
      } catch (rollbackErr) {
        console.error("[DB-RESET] Rollback failed:", rollbackErr);
      }
    }
    console.error("[DB-RESET] Error:", e);
    return res.status(500).json({
      ok: false,
      error: "DB reset failed",
    });
  } finally {
    if (conn) conn.release();
  }
});

module.exports = router;

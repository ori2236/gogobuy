const db = require("../config/db");
const { sendWhatsAppText } = require("../config/whatsapp");
const {
  parseShopId,
  normalizeWaNumber,
} = require("../utilities/dashboardUtils");

const ALLOWED_STATUSES = new Set([
  "pending",
  "checkout_pending",
  "confirmed",
  "preparing",
  "ready",
  "delivering",
  "completed",
  "cancel_pending",
]);

function parseStatusList(statusParam) {
  const raw = String(statusParam || "").trim();
  if (!raw) return ["confirmed", "preparing"];

  const parts = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const statuses = parts.filter((s) => ALLOWED_STATUSES.has(s));
  return statuses.length ? statuses : ["confirmed", "preparing"];
}

let HAS_PICKER_NOTE_COL = null;

async function hasOrdersPickerNoteColumn(conn) {
  if (HAS_PICKER_NOTE_COL !== null) return HAS_PICKER_NOTE_COL;
  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'orders'
      AND COLUMN_NAME = 'picker_note'
    LIMIT 1
    `,
  );
  HAS_PICKER_NOTE_COL = rows.length > 0;
  return HAS_PICKER_NOTE_COL;
}

function buildPreparingMsg(orderId) {
  return `ההזמנה שלך (#${orderId}) התחילה להילקט\nנעדכן אותך כשהיא תהיה מוכנה.`;
}

function buildReadyMsg(orderId, note) {
  let msg =
    `ההזמנה שלך (#${orderId}) מוכנה 🎉\n` + `אפשר להגיע לסניף לאסוף אותה.`;
  if (note && String(note).trim()) {
    msg += `\n\nהערה מהמלקט/ת:\n${String(note).trim()}`;
  }
  return msg;
}

function buildEmptyOrderDeletedMsg(orderId) {
  return (
    `שמנו לב שההזמנה שלך (#${orderId}) נוצרה ללא מוצרים ולכן היא בוטלה.\n` +
    `אם זו טעות – אפשר לשלוח את ההזמנה מחדש ואנחנו נטפל בזה מיד.`
  );
}

async function cleanupEmptyPickerOrders(conn, shopId, { limit = 200 } = {}) {
  await conn.beginTransaction();
  try {
    const [rows] = await conn.query(
      `
      SELECT
        o.id AS order_id,
        c.phone AS customer_phone
      FROM orders o
      JOIN customer c ON c.id = o.customer_id
      WHERE o.shop_id = ?
        AND o.status IN ('confirmed','preparing')
        AND NOT EXISTS (
          SELECT 1
          FROM order_item oi
          WHERE oi.order_id = o.id
          LIMIT 1
        )
      ORDER BY o.created_at ASC
      LIMIT ?
      FOR UPDATE
      `,
      [shopId, limit],
    );

    if (!rows.length) {
      await conn.commit();
      return [];
    }

    const ids = rows.map((r) => Number(r.order_id));
    const placeholders = ids.map(() => "?").join(",");

    await conn.query(
      `
      DELETE FROM orders
      WHERE shop_id = ?
        AND status IN ('confirmed','preparing')
        AND id IN (${placeholders})
        AND NOT EXISTS (
          SELECT 1
          FROM order_item oi
          WHERE oi.order_id = orders.id
          LIMIT 1
        )
      `,
      [shopId, ...ids],
    );

    await conn.commit();

    return rows.map((r) => ({
      orderId: Number(r.order_id),
      customerPhone: normalizeWaNumber(r.customer_phone),
    }));
  } catch (e) {
    try {
      await conn.rollback();
    } catch {}
    throw e;
  }
}

exports.getPickerOrders = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    const statuses = parseStatusList(req.query.status);
    const limit = Math.min(Number(req.query.limit || 50), 200);

    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    let deletedEmpty = [];
    try {
      deletedEmpty = await cleanupEmptyPickerOrders(conn, shopId, {
        limit: 200,
      });
    } catch (e) {
      console.error("[dashboard.cleanupEmptyPickerOrders] failed:", e);
      deletedEmpty = [];
    }

    if (deletedEmpty.length) {
      (async () => {
        for (const x of deletedEmpty) {
          if (!x.customerPhone) continue;
          const text = buildEmptyOrderDeletedMsg(x.orderId);
          try {
            await sendWhatsAppText(x.customerPhone, text);
            console.log("[dashboard.cleanupEmptyPickerOrders] WhatsApp sent", {
              orderId: x.orderId,
              to: x.customerPhone,
            });
          } catch (err) {
            console.error(
              "[dashboard.cleanupEmptyPickerOrders] WhatsApp send failed:",
              err?.response?.data || err.message,
            );
          }
        }
      })();
    }
    
    const placeholders = statuses.map(() => "?").join(",");
    const hasPickerNoteCol = await hasOrdersPickerNoteColumn(conn);
    const pickerNoteSelect = hasPickerNoteCol
      ? "o.picker_note"
      : "NULL AS picker_note";

    const [ordersRows] = await conn.query(
      `
      SELECT
        o.id,
        o.shop_id,
        o.customer_id,
        o.status,
        o.price,
        o.payment_method,
        o.delivery_address,
        o.created_at,
        o.updated_at,
        ${pickerNoteSelect},
        c.name  AS customer_name,
        c.phone AS customer_phone
      FROM orders o
      JOIN customer c ON c.id = o.customer_id
      WHERE o.shop_id = ?
        AND o.status IN (${placeholders})
      ORDER BY o.created_at DESC
      LIMIT ?
      `,
      [shopId, ...statuses, limit],
    );

    if (!ordersRows.length) {
      return res.json({ ok: true, orders: [] });
    }

    const orderIds = ordersRows.map((o) => o.id);
    const itemPlaceholders = orderIds.map(() => "?").join(",");

    const [itemsRows] = await conn.query(
      `
      SELECT
        oi.id         AS order_item_id,
        oi.order_id   AS order_id,
        oi.product_id AS product_id,
        oi.amount     AS amount,
        oi.sold_by_weight AS sold_by_weight,
        oi.requested_units AS requested_units,
        COALESCE(p.name, dp.name, CONCAT('מוצר שנמחק (#', oi.product_id, ')')) AS product_name
      FROM order_item oi
      LEFT JOIN product p
        ON p.id = oi.product_id AND p.shop_id = ?
      LEFT JOIN deleted_product dp
        ON dp.id = oi.product_id AND dp.shop_id = ?
      WHERE oi.order_id IN (${itemPlaceholders})
      ORDER BY oi.order_id ASC, oi.id ASC
      `,
      [shopId, shopId, ...orderIds],
    );

    const itemsByOrder = new Map();
    for (const r of itemsRows) {
      if (!itemsByOrder.has(r.order_id)) itemsByOrder.set(r.order_id, []);
      itemsByOrder.get(r.order_id).push({
        id: Number(r.order_item_id),
        product_id: Number(r.product_id),
        name: r.product_name,
        amount: Number(r.amount),
        sold_by_weight: Boolean(r.sold_by_weight),
        requested_units:
          r.requested_units == null ? null : Number(r.requested_units),
        unit_label: Boolean(r.sold_by_weight) ? 'ק"ג' : "יח'",
      });
    }

    const orders = ordersRows.map((o) => ({
      id: Number(o.id),
      shop_id: Number(o.shop_id),
      status: o.status,
      created_at: o.created_at,
      updated_at: o.updated_at,
      picker_note: o.picker_note ?? null,
      customer_name: o.customer_name ?? null,
      customer_phone: o.customer_phone ?? null,
      delivery_address: o.delivery_address ?? null,
      price: o.price,
      payment_method: o.payment_method,
      items: itemsByOrder.get(o.id) || [],
    }));

    return res.json({ ok: true, orders });
  } catch (err) {
    console.error("[dashboard.getPickerOrders]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  } finally {
    conn.release();
  }
};

exports.updateOrderStatus = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const orderId = Number(req.params.orderId);
    const nextStatus = String(req.body?.status || "").trim();

    if (!Number.isFinite(orderId) || orderId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid orderId" });
    }

    if (!["preparing", "ready"].includes(nextStatus)) {
      return res.status(400).json({ ok: false, message: "Invalid status" });
    }

    const hasNoteInBody = Object.prototype.hasOwnProperty.call(
      req.body || {},
      "picker_note",
    );
    const pickerNoteFromBody = hasNoteInBody
      ? String(req.body.picker_note || "").trim()
      : null;

    await conn.beginTransaction();

    const hasPickerNoteCol = await hasOrdersPickerNoteColumn(conn);
    const noteSelect = hasPickerNoteCol ? ", o.picker_note AS picker_note" : "";

    const [rows] = await conn.query(
      `
      SELECT
        o.id,
        o.shop_id,
        o.status,
        o.customer_id,
        c.phone AS customer_phone,
        c.name  AS customer_name
        ${noteSelect}
      FROM orders o
      JOIN customer c ON c.id = o.customer_id
      WHERE o.id = ?
      FOR UPDATE
      `,
      [orderId],
    );

    if (!rows.length) {
      await conn.rollback();
      return res.status(404).json({ ok: false, message: "Order not found" });
    }

    const current = rows[0].status;
    const customerPhone = normalizeWaNumber(rows[0].customer_phone);
    const existingNote = hasPickerNoteCol ? rows[0].picker_note : null;

    // rules:
    // confirmed -> preparing
    // preparing -> ready
    // idempotent allowed
    if (nextStatus === "preparing") {
      if (current !== "confirmed" && current !== "preparing") {
        await conn.rollback();
        return res.status(409).json({
          ok: false,
          message: `Cannot move from ${current} to preparing`,
        });
      }
    }

    if (nextStatus === "ready") {
      if (current !== "preparing" && current !== "ready") {
        await conn.rollback();
        return res.status(409).json({
          ok: false,
          message: `Cannot move from ${current} to ready`,
        });
      }
    }

    const statusChanged = current !== nextStatus;

    const sets = [];
    const params = [];

    if (statusChanged) {
      sets.push("status = ?");
      params.push(nextStatus);
    }

    if (hasPickerNoteCol && hasNoteInBody) {
      sets.push("picker_note = ?");
      params.push(pickerNoteFromBody ? pickerNoteFromBody : null);
    }

    if (sets.length) {
      params.push(orderId);
      await conn.query(
        `UPDATE orders SET ${sets.join(", ")} WHERE id = ?`,
        params,
      );
    }

    await conn.commit();

    res.json({ ok: true, order: { id: orderId, status: nextStatus } });

    if (statusChanged && customerPhone) {
      const noteToSend =
        nextStatus === "ready"
          ? hasNoteInBody
            ? pickerNoteFromBody
            : existingNote
          : null;

      const text =
        nextStatus === "preparing"
          ? buildPreparingMsg(orderId)
          : buildReadyMsg(orderId, noteToSend);

      (async () => {
        try {
          await sendWhatsAppText(customerPhone, text);
          console.log("[dashboard.updateOrderStatus] WhatsApp sent", {
            orderId,
            nextStatus,
            to: customerPhone,
          });
        } catch (err) {
          console.error(
            "[dashboard.updateOrderStatus] WhatsApp send failed:",
            err?.response?.data || err.message,
          );
        }
      })();
    } else if (statusChanged && !customerPhone) {
      console.warn("[dashboard.updateOrderStatus] Missing customer phone", {
        orderId,
      });
    }

    return;
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    console.error("[dashboard.updateOrderStatus]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  } finally {
    conn.release();
  }
};

const db = require("../config/db");
const { sendWhatsAppText } = require("../utilities/whatsapp");
const {
  parseShopId,
  normalizeWaNumber,
} = require("../utilities/dashboardUtils");
const { ensureFulfillmentSchema } = require("../services/fulfillment");
const {
  ensureCartPromotionSchema,
  formatCartPromotionApplication,
} = require("../services/cartPromotions");
const { isEnglishFromCustomerName } = require("../utilities/i18n");

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

const ORDER_COLUMN_CACHE = new Map();

async function hasOrdersColumn(conn, columnName) {
  const key = String(columnName || "").trim();
  if (!key) return false;
  if (ORDER_COLUMN_CACHE.has(key)) return ORDER_COLUMN_CACHE.get(key);

  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'orders'
      AND COLUMN_NAME = ?
    LIMIT 1
    `,
    [key],
  );

  const exists = rows.length > 0;
  ORDER_COLUMN_CACHE.set(key, exists);
  return exists;
}

async function hasOrdersPickerNoteColumn(conn) {
  return hasOrdersColumn(conn, "picker_note");
}

async function hasOrdersCustomerNoteToPickerColumn(conn) {
  return hasOrdersColumn(conn, "customer_note_to_picker");
}


const ORDER_ITEM_COLUMN_CACHE = new Map();

async function hasOrderItemColumn(conn, columnName) {
  const key = String(columnName || "").trim();
  if (!key) return false;
  if (ORDER_ITEM_COLUMN_CACHE.has(key)) return ORDER_ITEM_COLUMN_CACHE.get(key);

  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'order_item'
      AND COLUMN_NAME = ?
    LIMIT 1
    `,
    [key],
  );

  const exists = rows.length > 0;
  ORDER_ITEM_COLUMN_CACHE.set(key, exists);
  return exists;
}

async function ensureOrderItemPickerColumns(conn) {
  if (!(await hasOrderItemColumn(conn, "supplied_amount"))) {
    await conn.query(
      "ALTER TABLE order_item ADD COLUMN supplied_amount DECIMAL(10,3) DEFAULT NULL AFTER requested_units",
    );
    ORDER_ITEM_COLUMN_CACHE.set("supplied_amount", true);
  }

  if (!(await hasOrderItemColumn(conn, "picker_note"))) {
    await conn.query(
      "ALTER TABLE order_item ADD COLUMN picker_note TEXT DEFAULT NULL AFTER supplied_amount",
    );
    ORDER_ITEM_COLUMN_CACHE.set("picker_note", true);
  }
}

function buildNoteBlock(note, isEnglish) {
  const cleanNote = String(note || "").trim();
  if (!cleanNote) return "";
  return isEnglish
    ? `

📝 Picker note:
${cleanNote}`
    : `

📝 הערה מהמלקט/ת:
${cleanNote}`;
}

function parsePackagingCount(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(999, Math.floor(n)));
}

function formatHebrewPackagePart(count, singular, plural) {
  const n = parsePackagingCount(count);
  if (!n) return "";
  return `${n} ${n === 1 ? singular : plural}`;
}

function formatEnglishPackagePart(count, singular, plural) {
  const n = parsePackagingCount(count);
  if (!n) return "";
  return `${n} ${n === 1 ? singular : plural}`;
}

function buildPackagingBlock(packaging = {}, isEnglish = false) {
  const bags = parsePackagingCount(packaging.bags ?? packaging.packaging_bags_count);
  const cartons = parsePackagingCount(packaging.cartons ?? packaging.packaging_cartons_count);
  if (!bags && !cartons) return "";

  const parts = isEnglish
    ? [
        formatEnglishPackagePart(cartons, "carton", "cartons"),
        formatEnglishPackagePart(bags, "bag", "bags"),
      ].filter(Boolean)
    : [
        formatHebrewPackagePart(cartons, "קרטון", "קרטונים"),
        formatHebrewPackagePart(bags, "שקית", "שקיות"),
      ].filter(Boolean);

  const text = parts.join(isEnglish ? " and " : " ו-");
  return isEnglish
    ? `

📦 Packaging: ${text}`
    : `

📦 אריזה: ${text}`;
}

function buildPreparingMsg(orderId, isEnglish = false) {
  return isEnglish
    ? `🛒 Your order (#${orderId}) is now being picked.
We’ll update you when it’s ready ✅`
    : `🛒 ההזמנה שלך (#${orderId}) התחילה להילקט.
נעדכן אותך כשהיא תהיה מוכנה ✅`;
}

function buildReadyMsg(orderId, note, fulfillmentMethod, isEnglish = false, packaging = {}) {
  const isDelivery = String(fulfillmentMethod || "") === "delivery";
  let msg;

  if (isEnglish) {
    msg = isDelivery
      ? `🎉 Your order (#${orderId}) is ready.
We’re handing it over to the courier and will update you when it leaves.`
      : `🎉 Your order (#${orderId}) is ready.
You can come to the branch to pick it up 🛍️`;
  } else {
    msg = isDelivery
      ? `🎉 ההזמנה שלך (#${orderId}) מוכנה.
אנחנו מעבירים אותה לשליח, ונעדכן אותך כשהיא יוצאת לדרך.`
      : `🎉 ההזמנה שלך (#${orderId}) מוכנה.
אפשר להגיע לסניף לאסוף אותה 🛍️`;
  }

  return msg + buildPackagingBlock(packaging, isEnglish) + buildNoteBlock(note, isEnglish);
}

function buildDeliveringMsg(orderId, note, isEnglish = false) {
  const msg = isEnglish
    ? `🏍️ Your order (#${orderId}) is out for delivery.
The courier is on the way to you.`
    : `🏍️ ההזמנה שלך (#${orderId}) יצאה למשלוח.
השליח בדרך אליך.`;
  return msg + buildNoteBlock(note, isEnglish);
}

function buildCompletedMsg(orderId, fulfillmentMethod, isEnglish = false) {
  const isDelivery = String(fulfillmentMethod || "") === "delivery";

  if (isEnglish) {
    return isDelivery
      ? `✅ Thank you for ordering from us 💚
Your order (#${orderId}) was delivered successfully.
We’ll be happy to see you again!`
      : `✅ Thank you for ordering from us 💚
Your order (#${orderId}) was picked up successfully.
We’ll be happy to see you again!`;
  }

  return isDelivery
    ? `✅ תודה שהזמנת אצלנו 💚
ההזמנה שלך (#${orderId}) נמסרה בהצלחה.
נשמח לראות אותך שוב!`
    : `✅ תודה שהזמנת אצלנו 💚
ההזמנה שלך (#${orderId}) נאספה בהצלחה.
נשמח לראות אותך שוב!`;
}

function buildEmptyOrderDeletedMsg(orderId, isEnglish = false) {
  return isEnglish
    ? `ℹ️ We noticed that your order (#${orderId}) was created without products, so it was cancelled.
If this was a mistake, you can send the order again and we’ll handle it right away 🙏`
    : `ℹ️ שמנו לב שההזמנה שלך (#${orderId}) נוצרה ללא מוצרים ולכן היא בוטלה.
אם זו טעות - אפשר לשלוח את ההזמנה מחדש ואנחנו נטפל בזה מיד 🙏`;
}

async function getShopWhatsAppPhoneNumberId(shopId) {
  const [[row]] = await db.query(
    `SELECT phone_number_id
       FROM shop_whatsapp_phone
      WHERE shop_id = ? AND is_active = 1
      ORDER BY id ASC
      LIMIT 1`,
    [shopId],
  );
  return row?.phone_number_id || null;
}

async function cleanupEmptyPickerOrders(conn, shopId, { limit = 200 } = {}) {
  await conn.beginTransaction();
  try {
    const [rows] = await conn.query(
      `
      SELECT
        o.id AS order_id,
        c.phone AS customer_phone,
        c.name AS customer_name
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
      customerName: r.customer_name,
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

    await ensureFulfillmentSchema(conn);

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
          const text = buildEmptyOrderDeletedMsg(
            x.orderId,
            isEnglishFromCustomerName(x.customerName),
          );
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
    const hasCustomerNoteToPickerCol =
      await hasOrdersCustomerNoteToPickerColumn(conn);
    const pickerNoteSelect = hasPickerNoteCol
      ? "o.picker_note"
      : "NULL AS picker_note";
    const customerNoteToPickerSelect = hasCustomerNoteToPickerCol
      ? "o.customer_note_to_picker"
      : "NULL AS customer_note_to_picker";

    await ensureOrderItemPickerColumns(conn);
    await ensureCartPromotionSchema(conn);

    const [ordersRows] = await conn.query(
      `
      SELECT
        o.id,
        o.shop_id,
        o.customer_id,
        o.status,
        o.price,
        o.payment_method,
        o.fulfillment_method,
        o.delivery_address,
        o.delivery_fee,
        o.delivery_notes,
        o.packaging_bags_count,
        o.packaging_cartons_count,
        o.created_at,
        o.updated_at,
        ${pickerNoteSelect},
        ${customerNoteToPickerSelect},
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
        oi.supplied_amount AS supplied_amount,
        oi.picker_note AS item_picker_note,
        oi.price AS line_price,
        CASE
          WHEN COALESCE(oi.is_gift, 0) = 1 THEN 1
          WHEN oi.cart_promotion_rule_id IS NOT NULL AND COALESCE(oi.price, 0) = 0 THEN 1
          ELSE 0
        END AS is_gift,
        oi.cart_promotion_rule_id AS cart_promotion_rule_id,
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
        supplied_amount:
          r.supplied_amount == null ? null : Number(r.supplied_amount),
        picker_note: r.item_picker_note ?? null,
        line_price: r.line_price == null ? null : Number(r.line_price),
        is_gift: Boolean(r.is_gift),
        cart_promotion_rule_id:
          r.cart_promotion_rule_id == null ? null : Number(r.cart_promotion_rule_id),
        unit_label: Boolean(r.sold_by_weight) ? 'ק"ג' : "יח'",
      });
    }

    const [applicationRows] = await conn.query(
      `
      SELECT
        opa.*,
        cpr.reward_product_id,
        p.name AS reward_product_name,
        p.display_name_en AS reward_display_name_en
      FROM order_promotion_application opa
      LEFT JOIN cart_promotion_rule cpr ON cpr.id = opa.cart_promotion_rule_id
      LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = opa.shop_id
      WHERE opa.order_id IN (${itemPlaceholders})
        AND opa.shop_id = ?
      ORDER BY opa.order_id ASC, opa.id ASC
      `,
      [...orderIds, shopId],
    );

    const applicationsByOrder = new Map();
    for (const row of applicationRows || []) {
      const orderId = Number(row.order_id);
      if (!applicationsByOrder.has(orderId)) applicationsByOrder.set(orderId, []);
      applicationsByOrder.get(orderId).push({
        id: Number(row.id),
        order_id: orderId,
        cart_promotion_rule_id: Number(row.cart_promotion_rule_id),
        rule_type: row.rule_type,
        title: row.title,
        discount_amount: row.discount_amount == null ? 0 : Number(row.discount_amount),
        applied_value: row.applied_value == null ? null : Number(row.applied_value),
        metadata: row.metadata || null,
        reward_product_id:
          row.reward_product_id == null ? null : Number(row.reward_product_id),
        reward_product_name: row.reward_product_name ?? null,
        reward_display_name_en: row.reward_display_name_en ?? null,
        text_he: formatCartPromotionApplication(row, false),
      });
    }

    const orders = ordersRows.map((o) => ({
      id: Number(o.id),
      shop_id: Number(o.shop_id),
      status: o.status,
      created_at: o.created_at,
      updated_at: o.updated_at,
      picker_note: o.picker_note ?? null,
      customer_note_to_picker: o.customer_note_to_picker ?? null,
      customer_name: o.customer_name ?? null,
      customer_phone: o.customer_phone ?? null,
      fulfillment_method: o.fulfillment_method ?? null,
      delivery_address: o.delivery_address ?? null,
      delivery_fee: o.delivery_fee == null ? 0 : Number(o.delivery_fee),
      delivery_notes: o.delivery_notes ?? null,
      packaging_bags_count: parsePackagingCount(o.packaging_bags_count),
      packaging_cartons_count: parsePackagingCount(o.packaging_cartons_count),
      price: o.price == null ? null : Number(o.price),
      payment_method: o.payment_method,
      cart_promotion_applications: applicationsByOrder.get(Number(o.id)) || [],
      cart_promotion_lines: (applicationsByOrder.get(Number(o.id)) || [])
        .map((app) => app.text_he)
        .filter(Boolean),
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


function normalizeSuppliedAmount(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) {
    const err = new Error("Invalid supplied_amount");
    err.status = 400;
    throw err;
  }
  return Math.round(n * 1000) / 1000;
}

function cleanItemPickerNote(value) {
  const s = String(value ?? "").trim();
  return s ? s.slice(0, 1000) : null;
}

exports.updateOrderItemPickerDetails = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    const orderId = Number(req.params.orderId);
    const itemId = Number(req.params.itemId);

    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isFinite(orderId) || orderId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid orderId" });
    }
    if (!Number.isFinite(itemId) || itemId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid itemId" });
    }

    const suppliedAmount = normalizeSuppliedAmount(req.body?.supplied_amount);
    const itemPickerNote = cleanItemPickerNote(req.body?.picker_note);

    await ensureOrderItemPickerColumns(conn);
    await conn.beginTransaction();

    const [[row]] = await conn.query(
      `
      SELECT
        oi.id,
        oi.order_id,
        oi.amount,
        o.status,
        o.shop_id
      FROM order_item oi
      JOIN orders o ON o.id = oi.order_id
      WHERE oi.id = ?
        AND oi.order_id = ?
        AND o.shop_id = ?
      FOR UPDATE
      `,
      [itemId, orderId, shopId],
    );

    if (!row) {
      await conn.rollback();
      return res.status(404).json({ ok: false, message: "Order item not found" });
    }

    if (row.status !== "preparing") {
      await conn.rollback();
      return res.status(409).json({
        ok: false,
        message: "אפשר לעדכן כמות שסופקה והערות מוצר רק כשההזמנה בסטטוס בליקוט",
      });
    }

    const amount = Number(row.amount);
    const storedSuppliedAmount =
      suppliedAmount === null || Math.abs(suppliedAmount - amount) < 0.0005
        ? null
        : suppliedAmount;

    await conn.query(
      `
      UPDATE order_item
      SET supplied_amount = ?, picker_note = ?
      WHERE id = ?
      `,
      [storedSuppliedAmount, itemPickerNote, itemId],
    );

    await conn.commit();

    return res.json({
      ok: true,
      item: {
        id: itemId,
        order_id: orderId,
        supplied_amount: storedSuppliedAmount,
        picker_note: itemPickerNote,
      },
    });
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    console.error("[dashboard.updateOrderItemPickerDetails]", err);
    return res.status(err.status || 500).json({
      ok: false,
      message: err.status ? err.message : "Server error",
    });
  } finally {
    conn.release();
  }
};

exports.updateOrderStatus = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    const orderId = Number(req.params.orderId);
    const nextStatus = String(req.body?.status || "").trim();

    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    if (!Number.isFinite(orderId) || orderId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid orderId" });
    }

    await ensureFulfillmentSchema(conn);

    if (!["preparing", "ready", "delivering", "completed"].includes(nextStatus)) {
      return res.status(400).json({ ok: false, message: "Invalid status" });
    }

    const hasNoteInBody = Object.prototype.hasOwnProperty.call(
      req.body || {},
      "picker_note",
    );
    const pickerNoteFromBody = hasNoteInBody
      ? String(req.body.picker_note || "").trim()
      : null;

    const hasPackagingBagsInBody =
      Object.prototype.hasOwnProperty.call(req.body || {}, "packaging_bags_count") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "packagingBagsCount");
    const hasPackagingCartonsInBody =
      Object.prototype.hasOwnProperty.call(req.body || {}, "packaging_cartons_count") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "packagingCartonsCount");
    const packagingBagsFromBody = hasPackagingBagsInBody
      ? parsePackagingCount(req.body.packaging_bags_count ?? req.body.packagingBagsCount)
      : null;
    const packagingCartonsFromBody = hasPackagingCartonsInBody
      ? parsePackagingCount(req.body.packaging_cartons_count ?? req.body.packagingCartonsCount)
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
        o.fulfillment_method,
        o.delivery_address,
        o.delivery_fee,
        o.packaging_bags_count,
        o.packaging_cartons_count,
        c.phone AS customer_phone,
        c.name  AS customer_name
        ${noteSelect}
      FROM orders o
      JOIN customer c ON c.id = o.customer_id
      WHERE o.id = ?
        AND o.shop_id = ?
      FOR UPDATE
      `,
      [orderId, shopId],
    );

    if (!rows.length) {
      await conn.rollback();
      return res.status(404).json({ ok: false, message: "Order not found" });
    }

    const current = rows[0].status;
    const currentOrder = rows[0];
    const isEnglishCustomer = isEnglishFromCustomerName(currentOrder.customer_name);
    const fulfillmentMethod = String(currentOrder.fulfillment_method || "pickup");
    const isDeliveryOrder = fulfillmentMethod === "delivery";
    const customerPhone = normalizeWaNumber(rows[0].customer_phone);
    const existingNote = hasPickerNoteCol ? rows[0].picker_note : null;
    const currentPackagingBags = parsePackagingCount(currentOrder.packaging_bags_count);
    const currentPackagingCartons = parsePackagingCount(currentOrder.packaging_cartons_count);
    const packagingBagsToSave = hasPackagingBagsInBody ? packagingBagsFromBody : currentPackagingBags;
    const packagingCartonsToSave = hasPackagingCartonsInBody ? packagingCartonsFromBody : currentPackagingCartons;

    // rules:
    // confirmed -> preparing
    // preparing -> ready
    // delivery: ready -> delivering -> completed
    // pickup: ready -> completed
    // idempotent allowed
    if (nextStatus === "preparing") {
      if (current !== "confirmed" && current !== "preparing") {
        await conn.rollback();
        return res.status(409).json({
          ok: false,
          message:
            current === "cancel_pending"
              ? "הלקוח שוקל בדיוק לבטל את ההזמנה"
              : `Cannot move from ${current} to preparing`,
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

    if (nextStatus === "delivering") {
      if (!isDeliveryOrder) {
        await conn.rollback();
        return res.status(409).json({ ok: false, message: "Only delivery orders can be marked as sent" });
      }
      if (current !== "ready" && current !== "delivering") {
        await conn.rollback();
        return res.status(409).json({
          ok: false,
          message: `Cannot move from ${current} to delivering`,
        });
      }
    }

    if (nextStatus === "completed") {
      const allowedCurrent = isDeliveryOrder ? ["delivering", "completed"] : ["ready", "completed"];
      if (!allowedCurrent.includes(current)) {
        await conn.rollback();
        return res.status(409).json({
          ok: false,
          message: `Cannot move from ${current} to completed`,
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

    if (nextStatus === "ready" && hasPackagingBagsInBody) {
      sets.push("packaging_bags_count = ?");
      params.push(packagingBagsToSave);
    }

    if (nextStatus === "ready" && hasPackagingCartonsInBody) {
      sets.push("packaging_cartons_count = ?");
      params.push(packagingCartonsToSave);
    }

    if (sets.length) {
      params.push(orderId);
      await conn.query(
        `UPDATE orders SET ${sets.join(", ")} WHERE id = ?`,
        params,
      );
    }

    await conn.commit();

    res.json({
      ok: true,
      order: {
        id: orderId,
        status: nextStatus,
        fulfillment_method: fulfillmentMethod,
        packaging_bags_count: packagingBagsToSave,
        packaging_cartons_count: packagingCartonsToSave,
      },
    });

    if (statusChanged && customerPhone) {
      const noteToSend =
        nextStatus === "ready" || nextStatus === "delivering"
          ? hasNoteInBody
            ? pickerNoteFromBody
            : existingNote
          : null;

      const text =
        nextStatus === "preparing"
          ? buildPreparingMsg(orderId, isEnglishCustomer)
          : nextStatus === "delivering"
            ? buildDeliveringMsg(orderId, noteToSend, isEnglishCustomer)
            : nextStatus === "completed"
              ? buildCompletedMsg(orderId, fulfillmentMethod, isEnglishCustomer)
              : buildReadyMsg(orderId, noteToSend, fulfillmentMethod, isEnglishCustomer, {
                  bags: packagingBagsToSave,
                  cartons: packagingCartonsToSave,
                });

      (async () => {
        try {
          const phoneNumberId = await getShopWhatsAppPhoneNumberId(shopId);
          await sendWhatsAppText(customerPhone, text, phoneNumberId);
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

const db = require("../config/db");
const { sendWhatsAppTemplate } = require("../utilities/whatsapp");

const DEFAULT_TEMPLATE_NAME = "new_order_staff_alert_v3";
const DEFAULT_TEMPLATE_LANG = "he";

function getStaffAlertConfig() {
  return {
    phoneNumberId: String(
      process.env.STAFF_ALERT_WHATSAPP_PHONE_ID || process.env.WHATSAPP_PHONE_ID || "",
    ).trim(),
    templateName: String(
      process.env.STAFF_NEW_ORDER_TEMPLATE || DEFAULT_TEMPLATE_NAME,
    ).trim(),
    languageCode: String(
      process.env.STAFF_NEW_ORDER_TEMPLATE_LANG || DEFAULT_TEMPLATE_LANG,
    ).trim(),
    enabled:
      String(process.env.STAFF_ORDER_ALERTS_ENABLED || "true").toLowerCase() !==
      "false",
  };
}

function normalizeStaffPhone(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (!digits) return "";

  if (digits.startsWith("972")) return digits;
  if (digits.startsWith("0")) return `972${digits.slice(1)}`;
  return digits;
}

function isValidStaffPhone(phone) {
  const normalized = normalizeStaffPhone(phone);
  return /^\d{8,15}$/.test(normalized);
}

function cleanText(value, max = 255) {
  const s = String(value ?? "").trim().replace(/\s+/g, " ");
  return s ? s.slice(0, max) : null;
}

function boolToDb(value, fallback = true) {
  if (value === undefined || value === null || value === "") return fallback ? 1 : 0;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number") return value ? 1 : 0;
  return ["1", "true", "yes", "on", "כן", "פעיל"].includes(
    String(value).trim().toLowerCase(),
  )
    ? 1
    : 0;
}

async function ensureStaffOrderAlertsSchema(conn = db) {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS shop_staff_whatsapp_recipient (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      shop_id INT UNSIGNED NOT NULL,
      name VARCHAR(120) DEFAULT NULL,
      phone VARCHAR(32) NOT NULL,
      is_active TINYINT(1) NOT NULL DEFAULT 1,
      notify_new_orders TINYINT(1) NOT NULL DEFAULT 1,
      created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
      PRIMARY KEY (id),
      UNIQUE KEY uq_shop_staff_phone (shop_id, phone),
      KEY idx_shop_staff_active (shop_id, is_active, notify_new_orders),
      CONSTRAINT fk_shop_staff_whatsapp_recipient_shop
        FOREIGN KEY (shop_id) REFERENCES shop(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS order_staff_whatsapp_notification (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      order_id BIGINT UNSIGNED NOT NULL,
      shop_id INT UNSIGNED NOT NULL,
      recipient_id BIGINT UNSIGNED DEFAULT NULL,
      phone VARCHAR(32) NOT NULL,
      template_name VARCHAR(128) NOT NULL,
      status ENUM('sent','failed','skipped') NOT NULL,
      whatsapp_message_id VARCHAR(255) DEFAULT NULL,
      error_message TEXT DEFAULT NULL,
      created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
      PRIMARY KEY (id),
      UNIQUE KEY uq_order_staff_template_phone (order_id, phone, template_name),
      KEY idx_order_staff_notification_shop (shop_id, created_at),
      KEY idx_order_staff_notification_order (order_id),
      KEY idx_order_staff_notification_recipient (recipient_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  `);
}

function normalizeRecipientRow(row) {
  return {
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    name: row.name || "",
    phone: row.phone || "",
    is_active: Boolean(row.is_active),
    notify_new_orders: Boolean(row.notify_new_orders),
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
  };
}

async function listStaffRecipients(shopId, conn = db) {
  await ensureStaffOrderAlertsSchema(conn);
  const [rows] = await conn.query(
    `
    SELECT id, shop_id, name, phone, is_active, notify_new_orders, created_at, updated_at
    FROM shop_staff_whatsapp_recipient
    WHERE shop_id = ?
    ORDER BY is_active DESC, notify_new_orders DESC, id DESC
    `,
    [shopId],
  );
  return rows.map(normalizeRecipientRow);
}

async function createStaffRecipient(shopId, payload, conn = db) {
  await ensureStaffOrderAlertsSchema(conn);
  const phone = normalizeStaffPhone(payload?.phone);
  if (!isValidStaffPhone(phone)) {
    const err = new Error("מספר WhatsApp לא תקין. יש להזין מספר בפורמט ישראלי או בינלאומי.");
    err.status = 400;
    throw err;
  }

  const name = cleanText(payload?.name, 120);
  const isActive = boolToDb(payload?.is_active, true);
  const notifyNewOrders = boolToDb(payload?.notify_new_orders, true);

  try {
    const [result] = await conn.query(
      `
      INSERT INTO shop_staff_whatsapp_recipient
        (shop_id, name, phone, is_active, notify_new_orders)
      VALUES (?, ?, ?, ?, ?)
      `,
      [shopId, name, phone, isActive, notifyNewOrders],
    );
    const [rows] = await conn.query(
      `SELECT id, shop_id, name, phone, is_active, notify_new_orders, created_at, updated_at
       FROM shop_staff_whatsapp_recipient
       WHERE id = ? AND shop_id = ?
       LIMIT 1`,
      [result.insertId, shopId],
    );
    return rows[0] ? normalizeRecipientRow(rows[0]) : null;
  } catch (err) {
    if (err?.code === "ER_DUP_ENTRY") {
      const e = new Error("המספר כבר מוגדר לקבלת התראות בחנות הזו.");
      e.status = 409;
      throw e;
    }
    throw err;
  }
}

async function updateStaffRecipient(shopId, recipientId, payload, conn = db) {
  await ensureStaffOrderAlertsSchema(conn);
  const updates = [];
  const params = [];

  if (Object.prototype.hasOwnProperty.call(payload || {}, "name")) {
    updates.push("name = ?");
    params.push(cleanText(payload.name, 120));
  }

  if (Object.prototype.hasOwnProperty.call(payload || {}, "phone")) {
    const phone = normalizeStaffPhone(payload.phone);
    if (!isValidStaffPhone(phone)) {
      const err = new Error("מספר WhatsApp לא תקין. יש להזין מספר בפורמט ישראלי או בינלאומי.");
      err.status = 400;
      throw err;
    }
    updates.push("phone = ?");
    params.push(phone);
  }

  if (Object.prototype.hasOwnProperty.call(payload || {}, "is_active")) {
    updates.push("is_active = ?");
    params.push(boolToDb(payload.is_active, true));
  }

  if (Object.prototype.hasOwnProperty.call(payload || {}, "notify_new_orders")) {
    updates.push("notify_new_orders = ?");
    params.push(boolToDb(payload.notify_new_orders, true));
  }

  if (!updates.length) {
    const err = new Error("לא נשלחו שדות לעדכון.");
    err.status = 400;
    throw err;
  }

  params.push(recipientId, shopId);

  try {
    const [result] = await conn.query(
      `
      UPDATE shop_staff_whatsapp_recipient
      SET ${updates.join(", ")}
      WHERE id = ? AND shop_id = ?
      `,
      params,
    );

    if (!result.affectedRows) {
      const err = new Error("המספר לא נמצא.");
      err.status = 404;
      throw err;
    }

    const [rows] = await conn.query(
      `SELECT id, shop_id, name, phone, is_active, notify_new_orders, created_at, updated_at
       FROM shop_staff_whatsapp_recipient
       WHERE id = ? AND shop_id = ?
       LIMIT 1`,
      [recipientId, shopId],
    );
    return rows[0] ? normalizeRecipientRow(rows[0]) : null;
  } catch (err) {
    if (err?.code === "ER_DUP_ENTRY") {
      const e = new Error("המספר כבר מוגדר לקבלת התראות בחנות הזו.");
      e.status = 409;
      throw e;
    }
    throw err;
  }
}

async function deleteStaffRecipient(shopId, recipientId, conn = db) {
  await ensureStaffOrderAlertsSchema(conn);
  const [result] = await conn.query(
    `DELETE FROM shop_staff_whatsapp_recipient WHERE id = ? AND shop_id = ?`,
    [recipientId, shopId],
  );
  return result.affectedRows > 0;
}

async function getOrderAlertData(orderId, shopId, conn = db) {
  const [rows] = await conn.query(
    `
    SELECT
      o.id AS order_id,
      o.price AS total_amount,
      COALESCE(NULLIF(c.name, ''), 'לקוח') AS customer_name,
      COALESCE(NULLIF(s.name, ''), CONCAT('סניף ', s.id)) AS branch_name
    FROM orders o
    JOIN customer c ON c.id = o.customer_id
    JOIN shop s ON s.id = o.shop_id
    WHERE o.id = ?
      AND o.shop_id = ?
    LIMIT 1
    `,
    [orderId, shopId],
  );

  return rows[0] || null;
}

async function getShopStaffAlertPhoneNumberId(shopId, conn = db) {
  const [[row]] = await conn.query(
    `
    SELECT phone_number_id
    FROM shop_whatsapp_phone
    WHERE shop_id = ?
      AND is_active = 1
    ORDER BY id ASC
    LIMIT 1
    `,
    [shopId],
  );

  return row?.phone_number_id ? String(row.phone_number_id).trim() : null;
}

function buildBodyParams(order) {
  return [
    {
      type: "text",
      parameter_name: "order_id",
      text: String(order.order_id),
    },
    {
      type: "text",
      parameter_name: "branch_name",
      text: String(order.branch_name || ""),
    },
    {
      type: "text",
      parameter_name: "customer_name",
      text: String(order.customer_name || "לקוח"),
    },
    {
      type: "text",
      parameter_name: "total_amount",
      text: Number(order.total_amount || 0).toFixed(2),
    },
  ];
}

async function getExistingNotification(orderId, phone, templateName, conn = db) {
  const [rows] = await conn.query(
    `
    SELECT id, status, whatsapp_message_id
    FROM order_staff_whatsapp_notification
    WHERE order_id = ? AND phone = ? AND template_name = ?
    LIMIT 1
    `,
    [orderId, phone, templateName],
  );
  return rows[0] || null;
}

async function saveNotificationLog({
  orderId,
  shopId,
  recipientId,
  phone,
  templateName,
  status,
  whatsappMessageId = null,
  errorMessage = null,
}, conn = db) {
  await conn.query(
    `
    INSERT INTO order_staff_whatsapp_notification
      (order_id, shop_id, recipient_id, phone, template_name, status, whatsapp_message_id, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      shop_id = VALUES(shop_id),
      recipient_id = VALUES(recipient_id),
      status = VALUES(status),
      whatsapp_message_id = VALUES(whatsapp_message_id),
      error_message = VALUES(error_message),
      updated_at = CURRENT_TIMESTAMP(6)
    `,
    [
      orderId,
      shopId,
      recipientId || null,
      phone,
      templateName,
      status,
      whatsappMessageId,
      errorMessage ? String(errorMessage).slice(0, 2000) : null,
    ],
  );
}

async function sendNewOrderTemplateToRecipient({
  order,
  shopId,
  recipient,
  allowDuplicate = false,
  phoneNumberId = null,
}) {
  const config = getStaffAlertConfig();
  if (!config.enabled) {
    return { status: "skipped", reason: "disabled" };
  }

  const alertPhoneNumberId = String(
    phoneNumberId || config.phoneNumberId || "",
  ).trim();

  if (!alertPhoneNumberId) {
    throw new Error("Missing shop WhatsApp phone_number_id for staff alert");
  }

  const to = normalizeStaffPhone(recipient.phone);
  if (!to) {
    return { status: "skipped", reason: "invalid_phone" };
  }

  await ensureStaffOrderAlertsSchema(db);

  if (!allowDuplicate) {
    const existing = await getExistingNotification(order.order_id, to, config.templateName);
    if (existing?.status === "sent") {
      return { status: "skipped", reason: "already_sent" };
    }
  }

  try {
    const data = await sendWhatsAppTemplate(
      to,
      config.templateName,
      config.languageCode,
      buildBodyParams(order),
      alertPhoneNumberId,
    );

    const whatsappMessageId = data?.messages?.[0]?.id || null;

    await saveNotificationLog({
      orderId: order.order_id,
      shopId,
      recipientId: recipient.id,
      phone: to,
      templateName: config.templateName,
      status: "sent",
      whatsappMessageId,
      errorMessage: null,
    });

    return { status: "sent", whatsappMessageId, data };
  } catch (err) {
    const apiError = err?.response?.data || err.message || err;
    await saveNotificationLog({
      orderId: order.order_id,
      shopId,
      recipientId: recipient.id,
      phone: to,
      templateName: config.templateName,
      status: "failed",
      errorMessage: typeof apiError === "string" ? apiError : JSON.stringify(apiError),
    });
    throw err;
  }
}

async function notifyStaffNewConfirmedOrder({ orderId, shopId }) {
  const config = getStaffAlertConfig();
  if (!config.enabled) {
    console.log("[staffOrderAlerts] disabled by STAFF_ORDER_ALERTS_ENABLED=false");
    return { sent: 0, failed: 0, skipped: 0 };
  }

  await ensureStaffOrderAlertsSchema(db);

  const phoneNumberId = await getShopStaffAlertPhoneNumberId(shopId);

  if (!phoneNumberId) {
    console.warn("[staffOrderAlerts] missing active shop WhatsApp phone_number_id", { shopId });
    return { sent: 0, failed: 0, skipped: 0 };
  }

  const order = await getOrderAlertData(orderId, shopId);
  if (!order) {
    console.warn("[staffOrderAlerts] order not found", { orderId, shopId });
    return { sent: 0, failed: 0, skipped: 0 };
  }

  const [recipients] = await db.query(
    `
    SELECT id, shop_id, name, phone, is_active, notify_new_orders
    FROM shop_staff_whatsapp_recipient
    WHERE shop_id = ?
      AND is_active = 1
      AND notify_new_orders = 1
    ORDER BY id ASC
    `,
    [shopId],
  );

  const summary = { sent: 0, failed: 0, skipped: 0 };

  for (const recipient of recipients) {
    try {
      const result = await sendNewOrderTemplateToRecipient({
        order,
        shopId,
        recipient,
        phoneNumberId,
      });

      if (result.status === "sent") summary.sent += 1;
      else summary.skipped += 1;
    } catch (err) {
      summary.failed += 1;
      console.error("[staffOrderAlerts] failed to send", {
        orderId,
        shopId,
        recipientId: recipient.id,
        phone: recipient.phone,
        error: err?.response?.data || err.message,
      });
    }
  }

  console.log("[staffOrderAlerts] completed", { orderId, shopId, ...summary });
  return summary;
}

async function sendStaffRecipientTestAlert({ shopId, recipientId }) {
  await ensureStaffOrderAlertsSchema(db);

  const [rows] = await db.query(
    `
    SELECT id, shop_id, name, phone, is_active, notify_new_orders
    FROM shop_staff_whatsapp_recipient
    WHERE id = ? AND shop_id = ?
    LIMIT 1
    `,
    [recipientId, shopId],
  );

  const recipient = rows[0];
  if (!recipient) {
    const err = new Error("המספר לא נמצא.");
    err.status = 404;
    throw err;
  }

  const [[shop]] = await db.query(
    `SELECT id, name FROM shop WHERE id = ? LIMIT 1`,
    [shopId],
  );

  const phoneNumberId = await getShopStaffAlertPhoneNumberId(shopId);
  if (!phoneNumberId) {
    const err = new Error("לא מוגדר מספר WhatsApp פעיל לסניף הזה.");
    err.status = 400;
    throw err;
  }

  const order = {
    order_id: "999",
    branch_name: shop?.name || "סניף בדיקה",
    customer_name: "בדיקה",
    total_amount: 0,
  };

  return await sendNewOrderTemplateToRecipient({
    order,
    shopId,
    recipient,
    allowDuplicate: true,
    phoneNumberId,
  });
}

module.exports = {
  ensureStaffOrderAlertsSchema,
  normalizeStaffPhone,
  isValidStaffPhone,
  getShopStaffAlertPhoneNumberId,
  listStaffRecipients,
  createStaffRecipient,
  updateStaffRecipient,
  deleteStaffRecipient,
  notifyStaffNewConfirmedOrder,
  sendStaffRecipientTestAlert,
};

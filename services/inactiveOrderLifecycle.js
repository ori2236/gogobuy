const db = require("../config/db");
const { saveChat } = require("../repositories/chat");
const { detectIsEnglish } = require("../utilities/lang");
const { sendWhatsAppText } = require("../utilities/whatsapp");
const { ensureShopInfoSchema } = require("../repositories/shopInfo");

const DEFAULT_INTERVAL_MS = 60_000;
const DEFAULT_BATCH_SIZE = 25;
const PENDING_STATUSES = new Set(["pending", "checkout_pending"]);

let schemaReadyPromise = null;
let workerState = {
  started: false,
  running: false,
  timer: null,
};

function asPositiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function money(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.round(n * 100) / 100;
}

function formatMinutes(minutes, isEnglish) {
  const n = Math.max(0, Math.floor(Number(minutes) || 0));
  if (isEnglish) {
    if (n <= 1) return "about a minute";
    if (n < 60) return `${n} minutes`;
    const hours = Math.floor(n / 60);
    const rest = n % 60;
    return rest ? `${hours}h ${rest}m` : `${hours} hours`;
  }

  if (n <= 1) return "כדקה";
  if (n < 60) return `${n} דקות`;
  const hours = Math.floor(n / 60);
  const rest = n % 60;
  return rest ? `${hours} שעות ו-${rest} דקות` : `${hours} שעות`;
}

function normalizePhone(phone) {
  return String(phone || "").trim().replace(/^\+/, "");
}

function getLateReminderGraceMinutes() {
  return asPositiveInt(process.env.INACTIVE_ORDER_LATE_REMINDER_GRACE_MINUTES, 10);
}

async function ensureExpiredOrderStatusEnum() {
  const [[row]] = await db.query(
    `SELECT COLUMN_TYPE
       FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'orders'
        AND COLUMN_NAME = 'status'
      LIMIT 1`,
  );

  const columnType = String(row?.COLUMN_TYPE || row?.column_type || "");
  if (columnType.includes("'expired'")) return;

  await db.query(`
    ALTER TABLE orders
    MODIFY COLUMN status ENUM(
      'pending',
      'checkout_pending',
      'confirmed',
      'preparing',
      'ready',
      'delivering',
      'completed',
      'cancel_pending',
      'expired'
    ) NOT NULL
  `);
}

async function ensureInactiveOrderLifecycleSchema() {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await ensureShopInfoSchema();
      await ensureExpiredOrderStatusEnum();

      await db.query(`
        CREATE TABLE IF NOT EXISTS shop_whatsapp_phone (
          id INT UNSIGNED NOT NULL AUTO_INCREMENT,
          shop_id INT UNSIGNED NOT NULL,
          phone_number_id VARCHAR(64) NOT NULL,
          display_phone_number VARCHAR(32) DEFAULT NULL,
          label VARCHAR(120) DEFAULT NULL,
          is_active TINYINT(1) NOT NULL DEFAULT 1,
          created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uniq_shop_whatsapp_phone_id (phone_number_id),
          KEY idx_shop_whatsapp_phone_shop_active (shop_id, is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);

      await db.query(`
        CREATE TABLE IF NOT EXISTS order_lifecycle_notice (
          order_id BIGINT UNSIGNED NOT NULL,
          shop_id INT UNSIGNED NOT NULL,
          customer_id INT UNSIGNED NOT NULL,
          cart_reminder_sent_at DATETIME(6) DEFAULT NULL,
          stock_released_at DATETIME(6) DEFAULT NULL,
          created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
          PRIMARY KEY (order_id),
          KEY idx_order_lifecycle_shop_reminder (shop_id, cart_reminder_sent_at),
          KEY idx_order_lifecycle_shop_release (shop_id, stock_released_at),
          KEY idx_order_lifecycle_customer (customer_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);

      await db.query(`
        CREATE TABLE IF NOT EXISTS idle_customer_reminder_notice (
          chat_id BIGINT UNSIGNED NOT NULL,
          shop_id INT UNSIGNED NOT NULL,
          customer_id INT UNSIGNED NOT NULL,
          sent_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          PRIMARY KEY (chat_id),
          KEY idx_idle_customer_notice_customer (customer_id, shop_id, sent_at),
          KEY idx_idle_customer_notice_shop_sent (shop_id, sent_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);

      await db.query(`
        UPDATE shop
           SET idle_customer_reminder_minutes = 10
         WHERE id > 0
           AND idle_customer_reminder_minutes IS NULL
      `);

      await db.query(`
        UPDATE shop
           SET cart_empty_reminder_minutes = 5
         WHERE id > 0
           AND (cart_empty_reminder_minutes IS NULL
                OR cart_empty_reminder_minutes = 0)
      `);

      await db.query(`
        UPDATE shop
           SET stock_release_after_inactive_minutes = 30
         WHERE id > 0
           AND (stock_release_after_inactive_minutes IS NULL
                OR stock_release_after_inactive_minutes = 0)
      `);
    })().catch((err) => {
      schemaReadyPromise = null;
      throw err;
    });
  }

  return schemaReadyPromise;
}

async function getRecentCustomerMessage({ customer_id, shop_id }) {
  const [[row]] = await db.query(
    `SELECT message
       FROM chat
      WHERE customer_id = ?
        AND shop_id = ?
        AND sender = 'customer'
      ORDER BY id DESC
      LIMIT 1`,
    [Number(customer_id), Number(shop_id)],
  );
  return row?.message || "";
}

function buildCartReminderMessage({ order, isEnglish }) {
  const orderId = Number(order.id);
  const minutesLeft = formatMinutes(order.minutes_until_release, isEnglish);
  const total = money(order.price);

  if (isEnglish) {
    const confirmLine =
      String(order.status) === "checkout_pending"
        ? `✅ To confirm it now, reply with the order number: ${orderId}`
        : `✅ To continue to final confirmation, reply: send order`;

    return [
      `⏰ Small reminder - your order #${orderId} is still waiting for confirmation.`,
      "",
      `🛒 We are currently keeping the products reserved for you${total > 0 ? ` (₪${total.toFixed(2)})` : ""}.`,
      `If the order is not confirmed in the next ${minutesLeft}, the products will return to stock automatically.`,
      "",
      confirmLine,
      `✏️ You can also keep editing the order by writing what you would like to change.`,
    ].join("\n");
  }

  const confirmLine =
    String(order.status) === "checkout_pending"
      ? `✅ לאישור וסיום עכשיו, השב עם מספר ההזמנה: ${orderId}`
      : `✅ כדי להתקדם לאישור הסופי, כתוב: שלח הזמנה`;

  return [
    `⏰ תזכורת קטנה - ההזמנה שלך #${orderId} עדיין מחכה לאישור.`,
    "",
    `🛒 כרגע אנחנו שומרים לך את המוצרים במלאי${total > 0 ? ` (₪${total.toFixed(2)})` : ""}.`,
    `אם ההזמנה לא תאושר במהלך ${minutesLeft}, המוצרים יחזרו אוטומטית למלאי לטובת לקוחות אחרים.`,
    "",
    confirmLine,
    `✏️ אפשר גם להמשיך לערוך את ההזמנה פשוט על ידי כתיבת השינוי הרצוי.`,
  ].join("\n");
}

function buildStockReleasedMessage({ order, isEnglish }) {
  const orderId = Number(order.id);

  if (isEnglish) {
    return [
      `⏳ Order #${orderId} was cancelled automatically because it was not confirmed in time.`,
      "",
      "🛒 The reserved products were returned to stock.",
      "You can start a new order whenever you like - just write what you would like to buy.",
    ].join("\n");
  }

  return [
    `⏳ ההזמנה שלך #${orderId} בוטלה אוטומטית כי היא לא אושרה בזמן.`,
    "",
    "🛒 המוצרים שהיו שמורים עבורך הוחזרו למלאי.",
    "אפשר להתחיל הזמנה חדשה בכל רגע - פשוט כתוב מה תרצה להזמין.",
  ].join("\n");
}

function buildIdleCustomerReminderMessage({ shopName, isEnglish }) {
  const cleanShopName = String(shopName || "").trim();

  if (isEnglish) {
    return [
      `👋 Just checking in${cleanShopName ? ` from ${cleanShopName}` : ""}.`,
      "Would you like to start an order or get help building one?",
      "You can simply write your shopping list, for example: milk, bread and eggs 🛒",
    ].join("\n");
  }

  return [
    `👋 רק רציתי לבדוק${cleanShopName ? ` מטעם ${cleanShopName}` : ""}.`,
    "רוצה להתחיל הזמנה או צריך עזרה להרכיב אחת?",
    "אפשר פשוט לכתוב את רשימת המוצרים, למשל: חלב, לחם וביצים 🛒",
  ].join("\n");
}

async function sendCustomerLifecycleMessage({ order, message }) {
  const phone = normalizePhone(order.customer_phone);
  if (!phone || !message) return false;

  const phoneNumberId = String(order.phone_number_id || "").trim() || null;
  await sendWhatsAppText(phone, message, phoneNumberId);
  await saveChat({
    customer_id: Number(order.customer_id),
    shop_id: Number(order.shop_id),
    sender: "bot",
    status: "classified",
    message,
  });
  return true;
}

async function fetchOrdersForReminder(limit = DEFAULT_BATCH_SIZE) {
  await ensureInactiveOrderLifecycleSchema();

  const [rows] = await db.query(
    `
    SELECT
      o.id,
      o.shop_id,
      o.customer_id,
      o.status,
      o.price,
      o.updated_at,
      c.phone AS customer_phone,
      s.name AS shop_name,
      s.cart_empty_reminder_minutes,
      s.stock_release_after_inactive_minutes,
      swp.phone_number_id,
      COALESCE(items.item_count, 0) AS item_count,
      TIMESTAMPDIFF(MINUTE, o.updated_at, NOW(6)) AS inactive_minutes,
      CASE
        WHEN TIMESTAMPDIFF(MINUTE, o.updated_at, NOW(6)) >= s.stock_release_after_inactive_minutes
          THEN ?
        ELSE GREATEST(
          0,
          s.stock_release_after_inactive_minutes - TIMESTAMPDIFF(MINUTE, o.updated_at, NOW(6))
        )
      END AS minutes_until_release,
      n.cart_reminder_sent_at
    FROM orders o
    JOIN customer c ON c.id = o.customer_id
    JOIN shop s ON s.id = o.shop_id
    LEFT JOIN (
      SELECT order_id, COUNT(*) AS item_count
      FROM order_item
      GROUP BY order_id
    ) items ON items.order_id = o.id
    LEFT JOIN order_lifecycle_notice n ON n.order_id = o.id
    LEFT JOIN (
      SELECT shop_id, MIN(phone_number_id) AS phone_number_id
      FROM shop_whatsapp_phone
      WHERE is_active = 1
      GROUP BY shop_id
    ) swp ON swp.shop_id = o.shop_id
    WHERE o.status IN ('pending', 'checkout_pending')
      AND COALESCE(items.item_count, 0) > 0
      AND s.cart_empty_reminder_minutes > 0
      AND s.stock_release_after_inactive_minutes > s.cart_empty_reminder_minutes
      AND TIMESTAMPDIFF(MINUTE, o.updated_at, NOW(6)) >= s.cart_empty_reminder_minutes
      AND (n.cart_reminder_sent_at IS NULL OR n.cart_reminder_sent_at < o.updated_at)
    ORDER BY o.updated_at ASC
    LIMIT ?
    `,
    [getLateReminderGraceMinutes(), Number(limit)],
  );

  return rows || [];
}

async function markReminderSent(order) {
  await db.query(
    `
    INSERT INTO order_lifecycle_notice
      (order_id, shop_id, customer_id, cart_reminder_sent_at, created_at, updated_at)
    VALUES (?, ?, ?, NOW(6), NOW(6), NOW(6))
    ON DUPLICATE KEY UPDATE
      shop_id = VALUES(shop_id),
      customer_id = VALUES(customer_id),
      cart_reminder_sent_at = NOW(6),
      updated_at = NOW(6)
    `,
    [Number(order.id), Number(order.shop_id), Number(order.customer_id)],
  );
}

async function processReminderBatch() {
  const rows = await fetchOrdersForReminder();
  let sent = 0;

  for (const order of rows) {
    try {
      const recentCustomerMessage = await getRecentCustomerMessage({
        customer_id: order.customer_id,
        shop_id: order.shop_id,
      });
      const isEnglish = detectIsEnglish(recentCustomerMessage);
      const message = buildCartReminderMessage({ order, isEnglish });
      await sendCustomerLifecycleMessage({ order, message });
      await markReminderSent(order);
      sent += 1;
    } catch (err) {
      console.error("[inactive-order-lifecycle.reminder]", {
        orderId: order?.id,
        error: err?.response?.data || err?.message || err,
      });
    }
  }

  return sent;
}

async function fetchIdleCustomersForReminder(limit = DEFAULT_BATCH_SIZE) {
  await ensureInactiveOrderLifecycleSchema();

  const [rows] = await db.query(
    `
    SELECT
      lc.id AS chat_id,
      lc.customer_id,
      lc.shop_id,
      lc.message AS recent_customer_message,
      lc.created_at AS last_customer_message_at,
      c.phone AS customer_phone,
      s.name AS shop_name,
      s.idle_customer_reminder_minutes,
      swp.phone_number_id,
      TIMESTAMPDIFF(MINUTE, lc.created_at, NOW(6)) AS inactive_minutes
    FROM chat lc
    JOIN (
      SELECT customer_id, shop_id, MAX(id) AS last_customer_chat_id
      FROM chat
      WHERE sender = 'customer'
      GROUP BY customer_id, shop_id
    ) latest
      ON latest.last_customer_chat_id = lc.id
    JOIN customer c ON c.id = lc.customer_id
    JOIN shop s ON s.id = lc.shop_id
    LEFT JOIN idle_customer_reminder_notice n ON n.chat_id = lc.id
    LEFT JOIN (
      SELECT shop_id, MIN(phone_number_id) AS phone_number_id
      FROM shop_whatsapp_phone
      WHERE is_active = 1
      GROUP BY shop_id
    ) swp ON swp.shop_id = lc.shop_id
    WHERE s.idle_customer_reminder_minutes > 0
      AND n.chat_id IS NULL
      AND TIMESTAMPDIFF(MINUTE, lc.created_at, NOW(6)) >= s.idle_customer_reminder_minutes
      AND NOT EXISTS (
        SELECT 1
        FROM orders o
        WHERE o.customer_id = lc.customer_id
          AND o.shop_id = lc.shop_id
          AND o.status IN ('pending', 'checkout_pending', 'confirmed', 'preparing', 'ready', 'delivering', 'cancel_pending')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM orders o
        WHERE o.customer_id = lc.customer_id
          AND o.shop_id = lc.shop_id
          AND o.updated_at >= lc.created_at
      )
      AND NOT EXISTS (
        SELECT 1
        FROM chat newer
        WHERE newer.customer_id = lc.customer_id
          AND newer.shop_id = lc.shop_id
          AND newer.sender = 'customer'
          AND newer.id > lc.id
      )
    ORDER BY lc.created_at ASC
    LIMIT ?
    `,
    [Number(limit)],
  );

  return rows || [];
}

async function markIdleCustomerReminderSent(row) {
  await db.query(
    `
    INSERT IGNORE INTO idle_customer_reminder_notice
      (chat_id, shop_id, customer_id, sent_at, created_at)
    VALUES (?, ?, ?, NOW(6), NOW(6))
    `,
    [Number(row.chat_id), Number(row.shop_id), Number(row.customer_id)],
  );
}

async function sendIdleCustomerReminder(row) {
  const phone = normalizePhone(row.customer_phone);
  if (!phone) return false;

  const isEnglish = detectIsEnglish(row.recent_customer_message);
  const message = buildIdleCustomerReminderMessage({
    shopName: row.shop_name,
    isEnglish,
  });
  const phoneNumberId = String(row.phone_number_id || "").trim() || null;

  await sendWhatsAppText(phone, message, phoneNumberId);
  await saveChat({
    customer_id: Number(row.customer_id),
    shop_id: Number(row.shop_id),
    sender: "bot",
    status: "unclassified",
    message,
  });
  return true;
}

async function processIdleCustomerReminderBatch() {
  const rows = await fetchIdleCustomersForReminder();
  let sent = 0;

  for (const row of rows) {
    try {
      const didSend = await sendIdleCustomerReminder(row);
      await markIdleCustomerReminderSent(row);
      if (didSend) sent += 1;
    } catch (err) {
      console.error("[inactive-order-lifecycle.idle-customer-reminder]", {
        chatId: row?.chat_id,
        customerId: row?.customer_id,
        error: err?.response?.data || err?.message || err,
      });
    }
  }

  return sent;
}

async function fetchOrdersForStockRelease(limit = DEFAULT_BATCH_SIZE) {
  await ensureInactiveOrderLifecycleSchema();

  const [rows] = await db.query(
    `
    SELECT
      o.id,
      o.shop_id,
      o.customer_id,
      o.status,
      o.price,
      o.updated_at,
      c.phone AS customer_phone,
      s.name AS shop_name,
      s.stock_release_after_inactive_minutes,
      swp.phone_number_id,
      n.cart_reminder_sent_at,
      TIMESTAMPDIFF(MINUTE, o.updated_at, NOW(6)) AS inactive_minutes
    FROM orders o
    JOIN customer c ON c.id = o.customer_id
    JOIN shop s ON s.id = o.shop_id
    LEFT JOIN order_lifecycle_notice n ON n.order_id = o.id
    LEFT JOIN (
      SELECT shop_id, MIN(phone_number_id) AS phone_number_id
      FROM shop_whatsapp_phone
      WHERE is_active = 1
      GROUP BY shop_id
    ) swp ON swp.shop_id = o.shop_id
    WHERE o.status IN ('pending', 'checkout_pending')
      AND s.stock_release_after_inactive_minutes > 0
      AND TIMESTAMPDIFF(MINUTE, o.updated_at, NOW(6)) >= s.stock_release_after_inactive_minutes
      AND n.cart_reminder_sent_at IS NOT NULL
      AND n.cart_reminder_sent_at >= o.updated_at
      AND (
        n.cart_reminder_sent_at < DATE_ADD(o.updated_at, INTERVAL s.stock_release_after_inactive_minutes MINUTE)
        OR TIMESTAMPDIFF(MINUTE, n.cart_reminder_sent_at, NOW(6)) >= ?
      )
    ORDER BY o.updated_at ASC
    LIMIT ?
    `,
    [getLateReminderGraceMinutes(), Number(limit)],
  );

  return rows || [];
}

async function releaseOrderStock(order) {
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const [[lockedOrder]] = await conn.query(
      `
      SELECT
        o.id,
        o.shop_id,
        o.customer_id,
        o.status,
        o.price,
        o.updated_at,
        s.stock_release_after_inactive_minutes,
        n.cart_reminder_sent_at,
        TIMESTAMPDIFF(MINUTE, o.updated_at, NOW(6)) AS inactive_minutes
      FROM orders o
      JOIN shop s ON s.id = o.shop_id
      LEFT JOIN order_lifecycle_notice n ON n.order_id = o.id
      WHERE o.id = ? AND o.shop_id = ?
      FOR UPDATE
      `,
      [Number(order.id), Number(order.shop_id)],
    );

    if (!lockedOrder || !PENDING_STATUSES.has(String(lockedOrder.status))) {
      await conn.rollback();
      return { released: false, reason: "not_pending" };
    }

    const releaseAfter = Number(lockedOrder.stock_release_after_inactive_minutes || 0);
    const inactiveMinutes = Number(lockedOrder.inactive_minutes || 0);
    if (!(releaseAfter > 0) || inactiveMinutes < releaseAfter) {
      await conn.rollback();
      return { released: false, reason: "not_expired" };
    }

    const reminderAt = lockedOrder.cart_reminder_sent_at
      ? new Date(lockedOrder.cart_reminder_sent_at).getTime()
      : NaN;
    const updatedAt = lockedOrder.updated_at
      ? new Date(lockedOrder.updated_at).getTime()
      : NaN;

    if (!Number.isFinite(reminderAt) || !Number.isFinite(updatedAt) || reminderAt < updatedAt) {
      await conn.rollback();
      return { released: false, reason: "reminder_not_sent" };
    }

    const scheduledReleaseAt = updatedAt + releaseAfter * 60_000;
    const lateGraceMs = getLateReminderGraceMinutes() * 60_000;
    if (reminderAt >= scheduledReleaseAt && Date.now() - reminderAt < lateGraceMs) {
      await conn.rollback();
      return { released: false, reason: "late_reminder_grace" };
    }

    const [items] = await conn.query(
      `SELECT product_id, amount
         FROM order_item
        WHERE order_id = ?`,
      [Number(order.id)],
    );

    if (items.length) {
      const ids = [...new Set(items.map((x) => Number(x.product_id)).filter(Boolean))];
      if (ids.length) {
        await conn.query(
          `SELECT id
             FROM product
            WHERE shop_id = ?
              AND id IN (${ids.map(() => "?").join(",")})
            FOR UPDATE`,
          [Number(order.shop_id), ...ids],
        );
      }

      for (const item of items) {
        await conn.query(
          `UPDATE product
              SET stock_amount = COALESCE(stock_amount, 0) + ?
            WHERE id = ? AND shop_id = ?`,
          [Number(item.amount || 0), Number(item.product_id), Number(order.shop_id)],
        );
      }
    }

    await conn.query(
      `DELETE FROM chat_open_question
        WHERE order_id = ? AND shop_id = ?`,
      [Number(order.id), Number(order.shop_id)],
    );

    await conn.query(
      `INSERT INTO order_lifecycle_notice
        (order_id, shop_id, customer_id, stock_released_at, created_at, updated_at)
       VALUES (?, ?, ?, NOW(6), NOW(6), NOW(6))
       ON DUPLICATE KEY UPDATE
        shop_id = VALUES(shop_id),
        customer_id = VALUES(customer_id),
        stock_released_at = NOW(6),
        updated_at = NOW(6)`,
      [Number(order.id), Number(order.shop_id), Number(order.customer_id)],
    );

    await conn.query(
      `UPDATE orders
          SET prev_status = status,
              status = 'expired',
              updated_at = NOW(6)
        WHERE id = ? AND shop_id = ?`,
      [Number(order.id), Number(order.shop_id)],
    );

    await conn.commit();
    return { released: true, itemCount: items.length };
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    throw err;
  } finally {
    conn.release();
  }
}

async function processStockReleaseBatch() {
  const rows = await fetchOrdersForStockRelease();
  let released = 0;

  for (const order of rows) {
    try {
      const result = await releaseOrderStock(order);
      if (!result.released) continue;

      const recentCustomerMessage = await getRecentCustomerMessage({
        customer_id: order.customer_id,
        shop_id: order.shop_id,
      });
      const isEnglish = detectIsEnglish(recentCustomerMessage);
      const message = buildStockReleasedMessage({ order, isEnglish });
      await sendCustomerLifecycleMessage({ order, message });
      released += 1;
    } catch (err) {
      console.error("[inactive-order-lifecycle.release]", {
        orderId: order?.id,
        error: err?.response?.data || err?.message || err,
      });
    }
  }

  return released;
}

async function runInactiveOrderLifecycleOnce() {
  if (workerState.running) return { skipped: true };
  workerState.running = true;

  try {
    const reminders = await processReminderBatch();
    const idleCustomerReminders = await processIdleCustomerReminderBatch();
    const released = await processStockReleaseBatch();
    if (released || reminders || idleCustomerReminders) {
      console.log("[inactive-order-lifecycle]", { released, reminders, idleCustomerReminders });
    }
    return { released, reminders, idleCustomerReminders };
  } catch (err) {
    console.error("[inactive-order-lifecycle.run]", err?.message || err);
    return { error: err };
  } finally {
    workerState.running = false;
  }
}

function startInactiveOrderLifecycleWorker() {
  const enabled = String(process.env.INACTIVE_ORDER_LIFECYCLE_ENABLED || "true").toLowerCase() !== "false";
  if (!enabled) {
    console.log("[inactive-order-lifecycle] disabled by INACTIVE_ORDER_LIFECYCLE_ENABLED=false");
    return null;
  }

  if (workerState.started) return workerState.timer;
  workerState.started = true;

  const intervalMs = asPositiveInt(process.env.INACTIVE_ORDER_LIFECYCLE_INTERVAL_MS, DEFAULT_INTERVAL_MS);

  setTimeout(() => {
    runInactiveOrderLifecycleOnce().catch((err) => {
      console.error("[inactive-order-lifecycle.initial]", err?.message || err);
    });
  }, 10_000).unref?.();

  workerState.timer = setInterval(() => {
    runInactiveOrderLifecycleOnce().catch((err) => {
      console.error("[inactive-order-lifecycle.interval]", err?.message || err);
    });
  }, intervalMs);
  workerState.timer.unref?.();
  return workerState.timer;
}

module.exports = {
  ensureInactiveOrderLifecycleSchema,
  runInactiveOrderLifecycleOnce,
  startInactiveOrderLifecycleWorker,
  buildCartReminderMessage,
  buildIdleCustomerReminderMessage,
  buildStockReleasedMessage,
};

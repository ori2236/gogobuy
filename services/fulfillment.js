const db = require("../config/db");
const { saveOpenQuestions, closeQuestionsByIds } = require("../utilities/openQuestions");
const { detectIsEnglish } = require("../utilities/lang");

const QUESTION_TYPES = {
  FULFILLMENT_METHOD: "FULFILLMENT_METHOD",
  DELIVERY_ADDRESS_CONFIRM: "DELIVERY_ADDRESS_CONFIRM",
  DELIVERY_ADDRESS_INPUT: "DELIVERY_ADDRESS_INPUT",
};

const FULFILLMENT_QUESTION_NAMES = new Set(Object.values(QUESTION_TYPES));

let schemaReadyPromise = null;
const ORDERS_COLUMN_CACHE = new Map();

function toBool(v) {
  return v === true || v === 1 || v === "1" || String(v).toLowerCase() === "true";
}

function money(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.round(n * 100) / 100;
}

function fmtMoney(value) {
  return money(value).toFixed(2);
}

function cleanText(value, limit = 2000) {
  const s = String(value ?? "").trim().replace(/\s+\n/g, "\n");
  return s ? s.slice(0, limit) : null;
}

async function hasOrdersColumn(conn, columnName) {
  const key = String(columnName || "").trim();
  if (!key) return false;
  if (ORDERS_COLUMN_CACHE.has(key)) return ORDERS_COLUMN_CACHE.get(key);

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
  ORDERS_COLUMN_CACHE.set(key, exists);
  return exists;
}

async function addOrdersColumnIfMissing(conn, columnName, definition) {
  if (await hasOrdersColumn(conn, columnName)) return;
  await conn.query(`ALTER TABLE orders ADD COLUMN ${columnName} ${definition}`);
  ORDERS_COLUMN_CACHE.set(columnName, true);
}

async function ensureFulfillmentSchema(conn = db) {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await addOrdersColumnIfMissing(
        conn,
        "fulfillment_method",
        "ENUM('pickup','delivery') DEFAULT NULL AFTER payment_method",
      );
      await addOrdersColumnIfMissing(
        conn,
        "delivery_fee",
        "DECIMAL(10,2) NOT NULL DEFAULT 0.00 AFTER delivery_address",
      );
      await addOrdersColumnIfMissing(
        conn,
        "delivery_notes",
        "TEXT DEFAULT NULL AFTER delivery_fee",
      );

      await conn.query(`
        CREATE TABLE IF NOT EXISTS customer_delivery_address (
          id INT UNSIGNED NOT NULL AUTO_INCREMENT,
          customer_id INT UNSIGNED NOT NULL,
          shop_id INT UNSIGNED NOT NULL,
          full_address TEXT NOT NULL,
          delivery_notes TEXT DEFAULT NULL,
          is_default TINYINT(1) NOT NULL DEFAULT 1,
          created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_customer_delivery_address_customer_shop (customer_id, shop_id, is_default),
          KEY idx_customer_delivery_address_shop (shop_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);

      await conn.query(`
        CREATE TABLE IF NOT EXISTS shop_delivery_zone (
          id INT UNSIGNED NOT NULL AUTO_INCREMENT,
          shop_id INT UNSIGNED NOT NULL,
          settlement_name VARCHAR(120) NOT NULL,
          is_active TINYINT(1) NOT NULL DEFAULT 1,
          delivery_fee_override DECIMAL(10,2) DEFAULT NULL,
          min_order_amount_override DECIMAL(10,2) DEFAULT NULL,
          created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uniq_shop_delivery_zone_name (shop_id, settlement_name),
          KEY idx_shop_delivery_zone_shop_active (shop_id, is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);
    })().catch((err) => {
      schemaReadyPromise = null;
      throw err;
    });
  }

  return schemaReadyPromise;
}

function parseQuestionOptions(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function isFulfillmentQuestion(q) {
  return FULFILLMENT_QUESTION_NAMES.has(String(q?.product_name || "").trim());
}

function getLatestFulfillmentQuestion(openQs = []) {
  return (openQs || []).find(isFulfillmentQuestion) || null;
}

function looksLikePhoneName(name, phone) {
  const n = String(name || "").trim().replace(/\D/g, "");
  const p = String(phone || "").trim().replace(/\D/g, "");
  return !!n && !!p && n === p;
}

async function getCustomer(customer_id) {
  const [[row]] = await db.query(
    `SELECT id, name, phone FROM customer WHERE id = ? LIMIT 1`,
    [customer_id],
  );
  return row || null;
}

function getCustomerFirstName(customer) {
  const raw = String(customer?.name || "").trim();
  if (!raw || looksLikePhoneName(raw, customer?.phone)) return "";
  return raw.split(/\s+/)[0] || raw;
}

async function getShopFulfillment(shop_id) {
  await ensureFulfillmentSchema();
  const [[row]] = await db.query(
    `
    SELECT
      id,
      name,
      supports_delivery,
      supports_pickup,
      delivery_fee
    FROM shop
    WHERE id = ?
    LIMIT 1
    `,
    [shop_id],
  );
  return row || null;
}

async function getDeliveryZones(shop_id) {
  await ensureFulfillmentSchema();
  const [rows] = await db.query(
    `
    SELECT id, settlement_name, delivery_fee_override, min_order_amount_override
    FROM shop_delivery_zone
    WHERE shop_id = ? AND is_active = 1
    ORDER BY id ASC
    `,
    [shop_id],
  );
  return rows || [];
}

async function getDefaultCustomerAddress(customer_id, shop_id) {
  await ensureFulfillmentSchema();
  const [[row]] = await db.query(
    `
    SELECT id, full_address, delivery_notes
    FROM customer_delivery_address
    WHERE customer_id = ? AND shop_id = ?
    ORDER BY is_default DESC, updated_at DESC, id DESC
    LIMIT 1
    `,
    [customer_id, shop_id],
  );
  return row || null;
}

function formatZonesList(zones, isEnglish) {
  if (!zones || !zones.length) return "";
  return zones
    .map((z, idx) => `${idx + 1}. ${String(z.settlement_name || "").trim()}`)
    .filter(Boolean)
    .join("\n");
}

function customerPrefix(firstName, isEnglish) {
  if (!firstName) return "";
  return isEnglish ? `${firstName}, ` : `${firstName}, `;
}

function buildFulfillmentMethodQuestion({ firstName, isEnglish }) {
  if (isEnglish) {
    return [
      `${customerPrefix(firstName, true)}we're just about ready to close the order 🛒`,
      "How would you like to receive it?",
      "",
      "📦 1. Home delivery",
      "🛍️ 2. Store pickup",
      "",
      "Just reply with 1 or 2 👇",
    ].join("\n");
  }

  return [
    `רגע לפני שאנחנו סוגרים ומכינים את ההזמנה, ${firstName ? `${firstName}... ` : ""}🛒`,
    "איך תרצה לקבל אותה?",
    "",
    "📦 1. משלוח עד הבית",
    "🛍️ 2. איסוף עצמי מהחנות",
    "",
    "פשוט השב לנו עם המספר 1 או 2 👇",
  ].join("\n");
}

function buildAddressPrompt({ firstName, zones, isEnglish }) {
  const zonesText = formatZonesList(zones, isEnglish);
  if (isEnglish) {
    return [
      "Great, we'll prepare a home delivery! 🏍️",
      "",
      `${customerPrefix(firstName, true)}please send your full delivery address: settlement, street and house number.`,
      "If it's a building, add apartment/floor/entrance. You can also add a courier note.",
      zonesText ? `\nWe currently deliver to:\n${zonesText}` : "",
      "",
      "Send the full address and we'll continue 👇",
    ].filter(Boolean).join("\n");
  }

  return [
    "מעולה, נעשה לך משלוח עד הבית! 🏍️",
    "",
    `${customerPrefix(firstName, false)}שלח לנו בבקשה את הכתובת המלאה שלך: יישוב, רחוב ומספר בית.`,
    "אם זה בניין, אפשר להוסיף דירה/קומה/כניסה. אפשר גם להוסיף הערה לשליח.",
    zonesText ? `\nאנחנו מגיעים כרגע ל:\n${zonesText}` : "",
    "",
    "שלח לנו את הכתובת המלאה ומיד נתקדם לסגירת ההזמנה 👇",
  ].filter(Boolean).join("\n");
}

function buildAddressReminder({ firstName, zones, isEnglish }) {
  const zonesText = formatZonesList(zones, isEnglish);
  if (isEnglish) {
    return [
      `${customerPrefix(firstName, true)}we really want to send the delivery out, but we need an exact address 🗺️`,
      "",
      "Without settlement, street and house number we can't close the order for delivery.",
      zonesText ? `\nAvailable delivery areas:\n${zonesText}` : "",
      "",
      "Please send the full address and the courier will be on the way 🏎️",
    ].filter(Boolean).join("\n");
  }

  return [
    `${customerPrefix(firstName, false)}אנחנו ממש רוצים להוציא אליך את המשלוח, אבל המערכת צריכה כתובת מדויקת... 🗺️`,
    "",
    "בלי יישוב, רחוב ומספר בית לא נוכל להתקדם לסגירת ההזמנה.",
    zonesText ? `\nאפשר לבצע משלוח ליישובים:\n${zonesText}` : "",
    "",
    "שלח לנו את הכתובת המלאה והשליח כבר בדרך! 🏎️",
  ].filter(Boolean).join("\n");
}

function buildSavedAddressQuestion({ firstName, address, isEnglish }) {
  const addr = String(address?.full_address || "").trim();
  const notes = String(address?.delivery_notes || "").trim();

  if (isEnglish) {
    return [
      `${customerPrefix(firstName, true)}we have a saved delivery address:`,
      "",
      addr,
      notes ? `Courier note: ${notes}` : "",
      "",
      "Should we send it there?",
      "1. Yes",
      "2. Different address",
    ].filter(Boolean).join("\n");
  }

  return [
    `${customerPrefix(firstName, false)}יש לנו כתובת שמורה למשלוח:`,
    "",
    addr,
    notes ? `הערה לשליח: ${notes}` : "",
    "",
    "לשלוח לשם?",
    "1. כן",
    "2. כתובת אחרת",
  ].filter(Boolean).join("\n");
}

function parseFulfillmentChoice(message) {
  const raw = String(message || "").trim().toLowerCase();
  if (!raw) return null;
  if (/^1$/.test(raw)) return "delivery";
  if (/^2$/.test(raw)) return "pickup";
  if (/(משלוח|שליח|עד הבית|delivery|deliver|shipping|ship)/i.test(raw)) return "delivery";
  if (/(איסוף|איסוף עצמי|חנות|pickup|pick up|collect|collection)/i.test(raw)) return "pickup";
  return null;
}

function parseYesNoAddressConfirm(message) {
  const raw = String(message || "").trim().toLowerCase();
  if (!raw) return null;
  if (/^1$/.test(raw) || /^(כן|כן תודה|בטח|אפשר|yes|y|ok|okay|sure)$/.test(raw)) return true;
  if (/^2$/.test(raw) || /(לא|כתובת אחרת|אחרת|שונה|חדש|no|different|another)/i.test(raw)) return false;
  return null;
}

function parseDeliveryAddressMessage(message) {
  const raw = cleanText(message, 2000) || "";
  const marker = raw.match(/(?:הערה\s*(?:לשליח|משלוח)|courier\s*note|delivery\s*note)\s*[:：-]\s*/i);
  if (!marker) return { full_address: raw, delivery_notes: null };

  const idx = marker.index;
  const fullAddress = cleanText(raw.slice(0, idx), 1500) || raw;
  const deliveryNotes = cleanText(raw.slice(idx + marker[0].length), 1000);
  return { full_address: fullAddress, delivery_notes: deliveryNotes };
}

function normalizeHebrew(value) {
  return String(value || "")
    .trim()
    .replace(/[\u0591-\u05C7]/g, "")
    .replace(/\s+/g, " ");
}

function findZoneInAddress(fullAddress, zones = []) {
  const address = normalizeHebrew(fullAddress).toLowerCase();
  if (!zones.length) return null;
  return zones.find((z) => {
    const name = normalizeHebrew(z.settlement_name).toLowerCase();
    return name && address.includes(name);
  }) || null;
}

function validateDeliveryAddress(fullAddress, zones = []) {
  const text = String(fullAddress || "").trim();
  if (text.length < 8) return { ok: false, reason: "too_short" };
  if (zones.length && !findZoneInAddress(text, zones)) {
    return { ok: false, reason: "unsupported_zone" };
  }
  if (!/\d/.test(text)) return { ok: false, reason: "missing_house_number" };
  return { ok: true, zone: findZoneInAddress(text, zones) };
}

async function closeFulfillmentQuestions(customer_id, shop_id, order_id = null) {
  const [rows] = await db.query(
    `
    SELECT id
    FROM chat_open_question
    WHERE customer_id = ?
      AND shop_id = ?
      AND status = 'open'
      AND product_name IN (${Object.values(QUESTION_TYPES).map(() => "?").join(",")})
      ${order_id ? "AND (order_id = ? OR order_id IS NULL)" : ""}
    `,
    order_id
      ? [customer_id, shop_id, ...Object.values(QUESTION_TYPES), order_id]
      : [customer_id, shop_id, ...Object.values(QUESTION_TYPES)],
  );

  const ids = (rows || []).map((r) => Number(r.id)).filter(Boolean);
  if (ids.length) await closeQuestionsByIds(ids);
}

async function saveFulfillmentQuestion({ customer_id, shop_id, order_id, type, question, options }) {
  await closeFulfillmentQuestions(customer_id, shop_id, order_id);
  await saveOpenQuestions({
    customer_id,
    shop_id,
    order_id,
    questions: [
      {
        name: type,
        question,
        options: options || [],
      },
    ],
  });
}

async function saveCustomerAddress({ customer_id, shop_id, full_address, delivery_notes }) {
  await ensureFulfillmentSchema();
  const addr = cleanText(full_address, 2000);
  if (!addr) return null;

  await db.query(
    `UPDATE customer_delivery_address
        SET is_default = 0
      WHERE customer_id = ? AND shop_id = ?`,
    [customer_id, shop_id],
  );

  const [ins] = await db.query(
    `
    INSERT INTO customer_delivery_address
      (customer_id, shop_id, full_address, delivery_notes, is_default, created_at, updated_at)
    VALUES (?, ?, ?, ?, 1, NOW(), NOW())
    `,
    [customer_id, shop_id, addr, cleanText(delivery_notes, 1000)],
  );

  return {
    id: ins.insertId,
    full_address: addr,
    delivery_notes: cleanText(delivery_notes, 1000),
  };
}

async function getOrderItemsTotal(conn, order_id) {
  const [[sumRow]] = await conn.query(
    `SELECT COALESCE(ROUND(SUM(price), 2), 0) AS total
       FROM order_item
      WHERE order_id = ?`,
    [Number(order_id)],
  );
  return money(sumRow?.total || 0);
}

async function recalculateOrderTotalWithFulfillment(conn, { order_id }) {
  await ensureFulfillmentSchema(conn);
  const itemTotal = await getOrderItemsTotal(conn, order_id);
  const [[order]] = await conn.query(
    `SELECT fulfillment_method, delivery_fee FROM orders WHERE id = ? LIMIT 1`,
    [Number(order_id)],
  );
  const fee = String(order?.fulfillment_method || "") === "delivery" ? money(order?.delivery_fee) : 0;
  const total = money(itemTotal + fee);
  await conn.query(
    `UPDATE orders
        SET price = ?, updated_at = NOW(6)
      WHERE id = ?`,
    [total, Number(order_id)],
  );
  return { itemTotal, deliveryFee: fee, total };
}

async function getDeliveryFeeForOrder({ shop_id, zone }) {
  const shop = await getShopFulfillment(shop_id);
  const override = zone?.delivery_fee_override;
  const fee = override === null || override === undefined ? shop?.delivery_fee : override;
  return money(fee || 0);
}

async function applyOrderFulfillment({
  order_id,
  shop_id,
  method,
  delivery_address = null,
  delivery_notes = null,
  delivery_fee = null,
}) {
  await ensureFulfillmentSchema();
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();
    const [[order]] = await conn.query(
      `SELECT id, status FROM orders WHERE id = ? AND shop_id = ? FOR UPDATE`,
      [Number(order_id), Number(shop_id)],
    );

    if (!order) {
      await conn.rollback();
      return null;
    }

    const normalizedMethod = method === "delivery" ? "delivery" : "pickup";
    const fee = normalizedMethod === "delivery" ? money(delivery_fee || 0) : 0;

    await conn.query(
      `
      UPDATE orders
         SET fulfillment_method = ?,
             delivery_address = ?,
             delivery_fee = ?,
             delivery_notes = ?,
             updated_at = NOW(6)
       WHERE id = ? AND shop_id = ?
      `,
      [
        normalizedMethod,
        normalizedMethod === "delivery" ? cleanText(delivery_address, 2000) : null,
        fee,
        normalizedMethod === "delivery" ? cleanText(delivery_notes, 1000) : null,
        Number(order_id),
        Number(shop_id),
      ],
    );

    const totals = await recalculateOrderTotalWithFulfillment(conn, { order_id });
    await conn.commit();

    return {
      id: Number(order_id),
      fulfillment_method: normalizedMethod,
      delivery_address: normalizedMethod === "delivery" ? cleanText(delivery_address, 2000) : null,
      delivery_fee: fee,
      delivery_notes: normalizedMethod === "delivery" ? cleanText(delivery_notes, 1000) : null,
      price: totals.total,
      itemTotal: totals.itemTotal,
    };
  } catch (err) {
    try { await conn.rollback(); } catch {}
    throw err;
  } finally {
    conn.release();
  }
}

async function getOrderForCheckout(order_id, shop_id) {
  await ensureFulfillmentSchema();
  const [[row]] = await db.query(
    `
    SELECT id, shop_id, customer_id, status, price, fulfillment_method,
           delivery_address, delivery_fee, delivery_notes
    FROM orders
    WHERE id = ? AND shop_id = ?
    LIMIT 1
    `,
    [Number(order_id), Number(shop_id)],
  );
  return row || null;
}

function buildFulfillmentSummaryForCheckout(order, isEnglish) {
  if (!order) return "";
  const method = String(order.fulfillment_method || "");
  const lines = [];

  if (method === "delivery") {
    if (isEnglish) {
      lines.push("📦 Receiving method: home delivery");
      if (order.delivery_address) lines.push(`📍 Address: ${order.delivery_address}`);
      if (Number(order.delivery_fee) > 0) lines.push(`🏍️ Delivery fee: ₪${fmtMoney(order.delivery_fee)}`);
      if (order.delivery_notes) lines.push(`📝 Courier note: ${order.delivery_notes}`);
      lines.push(`💰 Total including delivery: ₪${fmtMoney(order.price)}`);
    } else {
      lines.push("📦 אופן קבלה: משלוח עד הבית");
      if (order.delivery_address) lines.push(`📍 כתובת למשלוח: ${order.delivery_address}`);
      if (Number(order.delivery_fee) > 0) lines.push(`🏍️ דמי משלוח: ₪${fmtMoney(order.delivery_fee)}`);
      if (order.delivery_notes) lines.push(`📝 הערה לשליח: ${order.delivery_notes}`);
      lines.push(`💰 סה״כ כולל משלוח: ₪${fmtMoney(order.price)}`);
    }
  } else if (method === "pickup") {
    if (isEnglish) {
      lines.push("🛍️ Receiving method: store pickup");
      lines.push(`💰 Total to pay: ₪${fmtMoney(order.price)}`);
    } else {
      lines.push("🛍️ אופן קבלה: איסוף עצמי מהחנות");
      lines.push(`💰 סה״כ לתשלום: ₪${fmtMoney(order.price)}`);
    }
  }

  return lines.join("\n");
}

async function buildCheckoutInstructionForOrder({ order_id, shop_id, isEnglish }) {
  const order = await getOrderForCheckout(order_id, shop_id);
  const summary = buildFulfillmentSummaryForCheckout(order, isEnglish);

  const lines = [];
  if (summary) lines.push(summary, "");

  if (isEnglish) {
    lines.push(
      `To confirm your order (#${order_id}), reply with:`,
      String(order_id),
      "",
      "If you’d like to add a note for the picker, write it after the number.",
      `For example: ${order_id} Please choose ripe bananas`,
    );
  } else {
    lines.push(
      `כדי לסיים את ההזמנה שלך (#${order_id}), השב עם:`,
      String(order_id),
      "",
      "אם תרצה להוסיף הערה למלקט, אפשר לכתוב אותה אחרי המספר.",
      `לדוגמה: ${order_id} בלי שקיות בבקשה`,
    );
  }

  return lines.join("\n");
}

async function moveOrderToCheckoutPending({ order_id, shop_id, isEnglish }) {
  const [res] = await db.query(
    `UPDATE orders
        SET prev_status = status,
            status = 'checkout_pending',
            updated_at = NOW()
      WHERE id = ? AND shop_id = ? AND status = 'pending'`,
    [Number(order_id), Number(shop_id)],
  );

  if (res.affectedRows === 0) {
    const order = await getOrderForCheckout(order_id, shop_id);
    if (order?.status === "checkout_pending") {
      return buildCheckoutInstructionForOrder({ order_id, shop_id, isEnglish });
    }
    return isEnglish
      ? `Order (#${order_id}) can't be checked out at this stage.`
      : `אי אפשר לסיים את ההזמנה (#${order_id}) בשלב הזה.`;
  }

  return buildCheckoutInstructionForOrder({ order_id, shop_id, isEnglish });
}

async function prepareFulfillmentBeforeCheckout({ activeOrder, isEnglish, customer_id, shop_id }) {
  if (!activeOrder || activeOrder.status !== "pending") return null;

  await ensureFulfillmentSchema();
  const shop = await getShopFulfillment(shop_id);
  if (!shop) return null;

  const supportsDelivery = toBool(shop.supports_delivery);
  const supportsPickup = toBool(shop.supports_pickup);
  const customer = await getCustomer(customer_id);
  const firstName = getCustomerFirstName(customer);
  const zones = await getDeliveryZones(shop_id);

  if (!supportsDelivery && !supportsPickup) {
    await applyOrderFulfillment({ order_id: activeOrder.id, shop_id, method: "pickup" });
    return null;
  }

  if (supportsPickup && !supportsDelivery) {
    await applyOrderFulfillment({ order_id: activeOrder.id, shop_id, method: "pickup" });
    return null;
  }

  let method = String(activeOrder.fulfillment_method || "").trim();

  if (supportsDelivery && supportsPickup && !method) {
    const question = buildFulfillmentMethodQuestion({ firstName, isEnglish });
    await saveFulfillmentQuestion({
      customer_id,
      shop_id,
      order_id: activeOrder.id,
      type: QUESTION_TYPES.FULFILLMENT_METHOD,
      question,
      options: ["1", "2", "משלוח", "איסוף עצמי"],
    });
    return question;
  }

  if (supportsDelivery && !supportsPickup && !method) {
    method = "delivery";
    await applyOrderFulfillment({
      order_id: activeOrder.id,
      shop_id,
      method: "delivery",
      delivery_fee: await getDeliveryFeeForOrder({ shop_id, zone: null }),
    });
  }

  const refreshed = await getOrderForCheckout(activeOrder.id, shop_id);
  if (String(refreshed?.fulfillment_method || method) !== "delivery") return null;

  if (refreshed?.delivery_address) return null;

  const savedAddress = await getDefaultCustomerAddress(customer_id, shop_id);
  if (savedAddress) {
    const question = buildSavedAddressQuestion({ firstName, address: savedAddress, isEnglish });
    await saveFulfillmentQuestion({
      customer_id,
      shop_id,
      order_id: activeOrder.id,
      type: QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM,
      question,
      options: {
        address_id: savedAddress.id,
        full_address: savedAddress.full_address,
        delivery_notes: savedAddress.delivery_notes,
      },
    });
    return question;
  }

  const question = buildAddressPrompt({ firstName, zones, isEnglish });
  await saveFulfillmentQuestion({
    customer_id,
    shop_id,
    order_id: activeOrder.id,
    type: QUESTION_TYPES.DELIVERY_ADDRESS_INPUT,
    question,
    options: { zones: zones.map((z) => z.settlement_name) },
  });
  return question;
}

async function finishFulfillmentStep({ activeOrder, shop_id, isEnglish, prefix = "" }) {
  if (!activeOrder) return prefix;
  if (activeOrder.status === "pending" || activeOrder.status === "checkout_pending") {
    const instruction = await moveOrderToCheckoutPending({
      order_id: activeOrder.id,
      shop_id,
      isEnglish,
    });
    return [prefix, instruction].filter(Boolean).join("\n\n");
  }
  return prefix || (isEnglish ? "Updated." : "עודכן.");
}

async function askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish }) {
  const customer = await getCustomer(customer_id);
  const firstName = getCustomerFirstName(customer);
  const zones = await getDeliveryZones(shop_id);
  const question = buildAddressPrompt({ firstName, zones, isEnglish });
  await saveFulfillmentQuestion({
    customer_id,
    shop_id,
    order_id: activeOrder.id,
    type: QUESTION_TYPES.DELIVERY_ADDRESS_INPUT,
    question,
    options: { zones: zones.map((z) => z.settlement_name) },
  });
  return question;
}

async function handleDirectFulfillmentChangeRequest({ message, customer_id, shop_id, activeOrder, isEnglish }) {
  if (!activeOrder) return null;
  if (!["pending", "checkout_pending", "confirmed"].includes(String(activeOrder.status))) return null;

  const raw = String(message || "").trim();
  const hasChangeIntent = /(שנה|תשנה|תחליף|להחליף|רוצה|עדיף|אפשר|תעשה|תעביר|change|switch|prefer)/i.test(raw);
  const choice = parseFulfillmentChoice(raw);
  if (!choice || !hasChangeIntent) return null;

  const shop = await getShopFulfillment(shop_id);
  if (!shop) return null;
  const supportsDelivery = toBool(shop.supports_delivery);
  const supportsPickup = toBool(shop.supports_pickup);

  if (choice === "pickup") {
    if (!supportsPickup) {
      return isEnglish ? "This branch does not support pickup." : "הסניף הזה לא תומך באיסוף עצמי.";
    }
    await closeFulfillmentQuestions(customer_id, shop_id, activeOrder.id);
    await applyOrderFulfillment({ order_id: activeOrder.id, shop_id, method: "pickup" });
    return finishFulfillmentStep({
      activeOrder,
      shop_id,
      isEnglish,
      prefix: isEnglish
        ? "No problem, I changed the order to store pickup."
        : "אין בעיה, שיניתי את ההזמנה לאיסוף עצמי מהחנות.",
    });
  }

  if (choice === "delivery") {
    if (!supportsDelivery) {
      return isEnglish ? "This branch does not support delivery." : "הסניף הזה לא תומך במשלוחים.";
    }
    await closeFulfillmentQuestions(customer_id, shop_id, activeOrder.id);
    await applyOrderFulfillment({
      order_id: activeOrder.id,
      shop_id,
      method: "delivery",
      delivery_fee: await getDeliveryFeeForOrder({ shop_id, zone: null }),
    });

    const savedAddress = await getDefaultCustomerAddress(customer_id, shop_id);
    const customer = await getCustomer(customer_id);
    const firstName = getCustomerFirstName(customer);
    if (savedAddress) {
      const question = buildSavedAddressQuestion({ firstName, address: savedAddress, isEnglish });
      await saveFulfillmentQuestion({
        customer_id,
        shop_id,
        order_id: activeOrder.id,
        type: QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM,
        question,
        options: {
          address_id: savedAddress.id,
          full_address: savedAddress.full_address,
          delivery_notes: savedAddress.delivery_notes,
        },
      });
      return question;
    }

    return askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
  }

  return null;
}

async function handleFulfillmentReply({ message, customer_id, shop_id, activeOrder, openQs, saveChat }) {
  if (!activeOrder) return null;
  await ensureFulfillmentSchema();

  const isEnglish = detectIsEnglish(message);
  const question = getLatestFulfillmentQuestion(openQs);

  const saveBoth = async (botText) => {
    await saveChat({ customer_id, shop_id, sender: "customer", status: "classified", message });
    await saveChat({ customer_id, shop_id, sender: "bot", status: "classified", message: botText });
    return botText;
  };

  if (!question) {
    const direct = await handleDirectFulfillmentChangeRequest({
      message,
      customer_id,
      shop_id,
      activeOrder,
      isEnglish,
    });
    return direct ? saveBoth(direct) : null;
  }

  const type = String(question.product_name || "").trim();
  const customer = await getCustomer(customer_id);
  const firstName = getCustomerFirstName(customer);
  const zones = await getDeliveryZones(shop_id);
  const shop = await getShopFulfillment(shop_id);

  if (type === QUESTION_TYPES.FULFILLMENT_METHOD) {
    const choice = parseFulfillmentChoice(message);
    if (!choice) {
      const botText = buildFulfillmentMethodQuestion({ firstName, isEnglish });
      return saveBoth(botText);
    }

    if (choice === "pickup") {
      if (!toBool(shop?.supports_pickup)) {
        return saveBoth(isEnglish ? "This branch does not support pickup." : "הסניף הזה לא תומך באיסוף עצמי.");
      }
      await closeQuestionsByIds([question.id]);
      await applyOrderFulfillment({ order_id: activeOrder.id, shop_id, method: "pickup" });
      const botText = await finishFulfillmentStep({
        activeOrder,
        shop_id,
        isEnglish,
        prefix: isEnglish
          ? "Great, store pickup it is."
          : "מעולה, ההזמנה תהיה באיסוף עצמי מהחנות.",
      });
      return saveBoth(botText);
    }

    if (!toBool(shop?.supports_delivery)) {
      return saveBoth(isEnglish ? "This branch does not support delivery." : "הסניף הזה לא תומך במשלוחים.");
    }

    await closeQuestionsByIds([question.id]);
    await applyOrderFulfillment({
      order_id: activeOrder.id,
      shop_id,
      method: "delivery",
      delivery_fee: await getDeliveryFeeForOrder({ shop_id, zone: null }),
    });

    const savedAddress = await getDefaultCustomerAddress(customer_id, shop_id);
    if (savedAddress) {
      const botText = buildSavedAddressQuestion({ firstName, address: savedAddress, isEnglish });
      await saveFulfillmentQuestion({
        customer_id,
        shop_id,
        order_id: activeOrder.id,
        type: QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM,
        question: botText,
        options: {
          address_id: savedAddress.id,
          full_address: savedAddress.full_address,
          delivery_notes: savedAddress.delivery_notes,
        },
      });
      return saveBoth(botText);
    }

    const botText = buildAddressPrompt({ firstName, zones, isEnglish });
    await saveFulfillmentQuestion({
      customer_id,
      shop_id,
      order_id: activeOrder.id,
      type: QUESTION_TYPES.DELIVERY_ADDRESS_INPUT,
      question: botText,
      options: { zones: zones.map((z) => z.settlement_name) },
    });
    return saveBoth(botText);
  }

  if (type === QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM) {
    const yes = parseYesNoAddressConfirm(message);
    if (yes === null) {
      const options = parseQuestionOptions(question.option_set) || {};
      const botText = buildSavedAddressQuestion({ firstName, address: options, isEnglish });
      return saveBoth(botText);
    }

    if (!yes) {
      await closeQuestionsByIds([question.id]);
      const botText = await askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
      return saveBoth(botText);
    }

    const savedAddress = await getDefaultCustomerAddress(customer_id, shop_id);
    if (!savedAddress) {
      await closeQuestionsByIds([question.id]);
      const botText = await askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
      return saveBoth(botText);
    }

    const validation = validateDeliveryAddress(savedAddress.full_address, zones);
    if (!validation.ok) {
      await closeQuestionsByIds([question.id]);
      const botText = await askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
      return saveBoth(botText);
    }

    await closeQuestionsByIds([question.id]);
    const fee = await getDeliveryFeeForOrder({ shop_id, zone: validation.zone });
    await applyOrderFulfillment({
      order_id: activeOrder.id,
      shop_id,
      method: "delivery",
      delivery_address: savedAddress.full_address,
      delivery_notes: savedAddress.delivery_notes,
      delivery_fee: fee,
    });

    const botText = await finishFulfillmentStep({
      activeOrder,
      shop_id,
      isEnglish,
      prefix: isEnglish
        ? "Great, I saved the delivery to your saved address."
        : "מעולה, שמרתי את המשלוח לכתובת השמורה.",
    });
    return saveBoth(botText);
  }

  if (type === QUESTION_TYPES.DELIVERY_ADDRESS_INPUT) {
    const parsed = parseDeliveryAddressMessage(message);
    const validation = validateDeliveryAddress(parsed.full_address, zones);
    if (!validation.ok) {
      const botText = buildAddressReminder({ firstName, zones, isEnglish });
      return saveBoth(botText);
    }

    await closeQuestionsByIds([question.id]);
    const saved = await saveCustomerAddress({
      customer_id,
      shop_id,
      full_address: parsed.full_address,
      delivery_notes: parsed.delivery_notes,
    });
    const fee = await getDeliveryFeeForOrder({ shop_id, zone: validation.zone });

    await applyOrderFulfillment({
      order_id: activeOrder.id,
      shop_id,
      method: "delivery",
      delivery_address: saved.full_address,
      delivery_notes: saved.delivery_notes,
      delivery_fee: fee,
    });

    const botText = await finishFulfillmentStep({
      activeOrder,
      shop_id,
      isEnglish,
      prefix: isEnglish
        ? "Great, I saved the delivery address."
        : "מעולה, שמרתי את כתובת המשלוח.",
    });
    return saveBoth(botText);
  }

  return null;
}

module.exports = {
  QUESTION_TYPES,
  ensureFulfillmentSchema,
  prepareFulfillmentBeforeCheckout,
  handleFulfillmentReply,
  recalculateOrderTotalWithFulfillment,
  buildCheckoutInstructionForOrder,
  moveOrderToCheckoutPending,
  getOrderForCheckout,
  buildFulfillmentSummaryForCheckout,
  getDeliveryZones,
};

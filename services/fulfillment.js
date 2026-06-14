const db = require("../config/db");
const { buildDeliveryTimingMessage, buildStoredDeliveryTimingMessage, calculateDeliveryTiming } = require("../utilities/deliveryTiming");
const { saveOpenQuestions, closeQuestionsByIds } = require("../utilities/openQuestions");
const { detectIsEnglish } = require("../utilities/lang");
const { chat } = require("../config/openai");
const { getPromptFromDB } = require("../repositories/prompt");
const { validateMinimumOrderBeforeCheckout } = require("./minimumOrder");
const { applyCartPromotionsToOrder, buildOrderCartPromotionLines } = require("./cartPromotions");

const QUESTION_TYPES = {
  FULFILLMENT_METHOD: "FULFILLMENT_METHOD",
  DELIVERY_ADDRESS_CONFIRM: "DELIVERY_ADDRESS_CONFIRM",
  DELIVERY_ADDRESS_INPUT: "DELIVERY_ADDRESS_INPUT",
};

const FULFILLMENT_QUESTION_NAMES = new Set(Object.values(QUESTION_TYPES));
const FULFILLMENT_INTENT_PROMPT_CAT = "ORD";
const FULFILLMENT_INTENT_PROMPT_SUB = "FULFILLMENT_INTENT";
const FULFILLMENT_INTENT_PROMPT_CACHE_KEY = "ord_fulfillment_intent_v1";
const ORDERS_COLUMN_CACHE = new Map();
let schemaReadyPromise = null;
let warnedMissingFulfillmentIntentPrompt = false;

const FULFILLMENT_INTENT_FALLBACK_PROMPT = `You classify a supermarket WhatsApp customer's intent during checkout fulfillment only.
Return JSON only. Do not answer the customer.

Allowed intents:
- choose_delivery: user wants home delivery or wants to switch to delivery.
- choose_pickup: user wants store pickup / self pickup / to come take it themselves.
- confirm_saved_address: user agrees to use the saved delivery address.
- different_address: user rejects the saved address or wants to enter another address.
- provide_address: user provides a concrete delivery address.
- not_fulfillment: unrelated to delivery/pickup/address selection.

Critical rules:
- If the user corrects themselves with words like בעצם / לא בעצם / actually, classify the final intended method.
- Bare numbers depend on pending_question:
  * FULFILLMENT_METHOD => 1 choose_delivery, 2 choose_pickup.
  * DELIVERY_ADDRESS_CONFIRM => 1 confirm_saved_address, 2 different_address.
- In DELIVERY_ADDRESS_CONFIRM, yes/כן means confirm_saved_address unless the text clearly asks pickup/delivery instead.
- In DELIVERY_ADDRESS_CONFIRM or DELIVERY_ADDRESS_INPUT, phrases like לקחת לבד / לקחת אותה לבד / אני אקח / אני אאסוף / אגיע לקחת / בלי משלוח / לא משלוח mean choose_pickup, not provide_address.
- Only use provide_address when the text looks like an actual delivery address, not when it asks for pickup.

Hebrew examples for choose_pickup:
לקחת לבד, לקחת אותה לבד, אני אקח, אני אאסוף, לבוא לקחת, אגיע לקחת, אני בא לקחת, לא משלוח, בלי משלוח, לקחת מהחנות.

Hebrew examples for choose_delivery:
משלוח, שליח, עד הבית, תשלחו לי, בא לי משלוח, אני רוצה משלוח, לא איסוף עצמי.

Output schema:
{
  "intent": "choose_delivery" | "choose_pickup" | "confirm_saved_address" | "different_address" | "provide_address" | "not_fulfillment",
  "confidence": number between 0 and 1,
  "address": string or null
}`;

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
  const s = String(value ?? "").trim().replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n");
  return s ? s.slice(0, limit) : null;
}

function stripAddressPrefix(value) {
  let s = String(value || "").trim();
  s = s.replace(/^כתובת\s*[:：-]\s*/i, "").trim();
  s = s.replace(/^address\s*[:：-]\s*/i, "").trim();
  s = s.replace(/^(אני\s+רוצה\s+(?:משלוח\s+)?ל)/i, "").trim();
  s = s.replace(/^(תשלחו\s+לי\s+ל|שלחו\s+ל|משלוח\s+ל)/i, "").trim();
  return s;
}

function isAddressUpdateMessage(message) {
  return /^\s*(כתובת|address)\s*[:：-]\s*\S+/i.test(String(message || ""));
}

function extractAddressUpdateText(message) {
  return stripAddressPrefix(String(message || "").replace(/^\s*(כתובת|address)\s*[:：-]\s*/i, ""));
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
        "delivery_fee_before_promo",
        "DECIMAL(10,2) DEFAULT NULL AFTER delivery_fee",
      );
      await addOrdersColumnIfMissing(
        conn,
        "delivery_notes",
        "TEXT DEFAULT NULL AFTER delivery_fee_before_promo",
      );
      await addOrdersColumnIfMissing(
        conn,
        "delivery_expected_date",
        "DATE DEFAULT NULL AFTER delivery_notes",
      );
      await addOrdersColumnIfMissing(
        conn,
        "delivery_expected_start_time",
        "TIME DEFAULT NULL AFTER delivery_expected_date",
      );
      await addOrdersColumnIfMissing(
        conn,
        "delivery_expected_end_time",
        "TIME DEFAULT NULL AFTER delivery_expected_start_time",
      );
      await addOrdersColumnIfMissing(
        conn,
        "packaging_bags_count",
        "INT UNSIGNED NOT NULL DEFAULT 0",
      );
      await addOrdersColumnIfMissing(
        conn,
        "packaging_cartons_count",
        "INT UNSIGNED NOT NULL DEFAULT 0",
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

function isFulfillmentQuestion(q) {
  return FULFILLMENT_QUESTION_NAMES.has(String(q?.product_name || "").trim());
}

function getLatestFulfillmentQuestion(openQs = []) {
  return (openQs || []).find(isFulfillmentQuestion) || null;
}

async function getCustomer(customer_id) {
  const [[row]] = await db.query(
    `SELECT id, name, phone, chain_id FROM customer WHERE id = ? LIMIT 1`,
    [customer_id],
  );
  return row || null;
}

function looksLikePhoneName(name, phone) {
  const n = String(name || "").trim().replace(/\D/g, "");
  const p = String(phone || "").trim().replace(/\D/g, "");
  return !!n && !!p && n === p;
}

function getCustomerFirstName(customer) {
  const raw = String(customer?.name || "").trim();
  if (!raw || looksLikePhoneName(raw, customer?.phone)) return "";
  return raw.split(/\s+/)[0] || raw;
}

async function getShopFulfillment(shop_id) {
  await ensureFulfillmentSchema();
  const [[row]] = await db.query(
    `SELECT id, name, supports_delivery, supports_pickup, delivery_fee, order_same_day_cutoff_time, delivery_arrival_start_time, delivery_arrival_end_time FROM shop WHERE id = ? LIMIT 1`,
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

function formatZonesList(zones) {
  return (zones || [])
    .map((z, idx) => `${idx + 1}. ${String(z.settlement_name || "").trim()}`)
    .filter(Boolean)
    .join("\n");
}

function customerPrefix(firstName) {
  return firstName ? `${firstName}, ` : "";
}

function buildFulfillmentMethodQuestion({ firstName, isEnglish }) {
  if (isEnglish) {
    return [
      `${customerPrefix(firstName)}we're just about ready to close the order 🛒`,
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

function buildInvalidFulfillmentChoiceMessage({ isEnglish }) {
  if (isEnglish) {
    return [
      "It looks like the answer wasn't in the format we asked for 🙂",
      "To continue, please reply with just one number:",
      "",
      "📦 1 - Home delivery",
      "🛍️ 2 - Store pickup",
    ].join("\n");
  }

  return [
    "נשמע שלא ענית בפורמט שביקשנו 🙂",
    "כדי שנדע איך להמשיך, השב רק עם מספר:",
    "",
    "📦 1 - משלוח עד הבית",
    "🛍️ 2 - איסוף עצמי מהחנות",
  ].join("\n");
}

function pickRotatingTemplate(templates) {
  if (!Array.isArray(templates) || templates.length === 0) return null;
  if (templates.length === 1) return templates[0];
  const idx = Math.floor(Math.random() * templates.length);
  return templates[idx];
}

function buildAddressPrompt({ firstName, zones, isEnglish }) {
  const zonesText = formatZonesList(zones);
  const namePrefix = customerPrefix(firstName);

  if (isEnglish) {
    const templates = [
      [
        "Great, we'll prepare a home delivery! 🏍️",
        "",
        `${namePrefix}please send your full delivery address: settlement, street and house number.`,
        "If it's a building, add apartment/floor/entrance.",
        zonesText ? `\nWe currently deliver to:\n${zonesText}` : "",
        "",
        "Important: sending the address is not the final order confirmation ✅",
        "You can still add, remove or change products after that 🛒",
        "",
        "Send the full address and we'll continue 👇",
      ],
      [
        "We're almost there 🛒",
        "",
        "For home delivery, I just need a full address:",
        "📍 Settlement",
        "🏠 Street and house/building number",
        "If needed, add apartment/floor/entrance.",
        zonesText ? `\nAvailable delivery areas:\n${zonesText}` : "",
        "",
        "This does not confirm the order yet - it only lets us prepare the delivery details.",
      ],
      [
        "To continue with delivery, please send the address 🗺️",
        "",
        `${namePrefix}write settlement, street and house number in one message.`,
        zonesText ? `\nWe deliver to:\n${zonesText}` : "",
        "",
        "No worries - the order is still editable after you send the address 🛍️",
      ],
    ];
    return pickRotatingTemplate(templates).filter(Boolean).join("\n");
  }

  const templates = [
    [
      "מעולה, נעשה לך משלוח עד הבית! 🏍️",
      "",
      `${namePrefix}שלח לנו בבקשה את הכתובת המלאה שלך: יישוב, רחוב ומספר בית.`,
      "אם זה בניין, אפשר להוסיף דירה/קומה/כניסה.",
      zonesText ? `\nאנחנו מגיעים כרגע ל:\n${zonesText}` : "",
      "",
      "חשוב לדעת: שליחת כתובת היא עדיין לא אישור סופי של ההזמנה ✅",
      "אפשר להמשיך להוסיף, להסיר או לשנות מוצרים גם אחרי זה 🛒",
      "",
      "שלח לנו את הכתובת המלאה ונמשיך משם 👇",
    ],
    [
      "אנחנו כמעט שם 🛒",
      "",
      "בשביל משלוח עד הבית צריך כתובת מלאה:",
      "📍 יישוב",
      "🏠 רחוב ומספר בית",
      "אם צריך, אפשר להוסיף דירה/קומה/כניסה.",
      zonesText ? `\nאפשר לבצע משלוח ליישובים:\n${zonesText}` : "",
      "",
      "זה לא סוגר את ההזמנה סופית - זה רק מאפשר לנו להכין את פרטי המשלוח.",
    ],
    [
      "כדי להכין את המשלוח חסרה לנו רק כתובת 🗺️",
      "",
      `${namePrefix}שלח בבקשה יישוב + רחוב + מספר בית בהודעה אחת.`,
      zonesText ? `\nאנחנו מגיעים כרגע ל:\n${zonesText}` : "",
      "",
      "אל דאגה, ההזמנה עדיין פתוחה לשינויים אחרי שליחת הכתובת 🛍️",
    ],
  ];

  return pickRotatingTemplate(templates).filter(Boolean).join("\n");
}

function buildAddressReminder({ firstName, zones, isEnglish, reason }) {
  const zonesText = formatZonesList(zones);
  const namePrefix = customerPrefix(firstName);

  if (isEnglish) {
    const reasonLine =
      reason === "multiple_zones"
        ? "I found more than one delivery area in the address, so I need one clear settlement."
        : reason === "unsupported_zone"
          ? "The settlement must be one of our supported delivery areas."
          : reason === "missing_house_number"
            ? "Please include a house/building number."
            : "Without settlement, street and house number we can't prepare the delivery details.";

    const templates = [
      [
        `${namePrefix}we're almost there, but I need a clearer delivery address 🗺️`,
        "",
        reasonLine,
        zonesText ? `\nAvailable delivery areas:\n${zonesText}` : "",
        "",
        "This is still not the final order confirmation - you can keep editing the cart after sending the address 🛒",
        "Please send the full address again.",
      ],
      [
        "I couldn't save the delivery address yet 📍",
        "",
        reasonLine,
        zonesText ? `\nWe currently deliver to:\n${zonesText}` : "",
        "",
        "Send settlement, street and house number. The order will remain editable after that.",
      ],
    ];

    return pickRotatingTemplate(templates).filter(Boolean).join("\n");
  }

  const reasonLine =
    reason === "multiple_zones"
      ? "זיהיתי יותר מיישוב משלוח אחד בכתובת, אז אני צריך יישוב אחד ברור."
      : reason === "unsupported_zone"
        ? "היישוב בכתובת חייב להיות אחד מיישובי המשלוח שלנו."
        : reason === "missing_house_number"
          ? "חסר מספר בית/בניין בכתובת."
          : "בלי יישוב, רחוב ומספר בית לא נוכל להכין את פרטי המשלוח.";

  const templates = [
    [
      `${namePrefix}אנחנו כמעט שם, רק צריך כתובת מדויקת יותר 🗺️`,
      "",
      reasonLine,
      zonesText ? `\nאפשר לבצע משלוח ליישובים:\n${zonesText}` : "",
      "",
      "זה עדיין לא אישור סופי של ההזמנה - אפשר להמשיך לערוך את הסל אחרי שליחת הכתובת 🛒",
      "שלח לנו שוב את הכתובת המלאה ונמשיך משם 👇",
    ],
    [
      "לא הצלחתי לשמור את הכתובת למשלוח עדיין 📍",
      "",
      reasonLine,
      zonesText ? `\nאנחנו מגיעים כרגע ל:\n${zonesText}` : "",
      "",
      "שלח יישוב, רחוב ומספר בית. ההזמנה תישאר פתוחה לשינויים גם אחרי זה.",
    ],
  ];

  return pickRotatingTemplate(templates).filter(Boolean).join("\n");
}

function buildSavedAddressQuestion({ firstName, address, isEnglish }) {
  const addr = String(address?.full_address || "").trim();
  if (isEnglish) {
    return [
      `${customerPrefix(firstName)}we have a saved delivery address:`,
      "",
      addr,
      "",
      "Should we send it there?",
      "1. Yes",
      "2. Different address",
    ].filter(Boolean).join("\n");
  }

  return [
    `${customerPrefix(firstName)}יש לנו כתובת שמורה למשלוח:`,
    "",
    addr,
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
  if (/^(משלוח|שליח|עד הבית|delivery|deliver|shipping|ship)$/i.test(raw)) return "delivery";
  if (/^(איסוף|איסוף עצמי|חנות|pickup|pick up|collect|collection)$/i.test(raw)) return "pickup";

  const pickupMatch = raw.match(/איסוף|איסוף עצמי|חנות|pickup|pick up|collect|collection/i);
  const deliveryMatch = raw.match(/משלוח|שליח|עד הבית|delivery|deliver|shipping|ship/i);
  if (pickupMatch && !deliveryMatch) return "pickup";
  if (deliveryMatch && !pickupMatch) return "delivery";
  if (pickupMatch && deliveryMatch) {
    return pickupMatch.index > deliveryMatch.index ? "pickup" : "delivery";
  }

  return null;
}

function hasExplicitFulfillmentMethodMention(message) {
  return /(משלוח|שליח|עד הבית|איסוף|איסוף עצמי|חנות|לקחת|אאסוף|לאסוף|אקח|take it|take myself|come get|come pick|delivery|deliver|shipping|ship|pickup|pick up|collect|collection)/i.test(
    String(message || ""),
  );
}

function safeJsonParse(raw) {
  try {
    const text = String(raw || "").trim();
    if (!text) return null;
    const direct = JSON.parse(text);
    return direct && typeof direct === "object" ? direct : null;
  } catch {
    const match = String(raw || "").match(/\{[\s\S]*\}/);
    if (!match) return null;
    try {
      const parsed = JSON.parse(match[0]);
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch {
      return null;
    }
  }
}

function normalizeFulfillmentIntent(value) {
  const intent = String(value || "").trim().toLowerCase();
  return [
    "choose_delivery",
    "choose_pickup",
    "confirm_saved_address",
    "different_address",
    "provide_address",
    "not_fulfillment",
  ].includes(intent)
    ? intent
    : "not_fulfillment";
}

async function getFulfillmentIntentSystemPrompt() {
  const dbPrompt = await getPromptFromDB(
    FULFILLMENT_INTENT_PROMPT_CAT,
    FULFILLMENT_INTENT_PROMPT_SUB,
  );

  if (dbPrompt && String(dbPrompt).trim()) return String(dbPrompt).trim();

  if (!warnedMissingFulfillmentIntentPrompt) {
    warnedMissingFulfillmentIntentPrompt = true;
    console.warn(
      `[fulfillment.intent] Missing DB prompt ${FULFILLMENT_INTENT_PROMPT_CAT}.${FULFILLMENT_INTENT_PROMPT_SUB}; using fallback prompt`,
    );
  }

  return FULFILLMENT_INTENT_FALLBACK_PROMPT;
}

function parseRuleBasedFulfillmentIntent({ message, questionType }) {
  const raw = String(message || "").trim();
  if (!raw) return { intent: "not_fulfillment", confidence: 0 };

  if (questionType === QUESTION_TYPES.FULFILLMENT_METHOD && /^1$/.test(raw)) {
    return { intent: "choose_delivery", confidence: 0.95 };
  }
  if (questionType === QUESTION_TYPES.FULFILLMENT_METHOD && /^2$/.test(raw)) {
    return { intent: "choose_pickup", confidence: 0.95 };
  }

  if (questionType === QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM && /^1$/.test(raw)) {
    return { intent: "confirm_saved_address", confidence: 0.95 };
  }
  if (questionType === QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM && /^2$/.test(raw)) {
    return { intent: "different_address", confidence: 0.95 };
  }

  // Common Hebrew phrasing that means pickup but does not contain the literal word "איסוף".
  if (/(לקחת\s+לבד|לקחת\s+אותה\s+לבד|אני\s+אקח|אני\s+אאסוף|לבוא\s+לקחת|בא\s+לי\s+לקחת|אקח\s+מהחנות|אאסוף\s+מהחנות|אגיע\s+לקחת|אני\s+בא\s+לקחת|לא\s+משלוח|בלי\s+משלוח)/i.test(raw)) {
    return { intent: "choose_pickup", confidence: 0.9 };
  }

  const nonNumericRaw = /^\d+$/.test(raw) ? "" : raw;
  const choice = parseFulfillmentChoice(nonNumericRaw);
  if (choice === "delivery") return { intent: "choose_delivery", confidence: 0.95 };
  if (choice === "pickup") return { intent: "choose_pickup", confidence: 0.95 };

  if (questionType === QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM) {
    const yes = parseYesNoAddressConfirm(raw);
    if (yes === true) return { intent: "confirm_saved_address", confidence: 0.95 };
    if (yes === false) return { intent: "different_address", confidence: 0.9 };
  }

  if (isAddressUpdateMessage(raw)) {
    return { intent: "provide_address", confidence: 0.9, address: extractAddressUpdateText(raw) };
  }

  return { intent: "not_fulfillment", confidence: 0 };
}

async function classifyFulfillmentIntentWithAI({ message, questionType, activeOrder, isEnglish }) {
  const raw = String(message || "").trim();
  if (!raw) return { intent: "not_fulfillment", confidence: 0 };

  const rule = parseRuleBasedFulfillmentIntent({ message: raw, questionType });
  if (rule.intent !== "not_fulfillment" && rule.confidence >= 0.9) return rule;

  // Avoid calling the model for long unrelated messages unless we are inside a fulfillment question.
  if (!questionType && raw.length > 120 && !hasExplicitFulfillmentMethodMention(raw)) {
    return rule;
  }

  const systemPrompt = await getFulfillmentIntentSystemPrompt();

  const userContext = [
    `pending_question=${questionType || "none"}`,
    `current_order_status=${activeOrder?.status || "none"}`,
    `current_fulfillment_method=${activeOrder?.fulfillment_method || "none"}`,
    `language=${isEnglish ? "English" : "Hebrew"}`,
    `message=${raw}`,
  ].join("\n");

  try {
    const rawAnswer = await chat({
      message: raw,
      systemPrompt,
      userContext,
      use: "main",
      prompt_cache_key: FULFILLMENT_INTENT_PROMPT_CACHE_KEY,
      response_format: {
        type: "json_schema",
        json_schema: {
          name: "fulfillment_intent",
          strict: true,
          schema: {
            type: "object",
            additionalProperties: false,
            properties: {
              intent: {
                type: "string",
                enum: [
                  "choose_delivery",
                  "choose_pickup",
                  "confirm_saved_address",
                  "different_address",
                  "provide_address",
                  "not_fulfillment",
                ],
              },
              confidence: { type: "number" },
              address: { type: ["string", "null"] },
            },
            required: ["intent", "confidence", "address"],
          },
        },
      },
    });

    const parsed = safeJsonParse(rawAnswer) || {};
    const confidence = Number(parsed.confidence);
    return {
      intent: normalizeFulfillmentIntent(parsed.intent),
      confidence: Number.isFinite(confidence) ? confidence : 0,
      address: typeof parsed.address === "string" && parsed.address.trim() ? parsed.address.trim() : null,
    };
  } catch (err) {
    console.error("[fulfillment.intent.ai]", err?.message || err);
    return rule;
  }
}

function parseYesNoAddressConfirm(message) {
  const raw = String(message || "").trim().toLowerCase();
  if (!raw) return null;
  if (/^1$/.test(raw) || /^(כן|כן תודה|בטח|אפשר|yes|y|ok|okay|sure)$/.test(raw)) return true;
  if (/^2$/.test(raw) || /(לא|כתובת אחרת|אחרת|שונה|חדש|no|different|another)/i.test(raw)) return false;
  return null;
}

function normalizeHebrew(value) {
  return String(value || "")
    .trim()
    .replace(/[\u0591-\u05C7]/g, "")
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function findZonesInAddress(fullAddress, zones = []) {
  const address = normalizeHebrew(fullAddress);
  return (zones || []).filter((z) => {
    const name = normalizeHebrew(z.settlement_name);
    if (!name) return false;
    return address.includes(name);
  });
}

function validateDeliveryAddress(fullAddress, zones = []) {
  const text = stripAddressPrefix(fullAddress);
  if (text.length < 8) return { ok: false, reason: "too_short", cleanedAddress: text };
  const matches = findZonesInAddress(text, zones);
  if (zones.length && matches.length === 0) return { ok: false, reason: "unsupported_zone", cleanedAddress: text };
  if (matches.length > 1) return { ok: false, reason: "multiple_zones", zones: matches, cleanedAddress: text };
  if (!/\d/.test(text)) return { ok: false, reason: "missing_house_number", cleanedAddress: text };
  return { ok: true, zone: matches[0] || null, cleanedAddress: text };
}

async function closeFulfillmentQuestions(customer_id, shop_id, order_id = null) {
  const types = Object.values(QUESTION_TYPES);
  const [rows] = await db.query(
    `
    SELECT id
    FROM chat_open_question
    WHERE customer_id = ?
      AND shop_id = ?
      AND status = 'open'
      AND product_name IN (${types.map(() => "?").join(",")})
      ${order_id ? "AND (order_id = ? OR order_id IS NULL)" : ""}
    `,
    order_id ? [customer_id, shop_id, ...types, order_id] : [customer_id, shop_id, ...types],
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
    questions: [{ name: type, question, options: options || [] }],
  });
}

async function saveCustomerAddress({ customer_id, shop_id, full_address }) {
  await ensureFulfillmentSchema();
  const addr = cleanText(stripAddressPrefix(full_address), 2000);
  if (!addr) return null;

  await db.query(
    `UPDATE customer_delivery_address SET is_default = 0 WHERE customer_id = ? AND shop_id = ?`,
    [customer_id, shop_id],
  );

  const [ins] = await db.query(
    `
    INSERT INTO customer_delivery_address
      (customer_id, shop_id, full_address, delivery_notes, is_default, created_at, updated_at)
    VALUES (?, ?, ?, NULL, 1, NOW(), NOW())
    `,
    [customer_id, shop_id, addr],
  );

  return { id: ins.insertId, full_address: addr, delivery_notes: null };
}

async function getOrderItemsTotal(conn, order_id) {
  const [[sumRow]] = await conn.query(
    `SELECT COALESCE(ROUND(SUM(price), 2), 0) AS total FROM order_item WHERE order_id = ?`,
    [Number(order_id)],
  );
  return money(sumRow?.total || 0);
}

async function recalculateOrderTotalWithFulfillment(conn, { order_id, skipCartPromotions = false }) {
  await ensureFulfillmentSchema(conn);

  if (!skipCartPromotions) {
    const promoTotals = await applyCartPromotionsToOrder(conn, { order_id });
    if (promoTotals) {
      return {
        itemTotal: promoTotals.itemsSubtotal,
        deliveryFee: promoTotals.deliveryFee,
        total: promoTotals.total,
      };
    }
  }

  const itemTotal = await getOrderItemsTotal(conn, order_id);
  const [[order]] = await conn.query(
    `SELECT fulfillment_method, delivery_fee FROM orders WHERE id = ? LIMIT 1`,
    [Number(order_id)],
  );
  const fee = String(order?.fulfillment_method || "") === "delivery" ? money(order?.delivery_fee) : 0;
  const total = money(itemTotal + fee);
  await conn.query(`UPDATE orders SET price = ?, updated_at = NOW(6) WHERE id = ?`, [total, Number(order_id)]);
  return { itemTotal, deliveryFee: fee, total };
}
async function getDeliveryFeeForOrder({ shop_id, zone }) {
  const shop = await getShopFulfillment(shop_id);
  const override = zone?.delivery_fee_override;
  const fee = override === null || override === undefined ? shop?.delivery_fee : override;
  return money(fee || 0);
}

async function applyOrderFulfillment({ order_id, shop_id, method, delivery_address = null, delivery_fee = null }) {
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
             delivery_fee_before_promo = IF(? = 'delivery', ?, NULL),
             delivery_notes = NULL,
             delivery_expected_date = IF(? = 'delivery', delivery_expected_date, NULL),
             delivery_expected_start_time = IF(? = 'delivery', delivery_expected_start_time, NULL),
             delivery_expected_end_time = IF(? = 'delivery', delivery_expected_end_time, NULL),
             updated_at = NOW(6)
       WHERE id = ? AND shop_id = ?
      `,
      [
        normalizedMethod,
        normalizedMethod === "delivery" ? cleanText(stripAddressPrefix(delivery_address), 2000) : null,
        fee,
        normalizedMethod,
        fee,
        normalizedMethod,
        normalizedMethod,
        normalizedMethod,
        Number(order_id),
        Number(shop_id),
      ],
    );

    const totals = await recalculateOrderTotalWithFulfillment(conn, { order_id });
    await conn.commit();
    return { id: Number(order_id), fulfillment_method: normalizedMethod, price: totals.total };
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
           delivery_address, delivery_fee, delivery_notes,
           DATE_FORMAT(delivery_expected_date, '%Y-%m-%d') AS delivery_expected_date,
           TIME_FORMAT(delivery_expected_start_time, '%H:%i') AS delivery_expected_start_time,
           TIME_FORMAT(delivery_expected_end_time, '%H:%i') AS delivery_expected_end_time
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
      lines.push(`💰 Total including delivery: ₪${fmtMoney(order.price)}`);
    } else {
      lines.push("📦 אופן קבלה: משלוח עד הבית");
      if (order.delivery_address) lines.push(`📍 כתובת למשלוח: ${order.delivery_address}`);
      if (Number(order.delivery_fee) > 0) lines.push(`🏍️ דמי משלוח: ₪${fmtMoney(order.delivery_fee)}`);
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
  const isDelivery = String(order?.fulfillment_method || "") === "delivery";

  const lines = [];
  if (summary) lines.push(summary, "");

  const cartPromotionLines = await buildOrderCartPromotionLines(order_id, shop_id, isEnglish);
  if (cartPromotionLines.length) {
    lines.push(isEnglish ? "🎁 Basket promotions:" : "🎁 מבצעי סל:");
    for (const line of cartPromotionLines) lines.push(`• ${line}`);
    lines.push("");
  }

  if (isDelivery) {
    const storedDeliveryTiming = buildStoredDeliveryTimingMessage({
      expectedDate: order?.delivery_expected_date,
      arrivalStart: order?.delivery_expected_start_time,
      arrivalEnd: order?.delivery_expected_end_time,
      isEnglish,
    });

    if (storedDeliveryTiming) {
      lines.push(storedDeliveryTiming, "");
    } else {
      const shop = await getShopFulfillment(shop_id);
      const deliveryTiming = buildDeliveryTimingMessage({ shop, isEnglish, includeCutoff: true }).text;
      if (deliveryTiming) lines.push(deliveryTiming, "");
    }
  }

  if (isEnglish) {
    lines.push(
      `To confirm your order (#${order_id}), reply with:`,
      String(order_id),
      "",
    );

    if (isDelivery) {
      lines.push(
        `To send a different delivery address, reply: Address: your new address`,
        "",
      );
    }

    lines.push(
      "If you’d like to add a note for the picker, write it after the number.",
      `For example: ${order_id} Please choose ripe bananas`,
    );
  } else {
    lines.push(
      `לאישור וסיום ההזמנה שלך (#${order_id}), השב עם:`,
      String(order_id),
      "",
    );

    if (isDelivery) {
      lines.push(
        `לשליחת כתובת משלוח חדשה, הגב: כתובת: הכתובת החדשה`,
        "",
      );
    }

    lines.push(
      "אם תרצה להוסיף הערה למלקט, אפשר לכתוב אותה אחרי המספר.",
      `לדוגמה: ${order_id} בלי שקיות בבקשה`,
    );
  }

  return lines.join("\n");
}

async function moveOrderToCheckoutPending({ order_id, shop_id, isEnglish, customer_id = null, maxPerProduct = null }) {
  const minimumBlock = await validateMinimumOrderBeforeCheckout({
    order_id,
    shop_id,
    customer_id,
    isEnglish,
    maxPerProduct,
  });
  if (minimumBlock) return minimumBlock.message;

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

  if (!supportsDelivery || (supportsPickup && !supportsDelivery)) {
    await applyOrderFulfillment({ order_id: activeOrder.id, shop_id, method: "pickup" });
    return null;
  }

  if (supportsDelivery && supportsPickup && !activeOrder.fulfillment_method) {
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

  if (supportsDelivery && !supportsPickup && !activeOrder.fulfillment_method) {
    await applyOrderFulfillment({
      order_id: activeOrder.id,
      shop_id,
      method: "delivery",
      delivery_fee: await getDeliveryFeeForOrder({ shop_id, zone: null }),
    });
  }

  const refreshed = await getOrderForCheckout(activeOrder.id, shop_id);
  if (String(refreshed?.fulfillment_method || "") !== "delivery") return null;
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
      options: { address_id: savedAddress.id, full_address: savedAddress.full_address },
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

async function finishFulfillmentStep({ activeOrder, shop_id, isEnglish, prefix = "", customer_id = null, maxPerProduct = null }) {
  if (!activeOrder) return prefix;
  if (activeOrder.status === "pending" || activeOrder.status === "checkout_pending") {
    const instruction = await moveOrderToCheckoutPending({ order_id: activeOrder.id, shop_id, isEnglish, customer_id: customer_id || activeOrder.customer_id, maxPerProduct });
    return [prefix, instruction].filter(Boolean).join("\n\n");
  }

  const order = await getOrderForCheckout(activeOrder.id, shop_id);
  if (String(order?.fulfillment_method || "") === "delivery") {
    const storedDeliveryTiming = buildStoredDeliveryTimingMessage({
      expectedDate: order.delivery_expected_date,
      arrivalStart: order.delivery_expected_start_time,
      arrivalEnd: order.delivery_expected_end_time,
      isEnglish,
    });
    if (storedDeliveryTiming) {
      return [prefix || (isEnglish ? "Updated." : "עודכן."), storedDeliveryTiming].filter(Boolean).join("\n\n");
    }
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

async function saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat }) {
  await saveChat({ customer_id, shop_id, sender: "customer", status: "classified", message });
  await saveChat({ customer_id, shop_id, sender: "bot", status: "classified", message: botText });
  return botText;
}

async function updateDeliveryAddressForOrder({ message, addressText, customer_id, shop_id, activeOrder, isEnglish, saveChat, questionId = null, maxPerProduct = null }) {
  await ensureFulfillmentSchema();
  if (!activeOrder || !["pending", "checkout_pending", "confirmed"].includes(String(activeOrder.status))) return null;

  const currentOrder = await getOrderForCheckout(activeOrder.id, shop_id);
  if (String(currentOrder?.fulfillment_method || "") !== "delivery") return null;

  const customer = await getCustomer(customer_id);
  const firstName = getCustomerFirstName(customer);
  const zones = await getDeliveryZones(shop_id);
  const validation = validateDeliveryAddress(addressText, zones);

  if (!validation.ok) {
    const botText = buildAddressReminder({ firstName, zones, isEnglish, reason: validation.reason });
    return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
  }

  if (questionId) await closeQuestionsByIds([questionId]);

  const saved = await saveCustomerAddress({
    customer_id,
    shop_id,
    full_address: validation.cleanedAddress,
  });
  const fee = await getDeliveryFeeForOrder({ shop_id, zone: validation.zone });
  await applyOrderFulfillment({
    order_id: activeOrder.id,
    shop_id,
    method: "delivery",
    delivery_address: saved.full_address,
    delivery_fee: fee,
  });

  const prefix = isEnglish ? "Great, I updated the delivery address." : "מעולה, עדכנתי את כתובת המשלוח.";
  const botText = await finishFulfillmentStep({ activeOrder, shop_id, isEnglish, prefix, customer_id, maxPerProduct });
  return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
}

async function switchOrderToPickup({ message, customer_id, shop_id, activeOrder, isEnglish, saveChat, maxPerProduct = null }) {
  const shop = await getShopFulfillment(shop_id);
  if (!toBool(shop?.supports_pickup)) {
    const botText = isEnglish ? "This branch does not support pickup." : "הסניף הזה לא תומך באיסוף עצמי.";
    return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
  }

  await closeFulfillmentQuestions(customer_id, shop_id, activeOrder.id);
  await applyOrderFulfillment({ order_id: activeOrder.id, shop_id, method: "pickup" });
  const botText = await finishFulfillmentStep({
    activeOrder,
    shop_id,
    isEnglish,
    prefix: isEnglish ? "No problem, I changed the order to store pickup." : "אין בעיה, שיניתי את ההזמנה לאיסוף עצמי מהחנות.",
    customer_id,
    maxPerProduct,
  });
  return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
}

async function switchOrderToDelivery({ message, customer_id, shop_id, activeOrder, isEnglish, saveChat }) {
  const shop = await getShopFulfillment(shop_id);
  if (!toBool(shop?.supports_delivery)) {
    const botText = isEnglish ? "This branch does not support delivery." : "הסניף הזה לא תומך במשלוחים.";
    return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
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
    const botText = buildSavedAddressQuestion({ firstName, address: savedAddress, isEnglish });
    await saveFulfillmentQuestion({
      customer_id,
      shop_id,
      order_id: activeOrder.id,
      type: QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM,
      question: botText,
      options: { address_id: savedAddress.id, full_address: savedAddress.full_address },
    });
    return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
  }

  const botText = await askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
  return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
}

async function confirmSavedDeliveryAddress({ message, customer_id, shop_id, activeOrder, isEnglish, saveChat, questionId = null, maxPerProduct = null }) {
  const savedAddress = await getDefaultCustomerAddress(customer_id, shop_id);
  if (!savedAddress) {
    if (questionId) await closeQuestionsByIds([questionId]);
    const botText = await askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
    return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
  }

  const zones = await getDeliveryZones(shop_id);
  const validation = validateDeliveryAddress(savedAddress.full_address, zones);
  if (!validation.ok) {
    if (questionId) await closeQuestionsByIds([questionId]);
    const botText = await askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
    return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
  }

  if (questionId) await closeQuestionsByIds([questionId]);
  const fee = await getDeliveryFeeForOrder({ shop_id, zone: validation.zone });
  await applyOrderFulfillment({
    order_id: activeOrder.id,
    shop_id,
    method: "delivery",
    delivery_address: savedAddress.full_address,
    delivery_fee: fee,
  });

  const botText = await finishFulfillmentStep({
    activeOrder,
    shop_id,
    isEnglish,
    prefix: isEnglish ? "Great, I saved the delivery to your saved address." : "מעולה, שמרתי את המשלוח לכתובת השמורה.",
    customer_id,
    maxPerProduct,
  });
  return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
}

async function handleFulfillmentIntentResult({ intentResult, message, customer_id, shop_id, activeOrder, isEnglish, saveChat, questionId = null, maxPerProduct = null }) {
  const intent = normalizeFulfillmentIntent(intentResult?.intent);
  const confidence = Number(intentResult?.confidence || 0);
  if (!intent || intent === "not_fulfillment" || confidence < 0.55) return null;

  if (intent === "choose_pickup") {
    return switchOrderToPickup({ message, customer_id, shop_id, activeOrder, isEnglish, saveChat, maxPerProduct });
  }

  if (intent === "choose_delivery") {
    return switchOrderToDelivery({ message, customer_id, shop_id, activeOrder, isEnglish, saveChat });
  }

  if (intent === "confirm_saved_address") {
    return confirmSavedDeliveryAddress({ message, customer_id, shop_id, activeOrder, isEnglish, saveChat, questionId, maxPerProduct });
  }

  if (intent === "different_address") {
    if (questionId) await closeQuestionsByIds([questionId]);
    const botText = await askForDeliveryAddress({ activeOrder, customer_id, shop_id, isEnglish });
    return saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
  }

  if (intent === "provide_address") {
    return updateDeliveryAddressForOrder({
      message,
      addressText: intentResult?.address || message,
      customer_id,
      shop_id,
      activeOrder,
      isEnglish,
      saveChat,
      questionId,
      maxPerProduct,
    });
  }

  return null;
}

async function handleDirectFulfillmentChangeRequest({
  message,
  customer_id,
  shop_id,
  activeOrder,
  isEnglish,
  saveChat,
  allowMethodOnly = false,
  maxPerProduct = null,
}) {
  if (!activeOrder) return null;
  if (!["pending", "checkout_pending", "confirmed"].includes(String(activeOrder.status))) return null;

  if (isAddressUpdateMessage(message)) {
    return updateDeliveryAddressForOrder({
      message,
      addressText: extractAddressUpdateText(message),
      customer_id,
      shop_id,
      activeOrder,
      isEnglish,
      saveChat,
      maxPerProduct,
    });
  }

  const raw = String(message || "").trim();
  const hasChangeIntent = /(שנה|תשנה|תחליף|להחליף|רוצה|עדיף|אפשר|תעשה|תעביר|בעצם|לא משנה|בא\s+לי|בלי\s+משלוח|לא\s+משלוח|change|switch|prefer|actually|instead)/i.test(raw);
  const hasMethodMention = hasExplicitFulfillmentMethodMention(raw);
  if (!hasChangeIntent && !hasMethodMention && !allowMethodOnly) return null;

  const intentResult = await classifyFulfillmentIntentWithAI({
    message: raw,
    questionType: null,
    activeOrder,
    isEnglish,
  });

  if (!allowMethodOnly && intentResult?.intent === "provide_address") {
    return handleFulfillmentIntentResult({
      intentResult,
      message,
      customer_id,
      shop_id,
      activeOrder,
      isEnglish,
      saveChat,
      maxPerProduct,
    });
  }

  if (!["choose_pickup", "choose_delivery"].includes(intentResult?.intent)) return null;
  return handleFulfillmentIntentResult({
    intentResult,
    message,
    customer_id,
    shop_id,
    activeOrder,
    isEnglish,
    saveChat,
    maxPerProduct,
  });
}

async function handleFulfillmentReply({ message, customer_id, shop_id, activeOrder, openQs, saveChat, maxPerProduct = null }) {
  if (!activeOrder) return null;
  await ensureFulfillmentSchema();

  const isEnglish = detectIsEnglish(message);
  const question = getLatestFulfillmentQuestion(openQs);

  if (!question) {
    return handleDirectFulfillmentChangeRequest({ message, customer_id, shop_id, activeOrder, isEnglish, saveChat, maxPerProduct });
  }

  const type = String(question.product_name || "").trim();
  const saveBoth = async (botText) => saveBotAndCustomer({ message, botText, customer_id, shop_id, saveChat });
  const customer = await getCustomer(customer_id);
  const firstName = getCustomerFirstName(customer);
  const zones = await getDeliveryZones(shop_id);
  const shop = await getShopFulfillment(shop_id);

  const intentResult = await classifyFulfillmentIntentWithAI({
    message,
    questionType: type,
    activeOrder,
    isEnglish,
  });

  if (type === QUESTION_TYPES.FULFILLMENT_METHOD) {
    if (["choose_pickup", "choose_delivery"].includes(intentResult?.intent)) {
      return handleFulfillmentIntentResult({
        intentResult,
        message,
        customer_id,
        shop_id,
        activeOrder,
        isEnglish,
        saveChat,
        questionId: question.id,
        maxPerProduct,
      });
    }

    const botText = buildInvalidFulfillmentChoiceMessage({ isEnglish });
    return saveBoth(botText);
  }

  if (type === QUESTION_TYPES.DELIVERY_ADDRESS_CONFIRM) {
    if (["choose_pickup", "choose_delivery", "confirm_saved_address", "different_address", "provide_address"].includes(intentResult?.intent)) {
      const handled = await handleFulfillmentIntentResult({
        intentResult,
        message,
        customer_id,
        shop_id,
        activeOrder,
        isEnglish,
        saveChat,
        questionId: question.id,
        maxPerProduct,
      });
      if (handled) return handled;
    }

    const botText = isEnglish
      ? "Please reply with 1 to use the saved address, 2 to send a different address, or write that you'd prefer store pickup."
      : "כדי להמשיך, השב 1 לשימוש בכתובת השמורה, 2 להזנת כתובת אחרת, או כתוב שאתה מעדיף איסוף עצמי.";
    return saveBoth(botText);
  }

  if (type === QUESTION_TYPES.DELIVERY_ADDRESS_INPUT) {
    if (["choose_pickup", "choose_delivery", "provide_address"].includes(intentResult?.intent)) {
      const handled = await handleFulfillmentIntentResult({
        intentResult,
        message,
        customer_id,
        shop_id,
        activeOrder,
        isEnglish,
        saveChat,
        questionId: question.id,
        maxPerProduct,
      });
      if (handled) return handled;
    }

    const validation = validateDeliveryAddress(message, zones);
    if (!validation.ok) {
      const botText = buildAddressReminder({ firstName, zones, isEnglish, reason: validation.reason });
      return saveBoth(botText);
    }

    await closeQuestionsByIds([question.id]);
    const saved = await saveCustomerAddress({ customer_id, shop_id, full_address: validation.cleanedAddress });
    const fee = await getDeliveryFeeForOrder({ shop_id, zone: validation.zone });

    await applyOrderFulfillment({
      order_id: activeOrder.id,
      shop_id,
      method: "delivery",
      delivery_address: saved.full_address,
      delivery_fee: fee,
    });

    const botText = await finishFulfillmentStep({
      activeOrder,
      shop_id,
      isEnglish,
      prefix: isEnglish ? "Great, I saved the delivery address." : "מעולה, שמרתי את כתובת המשלוח.",
      customer_id,
      maxPerProduct,
    });
    return saveBoth(botText);
  }

  // A fulfillment-related open question exists, but it is not one of the known types.
  // Let the regular classification flow continue instead of blocking the customer.
  if (toBool(shop?.supports_delivery) || toBool(shop?.supports_pickup)) return null;
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
  validateDeliveryAddress,
  isAddressUpdateMessage,
};

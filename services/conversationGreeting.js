const db = require("../config/db");
const { detectIsEnglish } = require("../utilities/lang");
const { isCustomerNameMissing } = require("./customerOnboarding");

const IDLE_GREETING_SECONDS = 12 * 60 * 60;
const DEFAULT_TIME_ZONE = "Asia/Jerusalem";

function cleanText(value) {
  return String(value || "")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function getFirstName(fullName) {
  const parts = cleanText(fullName).split(/\s+/).filter(Boolean);
  return parts[0] || "";
}

function pickRandom(items) {
  if (!Array.isArray(items) || items.length === 0) return "";
  return items[Math.floor(Math.random() * items.length)];
}

function getIsraelHour(date = new Date()) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: process.env.GREETING_TIME_ZONE || DEFAULT_TIME_ZONE,
      hour: "2-digit",
      hourCycle: "h23",
    }).formatToParts(date);

    const hourPart = parts.find((part) => part.type === "hour");
    const hour = Number(hourPart?.value);
    return Number.isFinite(hour) ? hour : date.getHours();
  } catch (err) {
    console.error("[conversationGreeting.getIsraelHour]", err?.message || err);
    return date.getHours();
  }
}

function getTimeSlot(hour) {
  if (hour >= 5 && hour <= 11) return "morning";
  if (hour >= 12 && hour <= 16) return "noon";
  if (hour >= 17 && hour <= 21) return "evening";
  return "night";
}

const TIME_GREETINGS_HE = {
  morning: {
    withName: [
      "בוקר אור {name}! ☀️ איזה כיף להתחיל את היום איתך 🥰",
      "בוקר טוב {name}! 👋 הקפה כבר מוכן, ועכשיו הזמן למלא את המקרר.",
      "יום חדש, הזמנה חדשה! בוקר מצוין {name} 🛒",
      "בוקר טוב {name}! ☀️ המדפים כבר מחכים לך עם כל הדברים הטובים.",
      "בוקר מעולה {name}! 🥐 מתחילים את היום עם סל קניות מסודר?",
    ],
    withoutName: [
      "בוקר טוב! ☀️ איזה כיף להתחיל את היום יחד 🥰",
      "בוקר אור! 👋 הקפה כבר מוכן, ועכשיו הזמן למלא את המקרר.",
      "יום חדש, הזמנה חדשה! בוקר מצוין 🛒",
      "בוקר טוב! ☀️ המדפים כבר מחכים עם כל הדברים הטובים.",
    ],
  },
  noon: {
    withName: [
      "צהריים טובים {name}! 👋 מאחלים לך המשך יום פרודוקטיבי וטעים.",
      "היי {name}, צהריים מצוינים! אנחנו כאן וערוכים להזמנה שלך 🍅🥩",
      "צהריים טובים {name}! 🌤️ זמן מצוין להשלים את כל מה שחסר בבית.",
      "היי {name}! 🌤️ מקווים שהיום עובר מעולה - איך אפשר לעזור?",
      "צהריים נעימים {name}! 🛒 הסופר פתוח והשירות מחכה לך.",
    ],
    withoutName: [
      "צהריים טובים! 👋 מאחלים לך המשך יום פרודוקטיבי וטעים.",
      "צהריים מצוינים! אנחנו כאן וערוכים להזמנה שלך 🍅🥩",
      "צהריים טובים! 🌤️ זמן מצוין להשלים את כל מה שחסר בבית.",
      "היי! 🌤️ מקווים שהיום עובר מעולה - איך אפשר לעזור?",
    ],
  },
  evening: {
    withName: [
      "ערב טוב {name}! ✨ הגעת בדיוק בזמן, המדפים מלאים בהפתעות.",
      "היי {name}, ערב מצוין! 🌙 סוגרים את היום עם קנייה מפנקת?",
      "ערב נעים {name}! ✨ אנחנו כאן כדי לדאוג שלא יחסר כלום בבית.",
      "ערב טוב {name}! 🛒 זמן מעולה לסגור את רשימת הקניות.",
      "היי {name}! ✨ נשמח לעזור לך לסיים את היום עם סל מסודר.",
    ],
    withoutName: [
      "ערב טוב! ✨ הגעת בדיוק בזמן, המדפים מלאים בהפתעות.",
      "ערב מצוין! 🌙 סוגרים את היום עם קנייה מפנקת?",
      "ערב נעים! ✨ אנחנו כאן כדי לדאוג שלא יחסר כלום בבית.",
      "ערב טוב! 🛒 זמן מעולה לסגור את רשימת הקניות.",
    ],
  },
  night: {
    withName: [
      "לילה טוב {name}! 🌙 אנחנו עדיין כאן כדי לעזור עם מה שחסר.",
      "היי {name}, לילה רגוע! 🌙 אפשר לסגור הזמנה גם בשעות הקטנות.",
      "לילה טוב {name}! ✨ נסדר לך את הסל כדי שמחר יתחיל קל יותר.",
      "לילה נעים {name}! 🛒 גם עכשיו אפשר לדאוג למקרר מלא.",
    ],
    withoutName: [
      "לילה טוב! 🌙 אנחנו עדיין כאן כדי לעזור עם מה שחסר.",
      "לילה רגוע! 🌙 אפשר לסגור הזמנה גם בשעות הקטנות.",
      "לילה טוב! ✨ נסדר לך את הסל כדי שמחר יתחיל קל יותר.",
      "לילה נעים! 🛒 גם עכשיו אפשר לדאוג למקרר מלא.",
    ],
  },
};

const RETURNING_GREETINGS_HE = {
  withName: [
    "שלום {name}, איזה כיף שחזרת אלינו! 🥰 התגעגענו.",
    "היי {name}, תמיד תענוג לראות אותך פה שוב! 👋",
    "{name}, ברוך השב לסופר גלסנר! 🥳 הסל שלך ריק ומחכה לכל הדברים הטובים.",
    "איזה כיף לראות אותך שוב, {name}! 🛒 מוכנים לעזור עם הקנייה הבאה.",
    "ברוך השב {name}! 😊 שמחים שחזרת לעשות קניות איתנו.",
    "היי {name}! 👋 חזרת בדיוק בזמן - המדפים מחכים לך.",
  ],
  withoutName: [
    "שלום, איזה כיף שחזרת אלינו! 🥰 התגעגענו.",
    "היי, תמיד תענוג לראות אותך פה שוב! 👋",
    "ברוך השב לסופר גלסנר! 🥳 הסל שלך ריק ומחכה לכל הדברים הטובים.",
    "איזה כיף לראות אותך שוב! 🛒 מוכנים לעזור עם הקנייה הבאה.",
    "ברוך השב! 😊 שמחים שחזרת לעשות קניות איתנו.",
  ],
};

const TIME_GREETINGS_EN = {
  morning: {
    withName: [
      "Good morning, {name}! ☀️ Great to start the day with you.",
      "Morning, {name}! 👋 Time to fill up the fridge.",
    ],
    withoutName: [
      "Good morning! ☀️ Great to start the day together.",
      "Morning! 👋 Time to fill up the fridge.",
    ],
  },
  noon: {
    withName: [
      "Good afternoon, {name}! 🌤️ Hope your day is going great.",
      "Hi {name}, good afternoon! We are ready for your order 🍅🥩",
    ],
    withoutName: [
      "Good afternoon! 🌤️ Hope your day is going great.",
      "Good afternoon! We are ready for your order 🍅🥩",
    ],
  },
  evening: {
    withName: [
      "Good evening, {name}! ✨ You arrived just in time.",
      "Hi {name}, good evening! 🌙 Let’s take care of your groceries.",
    ],
    withoutName: [
      "Good evening! ✨ You arrived just in time.",
      "Good evening! 🌙 Let’s take care of your groceries.",
    ],
  },
  night: {
    withName: [
      "Good night, {name}! 🌙 We are still here to help.",
      "Hi {name}, hope you are having a calm night 🌙",
    ],
    withoutName: [
      "Good night! 🌙 We are still here to help.",
      "Hope you are having a calm night 🌙",
    ],
  },
};

const RETURNING_GREETINGS_EN = {
  withName: [
    "Welcome back, {name}! 🥰 Great to see you again.",
    "Hi {name}, always a pleasure to have you here again! 👋",
    "{name}, welcome back to Super Glasner! 🥳 Your cart is ready for good things.",
  ],
  withoutName: [
    "Welcome back! 🥰 Great to see you again.",
    "Hi, always a pleasure to have you here again! 👋",
    "Welcome back to Super Glasner! 🥳 Your cart is ready for good things.",
  ],
};

function formatGreeting(template, firstName) {
  return cleanText(template).replace(/\{name\}/g, firstName || "");
}

async function getGreetingContext(customer_id, shop_id) {
  const [[customer]] = await db.query(
    `
    SELECT id, name, phone
    FROM customer
    WHERE id = ?
    LIMIT 1
    `,
    [customer_id],
  );

  const [[lastChat]] = await db.query(
    `
    SELECT id, created_at, TIMESTAMPDIFF(SECOND, created_at, NOW(6)) AS seconds_since_last_chat
    FROM chat
    WHERE customer_id = ?
      AND shop_id = ?
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    `,
    [customer_id, shop_id],
  );

  const [[ordersCountRow]] = await db.query(
    `
    SELECT COUNT(*) AS order_count
    FROM orders
    WHERE customer_id = ?
      AND shop_id = ?
    `,
    [customer_id, shop_id],
  );

  return {
    customer: customer || null,
    hasPreviousChat: Boolean(lastChat?.id),
    secondsSinceLastChat: Number(lastChat?.seconds_since_last_chat || 0),
    previousOrderCount: Number(ordersCountRow?.order_count || 0),
  };
}

function buildGreetingFromContext({ context, message, now = new Date() }) {
  const hasPreviousChat = Boolean(context?.hasPreviousChat);
  const secondsSinceLastChat = Number(context?.secondsSinceLastChat || 0);
  const shouldGreet = !hasPreviousChat || secondsSinceLastChat > IDLE_GREETING_SECONDS;

  if (!shouldGreet) return "";

  const customer = context?.customer || null;
  const hasName = customer && !isCustomerNameMissing(customer);
  const firstName = hasName ? getFirstName(customer.name) : "";
  const isEnglish = detectIsEnglish(message);
  const previousOrderCount = Number(context?.previousOrderCount || 0);
  const isReturningCustomer = hasPreviousChat && previousOrderCount > 0;

  const returningBank = isEnglish ? RETURNING_GREETINGS_EN : RETURNING_GREETINGS_HE;
  const timeBank = isEnglish ? TIME_GREETINGS_EN : TIME_GREETINGS_HE;

  if (isReturningCustomer) {
    const options = firstName ? returningBank.withName : returningBank.withoutName;
    return formatGreeting(pickRandom(options), firstName);
  }

  const slot = getTimeSlot(getIsraelHour(now));
  const slotBank = timeBank[slot] || timeBank.morning;
  const options = firstName ? slotBank.withName : slotBank.withoutName;
  return formatGreeting(pickRandom(options), firstName);
}

async function buildConversationGreetingPrefix({ customer_id, shop_id, message }) {
  try {
    const context = await getGreetingContext(customer_id, shop_id);
    return buildGreetingFromContext({ context, message });
  } catch (err) {
    console.error("[conversationGreeting.buildConversationGreetingPrefix]", err?.message || err);
    return "";
  }
}

module.exports = {
  IDLE_GREETING_SECONDS,
  buildConversationGreetingPrefix,
  buildGreetingFromContext,
  getTimeSlot,
};

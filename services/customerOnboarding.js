const db = require("../config/db");
const { detectIsEnglish } = require("../utilities/lang");

function cleanText(value) {
  return String(value || "")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function digitsOnly(value) {
  return String(value || "").replace(/\D/g, "");
}

function normalizeFullName(value) {
  return cleanText(value)
    .replace(/^["'״׳`]+|["'״׳`]+$/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function getFirstName(fullName) {
  const parts = normalizeFullName(fullName).split(/\s+/).filter(Boolean);
  return parts[0] || "";
}

function looksLikeSystemOrPhoneName(name, phone) {
  const value = cleanText(name).toLowerCase();
  if (!value) return true;

  const nameDigits = digitsOnly(value);
  const phoneDigits = digitsOnly(phone);

  if (phoneDigits && nameDigits && nameDigits === phoneDigits) return true;
  if (/^qa_\d+/i.test(value)) return true;
  if (/^[+()\-\s\d]+$/.test(value) && nameDigits.length >= 2) return true;

  return false;
}

function isCustomerNameMissing(customer) {
  if (!customer) return true;
  return looksLikeSystemOrPhoneName(customer.name, customer.phone);
}

const HEBREW_OR_ENGLISH_NAME_TOKEN_RE = /^[\p{L}\p{M}][\p{L}\p{M}'’׳״-]*[\p{L}\p{M}]$/u;
const LETTER_RE = /[\p{L}\p{M}]/gu;

const NON_NAME_PATTERNS = [
  /[?؟!@#$%^&*_+=\[\]{}<>\\/|~`]/,
  /https?:\/\//i,
  /www\./i,
  /\d/,
  /\b(order|cart|basket|delivery|pickup|price|sale|sales|discount|hours|open|closed|cancel|status|milk|bread|eggs|tomato|cucumber)\b/i,
  /(תוסיף|תוסיפי|שים|שימי|רוצה|להזמין|הזמנה|סל|עגלה|משלוח|איסוף|מחיר|כמה|עולה|מבצע|מבצעים|שעות|פתוח|סגור|כתובת|ביטול|סטטוס|חלב|לחם|ביצים|עגבני|מלפפון|כן|לא)(\b|\s|$)/i,
];

function countLetters(token) {
  const matches = token.match(LETTER_RE);
  return matches ? matches.length : 0;
}

function isLikelyFullName(value) {
  const fullName = normalizeFullName(value);
  if (!fullName || fullName.length < 5 || fullName.length > 120) return false;

  for (const pattern of NON_NAME_PATTERNS) {
    if (pattern.test(fullName)) return false;
  }

  const parts = fullName.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 6) return false;

  return parts.every((part) => {
    if (!HEBREW_OR_ENGLISH_NAME_TOKEN_RE.test(part)) return false;
    return countLetters(part) >= 2;
  });
}

async function getCustomerOnboardingState(customer_id) {
  const [[customer]] = await db.query(
    `
    SELECT id, name, phone, pending_message_before_name, name_requested_at, full_name_collected_at
    FROM customer
    WHERE id = ?
    LIMIT 1
    `,
    [customer_id],
  );

  return customer || null;
}

async function getShopName(shop_id) {
  try {
    const [[shop]] = await db.query(
      `SELECT name FROM shop WHERE id = ? LIMIT 1`,
      [shop_id],
    );
    return cleanText(shop?.name) || "חנות";
  } catch (err) {
    console.error("[customerOnboarding.getShopName]", err?.message || err);
    return "חנות";
  }
}

function buildAskFullNameReply({ shopName, isEnglish }) {
  const hebrewPlace = shopName ? `ל${shopName}` : "לחנות";

  if (isEnglish) {
    return [
      `Great to see you at ${shopName || "our store"} 🥳🛒`,
      "Before we open your personal shopping cart, please send your full name.",
      "First name and last name 👇😊",
    ].join("\n");
  }

  return [
    `איזה כיף שהגעת אלינו ${hebrewPlace} 🥳🛒`,
    "כדי שנוכל לפתוח עבורך סל קניות אישי ולשמור את ההזמנה בצורה מסודרת, נשמח לקבל קודם את שמך המלא.",
    "שם פרטי ושם משפחה 👇😊",
  ].join("\n\n");
}

function buildNeedFullNameReply({ isEnglish }) {
  if (isEnglish) {
    return [
      "We will be happy to help, but before opening an order we need your full name 🧑‍💻",
      "Without first name and last name, we cannot open a cart and save the products for you.",
      "Just write your full name here and we will continue right away 🛒🎉",
    ].join("\n\n");
  }

  return [
    "אנחנו ממש נשמח לעזור, אבל לפני פתיחת הזמנה המערכת צריכה שם מלא 🧑‍💻",
    "בלי שם פרטי ושם משפחה לא נוכל לפתוח סל ולשמור את המוצרים עבורך.",
    "פשוט כותבים כאן את השם המלא ומיד ממשיכים 🛒🎉",
  ].join("\n\n");
}

function buildNameSavedPrefix({ fullName, isEnglish }) {
  const firstName = getFirstName(fullName);

  if (isEnglish) {
    return firstName
      ? `Nice to meet you, ${firstName} 😊\nI am opening your cart and adding the products you asked for...`
      : "Nice to meet you 😊\nI am opening your cart and adding the products you asked for...";
  }

  return firstName
    ? `נעים מאוד ${firstName} 😊\nאני פותח לך סל ומוסיף את המוצרים שביקשת...`
    : "נעים מאוד 😊\nאני פותח לך סל ומוסיף את המוצרים שביקשת...";
}

async function shouldRequireNameBeforeOrder(customer_id) {
  const customer = await getCustomerOnboardingState(customer_id);
  return isCustomerNameMissing(customer);
}

async function requestFullNameBeforeOrder({ customer_id, shop_id, message }) {
  const pendingMessage = cleanText(message);
  await db.query(
    `
    UPDATE customer
    SET pending_message_before_name = ?,
        name_requested_at = NOW()
    WHERE id = ?
    LIMIT 1
    `,
    [pendingMessage, customer_id],
  );

  const shopName = await getShopName(shop_id);
  return buildAskFullNameReply({
    shopName,
    isEnglish: detectIsEnglish(pendingMessage),
  });
}

async function handlePendingCustomerName({ customer_id, message }) {
  const customer = await getCustomerOnboardingState(customer_id);
  const pendingMessage = cleanText(customer?.pending_message_before_name);

  if (!pendingMessage) return null;

  const fullName = normalizeFullName(message);
  const isEnglish = detectIsEnglish(message || pendingMessage);

  if (!isLikelyFullName(fullName)) {
    return {
      type: "need_name_reply",
      reply: buildNeedFullNameReply({ isEnglish }),
    };
  }

  await db.query(
    `
    UPDATE customer
    SET name = ?,
        pending_message_before_name = NULL,
        name_requested_at = NULL,
        full_name_collected_at = NOW()
    WHERE id = ?
    LIMIT 1
    `,
    [fullName, customer_id],
  );

  return {
    type: "name_saved",
    fullName,
    pendingMessage,
    prefix: buildNameSavedPrefix({ fullName, isEnglish: detectIsEnglish(pendingMessage) }),
  };
}

module.exports = {
  handlePendingCustomerName,
  isCustomerNameMissing,
  isLikelyFullName,
  requestFullNameBeforeOrder,
  shouldRequireNameBeforeOrder,
};

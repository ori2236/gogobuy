const { detectIsEnglish } = require("./lang");

function toLang(input) {
  if (input === "en" || input === "english") return "en";
  if (input === "he" || input === "hebrew") return "he";
  if (typeof input === "boolean") return input ? "en" : "he";
  return detectIsEnglish(String(input || "")) ? "en" : "he";
}

function isEnglishLang(input) {
  return toLang(input) === "en";
}

function textByLang(input, he, en) {
  return isEnglishLang(input) ? en : he;
}

function formatTemplate(template, params = {}) {
  return String(template || "").replace(/\{(\w+)\}/g, (_, key) => {
    const value = params[key];
    return value === undefined || value === null ? "" : String(value);
  });
}

const BOT_TEXT = {
  modelNoReply: {
    he: "סליחה, הייתה תקלה רגעית 🙏\nאפשר לנסות שוב?",
    en: "Sorry, something went wrong for a moment 🙏\nCan you try again?",
  },
  invalidIntent: {
    he: "סליחה, לא הצלחתי להבין את הבקשה 🙏\nאפשר לכתוב לי שוב בקצרה מה תרצה לעשות?",
    en: "Sorry, I couldn’t understand that 🙏\nCan you briefly write what you’d like to do?",
  },
  unsupportedRequest: {
    he: "מצטערים, עדיין אין לנו תמיכה בבקשה הזו 🙏\nאפשר לנסות לנסח אותה בדרך אחרת?",
    en: "Sorry, this request isn’t supported yet 🙏\nCan you try wording it another way?",
  },
  createParseError: {
    he: "סליחה, הייתה תקלה בעיבוד ההזמנה 🙏\nאפשר לכתוב בקצרה מה תרצה להזמין?",
    en: "Sorry, there was a problem processing the order 🙏\nCan you briefly write what you’d like to order?",
  },
  modifyNoActiveOrder: {
    he: "כדי לערוך הזמנה צריכה להיות הזמנה פתוחה 🛒\nתרצה להתחיל הזמנה חדשה?",
    en: "To edit an order, you need to have an open order 🛒\nWould you like to start a new one?",
  },
  webhookDefaultAck: {
    he: "תודה 🙏",
    en: "Thanks 🙏",
  },
  legacyWhatsappWelcome: {
    he: "שלום וברוכים הבאים 👋\nאיך אפשר לעזור?",
    en: "Hi and welcome 👋\nHow can we help?",
  },
  noActiveOrder: {
    he: [
      "אין לך הזמנה פתוחה כרגע 🛒",
      "אם הכוונה להזמנה הקודמת — היא כבר בוטלה אוטומטית והמוצרים חזרו למלאי.",
      "כדי להתחיל הזמנה חדשה, פשוט כתוב את המוצרים שתרצה להזמין.",
    ].join("\n"),
    en: [
      "There is no open order right now 🛒",
      "If you mean the previous order, it was already cancelled automatically and the products were returned to stock.",
      "To start a new order, just write the products you’d like to buy.",
    ].join("\n"),
  },
};

function botText(key, langOrIsEnglish, params = {}) {
  const lang = toLang(langOrIsEnglish);
  const entry = BOT_TEXT[key];
  if (!entry) return "";
  return formatTemplate(entry[lang] || entry.he || "", params);
}

function isEnglishFromCustomerName(customerName) {
  return detectIsEnglish(String(customerName || ""));
}

module.exports = {
  botText,
  formatTemplate,
  isEnglishFromCustomerName,
  isEnglishLang,
  textByLang,
  toLang,
};

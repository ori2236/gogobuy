const crypto = require("crypto");
const {
  sendWhatsAppText,
  sendWhatsAppTypingIndicator,
} = require("../utilities/whatsapp");

const SLOW_INTENTS = new Set([
  "ORD.CREATE",
  "ORD.MODIFY",
  "INV.AVAIL",
  "INV.PRICE_AND_SALES",
]);

const PROGRESS_TEXTS = {
  he: {
    "ORD.CREATE": [
      "🛒 מסדר עבורך את ההזמנה ומזהה כמויות לכל מוצר.",
      "🛒 בודק את המוצרים שביקשת מול המלאי בחנות.",
      "🛒 מעדכן את ההזמנה לפי המחירים והמבצעים בחנות.",
      "🛒 מרכיב את הסל ומוודא שכל הפריטים מתאימים להזמנה.",
      "🛒 מתאים את המוצרים בקטלוג למה שכתבת כדי לדייק את ההזמנה.",
      "🛒 מאשר זמינות ומוסיף את הפריטים להזמנה.",
    ],
    "ORD.MODIFY": [
      "✏️ מעדכן את ההזמנה הקיימת לפי מה שביקשת.",
      "✏️ מעדכן מוצרים וכמויות ובודק זמינות במלאי.",
      "✏️ מזהה מה התווסף ומה הוסר ומעדכן את ההזמנה.",
      "✏️ מבצע את השינויים ומוודא שהמלאי מעודכן.",
      "✏️ מסנכרן את ההזמנה עם המחירים והמבצעים העדכניים.",
      "✏️ מאתר את הפריטים החדשים שציינת ומוודא זמינות.",
    ],
    "INV.AVAIL": [
      "🔎 בודק אילו אפשרויות זמינות במלאי כרגע.",
      "🔎 בודק את המוצר בחנות ומוודא כמות זמינה.",
      "🔎 מאתר את הסוגים הזמינים כרגע ומחזיר לך רשימה.",
      "🔎 בודק זמינות לפי סוג ומחזיר את האפשרויות שיש לנו.",
      "🔎 מאתר את המוצר ומוודא שהוא זמין לפני המענה.",
      "🔎 בודק מלאי כדי לוודא שהתשובה מדויקת.",
    ],
    "INV.PRICE_AND_SALES": [
      "💸 בודק מחיר ומבצעים על המוצר שביקשת.",
      "💸 בודק את האפשרויות הרלוונטיות ומחשב מה משתלם יותר.",
      "💸 בודק מחיר עדכני ומחשב את האפשרות המשתלמת ביותר.",
      "💸 בודק אם יש מבצע פעיל ומחשב מה יוצא הכי כדאי.",
      "💸 מאתר את המוצר בחנות ובודק מחיר עדכני.",
      "💸 עובר על מחירים ומבצעים כדי לתת תשובה מדויקת.",
    ],
  },

  en: {
    "ORD.CREATE": [
      "I am setting up your order and reading the requested quantities.",
      "I am checking the requested items against the store inventory.",
      "I am matching the products and verifying availability.",
      "I am building your cart and making sure the items fit the order.",
      "I am matching your request to the products in the catalog.",
      "I am checking prices and active deals before adding the items.",
    ],
    "ORD.MODIFY": [
      "I am updating your current order with the requested changes.",
      "I am updating items and quantities and checking inventory.",
      "I am identifying what was added, changed, or removed.",
      "I am applying the update and verifying current stock.",
      "I am syncing the order with the latest prices and deals.",
      "I am locating the new items you mentioned and confirming stock.",
    ],
    "INV.AVAIL": [
      "I am checking which options are currently available in stock.",
      "I am finding the item in the store and confirming the available quantity.",
      "I am pulling the available types currently in stock.",
      "I am checking availability by type and variant to keep the list accurate.",
      "I am verifying stock before I reply.",
      "I am checking what we have right now so the answer is precise.",
    ],
    "INV.PRICE_AND_SALES": [
      "I am checking the current price and any active promotion for that item.",
      "I am comparing the relevant options and calculating the best value.",
      "I am checking current prices and calculating the best value within your budget.",
      "I am checking deals and calculating the most cost-effective option.",
      "I am locating the product and pulling the latest price.",
      "I am reviewing prices and promotions to give an accurate answer.",
    ],
  },
};

function isSlowIntent(category, subcategory) {
  const key = `${String(category).toUpperCase()}.${String(subcategory).toUpperCase()}`;
  return SLOW_INTENTS.has(key);
}

function progressEmojiForIntentKey(key) {
  if (key === "ORD.CREATE") return "🛒";
  if (key === "ORD.MODIFY") return "✏️";
  if (key === "INV.AVAIL") return "🔎";
  if (key === "INV.PRICE_AND_SALES") return "💸";
  return "⏳";
}

function withProgressEmoji(text, key) {
  const clean = String(text || "").trim();
  if (!clean) return clean;
  if (/^[\p{Emoji_Presentation}\p{Extended_Pictographic}]/u.test(clean)) {
    return clean;
  }
  return `${progressEmojiForIntentKey(key)} ${clean}`;
}

function createSessionPicker(category, subcategory, isEnglish) {
  const key = `${String(category).toUpperCase()}.${String(subcategory).toUpperCase()}`;
  const lang = isEnglish ? "en" : "he";
  const arr = PROGRESS_TEXTS?.[lang]?.[key] || [];

  const fallback = isEnglish
    ? "One moment, I am checking that for you."
    : "רגע אחד, אני בודק את זה.";

  if (!arr.length) return () => withProgressEmoji(fallback, key);
  if (arr.length === 1) return () => withProgressEmoji(arr[0], key);

  let bag = [];
  const refill = () => {
    bag = arr.slice();
    for (let i = bag.length - 1; i > 0; i--) {
      const j = crypto.randomInt(0, i + 1);
      [bag[i], bag[j]] = [bag[j], bag[i]];
    }
  };

  refill();

  return () => {
    if (bag.length === 0) refill();
    return withProgressEmoji(bag.pop(), key);
  };
}

function startSlowProgression({
  category,
  subcategory,
  isEnglish,
  phone_number,
  waMessageId,
  receivedAt,
  businessPhoneNumberId = "",
  typingAtMs = 2000,
  progressEveryMs = 8000,
}) {
  let cancelled = false;
  let typingTimer = null;
  let progressTimeout = null;
  let progressInterval = null;

  const stop = () => {
    cancelled = true;
    if (typingTimer) clearTimeout(typingTimer);
    if (progressTimeout) clearTimeout(progressTimeout);
    if (progressInterval) clearInterval(progressInterval);
  };

  if (!isSlowIntent(category, subcategory)) return stop;

  const nextProgress = createSessionPicker(category, subcategory, isEnglish);
  const elapsed = Date.now() - (receivedAt || Date.now());

  const firstProgressDelay = Math.max(0, progressEveryMs - elapsed);

  if (waMessageId) {
    const typingDelay = Math.max(0, typingAtMs - elapsed);
    if (typingDelay < firstProgressDelay) {
      typingTimer = setTimeout(() => {
        if (cancelled) return;
        sendWhatsAppTypingIndicator(waMessageId, businessPhoneNumberId).catch(
          (e) => console.error(`[wa typing]`, e?.response?.data || e),
        );
      }, typingDelay);
    }
  }

  let sent = 0;
  const maxProgressMessages = 4;

  const sendProgressOnce = () => {
    if (cancelled) return;
    if (sent >= maxProgressMessages) return;
    sent++;

    const text = nextProgress();
    sendWhatsAppText(phone_number, text, businessPhoneNumberId).catch((e) =>
      console.error(`[wa progress]`, e?.response?.data || e),
    );
  };

  progressTimeout = setTimeout(() => {
    if (cancelled) return;

    sendProgressOnce();

    progressInterval = setInterval(() => {
      sendProgressOnce();
    }, progressEveryMs);
  }, firstProgressDelay);

  return stop;
}

module.exports = {
  isSlowIntent,
  startSlowProgression,
};

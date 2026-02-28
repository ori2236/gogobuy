// services/sendProgressionMessage.js
const crypto = require("crypto");

const SLOW_INTENTS = new Set([
  "ORD.CREATE",
  "ORD.MODIFY",
  "INV.AVAIL",
  "INV.PRICE_AND_SALES",
]);

const PROGRESS_TEXTS = {
  he: {
    "ORD.CREATE": [
      "רגע אחד, אני בונה עבורך הזמנה חדשה ומזהה כמויות לכל מוצר.",
      "אני מאתר את המוצרים שביקשת בחנות ובודק מה זמין במלאי.",
      "תן לי כמה שניות, אני מצליב את הבקשה מול המחירים והמבצעים בחנות.",
      "אני מרכיב את הסל ומחפש חלופות מתאימות אם משהו חסר.",
      "שנייה, אני מתאים את המוצרים בקטלוג למה שכתבת כדי לדייק את ההזמנה.",
      "עוד רגע, אני מאשר זמינות ומוסיף את הפריטים להזמנה.",
    ],
    "ORD.MODIFY": [
      "רגע אחד, אני פותח את ההזמנה הקיימת ומיישם את השינויים שביקשת.",
      "אני מעדכן מוצרים וכמויות לפי ההודעה שלך ובודק זמינות במלאי.",
      "תן לי כמה שניות, אני מזהה מה התווסף ומה הוסר ומעדכן בהתאם.",
      "אני מבצע את השינויים ומחפש חלופה אם מוצר לא זמין כרגע.",
      "עוד רגע, אני מסנכרן את ההזמנה עם המחירים והמבצעים העדכניים.",
      "שנייה, אני מאתר את הפריטים החדשים שציינת ומוודא שאין חוסרים.",
    ],
    "INV.AVAIL": [
      "רגע אחד, אני בודק אילו אפשרויות זמינות במלאי כרגע.",
      "אני מחפש את המוצר בחנות ומוודא שיש ממנו כמות זמינה.",
      "תן לי שנייה, אני מוצא את הסוגים שיש כרגע ומחזיר לך רשימה.",
      "אני בודק זמינות לפי מותג וסוג ומחזיר את האפשרויות שיש לנו.",
      "עוד רגע, אני מאתר את המוצר ומוודא שהוא זמין לפני שאני עונה.",
      "אני בודק מלאי כדי לוודא שהתשובה מדויקת.",
    ],
    "INV.PRICE_AND_SALES": [
      "רגע אחד, אני בודק מחיר ומבצעים על המוצר שביקשת.",
      "אני משווה מחירים בין האפשרויות הרלוונטיות ומחזיר את המשתלם יותר.",
      "תן לי כמה שניות, אני מחפש אלטרנטיבה זולה יותר באותה קטגוריה.",
      "אני בודק אם יש מבצע פעיל ומחשב מה יוצא הכי כדאי.",
      "עוד רגע, אני מאתר את המוצר בחנות ובודק מחיר עדכני.",
      "אני עובר על מחירים ומבצעים כדי לתת תשובה מדויקת.",
    ],
  },

  en: {
    "ORD.CREATE": [
      "One moment, I am creating a new order and parsing the quantities you requested.",
      "I am locating the requested items in the store and checking what is currently in stock.",
      "Give me a few seconds while I match products and verify availability.",
      "I am building your cart and looking for suitable alternatives if something is missing.",
      "One moment, I am matching your request to the closest products in the catalog.",
      "Just a second, I am checking prices and any active deals before adding the items.",
    ],
    "ORD.MODIFY": [
      "One moment, I am opening your current order and applying the changes you requested.",
      "I am updating items and quantities and verifying availability in stock.",
      "Give me a few seconds while I identify what was added, changed, or removed.",
      "I am applying the update and will suggest an alternative if an item is not available.",
      "Just a moment, I am syncing the order with the latest prices and deals.",
      "One second, I am locating any new items you mentioned and confirming stock.",
    ],
    "INV.AVAIL": [
      "One moment, I am checking which options are currently available in stock.",
      "I am finding the item in the store and confirming the available quantity.",
      "Give me a second while I pull the available types and brands.",
      "I am checking availability by brand and variant to make sure the list is accurate.",
      "One moment, I am verifying stock before I reply.",
      "Just a second, I am checking what we have right now so the answer is precise.",
    ],
    "INV.PRICE_AND_SALES": [
      "One moment, I am checking the current price and any active promotion for that item.",
      "I am comparing the relevant options and finding the cheaper one.",
      "Give me a few seconds while I look for a cheaper alternative within your budget.",
      "I am checking for deals and calculating the most cost-effective option.",
      "One moment, I am locating the product and pulling the latest price.",
      "Just a second, I am reviewing prices and promotions to give an accurate answer.",
    ],
  },
};

function isSlowIntent(category, subcategory) {
  const key = `${String(category).toUpperCase()}.${String(subcategory).toUpperCase()}`;
  return SLOW_INTENTS.has(key);
}

function pickRandom(arr) {
  if (!arr || arr.length === 0) return null;
  const idx = crypto.randomInt(0, arr.length);
  return arr[idx];
}

function pickProgressText(category, subcategory, isEnglish) {
  const key = `${String(category).toUpperCase()}.${String(subcategory).toUpperCase()}`;
  const lang = isEnglish ? "en" : "he";

  const arr = PROGRESS_TEXTS?.[lang]?.[key];

  const fallback = isEnglish
    ? "One moment, I am checking that for you."
    : "רגע אחד, אני בודק את זה.";

  return pickRandom(arr) || fallback;
}

module.exports = { isSlowIntent, pickProgressText };

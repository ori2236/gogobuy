require("dotenv").config();
const db = require("../config/db");

const SPECIFIC_EMOJI_RULES = [
  // Fruits & Vegetables
  { pattern: /\b(עגבנייה|עגבניה|עגבניות|שרי)\b/i, emoji: "🍅", category: "עגבניות" },
  { pattern: /\b(מלפפון|מלפפונים)\b/i, emoji: "🥒", category: "מלפפונים" },
  { pattern: /\b(בצל|בצלים)\b/i, emoji: "🧅", category: "בצלים" },
  { pattern: /\b(תפוח אדמה|תפוחי אדמה|בטטה|בטטות)\b/i, emoji: "🥔", category: "תפוחי אדמה ובטטות" },
  { pattern: /\b(גזר|גזרים)\b/i, emoji: "🥕", category: "גזר" },
  { pattern: /\b(חסה|עלים|רוקולה|פטרוזיליה|כוסברה|שמיר|נענע|תרד|סלרי)\b/i, emoji: "🥬", category: "עלים וירק" },
  { pattern: /\b(שום)\b/i, emoji: "🧄", category: "שום" },
  { pattern: /\b(אבטיח|אבטיחים)\b/i, emoji: "🍉", category: "אבטיח" },
  { pattern: /\b(מנגו)\b/i, emoji: "🥭", category: "מנגו" },
  { pattern: /\b(בננה|בננות)\b/i, emoji: "🍌", category: "בננות" },
  { pattern: /\b(תפוח|תפוחים)\b/i, emoji: "🍎", category: "תפוחים" },
  { pattern: /\b(ענבים)\b/i, emoji: "🍇", category: "ענבים" },
  { pattern: /\b(פטריות|פטריה|פורטובלו|שמפיניון)\b/i, emoji: "🍄", category: "פטריות" },
  { pattern: /\b(לימון|לימונים)\b/i, emoji: "🍋", category: "לימון" },

  // Bakery & Breads
  { pattern: /\b(לחם|חלה|חלות)\b/i, emoji: "🍞", category: "לחמים" },
  { pattern: /\b(לחמניה|לחמניות|בייגל)\b/i, emoji: "🥯", category: "לחמניות ובייגל" },
  { pattern: /\b(פיתה|פיתות|סלוף|לאפה)\b/i, emoji: "🫓", category: "פיתות ולאפות" },
  { pattern: /\b(עוגה|עוגות|קראנצ'|טורט)\b/i, emoji: "🍰", category: "עוגות" },
  { pattern: /\b(עוגיה|עוגיות|ביסקויט)\b/i, emoji: "🍪", category: "עוגיות" },
  { pattern: /\b(רוגעלך|קרואסון)\b/i, emoji: "🥐", category: "מאפים" },

  // Dairy & Refrigerated
  { pattern: /\b(חלב)\b/i, emoji: "🥛", category: "חלב" },
  { pattern: /\b(גבינה|גבינות|קוטג'|מוצרלה|צהובה|בולגרית|פטה|שמנת)\b/i, emoji: "🧀", category: "גבינות ושמנת" },
  { pattern: /\b(חמאה)\b/i, emoji: "🧈", category: "חמאה" },
  { pattern: /\b(ביצים|ביצה)\b/i, emoji: "🥚", category: "ביצים" },

  // Meat, Poultry & Fish
  { pattern: /\b(בשר|סטייק|אנטריקוט|צלעות|טחון בקר)\b/i, emoji: "🥩", category: "בשר בקר" },
  { pattern: /\b(עוף|פרגית|פרגיות|כנפיים|חזה עוף|כרעיים|חוקים)\b/i, emoji: "🍗", category: "עוף וחלקי עוף" },
  { pattern: /\b(שניצל|שניצלים)\b/i, emoji: "🍗", category: "שניצלים" },
  { pattern: /\b(נקניק|נקניקיות|סלמי|פסטרמה)\b/i, emoji: "🌭", category: "נקניקים" },
  { pattern: /\b(דג|דגים|סלמון|טונה|אמנון|נסיכת הנילוס|דניס)\b/i, emoji: "🐟", category: "דגים" },

  // Sweets, Snacks & Ice Creams
  { pattern: /\b(גלידה|גלידות|בן&ג'ריס|קרמסימו|קופסת גלידה)\b/i, emoji: "🍨", category: "גלידות" },
  { pattern: /\b(טילון|טילונים|ארטיק|ארטיקים)\b/i, emoji: "🍦", category: "ארטיקים וטילונים" },
  { pattern: /\b(שלוקים|איגלו)\b/i, emoji: "🧊", category: "שלוקים" },
  { pattern: /\b(שוקולד|חפיסת שוקולד|טורטית|טוויסט|פסק זמן|מקופלת)\b/i, emoji: "🍫", category: "שוקולד" },
  { pattern: /\b(במבה|ביסלי|תפוצ'יפס|חטיף|דורטוס|אפרופו|קראנץ')\b/i, emoji: "🍿", category: "חטיפים" },
  { pattern: /\b(סוכריה|סוכריות|גומיי|מסטיק|סוכריות גומי)\b/i, emoji: "🍬", category: "סוכריות ומסטיקים" },

  // Beverages & Alcohol
  { pattern: /\b(קולה|זירו|ספרייט|פנטה|בלו|אקסל|XL|XL Energy|משקה אנרגיה|סודה)\b/i, emoji: "🥤", category: "שתייה קלה ומוגזת" },
  { pattern: /\b(מיץ|נקטר|פריגת|ספרינג)\b/i, emoji: "🧃", category: "מיצים" },
  { pattern: /\b(מים|מים מינרליים|נביעות|מי עדן|סן פלגרינו)\b/i, emoji: "💧", category: "מים" },
  { pattern: /\b(בירה|בירות|גולדסטאר|היינכן|קרלסברג|טובורג)\b/i, emoji: "🍺", category: "בירה" },
  { pattern: /\b(יין|יינות|קברנה|מרלו|שרדונה)\b/i, emoji: "🍷", category: "יין" },

  // Household & Pharmacy
  { pattern: /\b(טישו|נייר טואלט|גלילי נייר|מגבוני נייר)\b/i, emoji: "🧻", category: "נייר וטישו" },
  { pattern: /\b(כפפות|כפפות אלסטיות|ספוג|אופס)\b/i, emoji: "🧽", category: "כפפות וספוגים" },
  { pattern: /\b(סבון|סבון ידיים|סבון גוף)\b/i, emoji: "🧼", category: "סבון" },
  { pattern: /\b(שמפו|מרכך|מרכך שיער|ג'ל רחצה)\b/i, emoji: "🧴", category: "טואלטיקה ושיער" },
  { pattern: /\b(אבקת כביסה|מרכך כביסה|ג'ל כביסה)\b/i, emoji: "🧺", category: "כביסה" },
  { pattern: /\b(טיטולים|חיתולים|חיתולי)\b/i, emoji: "🧷", category: "חיתולים" },
  { pattern: /\b(מגבונים|מגבונים לחים)\b/i, emoji: "👶", category: "מגבונים" }
];

async function run() {
  console.log("====================================================");
  console.log("🔎 Accurate Product Emoji Analysis for Shop ID 2");
  console.log("====================================================\n");

  const [products] = await db.query(`
    SELECT id, name, emoji
    FROM product
    WHERE shop_id = 2
  `);

  console.log(`Analyzing ${products.length} active products...\n`);

  const recommendations = [];

  for (const p of products) {
    const name = String(p.name || "").trim();
    const currentEmoji = p.emoji ? String(p.emoji).trim() : "ללא (NULL)";

    for (const rule of SPECIFIC_EMOJI_RULES) {
      if (rule.pattern.test(name)) {
        if (currentEmoji !== rule.emoji) {
          recommendations.push({
            id: p.id,
            name,
            currentEmoji,
            suggestedEmoji: rule.emoji,
            category: rule.category
          });
        }
        break;
      }
    }
  }

  console.log(`Found ${recommendations.length} high-confidence emoji improvement recommendations!\n`);

  // Group by category
  const byCategory = new Map();
  for (const rec of recommendations) {
    if (!byCategory.has(rec.category)) byCategory.set(rec.category, []);
    byCategory.get(rec.category).push(rec);
  }

  for (const [catName, list] of byCategory.entries()) {
    console.log(`--- ${catName} (${list.length} מוצרים) ---`);
    for (const item of list.slice(0, 10)) { // show sample
      console.log(`  • ID #${item.id} | "${item.name}" | נוכחי: ${item.currentEmoji} ➔ מוצע: ${item.suggestedEmoji}`);
    }
    if (list.length > 10) {
      console.log(`  ...ועוד ${list.length - 10} מוצרים בקטגוריה זו`);
    }
    console.log("");
  }

  process.exit(0);
}

run().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});

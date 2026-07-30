require("dotenv").config();
const db = require("../config/db");

async function run() {
  console.log("====================================================");
  console.log("🔍 Analyzing Product Emojis in Database");
  console.log("====================================================\n");

  const [products] = await db.query(`
    SELECT id, name, emoji
    FROM product
    WHERE shop_id = 2
  `);

  console.log(`Total products for Shop 2: ${products.length}`);

  const missingEmoji = [];
  const genericEmoji = [];
  const suspiciousEmoji = [];

  const genericEmojis = new Set(["📦", "🏷️", "❓", "🛒", "🛍️", "✨", "⭐"]);

  const rules = [
    { keywords: ["גלידת", "גלידה", "קרמסימו", "בן&ג'ריס", "טילון", "ארטיק", "שלוקים"], expected: ["🍨", "🍦", "🧊"], name: "גלידות ושלוקים" },
    { keywords: ["חלב", "גבינה", "יוגורט", "קוטג'", "שמנת", "חמאה"], expected: ["🥛", "🧀", "🧈"], name: "מוצרי חלב" },
    { keywords: ["בשר", "עוף", "פרגית", "שניצל", "נקניק", "המבורגר", "סטייק"], expected: ["🥩", "🍗", "🍖"], name: "בשר ועוף" },
    { keywords: ["דג", "סלמון", "טונה", "אמנון"], expected: ["🐟", "🎣", "🍣"], name: "דגים" },
    { keywords: ["לחם", "פיתה", "לחמניה", "חלה", "בגט"], expected: ["🍞", "🥖", "🥯"], name: "מאפים ולחמים" },
    { keywords: ["מים", "קולה", "זירו", "סודה", "מיץ", "פנטה", "ספרייט", "בלו", "אקסל", "XL"], expected: ["🥤", "🧃", "💧", "🍾"], name: "שתייה קלה" },
    { keywords: ["בירה", "יין", "וודקה", "ערק", "וויסקי"], expected: ["🍺", "🍷", "🍾", "🥃"], name: "אלכוהול" },
    { keywords: ["שוקולד", "חטיף", "במבה", "ביסלי", "תפוצ'יפס", "סוכריה", "מסטיק", "עוגיה", "עוגה"], expected: ["🍫", "🍿", "🍪", "🍬", "🍰"], name: "מתוקים וחטיפים" },
    { keywords: ["טיטולים", "חיתולים", "מגבונים"], expected: ["🧷", "👶"], name: "מוצרי תינוקות" },
    { keywords: ["נייר גלגול", "פילטר", "טבק", "סיגריות", "מצית"], expected: ["📄", "🚬", "🔥"], name: "עישון" },
    { keywords: ["טישו", "נייר טואלט", "כפפות", "אקונומיקה", "סבון", "שמפו", "מרכך", "אבקת כביסה", "תבניות"], expected: ["🧻", "🧽", "🧼", "🧴", "🧹"], name: "פארם וניקיון" },
    { keywords: ["עגבניה", "מלפפון", "בצל", "תפוח אדמה", "גזר", "גמבה", "חסה", "פלפל", "שום", "אבטיח", "מנגו", "בננה", "תפוח", "ענבים", "ירק"], expected: ["🍅", "🥒", "🧅", "🥔", "🥕", "🫑", "🥬", "🧄", "🍉", "🥭", "🍌", "🍎", "🍇"], name: "פירות וירקות" }
  ];

  for (const p of products) {
    const name = String(p.name || "").trim();
    const emoji = p.emoji ? String(p.emoji).trim() : null;

    if (!emoji) {
      missingEmoji.push(p);
      continue;
    }

    if (genericEmojis.has(emoji)) {
      genericEmoji.push(p);
      continue;
    }

    for (const rule of rules) {
      const matchKey = rule.keywords.some(k => name.includes(k));
      if (matchKey) {
        const emojiOk = rule.expected.includes(emoji);
        if (!emojiOk) {
          suspiciousEmoji.push({
            product: p,
            ruleName: rule.name,
            currentEmoji: emoji,
            suggestedEmoji: rule.expected[0]
          });
          break;
        }
      }
    }
  }

  console.log(`\n====================================================`);
  console.log(`📊 EMOJI AUDIT SUMMARY`);
  console.log(`====================================================`);
  console.log(`Products without Emoji:                   ${missingEmoji.length}`);
  console.log(`Products with Generic Emoji (📦/🏷️):       ${genericEmoji.length}`);
  console.log(`Products with Mismatched/Suspicious Emoji: ${suspiciousEmoji.length}`);
  console.log(`====================================================\n`);

  if (suspiciousEmoji.length > 0) {
    console.log(`⚠️  RECOMMENDED EMOJI CORRECTIONS (${suspiciousEmoji.length} products):`);
    for (const s of suspiciousEmoji) {
      console.log(`• ID #${s.product.id} | "${s.product.name}" | Current: ${s.currentEmoji} ➔ Proposed: ${s.suggestedEmoji} (${s.ruleName})`);
    }
  }

  process.exit(0);
}

run().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});

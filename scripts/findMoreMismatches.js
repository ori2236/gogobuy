require("dotenv").config();
const db = require("../config/db");

const MORE_RULES = [
  // 1. Fresh & Pickled Cucumbers (מלפפון)
  {
    category: "מלפפונים",
    pattern: /מלפפון|מלפפונים/i,
    exclude: /דאורדורנט|דאב|קרמה|סטיק|רולאון|תרחיץ|קרם גוף/i,
    targetEmoji: "🥒",
    badEmojis: ["🍅", "🫒", "🥛", "📄", "🍺", "📦", "🏷️", "❓"]
  },

  // 2. Carrots (גזר)
  {
    category: "גזר",
    pattern: /גזר|גזרים/i,
    exclude: /עוגת|מיץ|סחוט/i,
    targetEmoji: "🥕",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 3. Lettuce & Greens (חסה / עלי רוקולה / פטרוזיליה / כוסברה / שמיר / נענע / תרד / סלרי)
  {
    category: "עלים וירק",
    pattern: /חסה|רוקולה|פטרוזיליה|כוסברה|שמיר|נענע|תרד|סלרי|בצל ירוק|עלי בייבי/i,
    exclude: /סבון|חליטת|תה/i,
    targetEmoji: "🥬",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 4. Peppers & Gamba (פלפל / גמבה)
  {
    category: "פלפלים וגמבה",
    pattern: /פלפל אדום|פלפל צהוב|פלפל ירוק|פלפל גמבה|גמבה אדומה|גמבה צהובה|גמבה/i,
    exclude: /מלח|תבלין|טחון|שחור/i,
    targetEmoji: "🫑",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },
  {
    category: "פלפל חריף",
    pattern: /פלפל חריף|פלפל ירוק חריף|פלפל אדום חריף/i,
    targetEmoji: "🌶️",
    badEmojis: ["🍅", "🫑", "🥛", "📄", "🍺"]
  },

  // 5. Apples (תפוחים)
  {
    category: "תפוחים",
    pattern: /תפוח עץ|תפוח פינק|תפוח גרני|תפוח חרמון|תפוח זהוב|תפוח אדום|תפוח ירוק/i,
    exclude: /מיץ|סוכריות|גרבר|ויטמינצ'יק|תפוח אדמה/i,
    targetEmoji: "🍎",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 6. Watermelon (אבטיח)
  {
    category: "אבטיח",
    pattern: /אבטיח|אבטיחים/i,
    exclude: /מסטיק|סוכריה|נביעות|משקה|גלידת/i,
    targetEmoji: "🍉",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 7. Mango (מנגו)
  {
    category: "מנגו",
    pattern: /מנגו/i,
    exclude: /מיץ|ספרינג|פריגת|יוגורט|גלידה|מילקשייק|סבון/i,
    targetEmoji: "🥭",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 8. Bananas (בננה)
  {
    category: "בננות",
    pattern: /בננה|בננות/i,
    exclude: /גרבר|יוגורט|גלידה|חטיף|מילקשייק|סוכריות/i,
    targetEmoji: "🍌",
    badEmojis: ["🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 9. Grapes (ענבים)
  {
    category: "ענבים",
    pattern: /ענבים ירוקים|ענבים שחורים|ענבים אדומים|ענבים/i,
    exclude: /מיץ|נביעות|סוכריה|יין/i,
    targetEmoji: "🍇",
    badEmojis: ["🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 10. Citrus (תפוז / קלמנטינה / אשכולית)
  {
    category: "הדרים",
    pattern: /תפוז|תפוזים|קלמנטינה|קלמנטינות|אשכולית|אשכוליות/i,
    exclude: /מיץ|פריגת|ספרינג|קליפות|סבון/i,
    targetEmoji: "🍊",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 11. Lemons (לימון)
  {
    category: "לימונים",
    pattern: /לימון ארוז|לימון בתפזורת|לימון טרי|לימונים/i,
    exclude: /מיץ|ספרינג|פריגת|נענע|תרחיץ|סבון|מנקה/i,
    targetEmoji: "🍋",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 12. Avocado (אבוקדו)
  {
    category: "אבוקדו",
    pattern: /אבוקדו/i,
    exclude: /שמפו|מרכך|סבון|סלט אבוקדו/i,
    targetEmoji: "🥑",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 13. Corn (תירס טרי / קלחים)
  {
    category: "תירס",
    pattern: /קלוחי תירס|תירס טרי|קלח תירס|תירס קלחים/i,
    targetEmoji: "🌽",
    badEmojis: ["🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 14. Fresh Mushrooms (פטריות טריות)
  {
    category: "פטריות",
    pattern: /פטריות שמפיניון|פטריות פורטובלו|פטריות ירדן|פטריות יער|פטריות ארוזות|פטריות/i,
    exclude: /שימורי/i,
    targetEmoji: "🍄",
    badEmojis: ["🍅", "🥛", "📄", "🍺", "📦", "🏷️"]
  },

  // 15. Olives (זיתים בצנצנת/קופסה)
  {
    category: "זיתים",
    pattern: /זיתים ירוקים|זיתים שחורים|זיתים מבוקעים|זיתי קלמטה|זיתים דפוקים|זיתים שלמים|זיתים חתוכים/i,
    exclude: /שמן זית/i,
    targetEmoji: "🫒",
    badEmojis: ["🥫", "🥛", "📄", "🍺", "📦", "🏷️"]
  }
];

async function main() {
  console.log("====================================================");
  console.log("🔍 Scanning Produce & Fruit Emoji Mismatches");
  console.log("====================================================\n");

  const [products] = await db.query(`
    SELECT id, shop_id, name, emoji
    FROM product
    ORDER BY shop_id ASC, id ASC
  `);

  console.log(`Auditing ${products.length} total products...\n`);

  const recommendations = [];

  for (const p of products) {
    const name = String(p.name || "").trim();
    const currentEmoji = p.emoji ? String(p.emoji).trim() : "NULL";

    for (const rule of MORE_RULES) {
      if (rule.exclude && rule.exclude.test(name)) continue;

      if (rule.pattern.test(name)) {
        if (currentEmoji !== rule.targetEmoji) {
          recommendations.push({
            id: p.id,
            shop_id: p.shop_id,
            name,
            category: rule.category,
            currentEmoji,
            suggestedEmoji: rule.targetEmoji
          });
          break;
        }
      }
    }
  }

  console.log(`Found ${recommendations.length} produce & fruit emoji recommendations!\n`);

  const byCat = {};
  for (const item of recommendations) {
    if (!byCat[item.category]) byCat[item.category] = [];
    byCat[item.category].push(item);
  }

  for (const cat in byCat) {
    console.log(`=== קטגוריה: ${cat} (${byCat[cat].length} מוצרים) ===`);
    for (const item of byCat[cat]) {
      console.log(`  • ID #${item.id} (חנות ${item.shop_id}) | "${item.name}" | לפני: ${item.currentEmoji} ➔ מוצע: ${item.suggestedEmoji}`);
    }
    console.log("");
  }

  process.exit(0);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});

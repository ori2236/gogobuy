require("dotenv").config();
const db = require("../config/db");

const CORRECTION_RULES = [
  // 1. Garlic (שום) having Tomato (🍅) or Salt (🧂) -> Garlic (🧄)
  {
    name: "שום (שינוי מעגבנייה/מלח לשום)",
    pattern: /\bשום\b/i,
    exclude: /רוטב|תבלין/i,
    targetEmoji: "🧄",
    badEmojis: ["🍅", "🧂", "📦", "🏷️", "❓"]
  },

  // 2. Baguette (בגט) having Bread (🍞) or Generic -> Baguette (🥖)
  {
    name: "בגט (שינוי מלחם לבגט)",
    pattern: /\bבגט|\bבאגט/i,
    targetEmoji: "🥖",
    badEmojis: ["🍞", "📦", "🏷️", "❓", "🛒"]
  },

  // 3. Pretzels / Beigel (בייגלה) having Popcorn (🍿) or Generic -> Pretzel (🥨)
  {
    name: "בייגלה / בייגלה שטוחים (שינוי מפופקורן לבייגלה)",
    pattern: /\bבייגלה\b/i,
    exclude: /עוגיות|שוקולד/i,
    targetEmoji: "🥨",
    badEmojis: ["🍿", "📦", "🏷️", "❓", "🍞"]
  },

  // 4. Baby Wipes (מגבונים לחים) having Toilet Paper (🧻) -> Baby Wipes (👶)
  {
    name: "מגבונים לחים (שינוי מנייר טואלט למגבונים)",
    pattern: /מגבונים לחים/i,
    targetEmoji: "👶",
    badEmojis: ["🧻", "🛁", "📦", "🏷️"]
  },

  // 5. Fresh Milk (חלב) having Soft Drink (🥤) or Generic -> Milk (🥛)
  {
    name: "חלב טרי / עמיד (שינוי משתייה קלה לחלב)",
    pattern: /^חלב\b|\bחלב\s+(תנובה|טרה|יטבתה|3%|1%|עמיד|עיזים|סויה|שיבולת שועל|שקדים)/i,
    exclude: /שוקולד|שוקולית|מילקי|עוגיות/i,
    targetEmoji: "🥛",
    badEmojis: ["🥤", "📦", "🏷️", "❓"]
  },

  // 6. Mineral Water (מים מינרליים) having Soft Drink (🥤) -> Water (💧)
  {
    name: "מים מינרליים / סודה (שינוי משתייה קלה למים)",
    pattern: /מים מינרליים|מי עדן|נביעות|סן פלגרינו|\bסודה\b/i,
    exclude: /מיץ|ספרינג|פריגת|קולה/i,
    targetEmoji: "💧",
    badEmojis: ["🥤", "📦", "🏷️", "❓"]
  },

  // 7. Ice Cream Tubs (גלידה / קרמסימו / בן & ג'ריס) having Soft Drink (🥤) or Cookie (🍪) -> Ice Cream (🍨)
  {
    name: "גלידה בקופסה (שינוי משתייה/עוגיה לגלידה)",
    pattern: /גלידת|גלידה|קרמסימו|בן&ג'ריס|בן וג'ריס/i,
    exclude: /גביעי גלידה|ארטיק|טילון|שלגון/i,
    targetEmoji: "🍨",
    badEmojis: ["🥤", "🍪", "📦", "🏷️"]
  },

  // 8. Ice Cream Cone / Popsicle (ארטיק / טילון / מגנום) having Soft Drink (🥤) or Generic -> Cone (🍦)
  {
    name: "ארטיקים וטילונים (שינוי משתייה/גנרי לטילון)",
    pattern: /טילון|טילונים|ארטיק|שלגון|מגנום/i,
    targetEmoji: "🍦",
    badEmojis: ["🥤", "📦", "🏷️", "❓"]
  },

  // 9. Beer (בירה) having Soft Drink (🥤) or Generic -> Beer (🍺)
  {
    name: "בירה (שינוי משתייה קלה לבירה)",
    pattern: /\bבירה\s+(גולדסטאר|היינכן|קרלסברג|טובורג|קורונה|סטלה|330|500|שחורה)/i,
    targetEmoji: "🍺",
    badEmojis: ["🥤", "📦", "🏷️", "❓"]
  },

  // 10. Wine (יין) having Soft Drink (🥤) or Generic -> Wine (🍷)
  {
    name: "יין (שינוי משתייה קלה ליין)",
    pattern: /\bיין\s+(אדום|לבן|קברנה|מרלו|שרדונה|הר חרמון|גמלא|ירדן|סגל|ברקן)/i,
    targetEmoji: "🍷",
    badEmojis: ["🥤", "📦", "🏷️", "❓"]
  },

  // 11. Cigarettes / Tobacco (סיגריות / טבק) having Box (📦) or Generic -> Cigarette (🚬)
  {
    name: "סיגריות וטבק (שינוי מגנרי לסיגריה)",
    pattern: /סיגריות|סיגריה|טבק ניילון|טבק נלסון|טבק בולט/i,
    targetEmoji: "🚬",
    badEmojis: ["📦", "🏷️", "❓", "🛒"]
  },

  // 12. Eggs (מארז/תבנית ביצים) having Box (📦) or Generic -> Egg (🥚)
  {
    name: "מארז ביצים (שינוי מגנרי לביצה)",
    pattern: /^ביצים\b|מארז ביצים|תבנית ביצים|ביצי חופש|ביצים L|ביצים M|ביצים XL/i,
    exclude: /אטריות|נודלס|קינדר|פורס|שוקולד/i,
    targetEmoji: "🥚",
    badEmojis: ["📦", "🏷️", "❓", "🛒"]
  },

  // 13. Watermelon (אבטיח) having Tomato/Generic -> Watermelon (🍉)
  {
    name: "אבטיח (שינוי מגנרי/עגבנייה לאבטיח)",
    pattern: /\bאבטיח\b/i,
    targetEmoji: "🍉",
    badEmojis: ["🍅", "📦", "🏷️", "❓"]
  },

  // 14. Cucumbers (מלפפון) having Tomato/Generic -> Cucumber (🥒)
  {
    name: "מלפפון (שינוי מגנרי/עגבנייה למלפפון)",
    pattern: /\bמלפפון\s+(ארוז|בתפזורת|טרי|בייבי)/i,
    targetEmoji: "🥒",
    badEmojis: ["🍅", "📦", "🏷️", "❓"]
  },

  // 15. Potatoes (תפוח אדמה / בטטה) having Tomato/Generic -> Potato (🥔)
  {
    name: "תפוח אדמה / בטטה (שינוי מעגבנייה/גנרי לתפוח אדמה)",
    pattern: /תפוח אדמה|בטטה/i,
    exclude: /צ'יפס|חטיף/i,
    targetEmoji: "🥔",
    badEmojis: ["🍅", "📦", "🏷️", "❓"]
  },

  // 16. Onions (בצל יבש / סגול) having Tomato/Generic -> Onion (🧅)
  {
    name: "בצל יבש / סגול (שינוי מעגבנייה/גנרי לבצל)",
    pattern: /בצל יבש|בצל סגול|בצל לבן/i,
    targetEmoji: "🧅",
    badEmojis: ["🍅", "📦", "🏷️", "❓"]
  },

  // 17. Lemons (לימון) having Tomato/Generic -> Lemon (🍋)
  {
    name: "לימון (שינוי מעגבנייה/גנרי ללימון)",
    pattern: /לימון ארוז|לימון בתפזורת|לימונים/i,
    exclude: /מיץ|ספרינג|פריגת|נענע/i,
    targetEmoji: "🍋",
    badEmojis: ["🍅", "📦", "🏷️", "❓"]
  },

  // 18. Bananas (בננה) having Generic -> Banana (🍌)
  {
    name: "בננות (שינוי מגנרי לבננה)",
    pattern: /\bבננה\b|\bבננות\b/i,
    targetEmoji: "🍌",
    badEmojis: ["📦", "🏷️", "❓", "🛒"]
  },

  // 19. Grapes (ענבים) having Generic -> Grapes (🍇)
  {
    name: "ענבים (שינוי מגנרי לענבים)",
    pattern: /ענבים ירוקים|ענבים שחורים|ענבים אדומים/i,
    targetEmoji: "🍇",
    badEmojis: ["📦", "🏷️", "❓", "🛒"]
  },

  // 20. Diapers (חיתולים) having Generic -> Diaper (🧷)
  {
    name: "חיתולים / טיטולים (שינוי מגנרי לחיתול)",
    pattern: /חיתולים|חיתולי|טיטולים/i,
    targetEmoji: "🧷",
    badEmojis: ["📦", "🏷️", "❓", "🛒"]
  }
];

async function run() {
  console.log("====================================================");
  console.log("🔎 High-Confidence Emoji Correction Audit Across All Shops");
  console.log("====================================================\n");

  const [products] = await db.query(`
    SELECT id, shop_id, name, category, emoji
    FROM product
    ORDER BY shop_id ASC, id ASC
  `);

  console.log(`Auditing total ${products.length} products across all shops...\n`);

  const recommendations = [];

  for (const p of products) {
    const name = String(p.name || "").trim();
    const currentEmoji = p.emoji ? String(p.emoji).trim() : "ללא (NULL)";

    for (const rule of CORRECTION_RULES) {
      if (rule.exclude && rule.exclude.test(name)) continue;

      if (rule.pattern.test(name)) {
        // If current emoji matches any of the "badEmojis" or is NULL/empty or different from target
        if (rule.badEmojis.includes(currentEmoji) || currentEmoji === "ללא (NULL)" || (currentEmoji !== rule.targetEmoji && rule.badEmojis.length === 0)) {
          recommendations.push({
            id: p.id,
            shop_id: p.shop_id,
            name,
            ruleName: rule.name,
            currentEmoji,
            suggestedEmoji: rule.targetEmoji
          });
          break;
        }
      }
    }
  }

  console.log(`Total high-confidence corrections found: ${recommendations.length}\n`);

  // Group recommendations by ruleName
  const grouped = {};
  for (const item of recommendations) {
    if (!grouped[item.ruleName]) grouped[item.ruleName] = [];
    grouped[item.ruleName].push(item);
  }

  for (const ruleName in grouped) {
    console.log(`=== ${ruleName} (${grouped[ruleName].length} מוצרים) ===`);
    for (const item of grouped[ruleName]) {
      console.log(`  • ID #${item.id} (חנות ${item.shop_id}) | "${item.name}" | לפני: ${item.currentEmoji} ➔ מוצע: ${item.suggestedEmoji}`);
    }
    console.log("");
  }

  process.exit(0);
}

run().catch(err => {
  console.error("Error:", err);
  process.exit(1);
});

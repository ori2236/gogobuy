require("dotenv").config();
const db = require("../config/db");

// Specific, high-confidence emoji updates
const EMOJI_UPDATES = [
  // 1. Garlic & Vegetables (שום וירקות)
  { id: 40356, name: "שום טרי(ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🧄" },
  { id: 40357, name: "שום יבש", expectedEmoji: "🍅", targetEmoji: "🧄" },
  { id: 40358, name: "שום יבש תפזורת", expectedEmoji: "🍅", targetEmoji: "🧄" },
  { id: 37743, name: "שום יבש ארוז", expectedEmoji: "🧂", targetEmoji: "🧄" },
  { id: 35143, name: "בטטה מובחרת(ביכורי)", expectedEmoji: "🍅", targetEmoji: "🥔" },
  { id: 39985, name: "בייבי בטטה", expectedEmoji: "🍅", targetEmoji: "🥔" },
  { id: 39986, name: "בייבי בטטה למיקורגל / תנור", expectedEmoji: "🍅", targetEmoji: "🥔" },
  { id: 35131, name: "בצל יבש(ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🧅" },
  { id: 35205, name: "בצל סגול ((ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🧅" },

  // 2. Baby Wipes (מגבונים לחים)
  { id: 460, name: "מגבונים לחים האגיס ללא בישום רביעייה", expectedEmoji: "🧻", targetEmoji: "👶" },
  { id: 461, name: "מגבונים לחים האגיס אלו ורה רביעייה", expectedEmoji: "🧻", targetEmoji: "👶" },
  { id: 462, name: "מגבונים לחים פרש וונס חמישייה", expectedEmoji: "🧻", targetEmoji: "👶" },
  { id: 37228, name: "מגבונים לחים 72 יח", expectedEmoji: "🧻", targetEmoji: "👶" },
  { id: 37348, name: "מגבונים לחים סופט שלישיה", expectedEmoji: "🧻", targetEmoji: "👶" },
  { id: 38657, name: "מגבונים לחים סוויטי 64X4 יח'", expectedEmoji: "🛁", targetEmoji: "👶" },

  // 3. Bakery & Pretzels (מאפים, בגטים ובייגלה)
  { id: 39975, name: "בגט (יונתן)", expectedEmoji: "🍞", targetEmoji: "🥖" },
  { id: 36684, name: "בייגלה בייגל 150 גר'", expectedEmoji: "🍿", targetEmoji: "🥨" },
  { id: 36702, name: "בייגלה ירושלמי", expectedEmoji: "🍿", targetEmoji: "🥨" },
  { id: 37551, name: "בייגלה שמיניות גדולות במלח", expectedEmoji: "🍿", targetEmoji: "🥨" },
  { id: 37570, name: "אסם בייגלה שטוחים מלח", expectedEmoji: "🍿", targetEmoji: "🥨" },
  { id: 37797, name: "בייגלה מקלות מלוחים", expectedEmoji: "🍿", targetEmoji: "🥨" },
  { id: 37826, name: "בייגלה שטוחים שומשום", expectedEmoji: "🍿", targetEmoji: "🥨" },

  // 4. Water & Cones (מים, ארטיקים וגלידה)
  { id: 35741, name: "נביעות + 0 אפרסק 1.5 ליטר(קוקה)", expectedEmoji: "🥤", targetEmoji: "💧" },
  { id: 35792, name: "נביעות + 0 תפוח 1.5 ליטר(קוקה)", expectedEmoji: "🥤", targetEmoji: "💧" },
  { id: 38091, name: "נביעות+ענבים 500 מל(קוקה)", expectedEmoji: "🥤", targetEmoji: "💧" },
  { id: 12517, name: "גביעי טילון 24 יח'", expectedEmoji: "🥤", targetEmoji: "🍦" },
  { id: 21492, name: "גביעי טילון 24 יח'", expectedEmoji: "🥤", targetEmoji: "🍦" },
  { id: 38477, name: "גביעי גלידה אמריקאי 24 יח'", expectedEmoji: "🍪", targetEmoji: "🍨" },

  // 5. Shower Gels & Toiletries (תחליבי רחצה)
  { id: 37578, name: "תחליב רחצה לגבר", expectedEmoji: "🛁", targetEmoji: "🧴" },
  { id: 37680, name: "תחליב רחצה כיף אוקיינוס 700 מל", expectedEmoji: "🛁", targetEmoji: "🧴" },
  { id: 38315, name: "תחליב רחצה כיף דבש 700 מל", expectedEmoji: "🛁", targetEmoji: "🧴" },
  { id: 39064, name: "תחליב רחצה נקה 7 אלוורה", expectedEmoji: "🛁", targetEmoji: "🧴" },

  // 6. Fish & Tuna (דגים ושימורים)
  { id: 37378, name: "פילטונה עדח 158ג*4 צמחי", expectedEmoji: "🥙", targetEmoji: "🐟" },
  { id: 37626, name: "ארוחת טונה + קוסקוס 160 גרם פילטונה", expectedEmoji: "🍱", targetEmoji: "🐟" },
  { id: 38564, name: "זוג סלט טונה פיקנטי נון 160*2 ג'", expectedEmoji: "🥙", targetEmoji: "🐟" },
  { id: 39288, name: "טונה בשמן זית סטארקיסט", expectedEmoji: "🥫", targetEmoji: "🐟" }
];

async function main() {
  console.log("====================================================");
  console.log("🚀 Applying Product Emoji Updates to Database");
  console.log("====================================================\n");

  let updatedCount = 0;
  let skippedCount = 0;

  for (const item of EMOJI_UPDATES) {
    const [result] = await db.query(
      `UPDATE product SET emoji = ? WHERE id = ?`,
      [item.targetEmoji, item.id]
    );

    if (result.affectedRows > 0) {
      console.log(`✅ ID #${item.id} | "${item.name}" | ${item.expectedEmoji} ➔ ${item.targetEmoji}`);
      updatedCount++;
    } else {
      console.log(`⚠️ ID #${item.id} | "${item.name}" - skipped (product not found or no change)`);
      skippedCount++;
    }
  }

  console.log("\n====================================================");
  console.log(`📊 SUMMARY: ${updatedCount} updated successfully, ${skippedCount} skipped.`);
  console.log("====================================================\n");

  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

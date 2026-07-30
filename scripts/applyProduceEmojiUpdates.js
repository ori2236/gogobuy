require("dotenv").config();
const db = require("../config/db");

// Fresh produce emoji corrections
const PRODUCE_EMOJI_UPDATES = [
  // Cucumbers (מלפפון טרי)
  { id: 35906, name: "מלפפון בייבי", expectedEmoji: "🍅", targetEmoji: "🥒" },
  { id: 39549, name: "מלפפון בייבי ארוז", expectedEmoji: "🍅", targetEmoji: "🥒" },
  { id: 40152, name: "מלפפון בייבי 300 גר'", expectedEmoji: "🍅", targetEmoji: "🥒" },
  { id: 40153, name: "מלפפון מיני ארוז (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🥒" },
  { id: 40740, name: "מלפפון (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🥒" },
  { id: 40469, name: "מלפפון (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🥒" },
  { id: 40594, name: "מלפפון (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🥒" },

  // Peppers & Gamba (פלפל אדום/צהוב/ירוק)
  { id: 35134, name: "פלפל אדום (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 35255, name: "פלפל צהוב (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 40315, name: "פלפל ירוק(ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 40744, name: "פלפל אדום (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 40758, name: "פלפל צהוב (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 40494, name: "פלפל אדום (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 40511, name: "פלפל צהוב (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 40619, name: "פלפל אדום (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },
  { id: 40636, name: "פלפל צהוב (ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🫑" },

  // Corn (תירס קלחים)
  { id: 35458, name: "תירס קלחים", expectedEmoji: "🍅", targetEmoji: "🌽" },
  { id: 36141, name: "תירס קלחים(ביכורי שדה)", expectedEmoji: "🍅", targetEmoji: "🌽" },
  { id: 39502, name: "תירס קלחים 16 ( סנפרוסט)", expectedEmoji: "🥦", targetEmoji: "🌽" },

  // Fresh Watermelon (אבטיח טרי)
  { id: 40596, name: "אבטיח (ביכורי שדה )", expectedEmoji: "🍎", targetEmoji: "🍉" }
];

async function main() {
  console.log("====================================================");
  console.log("🚀 Applying Fresh Produce Emoji Updates to Database");
  console.log("====================================================\n");

  let updatedCount = 0;
  let skippedCount = 0;

  for (const item of PRODUCE_EMOJI_UPDATES) {
    const [result] = await db.query(
      `UPDATE product SET emoji = ? WHERE id = ?`,
      [item.targetEmoji, item.id]
    );

    if (result.affectedRows > 0) {
      console.log(`✅ ID #${item.id} | "${item.name}" | ${item.expectedEmoji} ➔ ${item.targetEmoji}`);
      updatedCount++;
    } else {
      console.log(`⚠️ ID #${item.id} | "${item.name}" - skipped`);
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

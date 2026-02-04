require("dotenv").config({ quiet: true });
const db = require("./config/db");

const SUBCATEGORY_GROUPS = {
  // ===== Dairy & Eggs =====
  Cheese: ["Cheese", "Spreads & Cream Cheese"],
  "Spreads & Cream Cheese": ["Spreads & Cream Cheese", "Cheese"],

  Yogurt: ["Yogurt", "Desserts & Puddings"],
  "Desserts & Puddings": ["Desserts & Puddings", "Yogurt"],

  Milk: ["Milk", "Milk Alternatives"],
  "Milk Alternatives": ["Milk Alternatives", "Milk"],

  // ===== Bakery =====
  Bread: ["Bread", "Rolls & Buns", "Baguettes & Artisan"],
  "Rolls & Buns": ["Rolls & Buns", "Bread"],
  "Baguettes & Artisan": ["Baguettes & Artisan", "Bread"],

  "Cakes & Pastries": ["Cakes & Pastries", "Cookies & Biscuits"],
  "Cookies & Biscuits": ["Cookies & Biscuits", "Cakes & Pastries"],

  "Pita & Flatbread": ["Pita & Flatbread", "Tortillas & Wraps"],
  "Tortillas & Wraps": ["Tortillas & Wraps", "Pita & Flatbread"],

  // ===== Produce =====
  Fruits: ["Fruits", "Organic Produce"],
  Vegetables: ["Vegetables", "Organic Produce"],
  "Organic Produce": ["Organic Produce", "Fruits", "Vegetables"],

  "Prepped Produce": ["Prepped Produce", "Vegetables"],

  // ===== Meat & Poultry =====
  Beef: ["Beef", "Ground/Minced"],
  "Ground/Minced": ["Ground/Minced", "Beef"],

  "Cold Cuts": ["Cold Cuts", "Turkey", "Chicken"],
  Turkey: ["Turkey", "Cold Cuts"],
  Chicken: ["Chicken", "Cold Cuts"],

  Sausages: ["Sausages", "Mixed & Other Meats"],
  "Mixed & Other Meats": ["Mixed & Other Meats", "Sausages"],

  // ===== Fish & Seafood =====
  "Fresh Fish": ["Fresh Fish", "Frozen Fish"],
  "Frozen Fish": ["Frozen Fish", "Fresh Fish"],

  // ===== Deli & Ready Meals =====
  "Ready-to-Eat Meals": ["Ready-to-Eat Meals", "Sushi & Sashimi"],
  "Sushi & Sashimi": ["Sushi & Sashimi", "Ready-to-Eat Meals"],

  // ===== Frozen =====
  "Pizza & Dough": ["Pizza & Dough", "Ready Meals"],
  "Ready Meals": ["Ready Meals", "Pizza & Dough"],

  // ===== Pantry =====
  "Flour & Baking": ["Flour & Baking", "Baking Mixes"],
  "Baking Mixes": ["Baking Mixes", "Flour & Baking"],

  "Breakfast Cereal": ["Breakfast Cereal", "Granola & Muesli"],
  "Granola & Muesli": ["Granola & Muesli", "Breakfast Cereal"],

  "Canned Vegetables": ["Canned Vegetables", "Canned Beans & Legumes"],
  "Canned Beans & Legumes": ["Canned Beans & Legumes", "Canned Vegetables"],

  "Honey & Spreads": ["Honey & Spreads", "Nut Butters", "Jams & Preserves"],
  "Nut Butters": ["Nut Butters", "Honey & Spreads"],
  "Jams & Preserves": ["Jams & Preserves", "Honey & Spreads"],

  "Asian Pantry": ["Asian Pantry", "Sauces & Condiments"],
  "Mediterranean Pantry": ["Mediterranean Pantry", "Sauces & Condiments"],
  "Mexican Pantry": ["Mexican Pantry", "Sauces & Condiments"],
  "Canned Tomatoes": ["Canned Tomatoes", "Sauces & Condiments"],
  "Sauces & Condiments": [
    "Sauces & Condiments",
    "Asian Pantry",
    "Mediterranean Pantry",
    "Mexican Pantry",
    "Canned Tomatoes",
  ],

  // ===== Snacks =====
  "Chips & Crisps": ["Chips & Crisps", "Pretzels & Popcorn"],
  "Pretzels & Popcorn": ["Pretzels & Popcorn", "Chips & Crisps"],

  // ===== Personal Care =====
  "Bath & Body": ["Bath & Body", "Hand Soap & Sanitizers"],
  "Hand Soap & Sanitizers": ["Hand Soap & Sanitizers", "Bath & Body"],

  // ===== Health & Wellness =====
  "Pain Relief": ["Pain Relief", "Cough & Cold"],
  "Cough & Cold": ["Cough & Cold", "Pain Relief"],
};

async function main() {
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    // טוענים את כל תתי-הקטגוריות לזיכרון
    const [subs] = await conn.query(
      `SELECT id, category_id, name FROM product_subcategory`,
    );

    // index: name -> [{id, category_id}]
    const byName = new Map();
    for (const r of subs) {
      const name = String(r.name || "").trim();
      if (!name) continue;
      if (!byName.has(name)) byName.set(name, []);
      byName
        .get(name)
        .push({ id: Number(r.id), category_id: Number(r.category_id) });
    }

    // index: category_id -> Map(name -> id)
    const byCategory = new Map();
    for (const r of subs) {
      const cid = Number(r.category_id);
      const name = String(r.name || "").trim();
      if (!cid || !name) continue;
      if (!byCategory.has(cid)) byCategory.set(cid, new Map());
      byCategory.get(cid).set(name, Number(r.id));
    }

    let inserted = 0;
    let skipped = 0;

    for (const [sourceNameRaw, candListRaw] of Object.entries(
      SUBCATEGORY_GROUPS,
    )) {
      const sourceName = String(sourceNameRaw || "").trim();
      const candList = Array.isArray(candListRaw)
        ? candListRaw.map((x) => String(x || "").trim()).filter(Boolean)
        : [];

      if (!sourceName || !candList.length) continue;

      const sourceRows = byName.get(sourceName) || [];
      if (!sourceRows.length) {
        console.warn(
          `[seed][WARN] source subcategory not found: "${sourceName}"`,
        );
        continue;
      }

      for (const src of sourceRows) {
        const nameToId = byCategory.get(src.category_id);
        if (!nameToId) continue;

        for (let i = 0; i < candList.length; i++) {
          const candName = candList[i];
          const candId = nameToId.get(candName);

          if (!candId) {
            skipped++;
            console.warn(
              `[seed][WARN] candidate "${candName}" not found in same category as source "${sourceName}" (category_id=${src.category_id})`,
            );
            continue;
          }

          const sortOrder = i + 1;

          const [res] = await conn.query(
            `
            INSERT INTO subcategory_candidates
              (source_subcategory_id, candidate_subcategory_id, sort_order)
            VALUES
              (?, ?, ?)
            ON DUPLICATE KEY UPDATE
              sort_order = VALUES(sort_order)
            `,
            [src.id, candId, sortOrder],
          );

          // mysql2: affectedRows=1 insert, =2 update
          if (Number(res.affectedRows || 0) > 0) inserted++;
        }
      }
    }

    await conn.commit();
    console.log(
      `✅ Seed done. inserted/updated=${inserted}, skipped=${skipped}`,
    );
  } catch (e) {
    await conn.rollback();
    console.error("❌ Seed failed:", e?.message || e);
    process.exitCode = 1;
  } finally {
    conn.release();
    await db.end?.().catch(() => {});
  }
}

main();

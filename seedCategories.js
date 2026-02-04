/* scripts/seedCategories.js */
require("dotenv").config({ quiet: true });

const pool = require("./config/db"); // mysql2/promise pool

const DEFAULT_ALLOWED_SUBCATEGORIES_MAP = {
  "Dairy & Eggs": [
    "Milk",
    "Milk Alternatives",
    "Yogurt",
    "Cheese",
    "Cream",
    "Butter & Margarine",
    "Eggs",
    "Spreads & Cream Cheese",
    "Desserts & Puddings",
  ],
  Bakery: [
    "Bread",
    "Rolls & Buns",
    "Pita & Flatbread",
    "Baguettes & Artisan",
    "Cakes & Pastries",
    "Cookies & Biscuits",
    "Tortillas & Wraps",
    "Gluten-Free Bakery",
  ],
  Produce: [
    "Fruits",
    "Vegetables",
    "Fresh Herbs",
    "Prepped Produce",
    "Organic Produce",
  ],
  "Meat & Poultry": [
    "Beef",
    "Chicken",
    "Turkey",
    "Lamb",
    "Mixed & Other Meats",
    "Ground/Minced",
    "Cold Cuts",
    "Sausages",
  ],
  "Fish & Seafood": [
    "Fresh Fish",
    "Frozen Fish",
    "Shellfish",
    "Smoked & Cured",
    "Canned Fish",
  ],
  "Deli & Ready Meals": [
    "Deli Meats",
    "Deli Cheeses",
    "Salads & Spreads",
    "Ready-to-Eat Meals",
    "Sushi & Sashimi",
  ],
  Frozen: [
    "Vegetables",
    "Fruits",
    "Meat & Poultry",
    "Fish & Seafood",
    "Pizza & Dough",
    "Ice Cream & Desserts",
    "Ready Meals",
    "Vegan & Veggie",
  ],
  Pantry: [
    "Flour & Baking",
    "Sugar & Sweeteners",
    "Spices & Seasonings",
    "Oils & Vinegar",
    "Sauces & Condiments",
    "Pasta",
    "Rice & Grains",
    "Canned Vegetables",
    "Canned Tomatoes",
    "Canned Beans & Legumes",
    "Canned Fish",
    "Pickles & Olives",
    "Honey & Spreads",
    "Breakfast Cereal",
    "Granola & Muesli",
    "Nut Butters",
    "Jams & Preserves",
    "Asian Pantry",
    "Mediterranean Pantry",
    "Mexican Pantry",
    "Baking Mixes",
  ],
  Snacks: [
    "Chips & Crisps",
    "Pretzels & Popcorn",
    "Nuts & Seeds",
    "Dried Fruit",
    "Cookies & Biscuits",
    "Crackers",
    "Candy & Chocolate",
    "Energy & Protein Bars",
  ],
  Beverages: [
    "Water",
    "Sparkling Water",
    "Soft Drinks",
    "Juices & Nectars",
    "Iced Tea & Lemonade",
    "Coffee",
    "Tea & Herbal",
    "Syrups & Concentrates",
    "Energy Drinks",
    "Sports Drinks",
  ],
  "Alcoholic Beverages": ["Beer", "Wine", "Spirits", "Cider", "Liqueurs"],
  Baby: [
    "Diapers",
    "Wipes",
    "Formula",
    "Baby Food",
    "Baby Snacks",
    "Bath & Care",
    "Accessories",
  ],
  Household: [
    "Paper Goods",
    "Cleaning Supplies",
    "Dishwashing",
    "Laundry",
    "Trash Bags",
    "Air Fresheners",
    "Food Storage & Wrap",
    "Light Bulbs & Batteries",
  ],
  "Personal Care": [
    "Oral Care",
    "Hair Care",
    "Skin Care",
    "Deodorants",
    "Shaving & Grooming",
    "Feminine Care",
    "Bath & Body",
    "Hand Soap & Sanitizers",
  ],
  "Health & Wellness": [
    "Vitamins & Supplements",
    "First Aid",
    "Pain Relief",
    "Cough & Cold",
    "Digestive Health",
    "Allergy",
  ],
  Pet: [
    "Dog Food",
    "Dog Care",
    "Cat Food",
    "Cat Care",
    "Bird & Small Pet",
    "Litter & Accessories",
  ],
  "Home & Leisure": [
    "Kitchenware & Utensils",
    "Disposable Tableware",
    "Charcoal & BBQ",
    "Seasonal & Holiday",
  ],
};

async function main() {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const categories = Object.keys(DEFAULT_ALLOWED_SUBCATEGORIES_MAP);

    for (let ci = 0; ci < categories.length; ci++) {
      const catName = categories[ci];
      // upsert category
      await conn.query(
        `
        INSERT INTO product_category (name, sort_order)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE sort_order = VALUES(sort_order)
        `,
        [catName, ci + 1],
      );

      // fetch category id
      const [catRows] = await conn.query(
        `SELECT id FROM product_category WHERE name = ? LIMIT 1`,
        [catName],
      );
      const categoryId = catRows[0]?.id;
      if (!categoryId)
        throw new Error(`Failed to get id for category: ${catName}`);

      const subs = DEFAULT_ALLOWED_SUBCATEGORIES_MAP[catName] || [];
      for (let si = 0; si < subs.length; si++) {
        const subName = subs[si];

        await conn.query(
          `
          INSERT INTO product_subcategory (category_id, name, sort_order)
          VALUES (?, ?, ?)
          ON DUPLICATE KEY UPDATE sort_order = VALUES(sort_order)
          `,
          [categoryId, subName, si + 1],
        );
      }
    }

    await conn.commit();
    console.log("✅ Categories + subcategories seeded successfully.");
  } catch (err) {
    await conn.rollback();
    console.error("❌ Seed failed:", err);
    process.exitCode = 1;
  } finally {
    conn.release();
    await pool.end();
  }
}

main();

const db = require("../config/db");

async function fetchCategoriesMap() {
  const query = `
    SELECT c.name AS category, s.name AS subcategory
    FROM product_category c
    JOIN product_subcategory s ON s.category_id = c.id
    ORDER BY c.sort_order, s.sort_order;
  `;

  const [rows] = await db.query(query);

  const categoryMap = {};

  rows.forEach((row) => {
    if (!categoryMap[row.category]) {
      categoryMap[row.category] = [];
    }
    categoryMap[row.category].push(row.subcategory);
  });

  return categoryMap;
}

async function buildCategorySubcategoryItemSchemas(commonProps, commonRequired) {
  const categoryMap = await fetchCategoriesMap();
  const CATEGORY_ENUM = Object.keys(categoryMap);

  return CATEGORY_ENUM.map((cat) => ({
    type: "object",
    additionalProperties: false,
    required: [...commonRequired, "category", "sub-category"],
    properties: {
      ...commonProps,
      category: { type: "string", const: cat },
      "sub-category": {
        type: "string",
        enum: categoryMap[cat],
      },
    },
  }));
}

async function buildNullableSubcategorySchemas(commonProps, commonRequired) {
  const categoryMap = await fetchCategoriesMap();
  const CATEGORY_ENUM = Object.keys(categoryMap);

  return CATEGORY_ENUM.map((cat) => ({
    type: "object",
    additionalProperties: false,
    required: [...commonRequired, "category", "sub-category"],
    properties: {
      ...commonProps,

      price_intent: { type: "string", enum: ["PROMOTION", "BUDGET_PICK"] },

      category: { type: "string", const: cat },

      "sub-category": {
        anyOf: [
          { type: "string", enum: categoryMap[cat] },
          { type: "null" },
        ],
      },
    },
  }));
}

module.exports = {
  fetchCategoriesMap,
  buildCategorySubcategoryItemSchemas,
  buildNullableSubcategorySchemas,
};

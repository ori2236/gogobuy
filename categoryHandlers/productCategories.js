const { fetchCategoriesMap } = require("../repositories/categories");

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
  buildCategorySubcategoryItemSchemas,
  buildNullableSubcategorySchemas,
};

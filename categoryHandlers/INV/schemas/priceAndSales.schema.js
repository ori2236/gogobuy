const {
  buildCategorySubcategoryItemSchemas,
  buildNullableSubcategorySchemas,
} = require("../../productCategories");
const { fetchCategoriesMap } = require("../../../repositories/categories");

const PRICE_INTENT_ENUM = [
  "PRICE",
  "PRICE_COMPARE",
  "PROMOTION",
  "PROMOTION_LIST",
  "CHEAPER_ALT",
  "BUDGET_PICK",
];

const COMMON_PRODUCT_PROPS = {
  name: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  outputName: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  original_user_text: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  search_terms: { type: "array", items: { type: "string", minLength: 1 } },
  amount: { type: "number", minimum: 0.001 },
  units: { anyOf: [{ type: "integer", minimum: 1 }, { type: "null" }] },
  sold_by_weight: { type: "boolean" },
  exclude_tokens: { type: "array", items: { type: "string", minLength: 1 } },
  compare_group: {
    anyOf: [{ type: "string", minLength: 1 }, { type: "null" }],
  },
  budget_ils: { anyOf: [{ type: "number", minimum: 0 }, { type: "null" }] },
  price_intent: { type: "string", enum: PRICE_INTENT_ENUM },
};

const COMMON_PRODUCT_REQUIRED = [
  "name",
  "outputName",
  "original_user_text",
  "search_terms",
  "amount",
  "units",
  "sold_by_weight",
  "exclude_tokens",
  "compare_group",
  "budget_ils",
  "price_intent",
];

async function buildPromotionListNullableCategorySchemas() {
  const categoryMap = await fetchCategoriesMap();
  const CATEGORY_ENUM = Object.keys(categoryMap).sort();

  return CATEGORY_ENUM.map((cat) => ({
    type: "object",
    additionalProperties: false,
    required: [...COMMON_PRODUCT_REQUIRED, "category", "sub-category"],
    properties: {
      ...COMMON_PRODUCT_PROPS,
      price_intent: { type: "string", const: "PROMOTION_LIST" },
      category: { type: "string", const: cat },
      "sub-category": {
        anyOf: [
          { type: "string", enum: [...categoryMap[cat]].sort() },
          { type: "null" },
        ],
      },
    },
  }));
}

function buildAllPromotionsSchema() {
  return {
    type: "object",
    additionalProperties: false,
    required: [...COMMON_PRODUCT_REQUIRED, "category", "sub-category"],
    properties: {
      ...COMMON_PRODUCT_PROPS,
      name: { type: "null" },
      outputName: { type: "null" },
      amount: { type: "number", const: 1 },
      units: { type: "null" },
      sold_by_weight: { type: "boolean", const: false },
      exclude_tokens: {
        type: "array",
        items: { type: "string", minLength: 1 },
      },
      compare_group: { type: "null" },
      budget_ils: { type: "null" },
      price_intent: { type: "string", const: "PROMOTION_LIST" },
      category: { type: "null" },
      "sub-category": { type: "null" },
    },
  };
}

async function buildInvPriceAndSalesSchema() {
  const categorySchemas = await buildCategorySubcategoryItemSchemas(
    COMMON_PRODUCT_PROPS,
    COMMON_PRODUCT_REQUIRED,
  );

  const nullableSchemas = await buildNullableSubcategorySchemas(
    COMMON_PRODUCT_PROPS,
    COMMON_PRODUCT_REQUIRED,
  );

  const promotionListNullableCategorySchemas =
    await buildPromotionListNullableCategorySchemas();

  const CATEGORY_SUBCATEGORY_ANYOF = [
    ...categorySchemas,
    ...nullableSchemas,
    ...promotionListNullableCategorySchemas,
    buildAllPromotionsSchema(),
  ];

  return {
    name: "inv_price_and_sales_extract",
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      required: ["products", "questions"],
      properties: {
        products: {
          type: "array",
          items: {
            anyOf: CATEGORY_SUBCATEGORY_ANYOF,
          },
        },
        questions: {
          type: "array",
          items: {
            type: "object",
            additionalProperties: false,
            required: ["name", "question", "options"],
            properties: {
              name: { anyOf: [{ type: "string" }, { type: "null" }] },
              question: { type: "string", minLength: 1 },
              options: {
                type: "array",
                items: { type: "string", minLength: 1 },
              },
            },
          },
        },
      },
    },
  };
}

module.exports = { buildInvPriceAndSalesSchema };

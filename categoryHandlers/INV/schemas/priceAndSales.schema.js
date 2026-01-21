const {
  buildCategorySubcategoryItemSchemas,
  buildNullableSubcategorySchemas,
} = require("../../productCategories");

const PRICE_INTENT_ENUM = [
  "PRICE",
  "PRICE_COMPARE",
  "PROMOTION",
  "CHEAPER_ALT",
  "BUDGET_PICK",
];

const COMMON_PRODUCT_PROPS = {
  name: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  outputName: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
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
  "amount",
  "units",
  "sold_by_weight",
  "exclude_tokens",
  "compare_group",
  "budget_ils",
  "price_intent",
];

const CATEGORY_SUBCATEGORY_ANYOF = [
  ...buildCategorySubcategoryItemSchemas(
    COMMON_PRODUCT_PROPS,
    COMMON_PRODUCT_REQUIRED
  ),
  ...buildNullableSubcategorySchemas(
    COMMON_PRODUCT_PROPS,
    COMMON_PRODUCT_REQUIRED
  ),
];


const INV_PRICE_AND_SALES_SCHEMA = {
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
            options: { type: "array", items: { type: "string", minLength: 1 } },
          },
        },
      },
    },
  },
};

module.exports = { INV_PRICE_AND_SALES_SCHEMA };

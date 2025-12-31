const {
  buildCategorySubcategoryItemSchemas,
} = require("../../productCategories");

const COMMON_PRODUCT_PROPS = {
  name: { type: "string", minLength: 1 },
  outputName: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  amount: { type: "number", minimum: 0.001 },
  units: { anyOf: [{ type: "integer", minimum: 1 }, { type: "null" }] },
  sold_by_weight: { type: "boolean" },
  exclude_tokens: { type: "array", items: { type: "string", minLength: 1 } },
};

const COMMON_PRODUCT_REQUIRED = [
  "name",
  "outputName",
  "amount",
  "units",
  "sold_by_weight",
  "exclude_tokens",
];

const CATEGORY_SUBCATEGORY_ANYOF = buildCategorySubcategoryItemSchemas(
  COMMON_PRODUCT_PROPS,
  COMMON_PRODUCT_REQUIRED
);

const CREATE_ORDER_SCHEMA = {
  name: "ord_create_extract",
  strict: true,
  schema: {
    type: "object",
    additionalProperties: false,
    required: ["products", "questions", "question_updates"],
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

      question_updates: {
        type: "object",
        additionalProperties: false,
        required: ["close_ids", "delete_ids"],
        properties: {
          close_ids: { type: "array", items: { type: "integer", minimum: 1 } },
          delete_ids: { type: "array", items: { type: "integer", minimum: 1 } },
        },
      },
    },
  },
};

module.exports = { CREATE_ORDER_SCHEMA };

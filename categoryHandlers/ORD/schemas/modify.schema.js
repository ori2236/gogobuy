const {
  buildCategorySubcategoryItemSchemas,
} = require("../../productCategories");

const ADD_PRODUCT_PROPS = {
  name: { type: "string", minLength: 1 },
  outputName: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  amount: { type: "number", minimum: 0.001 },
  units: { anyOf: [{ type: "integer", minimum: 1 }, { type: "null" }] },
  sold_by_weight: { type: "boolean" },
  exclude_tokens: { type: "array", items: { type: "string", minLength: 1 } },
};

const ADD_PRODUCT_REQUIRED = [
  "name",
  "outputName",
  "amount",
  "units",
  "sold_by_weight",
  "exclude_tokens",
];

const CATEGORY_SUBCATEGORY_ANYOF_FOR_ADD = buildCategorySubcategoryItemSchemas(
  ADD_PRODUCT_PROPS,
  ADD_PRODUCT_REQUIRED
);

const SET_ITEM_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["order_item.id", "amount", "sold_by_weight", "units"],
  properties: {
    "order_item.id": { type: "integer", minimum: 1 },
    amount: { type: "number", minimum: 0.001 },
    sold_by_weight: { type: "boolean" },
    units: { anyOf: [{ type: "integer", minimum: 1 }, { type: "null" }] },
  },
};

const REMOVE_ITEM_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["order_item.id"],
  properties: {
    "order_item.id": { type: "integer", minimum: 1 },
  },
};

const MODIFY_ORDER_SCHEMA = {
  name: "ord_modify_patch",
  strict: true,
  schema: {
    type: "object",
    additionalProperties: false,
    required: ["ops", "questions", "question_updates"],
    properties: {
      ops: {
        type: "object",
        additionalProperties: false,
        required: ["set", "remove", "add"],
        properties: {
          set: {
            type: "array",
            items: SET_ITEM_SCHEMA,
          },
          remove: {
            type: "array",
            items: REMOVE_ITEM_SCHEMA,
          },
          add: {
            type: "array",
            items: { anyOf: CATEGORY_SUBCATEGORY_ANYOF_FOR_ADD },
          },
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

module.exports = { MODIFY_ORDER_SCHEMA };
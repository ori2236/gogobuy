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

const QUESTIONS_SCHEMA = {
  type: "array",
  items: {
    type: "object",
    additionalProperties: false,
    required: ["name", "question", "options"],
    properties: {
      name: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
      question: { type: "string", minLength: 1 },
      options: {
        type: "array",
        items: { type: "string", minLength: 1 },
      },
    },
  },
};

const QUESTION_UPDATES_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["close_ids", "delete_ids"],
  properties: {
    close_ids: {
      type: "array",
      items: { type: "integer", minimum: 1 },
    },
    delete_ids: {
      type: "array",
      items: { type: "integer", minimum: 1 },
    },
  },
};

async function buildIntentRouterSchema() {
  const CATEGORY_SUBCATEGORY_ANYOF_FOR_ADD =
    await buildCategorySubcategoryItemSchemas(
      ADD_PRODUCT_PROPS,
      ADD_PRODUCT_REQUIRED,
    );

  // חסינות: לפעמים הפונקציה מחזירה array ולפעמים אובייקט
  const addItemSchema = Array.isArray(CATEGORY_SUBCATEGORY_ANYOF_FOR_ADD)
    ? { anyOf: CATEGORY_SUBCATEGORY_ANYOF_FOR_ADD }
    : CATEGORY_SUBCATEGORY_ANYOF_FOR_ADD;

  return {
    name: "intent_router_patch_v1",
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      required: [
        "mode",
        "classifier_line",
        "question",
        "ops",
        "questions",
        "question_updates",
      ],
      properties: {
        mode: {
          type: "string",
          // enum בתוך properties זה בסדר (האיסור הוא רק בטופ לבל)
          enum: ["PATCH", "CLASSIFY"],
        },
        classifier_line: { type: "string", minLength: 1 },
        question: {
          anyOf: [{ type: "string", minLength: 1 }, { type: "null" }],
        },

        ops: {
          type: "object",
          additionalProperties: false,
          required: ["set", "remove", "add"],
          properties: {
            set: { type: "array", items: SET_ITEM_SCHEMA },
            remove: { type: "array", items: REMOVE_ITEM_SCHEMA },
            add: { type: "array", items: addItemSchema },
          },
        },

        questions: QUESTIONS_SCHEMA,
        question_updates: QUESTION_UPDATES_SCHEMA,
      },
    },
  };
}


module.exports = { buildIntentRouterSchema };

const {
  buildCategorySubcategoryItemSchemas,
} = require("../../productCategories");

const AVAILABILITY_INTENT_ENUM = [
  "CHECK_AVAILABILITY",
  "ASK_VARIANTS",
  "ASK_BRANDS",
  "ASK_BOTH",
  "ASK_QUANTITY",
];

const COMMON_PROPS = {
  name: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  searchTerm: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  outputName: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  outputSearchTerm: {
    anyOf: [{ type: "string", minLength: 1 }, { type: "null" }],
  },

  category: { type: "string" },
  "sub-category": { type: "string" },

  requested_amount: { type: "number", minimum: 1 },
  availability_intent: { type: "string", enum: AVAILABILITY_INTENT_ENUM },
};

const COMMON_REQUIRED = [
  "name",
  "searchTerm",
  "outputName",
  "outputSearchTerm",
  "requested_amount",
  "availability_intent",
];

const CATEGORY_SUBCATEGORY_ANYOF = buildCategorySubcategoryItemSchemas(
  {
    name: COMMON_PROPS.name,
    searchTerm: COMMON_PROPS.searchTerm,
    outputName: COMMON_PROPS.outputName,
    outputSearchTerm: COMMON_PROPS.outputSearchTerm,
    requested_amount: COMMON_PROPS.requested_amount,
    availability_intent: COMMON_PROPS.availability_intent,
  },
  COMMON_REQUIRED
);

const NULL_CATEGORY_PAIR_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: [...COMMON_REQUIRED, "category", "sub-category"],
  properties: {
    name: COMMON_PROPS.name,
    searchTerm: COMMON_PROPS.searchTerm,
    outputName: COMMON_PROPS.outputName,
    outputSearchTerm: COMMON_PROPS.outputSearchTerm,
    requested_amount: COMMON_PROPS.requested_amount,
    availability_intent: COMMON_PROPS.availability_intent,
    category: { type: "null" },
    "sub-category": { type: "null" },
  },
};

const PRODUCT_SCHEMA = {
  anyOf: [NULL_CATEGORY_PAIR_SCHEMA, ...CATEGORY_SUBCATEGORY_ANYOF],
};

const INV_AVAIL_SCHEMA = {
  name: "inv_avail_extract",
  strict: true,
  schema: {
    type: "object",
    additionalProperties: false,
    required: ["products", "questions"],
    properties: {
      products: {
        type: "array",
        items: PRODUCT_SCHEMA,
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

module.exports = { INV_AVAIL_SCHEMA };

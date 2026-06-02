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
  original_user_text: { anyOf: [{ type: "string", minLength: 1 }, { type: "null" }] },
  search_terms: { type: "array", items: { type: "string", minLength: 1 } },
  outputSearchTerm: {
    anyOf: [{ type: "string", minLength: 1 }, { type: "null" }],
  },

  requested_amount: { type: "number", minimum: 1 },
  availability_intent: { type: "string", enum: AVAILABILITY_INTENT_ENUM },
  exclude_tokens: {
    type: "array",
    items: { type: "string", minLength: 1 },
  },
};

const COMMON_REQUIRED = [
  "name",
  "searchTerm",
  "outputName",
  "original_user_text",
  "search_terms",
  "outputSearchTerm",
  "requested_amount",
  "availability_intent",
  "exclude_tokens",
];

async function buildInvAvailSchema() {
  const CATEGORY_SUBCATEGORY_ANYOF = await buildCategorySubcategoryItemSchemas(
    {
      name: COMMON_PROPS.name,
      searchTerm: COMMON_PROPS.searchTerm,
      outputName: COMMON_PROPS.outputName,
      original_user_text: COMMON_PROPS.original_user_text,
      search_terms: COMMON_PROPS.search_terms,
      outputSearchTerm: COMMON_PROPS.outputSearchTerm,
      requested_amount: COMMON_PROPS.requested_amount,
      availability_intent: COMMON_PROPS.availability_intent,
      exclude_tokens: COMMON_PROPS.exclude_tokens,
    },
    COMMON_REQUIRED,
  );

  const NULL_CATEGORY_PAIR_SCHEMA = {
    type: "object",
    additionalProperties: false,
    required: [...COMMON_REQUIRED, "category", "sub-category"],
    properties: {
      name: COMMON_PROPS.name,
      searchTerm: COMMON_PROPS.searchTerm,
      outputName: COMMON_PROPS.outputName,
      original_user_text: COMMON_PROPS.original_user_text,
      search_terms: COMMON_PROPS.search_terms,
      outputSearchTerm: COMMON_PROPS.outputSearchTerm,
      requested_amount: COMMON_PROPS.requested_amount,
      availability_intent: COMMON_PROPS.availability_intent,
      exclude_tokens: COMMON_PROPS.exclude_tokens,
      category: { type: "null" },
      "sub-category": { type: "null" },
    },
  };

  const PRODUCT_SCHEMA = {
    anyOf: [NULL_CATEGORY_PAIR_SCHEMA, ...CATEGORY_SUBCATEGORY_ANYOF],
  };

  return {
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

module.exports = { buildInvAvailSchema };

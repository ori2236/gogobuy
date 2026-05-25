const {
  buildCategorySubcategoryItemSchemas,
} = require("../../productCategories");

const RECOMMENDATION_PROPS = {
  name: { type: "string", minLength: 1 },
  reason: { type: "string", minLength: 1 },
};

const RECOMMENDATION_REQUIRED = ["name", "reason"];

async function buildProductRecommendationSchema() {
  const CATEGORY_SUBCATEGORY_ANYOF = await buildCategorySubcategoryItemSchemas(
    RECOMMENDATION_PROPS,
    RECOMMENDATION_REQUIRED,
  );

  return {
    name: "ord_product_recommendations",
    strict: true,
    schema: {
      type: "object",
      additionalProperties: false,
      required: ["suggestions"],
      properties: {
        suggestions: {
          type: "array",
          items: {
            anyOf: CATEGORY_SUBCATEGORY_ANYOF,
          },
        },
      },
    },
  };
}

module.exports = { buildProductRecommendationSchema };

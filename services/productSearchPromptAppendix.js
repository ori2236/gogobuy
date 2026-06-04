const PRODUCT_SEARCH_PROMPT_APPENDIX_MARKER = "PRODUCT SEARCH TERMS APPENDIX";

const PRODUCT_SEARCH_PROMPT_APPENDIX = `

${PRODUCT_SEARCH_PROMPT_APPENDIX_MARKER}
For every product object you return, include these search helper fields in addition to the existing fields:
- original_user_text: the exact short product phrase from the customer's latest relevant message, in the same language the customer wrote it, or null if there is no clear phrase.
- search_terms: an array of short alternative search phrases that may help the backend find the same product.

Rules for original_user_text:
- Use only the core product phrase the customer actually wrote, not the full sentence.
- Keep it in the customer's language and wording.
- Remove quantities and generic action words when they are not part of the product itself.
- If the customer referred to a previous product with words like "it", "that one", "אותו", "זה", resolve the reference from the conversation when clear. If the original phrase cannot be recovered, use null.

Rules for search_terms:
- Include original_user_text when it is not null.
- Include name when it is not null.
- Include outputName/searchTerm/outputSearchTerm when relevant and available in the schema.
- You may include singular/plural or wording variants only when you are confident they refer to the same product.
- Do not invent strange variants, unrelated categories, brands, package sizes, flavors, weights, or volumes.
- Never return or add a supplier/brand/manufacturer name by itself as the product name or as a standalone search term unless the customer explicitly wrote only that brand as the thing they want to search.
- For generic requests like "חלב כלשהו", "איזה חלב", "any milk", keep the product type in the name/search_terms (for example "חלב"), and do not replace it with a supplier/brand such as a dairy company name.
- Keep the list short, usually 1-4 terms.
- If unsure, return a short conservative list rather than broad guesses.
- Use [] when no useful extra terms exist.

Backend compatibility:
- These fields help matching only. They must not change amount, units, sold_by_weight, category, sub-category, exclude_tokens, price_intent, or any existing field semantics.
`;

function appendProductSearchPromptAppendix(prompt) {
  const base = String(prompt || "").trim();
  if (base.includes(PRODUCT_SEARCH_PROMPT_APPENDIX_MARKER)) return base;
  return [base, PRODUCT_SEARCH_PROMPT_APPENDIX.trim()].filter(Boolean).join("\n\n");
}

module.exports = {
  PRODUCT_SEARCH_PROMPT_APPENDIX_MARKER,
  PRODUCT_SEARCH_PROMPT_APPENDIX,
  appendProductSearchPromptAppendix,
};

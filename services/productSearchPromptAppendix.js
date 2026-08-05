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

Rules for negative wording and exclude_tokens:
- Distinguish between a desired catalog attribute and a rejected variant.
- When the customer asks for a product whose desired name commonly includes the negative wording, keep the complete phrase in original_user_text and search_terms. Examples of the general pattern are "ללא X", "בלי X", "נטול X", "without X", and "X free".
- Do not remove the negative wording from all search phrases. The backend uses the complete phrase to prefer a product that explicitly has the requested attribute.
- exclude_tokens may still contain X when the customer rejects products containing X. The backend understands that a product explicitly named "ללא X" does not positively contain X.
- For a rejected brand, flavor, or variant that is not itself the desired product name, keep the generic product type in name/search_terms and put only the rejected words in exclude_tokens.
- Never invent negative attributes that the customer did not request.

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

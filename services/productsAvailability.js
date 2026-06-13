const {
  findBestProductForRequest,
  fetchCustomerDefaultProductIds,
} = require("./products");
const { getExcludeTokensFromReq } = require("../utilities/tokens");

async function findBestProductForAvailability(shop_id, req, opts = {}) {
  return await findBestProductForRequest(shop_id, req, opts);
}

async function searchProductsAvailability(shop_id, products, opts = {}) {
  const customerDefaultProductIds = await fetchCustomerDefaultProductIds({
    shop_id,
    customer_id: opts.customer_id,
  });

  const found = [];
  const notFound = [];

  for (let i = 0; i < products.length; i++) {
    const req = products[i];

    const row = await findBestProductForAvailability(shop_id, req, {
      customerDefaultProductIds,
    });

    if (row) {
      const n = Number(req?.amount);
      const u = Number(req?.units);
      const weightFlag = req?.sold_by_weight === true;

      found.push({
        originalIndex: i,
        product_id: row.id,
        matched_name: row.name,
        price: Number(row.price),
        stock_amount: Number(row.stock_amount),
        category: row.category,
        sub_category: row.sub_category,
        requested_name: req?.name || null,
        requested_original_user_text: req?.original_user_text || null,
        requested_search_terms: Array.isArray(req?.search_terms)
          ? req.search_terms
          : [],
        requested_amount: Number.isFinite(n) ? n : 1,
        requested_units: Number.isFinite(u) && u > 0 ? u : null,
        sold_by_weight: weightFlag === true,
        matched_display_name_en: row.display_name_en,
      });
    } else {
      const n = Number(req?.amount);
      const excludeTokens = getExcludeTokensFromReq(req);

      notFound.push({
        originalIndex: i,
        requested_name: req?.name || null,
        requested_output_name: req?.outputName || null,
        requested_original_user_text: req?.original_user_text || null,
        requested_search_terms: Array.isArray(req?.search_terms)
          ? req.search_terms
          : [],
        requested_amount: Number.isFinite(n) ? n : 1,
        category: req?.category || null,
        sub_category: req?.["sub-category"] || req?.sub_category || null,
        exclude_tokens: excludeTokens,
      });
    }
  }

  return { found, notFound };
}

module.exports = {
  searchProductsAvailability,
};

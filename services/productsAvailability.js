const db = require("../config/db");
const { findBestProductForRequest, searchVariants } = require("./products");
const { getExcludeTokensFromReq } = require("../utilities/tokens");

function isHebrewOrNumberToken(t) {
  const s = String(t || "");
  return /[\u0590-\u05FF]/.test(s) || /\d/.test(s);
}

function tokenizeSimple(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[^\w\u0590-\u05FF]+/g, " ")
    .split(/\s+/)
    .map((t) => t.trim())
    .filter(Boolean);
}

function tokenizeHebrewOnly(str) {
  return tokenizeSimple(str).filter(isHebrewOrNumberToken);
}

function sameTokenSet(a, b) {
  const A = Array.from(new Set(a));
  const B = Array.from(new Set(b));
  if (A.length !== B.length) return false;
  const setB = new Set(B);
  return A.every((t) => setB.has(t));
}

function rowContainsAllTokens(row, tokens) {
  if (!tokens || !tokens.length) return true;
  const hay = String(row?.name || "").toLowerCase();
  return tokens.every((t) => hay.includes(t));
}

function buildUniqueSearchTerm(...values) {
  const out = [];
  const seen = new Set();

  for (const value of values) {
    for (const token of tokenizeSimple(value)) {
      if (seen.has(token)) continue;
      seen.add(token);
      out.push(token);
    }
  }

  return out.join(" ");
}

function pickBestAvailabilityVariant(rows = []) {
  if (!rows || !rows.length) return null;

  const sorted = [...rows].sort((a, b) => {
    const aStock = Number(a.stock_amount);
    const bStock = Number(b.stock_amount);

    const aInStock = !Number.isFinite(aStock) || aStock > 0 ? 1 : 0;
    const bInStock = !Number.isFinite(bStock) || bStock > 0 ? 1 : 0;

    const aWords = tokenizeSimple(a.name || "").length || 9999;
    const bWords = tokenizeSimple(b.name || "").length || 9999;

    const aPrice = Number.isFinite(Number(a.price)) ? Number(a.price) : 999999;
    const bPrice = Number.isFinite(Number(b.price)) ? Number(b.price) : 999999;

    return (
      bInStock - aInStock ||
      aWords - bWords ||
      aPrice - bPrice ||
      Number(b.id || 0) - Number(a.id || 0)
    );
  });

  return sorted[0] || null;
}

async function fallbackFindRowFromVariants(shop_id, req) {
  const nameRaw = String(req?.name || "").trim();
  const searchTermRaw = String(req?.searchTerm || "").trim();
  const category = String(req?.category || "").trim();
  const subCategory = String(
    req?.["sub-category"] || req?.sub_category || "",
  ).trim();

  const effectiveSearchTerm = buildUniqueSearchTerm(nameRaw, searchTermRaw);
  const effectiveTokens = tokenizeHebrewOnly(effectiveSearchTerm);
  const searchTermTokens = tokenizeHebrewOnly(searchTermRaw);

  /*
    Conservative rescue before returning NOT_FOUND:
    - Try this only when there is a specific searchTerm, usually a brand/type.
    - Or when the product name itself has at least 2 tokens, like "בירה טובורג".
    - Do not run it for very general questions like "יש בירה?".
  */
  const shouldTry = searchTermTokens.length > 0 || effectiveTokens.length >= 2;

  if (!shouldTry || !effectiveSearchTerm) return null;

  const rows = await searchVariants(shop_id, {
    category: category || null,
    subCategory: subCategory || null,
    searchTerm: effectiveSearchTerm,
    limit: 10,
    excludeTokens: getExcludeTokensFromReq(req),
  });

  return pickBestAvailabilityVariant(rows || []);
}

async function fallbackFindRowWithSearchTerm(shop_id, req) {
  const nameRaw = String(req?.name || "").trim();
  const category = String(req?.category || "").trim();
  const subCategory = String(
    req?.["sub-category"] || req?.sub_category || "",
  ).trim();

  const stHeb = String(req?.searchTerm || "").trim();

  const nameTokens = tokenizeHebrewOnly(nameRaw);
  const stTokens = tokenizeHebrewOnly(stHeb);

  if (!nameTokens.length || !stTokens.length) return null;

  const excludeTokens = getExcludeTokensFromReq(req);

  let sql = `
    SELECT id, name, display_name_en, price, stock_amount, category, sub_category
    FROM product
    WHERE shop_id = ?
  `;
  const params = [shop_id];

  if (category) {
    sql += ` AND category = ?`;
    params.push(category);
  }
  if (subCategory) {
    sql += ` AND sub_category = ?`;
    params.push(subCategory);
  }

  for (const t of nameTokens) {
    sql += `
      AND (
        name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
      )
    `;
    params.push(t);
  }

  for (const t of stTokens) {
    sql += `
      AND (
        name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
      )
    `;
    params.push(t);
  }

  for (const t of excludeTokens) {
    sql += `
      AND (
        name COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
        AND display_name_en COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
      )
    `;
    params.push(t, t);
  }

  sql += `
    ORDER BY
      (stock_amount IS NULL OR stock_amount > 0) DESC,
      CHAR_LENGTH(name) ASC,
      id DESC
    LIMIT 1
  `;

  const [rows] = await db.query(sql, params);
  return rows?.[0] || null;
}

async function findBestProductForAvailability(shop_id, req) {
  const stHeb = String(req?.searchTerm || "").trim();

  const nameHeb = String(req?.name || "").trim();
  const redundant =
    nameHeb &&
    stHeb &&
    sameTokenSet(tokenizeHebrewOnly(nameHeb), tokenizeHebrewOnly(stHeb));

  const stTokens = redundant ? [] : tokenizeHebrewOnly(stHeb);

  if (stTokens.length) {
    const fb = await fallbackFindRowWithSearchTerm(shop_id, req);
    if (fb) return fb;
  }

  const row = await findBestProductForRequest(shop_id, req);

  if (row && (!stTokens.length || rowContainsAllTokens(row, stTokens))) {
    return row;
  }

  /*
    Last small rescue before NOT_FOUND:
    use the same broader search used by ASK_VARIANTS.
    This fixes cases like "בירה טובורג" where variants search can find Tuborg,
    while the single-product matcher failed.
  */
  const variantRow = await fallbackFindRowFromVariants(shop_id, req);
  if (variantRow) return variantRow;

  return null;
}

async function searchProductsAvailability(shop_id, products) {
  const found = [];
  const notFound = [];

  for (let i = 0; i < products.length; i++) {
    const req = products[i];

    const row = await findBestProductForAvailability(shop_id, req);

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

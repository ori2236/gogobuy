const db = require("../config/db");

const {
  findBestProductForRequest,
  getExcludeTokensFromReq,
} = require("./products");

function isHebrewOrNumberToken(t) {
  const s = String(t || "");
  return /[\u0590-\u05FF]/.test(s) || /\d/.test(s);
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
function tokenizeSimple(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[^\w\u0590-\u05FF]+/g, " ")
    .split(/\s+/)
    .map((t) => t.trim())
    .filter(Boolean);
}

function rowContainsAllTokens(row, tokens) {
  if (!tokens || !tokens.length) return true;
  const hay = String(row?.name || "").toLowerCase();
  return tokens.every((t) => hay.includes(t));
}

async function fallbackFindRowWithSearchTerm(shop_id, req) {
  const nameRaw = String(req?.name || "").trim();
  const category = String(req?.category || "").trim();
  const subCategory = String(
    req?.["sub-category"] || req?.sub_category || ""
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
)
    `;
    params.push(t);
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
  if (!row) return null;

  if (stTokens.length && !rowContainsAllTokens(row, stTokens)) {
    return null;
  }

  return row;
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

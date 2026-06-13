const db = require("../config/db");
const {
  tokenImportance,
  tokenizeName,
  getExcludeTokensFromReq,
  filterRowsByExcludeTokens,
} = require("../utilities/tokens");
const { getSubCategoryCandidates } = require("../repositories/categories");
const { ensureProductDefaultSchema } = require("../utilities/productDefaultSchema");

const MATCH_DEBUG = process.env.DEBUG_PRODUCT_MATCH === "1";
const DEFAULT_PRODUCT_SCORE_BONUS = Number.isFinite(Number(process.env.DEFAULT_PRODUCT_SCORE_BONUS))
  ? Number(process.env.DEFAULT_PRODUCT_SCORE_BONUS)
  : 2.5;

// Customer defaults are intentionally binary: if the customer ordered a product
// in one of their recent completed/confirmed orders, it receives this one-time
// preference boost. Ordering the same product many times does not increase it.
const CUSTOMER_PRODUCT_SCORE_BONUS = Number.isFinite(Number(process.env.CUSTOMER_PRODUCT_SCORE_BONUS))
  ? Number(process.env.CUSTOMER_PRODUCT_SCORE_BONUS)
  : 5;

const CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT = Number.isFinite(Number(process.env.CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT))
  ? Math.max(1, Math.trunc(Number(process.env.CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT)))
  : 10;

function matchLog(label, payload = null) {
  if (!MATCH_DEBUG) return;
  if (payload === null || payload === undefined) {
    console.log(`[MATCH] ${label}`);
    return;
  }
  try {
    console.log(`[MATCH] ${label}:`, JSON.stringify(payload, null, 2));
  } catch {
    console.log(`[MATCH] ${label}:`, payload);
  }
}

function compactRows(rows = []) {
  return rows.map((r) => ({
    id: Number(r.id),
    name: r.name,
    display_name_en: r.display_name_en,
    category: r.category,
    sub_category: r.sub_category,
    price: Number(r.price),
    stock_amount:
      r.stock_amount === null || r.stock_amount === undefined
        ? null
        : Number(r.stock_amount),
    is_default: Number(r.is_default || 0) === 1,
    customer_default: Number(r.customer_default || 0) === 1,
  }));
}


function normalizeSearchPhrase(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .normalize("NFKC")
    .replace(/[\u2018\u2019\u201A\u201B\u2032\u0060]/g, "'")
    .replace(/[\u05F3]/g, "'")
    .replace(/[\u201C\u201D\u201E\u201F\u2033\u05F4]/g, '"')
    .replace(/\s+/g, " ")
    .trim();
}

function termKey(value) {
  const normalized = normalizeSearchPhrase(value).toLowerCase();
  const tokens = tokenizeName(normalized);
  return tokens.length ? tokens.join(" ") : normalized;
}

function pushSearchTerm(out, seen, value, source) {
  const term = normalizeSearchPhrase(value);
  if (!term) return;

  const key = termKey(term);
  if (!key || seen.has(key)) return;

  const tokens = tokenizeName(term);
  if (!tokens.length) return;

  seen.add(key);
  out.push({ term, source, tokens });
}

function buildProductSearchTerms(req = {}) {
  const out = [];
  const seen = new Set();

  const original = req?.original_user_text;
  const name = req?.name;
  const outputName = req?.outputName;
  const searchTerm = req?.searchTerm;
  const outputSearchTerm = req?.outputSearchTerm;

  pushSearchTerm(out, seen, original, "original_user_text");

  const nameText = normalizeSearchPhrase(name);
  const searchText = normalizeSearchPhrase(searchTerm);
  if (nameText && searchText && termKey(nameText) !== termKey(searchText)) {
    pushSearchTerm(out, seen, `${nameText} ${searchText}`, "name+searchTerm");
  }

  pushSearchTerm(out, seen, name, "name");
  pushSearchTerm(out, seen, outputName, "outputName");

  if (Array.isArray(req?.search_terms)) {
    for (const term of req.search_terms) {
      pushSearchTerm(out, seen, term, "search_terms");
    }
  }

  pushSearchTerm(out, seen, searchTerm, "searchTerm");
  pushSearchTerm(out, seen, outputSearchTerm, "outputSearchTerm");

  return out;
}

function compactSearchTerms(terms = []) {
  return terms.map((t) => ({
    source: t.source,
    term: t.term,
    tokens: t.tokens,
  }));
}


function normalizeCustomerDefaultProductIds(ids = []) {
  const list =
    ids instanceof Set
      ? Array.from(ids)
      : Array.isArray(ids)
        ? ids
        : ids && typeof ids[Symbol.iterator] === "function"
          ? Array.from(ids)
          : [];

  return new Set(
    list
      .map((id) => Number(id))
      .filter((id) => Number.isFinite(id) && id > 0),
  );
}

function annotateCustomerDefaults(rows = [], customerDefaultProductIds = new Set()) {
  if (!rows || !rows.length || !customerDefaultProductIds?.size) return rows || [];
  return rows.map((r) => ({
    ...r,
    customer_default: customerDefaultProductIds.has(Number(r.id)) ? 1 : 0,
  }));
}

function sortByCustomerThenShopDefault(rows = []) {
  return (rows || [])
    .slice()
    .sort(
      (a, b) =>
        Number(b.customer_default || 0) - Number(a.customer_default || 0) ||
        Number(b.is_default || 0) - Number(a.is_default || 0) ||
        new Date(b.updated_at || 0).getTime() - new Date(a.updated_at || 0).getTime() ||
        Number(b.id || 0) - Number(a.id || 0),
    );
}

async function fetchCustomerDefaultProductIds({ shop_id, customer_id }) {
  const customerId = Number(customer_id);
  const shopId = Number(shop_id);

  if (!Number.isFinite(customerId) || customerId <= 0) return new Set();
  if (!Number.isFinite(shopId) || shopId <= 0) return new Set();

  // Important: choose the recent orders *after* verifying they actually have
  // product rows. During tests and normal conversations a customer can have
  // fresh empty/temporary orders; those must not hide older real purchases.
  const [rows] = await db.query(
    `
      SELECT DISTINCT oi.product_id
      FROM order_item oi
      JOIN (
        SELECT o.id, o.created_at
        FROM orders o
        WHERE o.shop_id = ?
          AND o.customer_id = ?
          AND o.status IN ('confirmed','preparing','ready','delivering','completed')
          AND EXISTS (
            SELECT 1
            FROM order_item oi_exists
            WHERE oi_exists.order_id = o.id
              AND oi_exists.product_id IS NOT NULL
          )
        ORDER BY o.created_at DESC, o.id DESC
        LIMIT ?
      ) recent_orders ON recent_orders.id = oi.order_id
      WHERE oi.product_id IS NOT NULL
    `,
    [shopId, customerId, CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT],
  );

  const ids = normalizeCustomerDefaultProductIds((rows || []).map((r) => r.product_id));

  matchLog("fetchCustomerDefaultProductIds.result", {
    shop_id: shopId,
    customer_id: customerId,
    recentOrdersLimit: CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT,
    rows: rows || [],
    productIds: Array.from(ids),
  });

  return ids;
}

async function queryRowsByTokens({
  shop_id,
  category = null,
  subCategories = null,
  tokenGroup,
  includeStockFilter = true,
  customerDefaultProductIds = new Set(),
}) {
  let sql = `
    SELECT id, name, display_name_en, price, stock_amount, is_default, category, sub_category, updated_at
    FROM product
    WHERE shop_id = ?
  `;
  const params = [shop_id];

  if (includeStockFilter) {
    sql += ` AND (stock_amount IS NULL OR stock_amount > 0)`;
  }

  if (category) {
    sql += ` AND category = ?`;
    params.push(category);
  }

  if (Array.isArray(subCategories) && subCategories.length) {
    sql += ` AND sub_category IN (${subCategories.map(() => "?").join(",")})`;
    params.push(...subCategories);
  } else if (typeof subCategories === "string" && subCategories.trim()) {
    sql += ` AND sub_category = ?`;
    params.push(subCategories.trim());
  }

  for (const t of tokenGroup.tokens) {
    sql += `
      AND (
        name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
        OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
      )
    `;
    params.push(t, t);
  }

  matchLog("queryRowsByTokens.query", {
    source: tokenGroup.source,
    term: tokenGroup.term,
    tokens: tokenGroup.tokens,
    category,
    subCategories,
    includeStockFilter,
    sql,
    params,
  });

  const [rows] = await db.query(sql, params);

  matchLog("queryRowsByTokens.rows", {
    source: tokenGroup.source,
    term: tokenGroup.term,
    tokens: tokenGroup.tokens,
    rows: compactRows(rows || []),
  });

  return annotateCustomerDefaults(rows || [], customerDefaultProductIds);
}

async function findBestByTermGroups({
  shop_id,
  termGroups,
  excludeTokens,
  category = null,
  subCategories = null,
  debugLabel = "",
  includeStockFilter = true,
  customerDefaultProductIds = new Set(),
}) {
  for (const tokenGroup of termGroups) {
    const rows = await queryRowsByTokens({
      shop_id,
      category,
      subCategories,
      tokenGroup,
      includeStockFilter,
      customerDefaultProductIds,
    });

    const best = await pickBestWeighted({
      shop_id,
      rows,
      reqTokens: tokenGroup.tokens,
      excludeTokens,
      debugLabel: `${debugLabel}:${tokenGroup.source}:${tokenGroup.term}`,
      customerDefaultProductIds,
    });

    if (best) {
      matchLog("findBestByTermGroups.best", {
        debugLabel,
        matchedBy: {
          source: tokenGroup.source,
          term: tokenGroup.term,
          tokens: tokenGroup.tokens,
        },
        matchedRow: {
          id: Number(best.id),
          name: best.name,
          display_name_en: best.display_name_en,
          category: best.category,
          sub_category: best.sub_category,
        },
      });
      return best;
    }
  }

  return null;
}

async function pickBestWeighted({
  shop_id,
  rows,
  reqTokens,
  excludeTokens,
  debugLabel = "",
  customerDefaultProductIds = new Set(),
}) {
  rows = annotateCustomerDefaults(rows, customerDefaultProductIds);

  matchLog("pickBestWeighted.input", {
    debugLabel,
    reqTokens,
    excludeTokens,
    rows_before_filter: compactRows(rows || []),
  });

  rows = filterRowsByExcludeTokens(rows, excludeTokens);

  matchLog("pickBestWeighted.afterExcludeFilter", {
    debugLabel,
    rows_after_filter: compactRows(rows || []),
  });

  if (!rows || !rows.length) {
    matchLog("pickBestWeighted.noRowsAfterFilter", { debugLabel });
    return null;
  }

  const reqSet = new Set(reqTokens);

  const allExtra = [];
  const meta = [];

  for (const r of rows) {
    const candTokens = tokenizeName(r.name || "");
    const extra = Array.from(new Set(candTokens.filter((t) => !reqSet.has(t))));
    meta.push({ r, candTokens, extra });
    allExtra.push(...extra);
  }

  const invDfMap = await fetchInvDfMap(shop_id, allExtra);

  matchLog("pickBestWeighted.invDfMap", {
    debugLabel,
    invDf: Object.fromEntries(invDfMap.entries()),
  });

  const scored = [];

  for (const m of meta) {
    const wordCount = m.candTokens.length || 9999;

    const price = Number(m.r.price);
    const priceScore = Number.isFinite(price) ? price : 999999;

    let extraScore = 0;
    const extraBreakdown = [];

    for (const t of m.extra) {
      const wRaw = invDfMap.has(t) ? invDfMap.get(t) : 1;
      const inv = Number(wRaw) || 1;
      const imp = tokenImportance(t);
      const add = inv * imp;

      extraScore += add;
      extraBreakdown.push({
        token: t,
        inv_df: inv,
        importance: imp,
        contribution: add,
      });
    }

    const isCustomerDefault = Number(m.r.customer_default || 0) === 1;
    const customerBonus = isCustomerDefault ? CUSTOMER_PRODUCT_SCORE_BONUS : 0;
    const isDefault = Number(m.r.is_default || 0) === 1;
    const defaultBonus = isDefault ? DEFAULT_PRODUCT_SCORE_BONUS : 0;
    const matchScore = extraScore - customerBonus - defaultBonus;

    scored.push({
      row: m.r,
      candTokens: m.candTokens,
      extraTokens: m.extra,
      extraBreakdown,
      extraScore,
      customerBonus,
      defaultBonus,
      matchScore,
      isCustomerDefault,
      isDefault,
      priceScore,
      wordCount,
      extraCount: m.extra.length,
    });
  }

  scored.sort(
    (a, b) =>
      a.matchScore - b.matchScore ||
      a.extraScore - b.extraScore ||
      a.priceScore - b.priceScore ||
      a.wordCount - b.wordCount ||
      b.row.id - a.row.id,
  );

  matchLog("pickBestWeighted.scored", {
    debugLabel,
    scored: scored.map((s) => ({
      id: Number(s.row.id),
      name: s.row.name,
      display_name_en: s.row.display_name_en,
      candTokens: s.candTokens,
      extraTokens: s.extraTokens,
      extraBreakdown: s.extraBreakdown,
      extraScore: s.extraScore,
      customerBonus: s.customerBonus,
      defaultBonus: s.defaultBonus,
      matchScore: s.matchScore,
      isCustomerDefault: s.isCustomerDefault,
      isDefault: s.isDefault,
      priceScore: s.priceScore,
      wordCount: s.wordCount,
      extraCount: s.extraCount,
    })),
  });

  const best = scored[0];

  matchLog("pickBestWeighted.best", {
    debugLabel,
    chosen: best
      ? {
          id: Number(best.row.id),
          name: best.row.name,
          display_name_en: best.row.display_name_en,
          extraScore: best.extraScore,
          customerBonus: best.customerBonus,
          defaultBonus: best.defaultBonus,
          matchScore: best.matchScore,
          isCustomerDefault: best.isCustomerDefault,
          isDefault: best.isDefault,
          priceScore: best.priceScore,
          wordCount: best.wordCount,
        }
      : null,
  });

  return best ? best.row : null;
}

async function findBestProductForRequest(shop_id, req, opts = {}) {
  await ensureProductDefaultSchema();

  let customerDefaultProductIds = normalizeCustomerDefaultProductIds(
    opts.customerDefaultProductIds || [],
  );

  if (!customerDefaultProductIds.size && opts.customer_id) {
    customerDefaultProductIds = await fetchCustomerDefaultProductIds({
      shop_id,
      customer_id: opts.customer_id,
    });
  }

  const category = (req?.category || "").trim();
  const subCategoryRaw = (
    req?.["sub-category"] ||
    req?.sub_category ||
    ""
  ).trim();
  const nameRaw = (req?.name || "").trim();

  const primarySub = subCategoryRaw || null;
  const subCandidates = primarySub
    ? await getSubCategoryCandidates(category, primarySub)
    : [];
  const otherSubs = primarySub
    ? subCandidates.filter((s) => s !== primarySub)
    : [];

  const searchTerms = buildProductSearchTerms(req);
  const reqTokens = searchTerms.length ? searchTerms[0].tokens : tokenizeName(nameRaw);
  const excludeTokens = getExcludeTokensFromReq(req);

  matchLog("findBestProductForRequest.start", {
    shop_id,
    req,
    normalized: {
      category,
      subCategoryRaw,
      nameRaw,
      primarySub,
      subCandidates,
      otherSubs,
      finalSearchTerms: compactSearchTerms(searchTerms),
      reqTokens,
      excludeTokens,
    },
  });

  if (!searchTerms.length) {
    matchLog("findBestProductForRequest.noSearchTerms", {
      shop_id,
      req,
      category,
      primarySub,
      otherSubs,
    });

    if (category && primarySub) {
      const params = [shop_id, category, primarySub];
      let sql = `
        SELECT id, name, display_name_en, price, stock_amount, is_default, category, sub_category, updated_at
        FROM product
        WHERE shop_id = ?
          AND category = ?
          AND sub_category = ?
      `;

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
        ORDER BY is_default DESC, updated_at DESC, id DESC
        LIMIT 1
      `;

      matchLog("findBestProductForRequest.query.noTerms.primary", {
        sql,
        params,
      });

      const [rawRows] = await db.query(sql, params);
      const rows = sortByCustomerThenShopDefault(
        annotateCustomerDefaults(rawRows || [], customerDefaultProductIds),
      );

      matchLog("findBestProductForRequest.rows.noTerms.primary", {
        rows: compactRows(rows),
      });

      if (rows && rows.length) {
        matchLog("findBestProductForRequest.return.noTerms.primary", rows[0]);
        return rows[0];
      }

      if (otherSubs.length) {
        const params2 = [shop_id, category, ...otherSubs];
        let sql2 = `
          SELECT id, name, display_name_en, price, stock_amount, is_default, category, sub_category
          FROM product
          WHERE shop_id = ?
            AND category = ?
            AND sub_category IN (${otherSubs.map(() => "?").join(",")})
        `;

        for (const t of excludeTokens) {
          sql2 += `
            AND (
              name COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
              AND display_name_en COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
            )
          `;
          params2.push(t, t);
        }

        sql2 += `
          ORDER BY is_default DESC, updated_at DESC, id DESC
          LIMIT 1
        `;

        matchLog("findBestProductForRequest.query.noTerms.otherSubs", {
          sql: sql2,
          params: params2,
        });

        const [rawRows2] = await db.query(sql2, params2);
        const rows2 = sortByCustomerThenShopDefault(
          annotateCustomerDefaults(rawRows2 || [], customerDefaultProductIds),
        );

        matchLog("findBestProductForRequest.rows.noTerms.otherSubs", {
          rows: compactRows(rows2),
        });

        if (rows2 && rows2.length) {
          matchLog(
            "findBestProductForRequest.return.noTerms.otherSubs",
            rows2[0],
          );
          return rows2[0];
        }
      }
    }

    matchLog("findBestProductForRequest.return.noTerms.null", { req });
    return null;
  }

  if (category && primarySub) {
    const bestPrimary = await findBestByTermGroups({
      shop_id,
      termGroups: searchTerms,
      excludeTokens,
      category,
      subCategories: primarySub,
      debugLabel: "primarySub",
      customerDefaultProductIds,
    });
    if (bestPrimary) {
      matchLog("findBestProductForRequest.return.primarySub", bestPrimary);
      return bestPrimary;
    }

    if (otherSubs.length) {
      const bestOther = await findBestByTermGroups({
        shop_id,
        termGroups: searchTerms,
        excludeTokens,
        category,
        subCategories: otherSubs,
        debugLabel: "otherSubs",
        customerDefaultProductIds,
      });
      if (bestOther) {
        matchLog("findBestProductForRequest.return.otherSubs", bestOther);
        return bestOther;
      }
    }
  }

  if (category) {
    const bestCategoryWide = await findBestByTermGroups({
      shop_id,
      termGroups: searchTerms,
      excludeTokens,
      category,
      subCategories: null,
      debugLabel: "categoryWide",
      customerDefaultProductIds,
    });
    if (bestCategoryWide) {
      matchLog("findBestProductForRequest.return.categoryWide", bestCategoryWide);
      return bestCategoryWide;
    }
  }

  const bestShopWide = await findBestByTermGroups({
    shop_id,
    termGroups: searchTerms,
    excludeTokens,
    category: null,
    subCategories: null,
    debugLabel: "shopWide",
    customerDefaultProductIds,
  });
  if (bestShopWide) {
    matchLog("findBestProductForRequest.return.shopWide", bestShopWide);
    return bestShopWide;
  }

  matchLog("findBestProductForRequest.return.null", {
    req,
    category,
    primarySub,
    finalSearchTerms: compactSearchTerms(searchTerms),
    excludeTokens,
  });

  return null;
}

async function fetchInvDfMap(shop_id, tokens) {
  const uniq = Array.from(new Set(tokens)).filter(Boolean);

  if (!uniq.length) {
    matchLog("fetchInvDfMap.empty", { shop_id });
    return new Map();
  }

  const placeholders = uniq.map(() => "?").join(",");
  const [rows] = await db.query(
    `
    SELECT token, inv_df
    FROM product_token_weight
    WHERE shop_id = ?
      AND token IN (${placeholders})
    `,
    [shop_id, ...uniq],
  );

  const map = new Map();
  for (const r of rows || []) {
    map.set(String(r.token), Number(r.inv_df));
  }

  matchLog("fetchInvDfMap.result", {
    shop_id,
    requestedTokens: uniq,
    foundRows: rows,
  });

  return map;
}

async function searchProducts(shop_id, products, opts = {}) {
  const customerDefaultProductIds = await fetchCustomerDefaultProductIds({
    shop_id,
    customer_id: opts.customer_id,
  });

  matchLog("searchProducts.start", {
    shop_id,
    customer_id: opts.customer_id || null,
    customerDefaultProductIds: Array.from(customerDefaultProductIds),
    products,
  });

  const found = [];
  const notFound = [];

  for (let i = 0; i < products.length; i++) {
    const req = products[i];

    matchLog("searchProducts.item.start", {
      index: i,
      req,
    });

    const row = await findBestProductForRequest(shop_id, req, {
      customerDefaultProductIds,
    });

    if (row) {
      const n = Number(req?.amount);
      const u = Number(req?.units);
      const weightFlag = req?.sold_by_weight === true;

      const foundItem = {
        originalIndex: i,
        product_id: row.id,
        matched_name: row.name,
        price: Number(row.price),
        stock_amount: Number(row.stock_amount),
        category: row.category,
        sub_category: row.sub_category,
        requested_name: req?.name || null,
        requested_original_user_text: req?.original_user_text || null,
        requested_search_terms: Array.isArray(req?.search_terms) ? req.search_terms : [],
        final_search_terms: compactSearchTerms(buildProductSearchTerms(req)),
        requested_amount: Number.isFinite(n) ? n : 1,
        requested_units: Number.isFinite(u) && u > 0 ? u : null,
        sold_by_weight: weightFlag === true,
        matched_display_name_en: row.display_name_en,
        is_default: Number(row.is_default || 0) === 1,
        customer_default: Number(row.customer_default || 0) === 1,
      };

      matchLog("searchProducts.item.found", foundItem);
      found.push(foundItem);
    } else {
      const n = Number(req?.amount);
      const excludeTokens = getExcludeTokensFromReq(req);

      const notFoundItem = {
        originalIndex: i,
        requested_name: req?.name || null,
        requested_output_name: req?.outputName || null,
        requested_original_user_text: req?.original_user_text || null,
        requested_search_terms: Array.isArray(req?.search_terms) ? req.search_terms : [],
        final_search_terms: compactSearchTerms(buildProductSearchTerms(req)),
        requested_amount: Number.isFinite(n) ? n : 1,
        category: req?.category || null,
        sub_category: req?.["sub-category"] || req?.sub_category || null,
        exclude_tokens: excludeTokens,
      };

      matchLog("searchProducts.item.notFound", notFoundItem);
      notFound.push(notFoundItem);
    }
  }

  matchLog("searchProducts.end", { found, notFound });

  return { found, notFound };
}

async function fetchAlternatives(
  shop_id,
  category,
  subCategory,
  excludeIds = [],
  limit = 3,
  requestedName = null,
  excludeTokens = [],
) {
  if (!category && !subCategory) return [];

  await ensureProductDefaultSchema();

  const reqTokens = typeof requestedName === "object"
    ? (buildProductSearchTerms(requestedName)[0]?.tokens || [])
    : tokenizeName(requestedName || "");

  async function fetchByCatSub(cat, sub, useGroup = true) {
    const params = [shop_id];
    let sql = `
      SELECT id, name, display_name_en, price, stock_amount, is_default, category, sub_category
      FROM product
      WHERE shop_id = ?
        AND (stock_amount IS NULL OR stock_amount > 0)
    `;

    if (cat) {
      sql += ` AND category = ?`;
      params.push(cat);
    }

    let subList = [];
    if (sub) {
      if (useGroup) {
        subList = await getSubCategoryCandidates(cat, sub);
      } else {
        subList = [sub];
      }
    }

    if (subList.length) {
      sql += ` AND sub_category IN (${subList.map(() => "?").join(",")})`;
      params.push(...subList);
    }

    if (Array.isArray(excludeIds) && excludeIds.length) {
      sql += ` AND id NOT IN (${excludeIds.map(() => "?").join(",")})`;
      params.push(...excludeIds);
    }

    const [rows] = await db.query(sql, params);
    if (!rows || !rows.length) return [];

    const filteredRows = filterRowsByExcludeTokens(rows, excludeTokens);
    if (!filteredRows.length) return [];

    // all tokens
    if (reqTokens.length) {
      const scored = filteredRows.map((r) => {
        const candTokens = tokenizeName(r.name || "");
        let hitW = 0;
        let totalW = 0;

        for (const t of reqTokens) {
          const w = tokenImportance(t);
          totalW += w;
          if (candTokens.includes(t)) hitW += w;
        }

        const score = totalW > 0 ? hitW / totalW : 0;

        const isPrimary = sub && String(r.sub_category) === String(sub);
        const isDefault = Number(r.is_default || 0) === 1;
        return {
          row: r,
          score,
          isPrimary,
          isDefault,
          wordCount: candTokens.length || 9999,
        };
      });

      const positive = scored
        .filter((s) => s.score > 0)
        .sort(
          (a, b) =>
            b.score - a.score ||
            (b.isDefault ? 1 : 0) - (a.isDefault ? 1 : 0) ||
            (b.isPrimary ? 1 : 0) - (a.isPrimary ? 1 : 0) ||
            a.wordCount - b.wordCount ||
            a.row.id - b.row.id,
        )

        .map((s) => s.row);

      if (positive.length >= limit) return positive.slice(0, limit);

      const zero = scored.filter((s) => s.score === 0).map((s) => s.row);
      return [...positive, ...zero].slice(0, limit);
    }

    return filteredRows
      .slice()
      .sort(
        (a, b) =>
          Number(b.is_default || 0) - Number(a.is_default || 0) ||
          String(a.name || "").localeCompare(String(b.name || ""), "he") ||
          Number(a.id || 0) - Number(b.id || 0),
      )
      .slice(0, limit);
  }

  let rows = [];

  //category + subCategory
  if (category && subCategory) {
    rows = await fetchByCatSub(category, subCategory, true);
    if (rows && rows.length) return rows;
  }

  //only category
  if (category) {
    rows = await fetchByCatSub(category, null, true);
    if (rows && rows.length) return rows;
  }

  return [];
}

const AVAIL_INTROS_HE = [
  (subject) =>
    subject
      ? `לצערנו אין לנו במלאי ${subject},`
      : `לצערנו המוצר שחיפשת חסר במלאי,`,
  (subject) =>
    subject
      ? `${subject} כרגע לא זמין במלאי,`
      : `המוצר שחיפשת כרגע לא זמין במלאי,`,
  (subject) =>
    subject
      ? `לא מצאנו את ${subject} במלאי,`
      : `לא מצאנו את המוצר שחיפשת במלאי,`,
  (subject) =>
    subject ? `${subject} חסר כרגע על המדף,` : `המוצר שחיפשת חסר כרגע על המדף,`,
];

function buildAvailabilityAltText(isEnglish, subject, names, idx) {
  const list = names.join(" , ");
  const intros = isEnglish ? AVAIL_INTROS_EN : AVAIL_INTROS_HE;
  const intro = intros[idx % intros.length](subject);

  const suffix = isEnglish
    ? ` But we do have ${list}.`
    : ` אבל כן יש לנו ${list}.`;

  return intro + suffix;
}

const AVAIL_INTROS_EN = [
  (subject) =>
    subject
      ? `Unfortunately we don't have ${subject} in stock.`
      : `Unfortunately this product is not in stock.`,
  (subject) =>
    subject
      ? `${subject} is currently out of stock.`
      : `The product you're looking for is currently out of stock.`,
  (subject) =>
    subject
      ? `We couldn’t find ${subject} in stock.`
      : `We couldn’t find this product in stock.`,
  (subject) =>
    subject
      ? `${subject} isn’t available right now.`
      : `This product isn’t available right now.`,
];

const ALT_TEMPLATES_HE = [
  (req, names) =>
    `לצערנו אין לנו במלאי ${req}. האם יתאים לך ${names.join(" / ")}?`,
  (req, names) =>
    `המוצר ${req} חסר במלאי. ${names.map((n) => `${n}?`).join(" ")}`,
  (req, names) => `${req} לא זמין כרגע. נוכל להחליף ב-${names.join(" / ")}?`,
  (req, names) =>
    `לא מצאנו את ${req}. אולי ${names.map((n) => `${n}?`).join(" ")}`,
];
const ALT_TEMPLATES_EN = [
  (req, names) => `We’re out of ${req}. Would ${names.join(" / ")} work?`,
  (req, names) =>
    `${req} is unavailable. ${names.map((n) => `${n}?`).join(" ")}`,
  (req, names) =>
    `${req} isn’t in stock now. Can we replace it with ${names.join(" / ")}?`,
  (req, names) =>
    `Couldn’t find ${req}. Maybe ${names.map((n) => `${n}?`).join(" ")}`,
];

const pickAltTemplate = (isEnglish, idx) =>
  (isEnglish ? ALT_TEMPLATES_EN : ALT_TEMPLATES_HE)[idx % 4];

async function buildAlternativeQuestions(
  shop_id,
  notFound,
  foundIdsSet,
  isEnglish,
  context = "",
  opts = {},
) {
  const altQuestions = [];
  const alternativesMap = {};
  const usedIds = new Set(foundIdsSet);
  let t = 0;

  const threshold = Number.isFinite(Number(opts.threshold))
    ? Number(opts.threshold)
    : 3;
  const shortLimit = Number.isFinite(Number(opts.shortLimit))
    ? Number(opts.shortLimit)
    : 2;
  const longLimit = Number.isFinite(Number(opts.longLimit))
    ? Number(opts.longLimit)
    : 3;

  const baseQuestionsCount = Number.isFinite(Number(opts.baseQuestionsCount))
    ? Number(opts.baseQuestionsCount)
    : 0;

  const forceShort = opts.forceShort === true;

  const nextLimit = () => {
    if (forceShort) return shortLimit;
    const nextQNum = baseQuestionsCount + altQuestions.length + 1;
    return nextQNum > threshold ? shortLimit : longLimit;
  };

  for (const nf of notFound) {
    const cat = (nf.category || "").trim();
    const sub = (nf.sub_category || "").trim();
    if (!cat && !sub) continue;

    const exclude = Array.from(usedIds);
    const mainName = nf.requested_name || nf.requested_output_name || null;

    const excludeTokens =
      Array.isArray(nf.exclude_tokens) && nf.exclude_tokens.length
        ? nf.exclude_tokens
        : [];

    const alts = await fetchAlternatives(
      shop_id,
      cat,
      sub,
      exclude,
      nextLimit(),
      mainName,
      excludeTokens,
    );

    if (!alts || !alts.length) continue;

    alts.forEach((a) => usedIds.add(a.id));
    alternativesMap[nf.originalIndex] = alts.map((a) => ({
      id: a.id,
      name: a.name,
      display_name_en: a.display_name_en,
      price: Number(a.price),
      stock_amount: Number(a.stock_amount),
      is_default: Number(a.is_default || 0) === 1,
      category: a.category,
      sub_category: a.sub_category,
    }));

    const names = alts.map((a) =>
      isEnglish
        ? (a.display_name_en && a.display_name_en.trim()) || a.name
        : a.name,
    );

    const he = (nf.requested_name || "").trim();
    const en = (nf.requested_output_name || "").trim();
    const subject = (isEnglish ? en || he : he || en).trim();

    let questionText;

    if (context === "availability") {
      questionText = buildAvailabilityAltText(isEnglish, subject, names, t++);
    } else {
      questionText = pickAltTemplate(isEnglish, t++)(subject, names);
    }

    altQuestions.push({
      name: nf.requested_name || null,
      question: questionText,
      options: names,
    });
  }

  return { altQuestions, alternativesMap };
}

async function searchVariants(
  shop_id,
  {
    category = null,
    subCategory = null,
    searchTerm = null,
    limit = 50,
    excludeTokens = [],
  } = {},
) {
  await ensureProductDefaultSchema();

  const searchTermGroups = buildProductSearchTerms({
    original_user_text: searchTerm,
    name: searchTerm,
  });
  const tokens = searchTermGroups[0]?.tokens || [];

  let sql = `
    SELECT id, name, display_name_en, price, stock_amount, is_default, category, sub_category
    FROM product
    WHERE shop_id = ?
      AND (stock_amount IS NULL OR stock_amount > 0)
  `;
  const params = [shop_id];

  if (category) {
    sql += ` AND category = ?`;
    params.push(category);
  }

  if (subCategory) {
    const subs = await getSubCategoryCandidates(category, subCategory);
    if (subs.length) {
      sql += ` AND sub_category IN (${subs.map(() => "?").join(",")})`;
      params.push(...subs);
    }
  }

  if (tokens.length) {
    for (const t of tokens) {
      sql += `
        AND (
          name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
          OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
        )
      `;
      params.push(t, t);
    }
  }

  const normalizedExcludeTokens = Array.isArray(excludeTokens)
    ? excludeTokens.map((t) => String(t || "").trim()).filter(Boolean)
    : [];

  for (const t of normalizedExcludeTokens) {
    sql += `
      AND (
        name COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
        AND display_name_en COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
      )
    `;
    params.push(t, t);
  }

  sql += `
    ORDER BY is_default DESC, name ASC, id DESC
    LIMIT ?
  `;
  params.push(limit);

  const [rows] = await db.query(sql, params);
  return rows || [];
}

module.exports = {
  findBestProductForRequest,
  searchProducts,

  fetchAlternatives,
  buildAlternativeQuestions,

  pickAltTemplate,

  searchVariants,

  buildProductSearchTerms,
  fetchCustomerDefaultProductIds,
};

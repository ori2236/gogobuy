const db = require("../config/db");
const {
  tokenImportance,
  tokenizeForMatching,
  tokenSimilarity,
  isNumericToken,
  isUnitToken,
  isNegationToken,
  extractPolarityTargets,
  candidateHasTargetWithPolarity,
  getExcludeTokensFromReq,
  filterRowsByExcludeTokens,
} = require("../utilities/tokens");
const { getSubCategoryCandidates } = require("../repositories/categories");
const { ensureProductDefaultSchema } = require("../utilities/productDefaultSchema");

const MATCH_DEBUG = true;
const DEFAULT_PRODUCT_SCORE_BONUS = Number.isFinite(Number(process.env.DEFAULT_PRODUCT_SCORE_BONUS))
  ? Number(process.env.DEFAULT_PRODUCT_SCORE_BONUS)
  : 2.5;
const CUSTOMER_PRODUCT_SCORE_BONUS = Number.isFinite(Number(process.env.CUSTOMER_PRODUCT_SCORE_BONUS))
  ? Number(process.env.CUSTOMER_PRODUCT_SCORE_BONUS)
  : 5;
const PROMOTION_PRODUCT_SCORE_BONUS = Number.isFinite(Number(process.env.PROMOTION_PRODUCT_SCORE_BONUS))
  ? Number(process.env.PROMOTION_PRODUCT_SCORE_BONUS)
  : 1;
const CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT = Number.isFinite(Number(process.env.CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT))
  ? Math.max(1, Math.trunc(Number(process.env.CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT)))
  : 10;

const WHOLE_SHOP_RECOVERY_GENERIC_SINGLE_TOKENS = new Set(
  [
    "מוצר",
    "מוצרים",
    "משהו",
    "סוג",
    "מותג",
    "טעם",
    "רגיל",
    "רגילה",
    "גדול",
    "גדולה",
    "קטן",
    "קטנה",
    "אחד",
    "אחת",
    "product",
    "products",
    "item",
    "items",
    "something",
    "type",
    "brand",
    "flavor",
    "flavour",
    "regular",
    "large",
    "small",
  ].flatMap((value) => tokenizeForMatching(value)),
);

function boundedEnvNumber(name, fallback, min, max) {
  const value = Number(process.env[name]);
  if (!Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, value));
}

const TOKEN_FUZZY_MATCH_THRESHOLD = boundedEnvNumber(
  "TOKEN_FUZZY_MATCH_THRESHOLD",
  0.86,
  0.82,
  0.95,
);
const MIN_SEARCH_TERM_SPECIFICITY_RATIO = boundedEnvNumber(
  "MIN_SEARCH_TERM_SPECIFICITY_RATIO",
  0.48,
  0.35,
  0.8,
);

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
  return rows.map((row) => ({
    id: Number(row.id),
    name: row.name,
    display_name_en: row.display_name_en,
    category: row.category,
    sub_category: row.sub_category,
    price: Number(row.price),
    stock_amount:
      row.stock_amount === null || row.stock_amount === undefined
        ? null
        : Number(row.stock_amount),
    is_default: Number(row.is_default || 0) === 1,
    is_consignment: Number(row.is_consignment || 0) === 1,
    customer_default: Number(row.customer_default || 0) === 1,
    has_active_promotion: Number(row.has_active_promotion || 0) === 1,
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

function normalizeRequestTokens(tokens = []) {
  return Array.from(new Set((tokens || []).filter(Boolean)));
}

function termKey(value) {
  const normalized = normalizeSearchPhrase(value).toLowerCase();
  const tokens = normalizeRequestTokens(tokenizeForMatching(normalized));
  return tokens.length ? tokens.join(" ") : normalized;
}

function pushSearchTerm(out, seen, value, source) {
  const term = normalizeSearchPhrase(value);
  if (!term) return;

  const key = termKey(term);
  if (!key || seen.has(key)) return;

  const tokens = normalizeRequestTokens(tokenizeForMatching(term));
  if (!tokens.length) return;

  seen.add(key);
  out.push({ term, source, tokens });
}

function buildProductSearchTerms(req = {}) {
  const out = [];
  const seen = new Set();

  pushSearchTerm(
    out,
    seen,
    req?.original_user_text,
    "original_user_text",
  );

  if (Array.isArray(req?.search_terms)) {
    for (const term of req.search_terms) {
      pushSearchTerm(out, seen, term, "search_terms");
    }
  }

  const nameText = normalizeSearchPhrase(req?.name);
  const searchText = normalizeSearchPhrase(req?.searchTerm);
  if (nameText && searchText && termKey(nameText) !== termKey(searchText)) {
    pushSearchTerm(out, seen, `${nameText} ${searchText}`, "name+searchTerm");
  }

  pushSearchTerm(out, seen, req?.name, "name");
  pushSearchTerm(out, seen, req?.searchTerm, "searchTerm");
  pushSearchTerm(out, seen, req?.outputName, "outputName");
  pushSearchTerm(out, seen, req?.outputSearchTerm, "outputSearchTerm");

  return out;
}

function compactSearchTerms(terms = []) {
  return terms.map((term) => ({
    source: term.source,
    term: term.term,
    tokens: term.tokens,
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
  return rows.map((row) => ({
    ...row,
    customer_default: customerDefaultProductIds.has(Number(row.id)) ? 1 : 0,
  }));
}

async function fetchCustomerDefaultProductIds({ shop_id, customer_id }) {
  const customerId = Number(customer_id);
  const shopId = Number(shop_id);

  if (!Number.isFinite(customerId) || customerId <= 0) return new Set();
  if (!Number.isFinite(shopId) || shopId <= 0) return new Set();

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

  const productIds = normalizeCustomerDefaultProductIds(
    (rows || []).map((row) => row.product_id),
  );

  matchLog("fetchCustomerDefaultProductIds.result", {
    shop_id: shopId,
    customer_id: customerId,
    recentOrdersLimit: CUSTOMER_DEFAULT_RECENT_ORDERS_LIMIT,
    productIds: Array.from(productIds),
  });

  return productIds;
}

function minimumMatchedTokenCount(reqTokens = []) {
  const count = normalizeRequestTokens(reqTokens).length;
  if (count <= 0) return 0;
  return Math.ceil(count * 0.75);
}

function tokenCoverageThreshold(reqTokens = []) {
  const count = normalizeRequestTokens(reqTokens).length;
  if (count <= 0) return 0;
  if (count <= 3) return 1;
  return 0.74;
}

function candidatePenaltyTokens(name) {
  const full = normalizeSearchPhrase(name || "");
  const beforeParentheses = full.split(/[\(\[]/, 1)[0].trim();
  return tokenizeForMatching(beforeParentheses || full);
}

function detectSearchLanguage(term = "") {
  const text = String(term || "");
  const hebrewCount = (text.match(/[\u0590-\u05FF]/g) || []).length;
  const latinCount = (text.match(/[A-Za-z]/g) || []).length;
  if (hebrewCount > 0 && latinCount > 0) return "mixed";
  if (hebrewCount > 0) return "he";
  if (latinCount > 0) return "en";
  return "mixed";
}

function candidateTokensForTerm(row, termGroup) {
  const language = detectSearchLanguage(termGroup?.term);
  const hebrewTokens = tokenizeForMatching(row?.name || "");
  const englishTokens = tokenizeForMatching(row?.display_name_en || "");

  if (language === "en") {
    const combinedTokens = normalizeRequestTokens([...englishTokens, ...hebrewTokens]);
    return {
      candidateTokens: combinedTokens,
      penaltyTokens: englishTokens.length
        ? candidatePenaltyTokens(row?.display_name_en || "")
        : candidatePenaltyTokens(row?.name || ""),
      candidateValue: `${row?.display_name_en || ""} ${row?.name || ""}`.trim(),
      matchedField: englishTokens.length ? "display_name_en+name" : "name",
    };
  }

  if (language === "mixed") {
    const combinedValue = `${row?.name || ""} ${row?.display_name_en || ""}`.trim();
    return {
      candidateTokens: normalizeRequestTokens([...hebrewTokens, ...englishTokens]),
      penaltyTokens: candidatePenaltyTokens(combinedValue),
      candidateValue: combinedValue,
      matchedField: "mixed",
    };
  }

  return {
    candidateTokens: hebrewTokens.length ? hebrewTokens : englishTokens,
    penaltyTokens: candidatePenaltyTokens(row?.name || row?.display_name_en || ""),
    candidateValue: hebrewTokens.length ? row?.name || "" : row?.display_name_en || "",
    matchedField: hebrewTokens.length ? "name" : "display_name_en",
  };
}

function isJoinableWordToken(token) {
  const value = String(token || "");
  return (
    value.length >= 2 &&
    !isNumericToken(value) &&
    !isUnitToken(value) &&
    !isNegationToken(value) &&
    /^[a-z\u0590-\u05FF]+$/i.test(value)
  );
}

function findJoinedCandidateTokensMatch(
  requestedToken,
  candidateTokens,
  usedCandidate = new Set(),
) {
  if (!isJoinableWordToken(requestedToken) || String(requestedToken).length < 6) return null;

  let best = null;
  for (let start = 0; start < candidateTokens.length; start += 1) {
    for (let length = 2; length <= 3 && start + length <= candidateTokens.length; length += 1) {
      const indices = Array.from({ length }, (_, offset) => start + offset);
      if (indices.some((index) => usedCandidate.has(index))) continue;

      const parts = indices.map((index) => candidateTokens[index]);
      if (!parts.every(isJoinableWordToken)) continue;

      const joined = parts.join("");
      const similarity = tokenSimilarity(requestedToken, joined);
      if (similarity < TOKEN_FUZZY_MATCH_THRESHOLD) continue;

      if (!best || similarity > best.similarity || (similarity === best.similarity && length < best.indices.length)) {
        best = { indices, joined, similarity };
      }
    }
  }

  return best;
}

function findJoinedRequestedTokensMatch(
  candidateToken,
  requestedTokens,
  usedRequested = new Set(),
) {
  if (!isJoinableWordToken(candidateToken) || String(candidateToken).length < 6) return null;

  let best = null;
  for (let start = 0; start < requestedTokens.length; start += 1) {
    for (let length = 2; length <= 3 && start + length <= requestedTokens.length; length += 1) {
      const indices = Array.from({ length }, (_, offset) => start + offset);
      if (indices.some((index) => usedRequested.has(index))) continue;

      const parts = indices.map((index) => requestedTokens[index]);
      if (!parts.every(isJoinableWordToken)) continue;

      const joined = parts.join("");
      const similarity = tokenSimilarity(joined, candidateToken);
      if (similarity < TOKEN_FUZZY_MATCH_THRESHOLD) continue;

      if (!best || similarity > best.similarity || (similarity === best.similarity && length < best.indices.length)) {
        best = { indices, joined, similarity };
      }
    }
  }

  return best;
}

function isPenaltyTokenCoveredByRequest(penaltyTokens, penaltyIndex, requestedTokens) {
  const candidateToken = penaltyTokens[penaltyIndex];
  if (
    requestedTokens.some(
      (requestedToken) =>
        tokenSimilarity(requestedToken, candidateToken) >= TOKEN_FUZZY_MATCH_THRESHOLD,
    )
  ) {
    return true;
  }

  for (let start = Math.max(0, penaltyIndex - 2); start <= penaltyIndex; start += 1) {
    for (let length = 2; length <= 3 && start + length <= penaltyTokens.length; length += 1) {
      if (penaltyIndex < start || penaltyIndex >= start + length) continue;
      const parts = penaltyTokens.slice(start, start + length);
      if (!parts.every(isJoinableWordToken)) continue;
      const joined = parts.join("");
      if (
        requestedTokens.some(
          (requestedToken) =>
            tokenSimilarity(requestedToken, joined) >= TOKEN_FUZZY_MATCH_THRESHOLD,
        )
      ) {
        return true;
      }
    }
  }

  const joinedRequestedMatch = findJoinedRequestedTokensMatch(
    candidateToken,
    requestedTokens,
  );
  return Boolean(joinedRequestedMatch);
}

function buildTokenMatchMeta(
  reqTokens = [],
  candTokens = [],
  penaltyTokens = null,
  options = {},
) {
  const requested = normalizeRequestTokens(reqTokens);
  const candidate = normalizeRequestTokens(candTokens);
  const pairs = [];

  for (let reqIndex = 0; reqIndex < requested.length; reqIndex += 1) {
    for (let candIndex = 0; candIndex < candidate.length; candIndex += 1) {
      const similarity = tokenSimilarity(requested[reqIndex], candidate[candIndex]);
      if (similarity < TOKEN_FUZZY_MATCH_THRESHOLD) continue;
      pairs.push({
        reqIndex,
        candIndex,
        similarity,
        importance: tokenImportance(requested[reqIndex]),
      });
    }
  }

  pairs.sort(
    (left, right) =>
      right.importance * right.similarity - left.importance * left.similarity ||
      right.similarity - left.similarity,
  );

  const usedRequested = new Set();
  const usedCandidate = new Set();
  const chosenPairs = [];
  const compoundMatches = new Map();

  for (const pair of pairs) {
    if (usedRequested.has(pair.reqIndex) || usedCandidate.has(pair.candIndex)) continue;
    usedRequested.add(pair.reqIndex);
    usedCandidate.add(pair.candIndex);
    chosenPairs.push(pair);
  }

  for (let reqIndex = 0; reqIndex < requested.length; reqIndex += 1) {
    if (usedRequested.has(reqIndex)) continue;
    const compound = findJoinedCandidateTokensMatch(
      requested[reqIndex],
      candidate,
      usedCandidate,
    );
    if (!compound) continue;

    usedRequested.add(reqIndex);
    compound.indices.forEach((index) => usedCandidate.add(index));
    compoundMatches.set(reqIndex, {
      candidate: compound.indices.map((index) => candidate[index]).join(" "),
      similarity: compound.similarity,
    });
  }

  for (let candIndex = 0; candIndex < candidate.length; candIndex += 1) {
    if (usedCandidate.has(candIndex)) continue;
    const compound = findJoinedRequestedTokensMatch(
      candidate[candIndex],
      requested,
      usedRequested,
    );
    if (!compound) continue;

    usedCandidate.add(candIndex);
    compound.indices.forEach((reqIndex) => {
      usedRequested.add(reqIndex);
      compoundMatches.set(reqIndex, {
        candidate: candidate[candIndex],
        similarity: compound.similarity,
      });
    });
  }

  const matches = requested.map((requestedToken, reqIndex) => {
    const pair = chosenPairs.find((item) => item.reqIndex === reqIndex);
    const compound = compoundMatches.get(reqIndex);
    return {
      requested: requestedToken,
      candidate: pair ? candidate[pair.candIndex] : compound?.candidate || null,
      similarity: pair ? pair.similarity : compound?.similarity || 0,
      matched: Boolean(pair || compound),
      importance: tokenImportance(requestedToken),
    };
  });

  const totalWeight = matches.reduce((sum, match) => sum + match.importance, 0);
  const matchedWeight = matches
    .filter((match) => match.matched)
    .reduce((sum, match) => sum + match.importance, 0);
  const similarityWeight = matches
    .filter((match) => match.matched)
    .reduce((sum, match) => sum + match.importance * match.similarity, 0);

  const coverage = totalWeight > 0 ? matchedWeight / totalWeight : 0;
  const averageSimilarity = matchedWeight > 0 ? similarityWeight / matchedWeight : 0;
  const missingTokens = matches.filter((match) => !match.matched).map((match) => match.requested);
  const matchedCount = matches.filter((match) => match.matched).length;
  const missingWeight = totalWeight - matchedWeight;

  const penaltySource = normalizeRequestTokens(
    Array.isArray(penaltyTokens) ? penaltyTokens : candidate,
  );
  const extraTokens = penaltySource.filter(
    (_candidateToken, penaltyIndex) =>
      !isPenaltyTokenCoveredByRequest(penaltySource, penaltyIndex, requested),
  );

  const criticalMissingTokens = [];
  const hardConstraintTokens = normalizeRequestTokens(options.hardConstraintTokens || []);
  for (const constraint of hardConstraintTokens) {
    if (!candidate.includes(constraint)) criticalMissingTokens.push(constraint);
  }

  const candidateValue = options.candidateValue || "";
  for (const target of normalizeRequestTokens(options.requiredNegatedTargets || [])) {
    if (!candidateHasTargetWithPolarity(candidateValue, target, true)) {
      criticalMissingTokens.push(`negated:${target}`);
    }
  }
  for (const target of normalizeRequestTokens(options.requiredPositiveTargets || [])) {
    if (!candidateHasTargetWithPolarity(candidateValue, target, false)) {
      criticalMissingTokens.push(`positive:${target}`);
    }
  }

  return {
    coverage,
    averageSimilarity,
    matchedWeight,
    totalWeight,
    missingWeight,
    matchedCount,
    matches,
    missingTokens,
    extraTokens,
    criticalMissingTokens: normalizeRequestTokens(criticalMissingTokens),
  };
}

function termSpecificity(termGroup) {
  const tokens = normalizeRequestTokens(termGroup?.tokens || []);
  return tokens.reduce((sum, token) => sum + tokenImportance(token), 0);
}

function orderedTermGroups(termGroups = []) {
  const sourcePriority = new Map([
    ["original_user_text", 0],
    ["search_terms", 1],
    ["name+searchTerm", 2],
    ["name", 3],
    ["searchTerm", 4],
    ["outputName", 5],
    ["outputSearchTerm", 6],
  ]);

  return (termGroups || [])
    .slice()
    .sort(
      (left, right) =>
        (sourcePriority.get(left.source) ?? 99) - (sourcePriority.get(right.source) ?? 99) ||
        termSpecificity(right) - termSpecificity(left),
    );
}

function sourcePriority(source) {
  return {
    original_user_text: 0,
    search_terms: 1,
    "name+searchTerm": 2,
    name: 3,
    searchTerm: 4,
    outputName: 5,
    outputSearchTerm: 6,
  }[source] ?? 99;
}

function scopeRankForRow(row, primarySub, relatedSubs = []) {
  const subCategory = String(row?.sub_category || "");
  if (primarySub && subCategory === primarySub) return 0;
  if (relatedSubs.includes(subCategory)) return 1;
  return 2;
}

function preferenceAdjustedScore(row) {
  const customerBonus = Number(row.customer_default || 0) === 1
    ? CUSTOMER_PRODUCT_SCORE_BONUS
    : 0;
  const defaultBonus = Number(row.is_default || 0) === 1
    ? DEFAULT_PRODUCT_SCORE_BONUS
    : 0;
  const promotionBonus = Number(row.has_active_promotion || 0) === 1
    ? PROMOTION_PRODUCT_SCORE_BONUS
    : 0;

  return {
    customerBonus,
    defaultBonus,
    promotionBonus,
    defaultScore: -(customerBonus + defaultBonus),
    promotionScore: -promotionBonus,
    score: -(customerBonus + defaultBonus + promotionBonus),
  };
}

function compareEvaluations(left, right) {
  const semanticDifference =
    right.tokenMatch.matchedWeight - left.tokenMatch.matchedWeight ||
    right.tokenMatch.coverage - left.tokenMatch.coverage ||
    right.tokenMatch.averageSimilarity - left.tokenMatch.averageSimilarity ||
    left.tokenMatch.missingWeight - right.tokenMatch.missingWeight;
  if (semanticDifference) return semanticDifference;

  const leftTokenCount = normalizeRequestTokens(left.termGroup?.tokens || []).length;
  const rightTokenCount = normalizeRequestTokens(right.termGroup?.tokens || []).length;
  const bothGeneric = leftTokenCount === 1 && rightTokenCount === 1;

  if (bothGeneric) {
    return (
      left.scopeRank - right.scopeRank ||
      left.sourcePriority - right.sourcePriority ||
      left.preference.defaultScore - right.preference.defaultScore ||
      left.extraScore - right.extraScore ||
      left.wordCount - right.wordCount ||
      left.preference.promotionScore - right.preference.promotionScore ||
      left.priceScore - right.priceScore ||
      Number(left.row.id || 0) - Number(right.row.id || 0)
    );
  }

  return (
    left.extraScore - right.extraScore ||
    left.scopeRank - right.scopeRank ||
    left.sourcePriority - right.sourcePriority ||
    left.preference.score - right.preference.score ||
    left.priceScore - right.priceScore ||
    left.wordCount - right.wordCount ||
    Number(left.row.id || 0) - Number(right.row.id || 0)
  );
}

async function pickBestWeighted({
  rows,
  reqTokens,
  excludeTokens,
  minCoverage = tokenCoverageThreshold(reqTokens),
  minMatchedCount = minimumMatchedTokenCount(reqTokens),
  debugLabel = "",
  customerDefaultProductIds = new Set(),
  termGroup = null,
  hardConstraintTokens = [],
  requiredNegatedTargets = [],
  requiredPositiveTargets = [],
  primarySub = null,
  relatedSubs = [],
}) {
  let candidates = annotateCustomerDefaults(rows || [], customerDefaultProductIds);
  candidates = filterRowsByExcludeTokens(candidates, excludeTokens || []);

  const scored = [];
  for (const row of candidates) {
    const tokenFields = candidateTokensForTerm(row, termGroup || { term: "" });
    const tokenMatch = buildTokenMatchMeta(
      reqTokens,
      tokenFields.candidateTokens,
      tokenFields.penaltyTokens,
      {
        hardConstraintTokens,
        requiredNegatedTargets,
        requiredPositiveTargets,
        candidateValue: tokenFields.candidateValue,
      },
    );

    const minimumAverageSimilarity = normalizeRequestTokens(reqTokens).length === 1
      ? 0.9
      : TOKEN_FUZZY_MATCH_THRESHOLD;

    if (tokenMatch.criticalMissingTokens.length) continue;
    if (tokenMatch.matchedCount < minMatchedCount) continue;
    if (tokenMatch.coverage + 1e-9 < minCoverage) continue;
    if (tokenMatch.averageSimilarity + 1e-9 < minimumAverageSimilarity) continue;

    const extraScore = tokenMatch.extraTokens.reduce(
      (sum, token) => sum + tokenImportance(token),
      0,
    );
    const preference = preferenceAdjustedScore(row);
    const price = Number(row.price);

    scored.push({
      row,
      termGroup,
      tokenMatch,
      matchedField: tokenFields.matchedField,
      extraScore,
      preference,
      scopeRank: scopeRankForRow(row, primarySub, relatedSubs),
      sourcePriority: sourcePriority(termGroup?.source),
      priceScore: Number.isFinite(price) ? price : Number.MAX_SAFE_INTEGER,
      wordCount: tokenFields.candidateTokens.length || Number.MAX_SAFE_INTEGER,
    });
  }

  scored.sort(compareEvaluations);

  matchLog("pickBestWeighted.scored", {
    debugLabel,
    scored: scored.slice(0, 20).map((entry) => ({
      id: Number(entry.row.id),
      name: entry.row.name,
      term: entry.termGroup?.term || null,
      source: entry.termGroup?.source || null,
      matchedField: entry.matchedField,
      scopeRank: entry.scopeRank,
      coverage: entry.tokenMatch.coverage,
      matchedWeight: entry.tokenMatch.matchedWeight,
      matchedCount: entry.tokenMatch.matchedCount,
      averageSimilarity: entry.tokenMatch.averageSimilarity,
      missingTokens: entry.tokenMatch.missingTokens,
      criticalMissingTokens: entry.tokenMatch.criticalMissingTokens,
      extraTokens: entry.tokenMatch.extraTokens,
      preferenceScore: entry.preference.score,
      customerBonus: entry.preference.customerBonus,
      defaultBonus: entry.preference.defaultBonus,
      promotionBonus: entry.preference.promotionBonus,
    })),
  });

  return scored[0]?.row || null;
}

async function loadCategoryProducts({ shop_id, category, categoryRowsCache = null }) {
  const cacheKey = `${Number(shop_id)}|${String(category || "").trim().toLowerCase()}`;
  if (categoryRowsCache instanceof Map && categoryRowsCache.has(cacheKey)) {
    return categoryRowsCache.get(cacheKey);
  }

  const [rows] = await db.query(
    `
      SELECT
        p.id,
        p.name,
        p.display_name_en,
        p.price,
        p.stock_amount,
        p.is_default,
        p.is_consignment,
        p.category,
        p.sub_category,
        p.updated_at,
        CASE
          WHEN EXISTS (
            SELECT 1
            FROM promotion pr
            WHERE pr.shop_id = p.shop_id
              AND pr.product_id = p.id
              AND (pr.start_at IS NULL OR pr.start_at <= NOW())
              AND (pr.end_at IS NULL OR pr.end_at >= NOW())
          )
          OR EXISTS (
            SELECT 1
            FROM product_group_promotion_item gpi
            JOIN product_group_promotion pgp
              ON pgp.id = gpi.group_promotion_id
             AND pgp.shop_id = gpi.shop_id
            WHERE gpi.shop_id = p.shop_id
              AND gpi.product_id = p.id
              AND pgp.is_active = 1
              AND (pgp.start_at IS NULL OR pgp.start_at <= NOW())
              AND (pgp.end_at IS NULL OR pgp.end_at >= NOW())
          )
          THEN 1 ELSE 0
        END AS has_active_promotion
      FROM product p
      WHERE p.shop_id = ?
        AND p.category = ?
        AND (COALESCE(p.is_consignment,0) = 1 OR p.stock_amount IS NULL OR p.stock_amount > 0)
      ORDER BY p.sub_category ASC, p.is_default DESC, p.updated_at DESC, p.id DESC
    `,
    [shop_id, category],
  );

  const result = rows || [];
  if (categoryRowsCache instanceof Map) categoryRowsCache.set(cacheKey, result);
  return result;
}


async function loadShopRecoveryProducts({ shop_id, shopRowsCache = null }) {
  const cacheKey = Number(shop_id);
  if (shopRowsCache instanceof Map && shopRowsCache.has(cacheKey)) {
    return shopRowsCache.get(cacheKey);
  }

  const [rows] = await db.query(
    `
      SELECT
        p.id,
        p.name,
        p.display_name_en,
        p.price,
        p.stock_amount,
        0 AS is_default,
        p.is_consignment,
        p.category,
        p.sub_category,
        0 AS customer_default,
        0 AS has_active_promotion
      FROM product p
      WHERE p.shop_id = ?
        AND p.category IS NOT NULL
        AND TRIM(p.category) <> ''
        AND (COALESCE(p.is_consignment,0) = 1 OR p.stock_amount IS NULL OR p.stock_amount > 0)
      ORDER BY p.category ASC, p.sub_category ASC, p.id ASC
    `,
    [shop_id],
  );

  const result = rows || [];
  if (shopRowsCache instanceof Map) shopRowsCache.set(cacheKey, result);
  return result;
}

function isUsableSingleTokenForWholeShopRecovery(token) {
  const normalized = String(token || "").trim().toLowerCase();
  if (!normalized || normalized.length < 3) return false;
  if (isNumericToken(normalized) || isUnitToken(normalized) || isNegationToken(normalized)) {
    return false;
  }
  return !WHOLE_SHOP_RECOVERY_GENERIC_SINGLE_TOKENS.has(normalized);
}

function compactWholeShopRecoveryMatch(match) {
  return {
    category: match.category,
    product_id: Number(match.meta?.row?.id || 0) || null,
    product_name: match.meta?.row?.name || null,
    sub_category: match.meta?.row?.sub_category || null,
    selected_source: match.meta?.selectedEvaluation?.termGroup?.source || null,
    selected_term: match.meta?.selectedEvaluation?.termGroup?.term || null,
    selected_tokens: match.meta?.selectedEvaluation?.termGroup?.tokens || [],
    specificity: match.specificity,
    coverage: match.meta?.selectedEvaluation?.tokenMatch?.coverage || 0,
    average_similarity: match.meta?.selectedEvaluation?.tokenMatch?.averageSimilarity || 0,
  };
}

async function findWholeShopRecoveryDecision({
  rows,
  termGroups,
  excludeTokens = [],
  requestedCategory = null,
}) {
  const grouped = new Map();
  for (const row of rows || []) {
    const category = normalizeSearchPhrase(row?.category);
    if (!category) continue;
    if (!grouped.has(category)) grouped.set(category, []);
    grouped.get(category).push(row);
  }

  const matches = [];
  for (const [category, categoryRows] of grouped.entries()) {
    if (requestedCategory && category === requestedCategory) continue;

    const meta = await findBestByTermGroups({
      rows: categoryRows,
      termGroups,
      excludeTokens,
      debugLabel: `wholeShopRecovery:${category}`,
      primarySub: null,
      relatedSubs: [],
      customerDefaultProductIds: new Set(),
      recoveryMode: true,
      returnMeta: true,
      emitLog: false,
    });

    if (!meta?.row || !meta?.selectedEvaluation) continue;

    const selectedSource = meta.selectedEvaluation.termGroup?.source;
    if (!["original_user_text", "search_terms", "searchTerm", "outputSearchTerm"].includes(selectedSource)) {
      continue;
    }

    matches.push({
      category,
      meta,
      specificity: termSpecificity(meta.selectedEvaluation.termGroup),
    });
  }

  if (!matches.length) {
    return {
      accepted: false,
      reason: "no_strong_whole_shop_match",
      matches: [],
    };
  }

  const originalGroundedMatches = matches.filter(
    (match) => match.meta.selectedEvaluation.termGroup?.source === "original_user_text",
  );

  let strongestMatches;
  if (originalGroundedMatches.length) {
    strongestMatches = originalGroundedMatches;
  } else {
    const maximumSpecificity = matches.reduce(
      (maximum, match) => Math.max(maximum, match.specificity),
      0,
    );
    strongestMatches = matches.filter(
      (match) => Math.abs(match.specificity - maximumSpecificity) < 1e-9,
    );
  }

  const categories = Array.from(new Set(strongestMatches.map((match) => match.category)));
  if (categories.length !== 1) {
    return {
      accepted: false,
      reason: "ambiguous_categories",
      matches: strongestMatches.map(compactWholeShopRecoveryMatch),
    };
  }

  const selectedMatch = strongestMatches.find((match) => match.category === categories[0]);
  const selectedEvaluation = selectedMatch.meta.selectedEvaluation;
  const selectedTokens = normalizeRequestTokens(selectedEvaluation.termGroup?.tokens || []);

  if (selectedTokens.length === 1) {
    if (!isUsableSingleTokenForWholeShopRecovery(selectedTokens[0])) {
      return {
        accepted: false,
        reason: "unsafe_generic_single_token",
        matches: strongestMatches.map(compactWholeShopRecoveryMatch),
      };
    }

    const matchingSubCategories = Array.from(
      new Set(
        selectedMatch.meta.evaluations
          .filter((entry) => entry.termGroup === selectedEvaluation.termGroup)
          .map((entry) => normalizeSearchPhrase(entry.row?.sub_category))
          .filter(Boolean),
      ),
    );

    if (matchingSubCategories.length !== 1) {
      return {
        accepted: false,
        reason: "ambiguous_single_token_subcategories",
        matches: strongestMatches.map(compactWholeShopRecoveryMatch),
        sub_categories: matchingSubCategories,
      };
    }
  }

  return {
    accepted: true,
    reason: "unique_strong_category",
    category: selectedMatch.category,
    sub_category: normalizeSearchPhrase(selectedMatch.meta.row?.sub_category) || null,
    selected_term: selectedEvaluation.termGroup?.term || null,
    selected_source: selectedEvaluation.termGroup?.source || null,
    selected_tokens: selectedTokens,
    product_id: Number(selectedMatch.meta.row?.id || 0) || null,
    product_name: selectedMatch.meta.row?.name || null,
    matches: strongestMatches.map(compactWholeShopRecoveryMatch),
  };
}

function extractHardConstraintTokens(tokens = []) {
  const normalized = normalizeRequestTokens(tokens);
  const hard = [];

  for (let index = 0; index < normalized.length; index += 1) {
    const token = normalized[index];
    const previous = normalized[index - 1];
    const next = normalized[index + 1];

    if (isNumericToken(token) && (isUnitToken(previous) || isUnitToken(next))) {
      hard.push(token);
      if (isUnitToken(previous)) hard.push(previous);
      if (isUnitToken(next)) hard.push(next);
      continue;
    }

    if (isUnitToken(token) && (isNumericToken(previous) || isNumericToken(next))) {
      hard.push(token);
    }
  }

  return normalizeRequestTokens(hard);
}

function collectRequestConstraints(termGroups, rows) {
  const original = termGroups.find((term) => term.source === "original_user_text");
  const constraintTerms = original ? [original] : termGroups;
  const hardConstraintTokens = extractHardConstraintTokens(original?.tokens || []);

  const negatedTargets = [];
  const strongNegatedTargets = [];
  const positiveTargets = [];
  for (const termGroup of constraintTerms) {
    const polarity = extractPolarityTargets(termGroup.term);
    negatedTargets.push(...polarity.negated);
    strongNegatedTargets.push(...(polarity.strongNegated || []));
    positiveTargets.push(...polarity.positive);
  }

  const candidateValues = rows.map((row) => `${row?.name || ""} ${row?.display_name_en || ""}`);
  const explicitNegatedTargets = normalizeRequestTokens(negatedTargets).filter((target) =>
    candidateValues.some((value) => candidateHasTargetWithPolarity(value, target, true)),
  );
  const requiredNegatedTargets = normalizeRequestTokens([
    ...strongNegatedTargets,
    ...explicitNegatedTargets,
  ]);
  const requiredPositiveTargets = normalizeRequestTokens(positiveTargets).filter((target) =>
    candidateValues.some((value) => candidateHasTargetWithPolarity(value, target, false)),
  );

  return {
    hardConstraintTokens,
    requiredNegatedTargets,
    requiredPositiveTargets,
  };
}

async function findBestByTermGroups({
  rows,
  termGroups,
  excludeTokens,
  debugLabel = "",
  customerDefaultProductIds = new Set(),
  primarySub = null,
  relatedSubs = [],
  recoveryMode = false,
  returnMeta = false,
  emitLog = true,
}) {
  const ordered = orderedTermGroups(termGroups);
  const constraints = collectRequestConstraints(ordered, rows);
  const evaluations = [];
  // The AI already separates product intent from order quantity and conversational text.
  // Compare fallback terms only against other structured product terms, not against the
  // raw original_user_text, which may contain quantities or filler words such as
  // "4 קורנפלקס" or "חלב אחד של יוטבתה".
  const structuredTerms = ordered.filter(
    (termGroup) => termGroup.source !== "original_user_text",
  );
  const specificityReferenceTerms = structuredTerms.length ? structuredTerms : ordered;
  const maximumSpecificity = specificityReferenceTerms.reduce(
    (maximum, termGroup) => Math.max(maximum, termSpecificity(termGroup)),
    0,
  );

  for (const termGroup of ordered) {
    const specificity = termSpecificity(termGroup);
    if (
      termGroup.source !== "original_user_text" &&
      maximumSpecificity > 0 &&
      specificity / maximumSpecificity + 1e-9 < MIN_SEARCH_TERM_SPECIFICITY_RATIO
    ) {
      matchLog("findBestByTermGroups.skipOverlyBroadTerm", {
        debugLabel,
        termGroup: compactSearchTerms([termGroup])[0],
        specificity,
        maximumSpecificity,
        minimumRatio: MIN_SEARCH_TERM_SPECIFICITY_RATIO,
      });
      continue;
    }
    let candidates = annotateCustomerDefaults(rows || [], customerDefaultProductIds);
    candidates = filterRowsByExcludeTokens(candidates, excludeTokens || []);

    for (const row of candidates) {
      const tokenFields = candidateTokensForTerm(row, termGroup);
      const tokenMatch = buildTokenMatchMeta(
        termGroup.tokens,
        tokenFields.candidateTokens,
        tokenFields.penaltyTokens,
        {
          ...constraints,
          candidateValue: tokenFields.candidateValue,
        },
      );

      const minimumAverageSimilarity = termGroup.tokens.length === 1
        ? 0.9
        : recoveryMode
          ? Math.max(0.9, TOKEN_FUZZY_MATCH_THRESHOLD)
          : TOKEN_FUZZY_MATCH_THRESHOLD;
      const baseCoverage = tokenCoverageThreshold(termGroup.tokens);
      const minimumCoverage = recoveryMode
        ? termGroup.tokens.length <= 3
          ? 1
          : Math.max(0.8, baseCoverage)
        : baseCoverage;
      const minimumMatches = minimumMatchedTokenCount(termGroup.tokens);

      if (tokenMatch.criticalMissingTokens.length) continue;
      if (tokenMatch.matchedCount < minimumMatches) continue;
      if (tokenMatch.coverage + 1e-9 < minimumCoverage) continue;
      if (tokenMatch.averageSimilarity + 1e-9 < minimumAverageSimilarity) continue;

      const extraScore = tokenMatch.extraTokens.reduce(
        (sum, token) => sum + tokenImportance(token),
        0,
      );
      const preference = preferenceAdjustedScore(row);
      const price = Number(row.price);

      evaluations.push({
        row,
        termGroup,
        tokenMatch,
        matchedField: tokenFields.matchedField,
        extraScore,
        preference,
        scopeRank: scopeRankForRow(row, primarySub, relatedSubs),
        sourcePriority: sourcePriority(termGroup.source),
        priceScore: Number.isFinite(price) ? price : Number.MAX_SAFE_INTEGER,
        wordCount: tokenFields.candidateTokens.length || Number.MAX_SAFE_INTEGER,
      });
    }
  }

  evaluations.sort(compareEvaluations);

  const originalEvaluations = evaluations
    .filter((entry) => entry.termGroup?.source === "original_user_text")
    .sort(compareEvaluations);
  const selectedEvaluation = originalEvaluations[0] || evaluations[0] || null;

  if (emitLog) {
    matchLog("findBestByTermGroups.scored", {
      debugLabel,
      constraints,
      selectedSource: selectedEvaluation?.termGroup?.source || null,
      candidates: evaluations.slice(0, 25).map((entry) => ({
        id: Number(entry.row.id),
        name: entry.row.name,
        sub_category: entry.row.sub_category,
        source: entry.termGroup.source,
        term: entry.termGroup.term,
        scopeRank: entry.scopeRank,
        matchedWeight: entry.tokenMatch.matchedWeight,
        coverage: entry.tokenMatch.coverage,
        averageSimilarity: entry.tokenMatch.averageSimilarity,
        missingTokens: entry.tokenMatch.missingTokens,
        extraTokens: entry.tokenMatch.extraTokens,
        preferenceScore: entry.preference.score,
        customerBonus: entry.preference.customerBonus,
        defaultBonus: entry.preference.defaultBonus,
        promotionBonus: entry.preference.promotionBonus,
      })),
    });
  }

  if (returnMeta) {
    return {
      row: selectedEvaluation?.row || null,
      selectedEvaluation,
      evaluations,
      constraints,
    };
  }

  return selectedEvaluation?.row || null;
}

async function findBestProductForRequest(shop_id, req, opts = {}) {
  await ensureProductDefaultSchema();

  const category = normalizeSearchPhrase(req?.category);
  const primarySub = normalizeSearchPhrase(req?.["sub-category"] || req?.sub_category) || null;
  const searchTerms = buildProductSearchTerms(req);
  const excludeTokens = getExcludeTokensFromReq(req);

  if (!category || !searchTerms.length) {
    matchLog("findBestProductForRequest.reject.missingBoundaryOrTerms", {
      shop_id,
      category: category || null,
      searchTerms: compactSearchTerms(searchTerms),
      req,
    });
    return null;
  }

  let customerDefaultProductIds = normalizeCustomerDefaultProductIds(
    opts.customerDefaultProductIds || [],
  );
  if (!customerDefaultProductIds.size && opts.customer_id) {
    customerDefaultProductIds = await fetchCustomerDefaultProductIds({
      shop_id,
      customer_id: opts.customer_id,
    });
  }

  let relatedSubs = [];
  if (primarySub) {
    try {
      const resolved = await getSubCategoryCandidates(category, primarySub);
      relatedSubs = normalizeRequestTokens(resolved || []).filter((sub) => sub !== primarySub);
    } catch (error) {
      console.error("[MATCH] getSubCategoryCandidates failed", {
        category,
        primarySub,
        error: error?.message || String(error),
      });
    }
  }

  const categoryRows = annotateCustomerDefaults(
    await loadCategoryProducts({
      shop_id,
      category,
      categoryRowsCache: opts.categoryRowsCache,
    }),
    customerDefaultProductIds,
  );

  matchLog("findBestProductForRequest.start", {
    shop_id,
    category,
    primarySub,
    relatedSubs,
    searchTerms: compactSearchTerms(searchTerms),
    excludeTokens,
    categoryRowCount: categoryRows.length,
  });

  const best = await findBestByTermGroups({
    rows: categoryRows,
    termGroups: searchTerms,
    excludeTokens,
    debugLabel: "categoryBounded",
    customerDefaultProductIds,
    primarySub,
    relatedSubs,
  });

  if (best) {
    matchLog("findBestProductForRequest.return.best", {
      id: Number(best.id),
      name: best.name,
      category: best.category,
      sub_category: best.sub_category,
    });
    return best;
  }

  let recoveryDecision = {
    accepted: false,
    reason: "recovery_not_run",
    matches: [],
  };

  try {
    const shopRecoveryRows = await loadShopRecoveryProducts({
      shop_id,
      shopRowsCache: opts.shopRowsCache,
    });

    recoveryDecision = await findWholeShopRecoveryDecision({
      rows: shopRecoveryRows,
      termGroups: searchTerms,
      excludeTokens,
      requestedCategory: category,
    });

    matchLog("findBestProductForRequest.wholeShopRecoveryDecision", {
      requested_category: category,
      requested_sub_category: primarySub,
      ...recoveryDecision,
    });

    if (recoveryDecision.accepted && recoveryDecision.category) {
      const recoveredCategory = recoveryDecision.category;
      const recoveredPrimarySub = recoveryDecision.sub_category || null;
      let recoveredRelatedSubs = [];

      if (recoveredPrimarySub) {
        try {
          const resolved = await getSubCategoryCandidates(
            recoveredCategory,
            recoveredPrimarySub,
          );
          recoveredRelatedSubs = normalizeRequestTokens(resolved || []).filter(
            (sub) => sub !== recoveredPrimarySub,
          );
        } catch (error) {
          console.error("[MATCH] recovered getSubCategoryCandidates failed", {
            recoveredCategory,
            recoveredPrimarySub,
            error: error?.message || String(error),
          });
        }
      }

      const recoveredRows = annotateCustomerDefaults(
        await loadCategoryProducts({
          shop_id,
          category: recoveredCategory,
          categoryRowsCache: opts.categoryRowsCache,
        }),
        customerDefaultProductIds,
      );

      const recoveredBest = await findBestByTermGroups({
        rows: recoveredRows,
        termGroups: searchTerms,
        excludeTokens,
        debugLabel: "wholeShopRecoveredCategory",
        customerDefaultProductIds,
        primarySub: recoveredPrimarySub,
        relatedSubs: recoveredRelatedSubs,
      });

      if (recoveredBest) {
        const result = {
          ...recoveredBest,
          _category_recovery: {
            from_category: category,
            from_sub_category: primarySub,
            to_category: recoveredCategory,
            to_sub_category: recoveredPrimarySub,
            selected_term: recoveryDecision.selected_term,
            selected_source: recoveryDecision.selected_source,
            reason: recoveryDecision.reason,
          },
        };

        matchLog("findBestProductForRequest.return.wholeShopRecovered", {
          id: Number(result.id),
          name: result.name,
          category: result.category,
          sub_category: result.sub_category,
          recovery: result._category_recovery,
        });
        return result;
      }
    }

  } catch (error) {
    recoveryDecision = {
      accepted: false,
      reason: "recovery_error",
      matches: [],
      error: error?.message || String(error),
    };
    console.error("[MATCH] whole-shop recovery failed", {
      shop_id,
      category,
      primarySub,
      error: recoveryDecision.error,
    });
  }

  matchLog("findBestProductForRequest.return.null", {
    req,
    category,
    primarySub,
    searchTerms: compactSearchTerms(searchTerms),
    excludeTokens,
    recoveryDecision,
  });
  return null;
}

async function searchProducts(shop_id, products, opts = {}) {
  const customerDefaultProductIds = await fetchCustomerDefaultProductIds({
    shop_id,
    customer_id: opts.customer_id,
  });

  const found = [];
  const notFound = [];
  const categoryRowsCache = new Map();
  const shopRowsCache = new Map();

  for (let index = 0; index < products.length; index += 1) {
    const req = products[index];
    const row = await findBestProductForRequest(shop_id, req, {
      customerDefaultProductIds,
      categoryRowsCache,
      shopRowsCache,
    });

    if (row) {
      const amount = Number(req?.amount);
      const units = Number(req?.units);
      const soldByWeight = req?.sold_by_weight === true;

      found.push({
        originalIndex: index,
        product_id: row.id,
        matched_name: row.name,
        price: Number(row.price),
        stock_amount: Number(row.is_consignment || 0) === 1 ? null : Number(row.stock_amount),
        is_consignment: Number(row.is_consignment || 0) === 1,
        category: row.category,
        sub_category: row.sub_category,
        requested_name: req?.name || null,
        requested_original_user_text: req?.original_user_text || null,
        requested_search_terms: Array.isArray(req?.search_terms) ? req.search_terms : [],
        final_search_terms: compactSearchTerms(buildProductSearchTerms(req)),
        requested_amount: Number.isFinite(amount) ? amount : 1,
        requested_units: Number.isFinite(units) && units > 0 ? units : null,
        sold_by_weight: soldByWeight,
        matched_display_name_en: row.display_name_en,
        is_default: Number(row.is_default || 0) === 1,
        customer_default: Number(row.customer_default || 0) === 1,
        category_recovered: Boolean(row._category_recovery),
        category_recovery: row._category_recovery || null,
      });
    } else {
      const amount = Number(req?.amount);
      notFound.push({
        originalIndex: index,
        requested_name: req?.name || null,
        requested_output_name: req?.outputName || null,
        requested_original_user_text: req?.original_user_text || null,
        requested_search_terms: Array.isArray(req?.search_terms) ? req.search_terms : [],
        final_search_terms: compactSearchTerms(buildProductSearchTerms(req)),
        requested_amount: Number.isFinite(amount) ? amount : 1,
        category: req?.category || null,
        sub_category: req?.["sub-category"] || req?.sub_category || null,
        exclude_tokens: getExcludeTokensFromReq(req),
      });
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
  const normalizedCategory = normalizeSearchPhrase(category);
  if (!normalizedCategory) return [];

  await ensureProductDefaultSchema();

  const categoryRows = await loadCategoryProducts({
    shop_id,
    category: normalizedCategory,
  });
  const excludedIds = new Set(
    (Array.isArray(excludeIds) ? excludeIds : [])
      .map((id) => Number(id))
      .filter(Number.isFinite),
  );

  let rows = categoryRows.filter((row) => !excludedIds.has(Number(row.id)));
  rows = filterRowsByExcludeTokens(rows, excludeTokens || []);
  if (!rows.length) return [];

  const normalizedSub = normalizeSearchPhrase(subCategory) || null;
  let relatedSubs = normalizedSub ? [normalizedSub] : [];
  if (normalizedSub) {
    try {
      relatedSubs = normalizeRequestTokens([
        normalizedSub,
        ...((await getSubCategoryCandidates(normalizedCategory, normalizedSub)) || []),
      ]);
    } catch (error) {
      console.error("[MATCH] fetchAlternatives subcategory lookup failed", {
        category: normalizedCategory,
        subCategory: normalizedSub,
        error: error?.message || String(error),
      });
    }
  }

  if (relatedSubs.length) {
    const relatedSet = new Set(relatedSubs);
    const relatedRows = rows.filter((row) => relatedSet.has(String(row.sub_category || "")));
    if (relatedRows.length) rows = relatedRows;
  }

  const termGroups = typeof requestedName === "object"
    ? buildProductSearchTerms(requestedName)
    : buildProductSearchTerms({
        original_user_text: requestedName,
        name: requestedName,
      });

  if (!termGroups.length) {
    return rows
      .slice()
      .sort(
        (a, b) =>
          Number(b.is_default || 0) - Number(a.is_default || 0) ||
          String(a.name || "").localeCompare(String(b.name || ""), "he") ||
          Number(a.id || 0) - Number(b.id || 0),
      )
      .slice(0, Math.max(0, Number(limit) || 0));
  }

  const ranked = [];
  for (const row of rows) {
    let best = null;
    for (const termGroup of orderedTermGroups(termGroups)) {
      const tokenFields = candidateTokensForTerm(row, termGroup);
      const tokenMatch = buildTokenMatchMeta(
        termGroup.tokens,
        tokenFields.candidateTokens,
        tokenFields.penaltyTokens,
      );
      if (tokenMatch.criticalMissingTokens.length) continue;
      if (tokenMatch.matchedCount === 0) continue;

      const score = {
        termSpecificity: termSpecificity(termGroup),
        coverage: tokenMatch.coverage,
        matchedCount: tokenMatch.matchedCount,
        averageSimilarity: tokenMatch.averageSimilarity,
        extraScore: tokenMatch.extraTokens.reduce(
          (sum, token) => sum + tokenImportance(token),
          0,
        ),
      };

      if (
        !best ||
        score.termSpecificity > best.termSpecificity ||
        (score.termSpecificity === best.termSpecificity && score.coverage > best.coverage) ||
        (score.termSpecificity === best.termSpecificity &&
          score.coverage === best.coverage &&
          score.averageSimilarity > best.averageSimilarity)
      ) {
        best = score;
      }
    }

    if (!best) continue;
    ranked.push({
      row,
      ...best,
      isPrimary: normalizedSub && String(row.sub_category || "") === normalizedSub,
      isDefault: Number(row.is_default || 0) === 1,
      price: Number.isFinite(Number(row.price)) ? Number(row.price) : Number.MAX_SAFE_INTEGER,
    });
  }

  return ranked
    .sort(
      (a, b) =>
        Number(b.isPrimary) - Number(a.isPrimary) ||
        b.termSpecificity - a.termSpecificity ||
        b.coverage - a.coverage ||
        b.matchedCount - a.matchedCount ||
        b.averageSimilarity - a.averageSimilarity ||
        Number(b.isDefault) - Number(a.isDefault) ||
        a.extraScore - b.extraScore ||
        a.price - b.price ||
        Number(a.row.id || 0) - Number(b.row.id || 0),
    )
    .slice(0, Math.max(0, Number(limit) || 0))
    .map((entry) => entry.row);
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
      stock_amount: Number(a.is_consignment || 0) === 1 ? null : Number(a.stock_amount),
      is_default: Number(a.is_default || 0) === 1,
      is_consignment: Number(a.is_consignment || 0) === 1,
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

  const normalizedCategory = normalizeSearchPhrase(category);
  if (!normalizedCategory) return [];

  const termGroups = buildProductSearchTerms({
    original_user_text: searchTerm,
    name: searchTerm,
  });

  let rows = await loadCategoryProducts({
    shop_id,
    category: normalizedCategory,
  });
  rows = filterRowsByExcludeTokens(rows, excludeTokens || []);

  const normalizedSub = normalizeSearchPhrase(subCategory) || null;
  if (normalizedSub) {
    let subCandidates = [normalizedSub];
    try {
      subCandidates = normalizeRequestTokens([
        normalizedSub,
        ...((await getSubCategoryCandidates(normalizedCategory, normalizedSub)) || []),
      ]);
    } catch (error) {
      console.error("[MATCH] searchVariants subcategory lookup failed", {
        category: normalizedCategory,
        subCategory: normalizedSub,
        error: error?.message || String(error),
      });
    }
    const allowed = new Set(subCandidates);
    rows = rows.filter((row) => allowed.has(String(row.sub_category || "")));
  }

  if (!termGroups.length) {
    return rows
      .slice()
      .sort(
        (a, b) =>
          Number(b.is_default || 0) - Number(a.is_default || 0) ||
          String(a.name || "").localeCompare(String(b.name || ""), "he") ||
          Number(a.id || 0) - Number(b.id || 0),
      )
      .slice(0, Math.max(0, Number(limit) || 0));
  }

  const ranked = [];
  for (const row of rows) {
    for (const termGroup of orderedTermGroups(termGroups)) {
      const tokenFields = candidateTokensForTerm(row, termGroup);
      const tokenMatch = buildTokenMatchMeta(
        termGroup.tokens,
        tokenFields.candidateTokens,
        tokenFields.penaltyTokens,
      );
      if (tokenMatch.criticalMissingTokens.length) continue;
      if (tokenMatch.matchedCount < minimumMatchedTokenCount(termGroup.tokens)) continue;
      if (tokenMatch.coverage + 1e-9 < tokenCoverageThreshold(termGroup.tokens)) continue;

      ranked.push({
        row,
        termSpecificity: termSpecificity(termGroup),
        coverage: tokenMatch.coverage,
        matchedCount: tokenMatch.matchedCount,
        averageSimilarity: tokenMatch.averageSimilarity,
        extraScore: tokenMatch.extraTokens.reduce(
          (sum, token) => sum + tokenImportance(token),
          0,
        ),
      });
      break;
    }
  }

  return ranked
    .sort(
      (a, b) =>
        b.termSpecificity - a.termSpecificity ||
        b.coverage - a.coverage ||
        b.matchedCount - a.matchedCount ||
        b.averageSimilarity - a.averageSimilarity ||
        Number(b.row.is_default || 0) - Number(a.row.is_default || 0) ||
        a.extraScore - b.extraScore ||
        String(a.row.name || "").localeCompare(String(b.row.name || ""), "he") ||
        Number(a.row.id || 0) - Number(b.row.id || 0),
    )
    .slice(0, Math.max(0, Number(limit) || 0))
    .map((entry) => entry.row);
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

  // Exported for deterministic regression tests.
  normalizeRequestTokens,
  extractHardConstraintTokens,
  buildTokenMatchMeta,
  tokenCoverageThreshold,
  minimumMatchedTokenCount,
  orderedTermGroups,
  pickBestWeighted,
  findBestByTermGroups,
  collectRequestConstraints,
  findWholeShopRecoveryDecision,
};

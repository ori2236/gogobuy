const db = require("../config/db");
const {
  tokenImportance,
  tokenizeName,
  getExcludeTokensFromReq,
  filterRowsByExcludeTokens,
} = require("../utilities/tokens");
const { getSubCategoryCandidates } = require("../repositories/categories");

async function pickBestWeighted({ shop_id, rows, reqTokens, excludeTokens }) {
  rows = filterRowsByExcludeTokens(rows, excludeTokens);
  if (!rows || !rows.length) return null;

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

  const scored = [];

  for (const m of meta) {
    const wordCount = m.candTokens.length || 9999;

    const price = Number(m.r.price);
    const priceScore = Number.isFinite(price) ? price : 999999;

    let extraScore = 0;

    for (const t of m.extra) {
      const wRaw = invDfMap.has(t) ? invDfMap.get(t) : 1; // fallback
      const inv = Number(wRaw) || 1;

      const imp = tokenImportance(t); // *0.5 for numbers
      const add = inv * imp;

      extraScore += add;
    }

    scored.push({
      row: m.r,
      extraScore,
      priceScore,
      wordCount,
      extraCount: m.extra.length,
    });
  }

  scored.sort(
    (a, b) =>
      a.extraScore - b.extraScore ||
      a.priceScore - b.priceScore ||
      a.wordCount - b.wordCount ||
      b.row.id - a.row.id,
  );

  const best = scored[0];
  return best.row;
}

async function findBestProductForRequest(shop_id, req) {
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

  const reqTokens = tokenizeName(nameRaw);
  const excludeTokens = getExcludeTokensFromReq(req);

  if (!reqTokens.length) {
    if (category && primarySub) {
      const params = [shop_id, category, primarySub];
      let sql = `
        SELECT id, name, display_name_en, price, stock_amount, category, sub_category
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
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
      `;

      const [rows] = await db.query(sql, params);
      if (rows && rows.length) {
        return rows[0];
      }

      if (otherSubs.length) {
        const params2 = [shop_id, category, ...otherSubs];
        let sql2 = `
          SELECT id, name, display_name_en, price, stock_amount, category, sub_category
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
          ORDER BY updated_at DESC, id DESC
          LIMIT 1
        `;

        const [rows2] = await db.query(sql2, params2);
        if (rows2 && rows2.length) {
          return rows2[0];
        }
      }
    }

    return null;
  }

  if (category && primarySub) {
    // 1) exact sub + token-filter -> pickBestWeighted
    {
      let sql = `
        SELECT id, name, display_name_en, price, stock_amount, category, sub_category
        FROM product
        WHERE shop_id = ?
          AND category = ?
          AND sub_category = ?
      `;
      const params = [shop_id, category, primarySub];

      for (const t of reqTokens) {
        sql += `
          AND (
            name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
            OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
          )
        `;
        params.push(t, t);
      }

      const [rows] = await db.query(sql, params);

      const best = await pickBestWeighted({
        shop_id,
        rows,
        reqTokens,
        excludeTokens,
      });
      if (best) {
        return best;
      }
    }

    // 2) other subs + token-filter -> pickBestWeighted
    if (otherSubs.length) {
      let sql = `
        SELECT id, name, display_name_en, price, stock_amount, category, sub_category
        FROM product
        WHERE shop_id = ?
          AND category = ?
          AND sub_category IN (${otherSubs.map(() => "?").join(",")})
      `;
      const params = [shop_id, category, ...otherSubs];

      for (const t of reqTokens) {
        sql += `
          AND (
            name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
            OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
          )
        `;
        params.push(t, t);
      }

      const [rows] = await db.query(sql, params);

      const best = await pickBestWeighted({
        shop_id,
        rows,
        reqTokens,
        excludeTokens,
      });
      if (best) {
        return best;
      }
    }
  }
  {
    let sql = `
      SELECT id, name, display_name_en, price, stock_amount, category, sub_category
      FROM product
      WHERE shop_id = ?
    `;
    const params = [shop_id];

    for (const t of reqTokens) {
      sql += `
        AND (
          name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
          OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
        )
      `;
      params.push(t, t);
    }

    const [rowsAll] = await db.query(sql, params);

    const rows = filterRowsByExcludeTokens(rowsAll, excludeTokens);

    if (rows && rows.length) {
      const best = await pickBestWeighted({
        shop_id,
        rows,
        reqTokens,
        excludeTokens,
      });
      if (best) {
        return best;
      }
    }
  }
  return null;
}

async function fetchInvDfMap(shop_id, tokens) {
  const uniq = Array.from(new Set(tokens)).filter(Boolean);

  if (!uniq.length) return new Map();

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
  return map;
}

async function searchProducts(shop_id, products) {
  const found = [];
  const notFound = [];

  for (let i = 0; i < products.length; i++) {
    const req = products[i];
    const row = await findBestProductForRequest(shop_id, req);
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

  const reqTokens = tokenizeName(requestedName || "");

  async function fetchByCatSub(cat, sub, useGroup = true) {
    const params = [shop_id];
    let sql = `
      SELECT id, name, display_name_en, price, stock_amount, category, sub_category
      FROM product
      WHERE shop_id = ?
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
        return {
          row: r,
          score,
          isPrimary,
          wordCount: candTokens.length || 9999,
        };
      });

      const positive = scored
        .filter((s) => s.score > 0)
        .sort(
          (a, b) =>
            b.score - a.score ||
            (b.isPrimary ? 1 : 0) - (a.isPrimary ? 1 : 0) ||
            a.wordCount - b.wordCount ||
            a.row.id - b.row.id,
        )

        .map((s) => s.row);

      if (positive.length >= limit) return positive.slice(0, limit);

      const zero = scored.filter((s) => s.score === 0).map((s) => s.row);
      return [...positive, ...zero].slice(0, limit);
    }

    return filteredRows.slice(0, limit);
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
) {
  const altQuestions = [];
  const alternativesMap = {};
  const usedIds = new Set(foundIdsSet);
  let t = 0;

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
      3,
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
  { category = null, subCategory = null, searchTerm = null, limit = 50 } = {},
) {
  const tokens = tokenizeName(searchTerm || "");

  let sql = `
    SELECT id, name, display_name_en, price, stock_amount, category, sub_category
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

  sql += `
    ORDER BY name ASC, id DESC
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
};
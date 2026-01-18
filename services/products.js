const db = require("../config/db");
const { normalizeIncomingQuestions } = require("../utilities/normalize");

const SUBCATEGORY_GROUPS = {
  // ===== Dairy & Eggs =====
  Cheese: ["Cheese", "Spreads & Cream Cheese"],
  "Spreads & Cream Cheese": ["Spreads & Cream Cheese", "Cheese"],

  Yogurt: ["Yogurt", "Desserts & Puddings"],
  "Desserts & Puddings": ["Desserts & Puddings", "Yogurt"],

  Milk: ["Milk", "Milk Alternatives"],
  "Milk Alternatives": ["Milk Alternatives", "Milk"],

  // ===== Bakery =====
  Bread: ["Bread", "Rolls & Buns", "Baguettes & Artisan"],
  "Rolls & Buns": ["Rolls & Buns", "Bread"],
  "Baguettes & Artisan": ["Baguettes & Artisan", "Bread"],

  "Cakes & Pastries": ["Cakes & Pastries", "Cookies & Biscuits"],
  "Cookies & Biscuits": ["Cookies & Biscuits", "Cakes & Pastries"],

  "Pita & Flatbread": ["Pita & Flatbread", "Tortillas & Wraps"],
  "Tortillas & Wraps": ["Tortillas & Wraps", "Pita & Flatbread"],

  // ===== Produce =====
  Fruits: ["Fruits", "Organic Produce"],
  Vegetables: ["Vegetables", "Organic Produce"],
  "Organic Produce": ["Organic Produce", "Fruits", "Vegetables"],

  "Prepped Produce": ["Prepped Produce", "Vegetables"],

  // ===== Meat & Poultry =====
  Beef: ["Beef", "Ground/Minced"],
  "Ground/Minced": ["Ground/Minced", "Beef"],

  "Cold Cuts": ["Cold Cuts", "Turkey", "Chicken"],
  Turkey: ["Turkey", "Cold Cuts"],
  Chicken: ["Chicken", "Cold Cuts"],

  Sausages: ["Sausages", "Mixed & Other Meats"],
  "Mixed & Other Meats": ["Mixed & Other Meats", "Sausages"],

  // ===== Fish & Seafood =====
  "Fresh Fish": ["Fresh Fish", "Frozen Fish"],
  "Frozen Fish": ["Frozen Fish", "Fresh Fish"],

  // ===== Deli & Ready Meals =====
  "Ready-to-Eat Meals": ["Ready-to-Eat Meals", "Sushi & Sashimi"],
  "Sushi & Sashimi": ["Sushi & Sashimi", "Ready-to-Eat Meals"],

  // ===== Frozen =====
  "Pizza & Dough": ["Pizza & Dough", "Ready Meals"],
  "Ready Meals": ["Ready Meals", "Pizza & Dough"],

  // ===== Pantry =====
  "Flour & Baking": ["Flour & Baking", "Baking Mixes"],
  "Baking Mixes": ["Baking Mixes", "Flour & Baking"],

  "Breakfast Cereal": ["Breakfast Cereal", "Granola & Muesli"],
  "Granola & Muesli": ["Granola & Muesli", "Breakfast Cereal"],

  "Canned Vegetables": ["Canned Vegetables", "Canned Beans & Legumes"],
  "Canned Beans & Legumes": ["Canned Beans & Legumes", "Canned Vegetables"],

  "Honey & Spreads": ["Honey & Spreads", "Nut Butters", "Jams & Preserves"],
  "Nut Butters": ["Nut Butters", "Honey & Spreads"],
  "Jams & Preserves": ["Jams & Preserves", "Honey & Spreads"],

  "Asian Pantry": ["Asian Pantry", "Sauces & Condiments"],
  "Mediterranean Pantry": ["Mediterranean Pantry", "Sauces & Condiments"],
  "Mexican Pantry": ["Mexican Pantry", "Sauces & Condiments"],
  "Canned Tomatoes": ["Canned Tomatoes", "Sauces & Condiments"],
  "Sauces & Condiments": [
    "Sauces & Condiments",
    "Asian Pantry",
    "Mediterranean Pantry",
    "Mexican Pantry",
    "Canned Tomatoes",
  ],

  // ===== Snacks =====
  "Chips & Crisps": ["Chips & Crisps", "Pretzels & Popcorn"],
  "Pretzels & Popcorn": ["Pretzels & Popcorn", "Chips & Crisps"],

  // ===== Personal Care =====
  "Bath & Body": ["Bath & Body", "Hand Soap & Sanitizers"],
  "Hand Soap & Sanitizers": ["Hand Soap & Sanitizers", "Bath & Body"],

  // ===== Health & Wellness =====
  "Pain Relief": ["Pain Relief", "Cough & Cold"],
  "Cough & Cold": ["Cough & Cold", "Pain Relief"],
};

function getSubCategoryCandidates(sub) {
  const s = (sub || "").trim();
  if (!s) return [];
  if (SUBCATEGORY_GROUPS[s]) return SUBCATEGORY_GROUPS[s];
  return [s];
}

const HEBREW_NOISE_TOKENS = new Set([
  "רגיל",
  "רגילה",
  "רגילים",
  "רגילות",
  "קטן",
  "קטנה",
  "קטנים",
  "קטנות",
  "גדול",
  "גדולה",
  "גדולים",
  "גדולות",
]);

const ENGLISH_NOISE_TOKENS = new Set([
  "regular",
  "normal",
  "plain",
  "classic",
  "small",
  "large",
  "big",
]);

function getExcludeTokensFromReq(req) {
  const raw = req && req.exclude_tokens;
  if (!Array.isArray(raw)) return [];
  return raw
    .map((x) => normalizeToken(typeof x === "string" ? x : String(x || "")))
    .map((x) => x.toLowerCase())
    .filter(Boolean);
}

function normForContains(s) {
  return normalizeToken(String(s || "").toLowerCase());
}

function filterRowsByExcludeTokens(rows, excludeTokens) {
  if (!rows || !rows.length || !excludeTokens.length) return rows || [];

  const ex = excludeTokens.map(normForContains).filter(Boolean);

  return rows.filter((r) => {
    const name = normForContains(r.name || "");
    const en = normForContains(r.display_name_en || "");
    return !ex.some((t) => (t && name.includes(t)) || (t && en.includes(t)));
  });
}

function isNoiseToken(t) {
  return HEBREW_NOISE_TOKENS.has(t) || ENGLISH_NOISE_TOKENS.has(t);
}

function normalizeToken(t) {
  return String(t || "")
    .normalize("NFKC")
    .replace(/['’"]/g, "")
    .trim();
}

function tokenImportance(token) {
  const t = String(token || "").toLowerCase();

  if (/^\d+(\.\d+)?$/.test(t)) return 0.5;

  if (/\d/.test(t)) return 0.7;

  return 1;
}

function tokenizeName(str) {
  if (!str) return [];

  const baseTokens = String(str)
    .toLowerCase()
    .replace(/[^\w\u0590-\u05FF]+/g, " ")
    .split(/\s+/)
    .map((t) => normalizeToken(t))
    .filter(Boolean);

  if (baseTokens.length <= 1) return baseTokens;

  const filtered = baseTokens.filter((t) => !isNoiseToken(t));
  return filtered.length ? filtered : baseTokens;
}

function safeParseJson(txt) {
  if (typeof txt !== "string") throw new Error("safeParseJson expects string");
  try {
    return JSON.parse(txt);
  } catch {}
  const fenced = txt.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) {
    try {
      return JSON.parse(fenced[1]);
    } catch {}
  }
  const i = txt.indexOf("{");
  const j = txt.lastIndexOf("}");
  if (i !== -1 && j !== -1 && j > i) {
    const slice = txt.slice(i, j + 1);
    try {
      return JSON.parse(slice);
    } catch {}
  }
  throw new Error("Not valid JSON");
}

function parseModelAnswer(answer) {
  if (!answer) throw new Error("Empty model answer");

  if (
    typeof answer === "object" &&
    ("products" in answer || "summary_line" in answer || "questions" in answer)
  ) {
    return answer;
  }

  const content =
    (typeof answer?.choices?.[0]?.message?.content === "string" &&
      answer.choices[0].message.content) ||
    (typeof answer?.message === "string" && answer.message) ||
    (typeof answer?.content === "string" && answer.content) ||
    (typeof answer === "string" ? answer : null);

  if (!content) {
    if (typeof answer?.choices?.[0]?.message?.content === "object")
      return answer.choices[0].message.content;
    if (typeof answer?.content === "object") return answer.content;
    throw new Error("Unknown model answer shape");
  }
  return safeParseJson(content);
}

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

      const imp = tokenImportance(t);// *0.5 for numbers
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
      b.row.id - a.row.id
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
  const subCandidates = primarySub ? getSubCategoryCandidates(primarySub) : [];
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

async function rebuildTokenWeightsForShop(shop_id) {
  const [rows] = await db.query(
    `
    SELECT id, name, display_name_en
    FROM product
    WHERE shop_id = ?
    `,
    [shop_id]
  );

  const df = new Map();

  for (const r of rows) {
    const tokens = new Set(tokenizeName(r.name || ""));
    for (const t of tokens) {
      df.set(t, (df.get(t) || 0) + 1);
    }
  }

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    await conn.query(`DELETE FROM product_token_weight WHERE shop_id = ?`, [
      shop_id,
    ]);

    const chunkSize = 500;
    const entries = Array.from(df.entries()); // [token, docFreq]

    for (let i = 0; i < entries.length; i += chunkSize) {
      const chunk = entries.slice(i, i + chunkSize);

      const values = [];
      const params = [];

      for (const [token, docFreq] of chunk) {
        values.push("(?, ?, ?, ?)");
        params.push(shop_id, token, docFreq, docFreq > 0 ? 1 / docFreq : 1);
      }

      await conn.query(
        `
        INSERT INTO product_token_weight (shop_id, token, doc_freq, inv_df)
        VALUES ${values.join(",")}
        `,
        params
      );
    }

    await conn.commit();
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
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
    [shop_id, ...uniq]
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
  excludeTokens = []
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
        subList = getSubCategoryCandidates(sub);
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
            a.row.id - b.row.id
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
  (req, names) => `${req} לא זמין כרגע. נוכל להחליף ב־${names.join(" / ")}?`,
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
  context = ""
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
      excludeTokens
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
        : a.name
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

function buildItemsBlock({ items, isEnglish, mode }) {
  if (!Array.isArray(items) || !items.length) return "";

  const lines = [];
  lines.push(
    isEnglish
      ? mode === "create"
        ? "Items added to your order:"
        : "Items in your order now:"
      : mode === "create"
      ? "המוצרים שהוספתי להזמנה:"
      : "המוצרים שכעת בהזמנה:"
  );

  for (const it of items) {
    if (!it || !it.name) continue;

    const name = it.name;
    const qty = Number(it.amount);
    const unitPrice = Number(it.price);

    if (!Number.isFinite(qty) || !Number.isFinite(unitPrice)) continue;

    const soldByWeightRaw = it.sold_by_weight;
    const isWeight =
      soldByWeightRaw === true ||
      soldByWeightRaw === 1 ||
      soldByWeightRaw === "1";

    const unitsRaw = it.units ?? it.requested_units ?? it.requestedUnits;
    const unitsNum = Number(unitsRaw);
    const units =
      isWeight && Number.isFinite(unitsNum) && unitsNum > 0 ? unitsNum : null;

    const lineTotal = Number((qty * unitPrice).toFixed(2));

    if (!isWeight) {
      if (qty === 1) {
        lines.push(`• ${name} - ₪${unitPrice.toFixed(2)}`);
      } else {
        const eachSuffix = isEnglish ? "each" : "ליח'";
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)} (₪${unitPrice.toFixed(
            2
          )} ${eachSuffix})`
        );
      }
      continue;
    }

    if (units) {
      if (isEnglish) {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)} (₪${unitPrice.toFixed(
            2
          )} per kg, approx price for ${units} units)`
        );
      } else {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)} (₪${unitPrice.toFixed(
            2
          )} לק"ג, מחיר משוערך ל${units} יחידות)`
        );
      }
    } else {
      if (isEnglish) {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)} (₪${unitPrice.toFixed(
            2
          )} per kg)`
        );
      } else {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)} (₪${unitPrice.toFixed(
            2
          )} לק"ג)`
        );
      }
    }
  }

  return lines.join("\n");
}

function buildQuestionsBlock({ questions, isEnglish }) {
  const qs = normalizeIncomingQuestions(questions);
  if (!qs.length) return "";
  const lines = [];
  lines.push("");
  lines.push(isEnglish ? "Questions:" : "שאלות:");
  for (const q of qs) lines.push(`• ${q.question}`);
  return lines.join("\n");
}

async function searchVariants(
  shop_id,
  { category = null, subCategory = null, searchTerm = null, limit = 50 } = {}
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
    const subs = getSubCategoryCandidates(subCategory);
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
  // JSON parsing
  safeParseJson,
  parseModelAnswer,

  // Product search & alternatives
  findBestProductForRequest,
  searchProducts,
  fetchAlternatives,
  buildAlternativeQuestions,

  pickAltTemplate,

  buildItemsBlock,
  buildQuestionsBlock,

  searchVariants,

  getExcludeTokensFromReq,

  rebuildTokenWeightsForShop,

  tokenizeName,
  getSubCategoryCandidates,
};

const db = require("../config/db");

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

function isEnglishSummary(summaryLine) {
  if (typeof summaryLine !== "string") return false;
  const hasLatin = /[A-Za-z]/.test(summaryLine);
  const hasHeb = /[\u0590-\u05FF]/.test(summaryLine);
  if (hasLatin && !hasHeb) return true;
  if (
    summaryLine.startsWith("Great, here’s") ||
    summaryLine.startsWith("To complete your order")
  )
    return true;
  return false;
}

function normalizeIncomingQuestions(qs) {
  if (!Array.isArray(qs)) return [];
  const out = [];
  for (const q of qs) {
    if (!q) continue;
    if (typeof q === "string" && q.trim()) {
      out.push({ name: null, question: q.trim() });
    } else if (
      typeof q === "object" &&
      typeof q.question === "string" &&
      q.question.trim()
    ) {
      out.push({
        name: typeof q.name === "string" ? q.name : null,
        question: q.question.trim(),
      });
    }
  }
  return out;
}

async function findBestProductForRequest(shop_id, req) {
  const category = (req?.category || "").trim();
  const subCategory = (req?.["sub-category"] || req?.sub_category || "").trim();
  const nameRaw = (req?.name || "").trim();

  const tokens = nameRaw
    .replace(/\s+/g, " ")
    .split(" ")
    .map((t) => t.trim())
    .filter(Boolean);

  if (tokens.length) {
    // category + sub_category + ALL tokens in name
    if (category && subCategory) {
      let sql = `
        SELECT id, name, price, stock_amount, category, sub_category
        FROM product
        WHERE shop_id = ?
          AND category = ?
          AND sub_category = ?
      `;
      const params = [shop_id, category, subCategory];
      for (const t of tokens) {
        sql += ` AND name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')`;
        params.push(t);
      }
      sql += ` ORDER BY updated_at DESC, id DESC LIMIT 1`;
      const [rows] = await db.query(sql, params);
      if (rows && rows.length) return rows[0];
    }

    // ALL tokens in name
    {
      let sql = `
        SELECT id, name, price, stock_amount, category, sub_category
        FROM product
        WHERE shop_id = ?
      `;
      const params = [shop_id];
      for (const t of tokens) {
        sql += ` AND name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')`;
        params.push(t);
      }
      sql += ` ORDER BY updated_at DESC, id DESC LIMIT 1`;
      const [rows] = await db.query(sql, params);
      if (rows && rows.length) return rows[0];
    }

    return null;
  }

  // by category if there is no name
  if (category && subCategory) {
    const [rows] = await db.query(
      `SELECT id, name, price, stock_amount, category, sub_category
         FROM product
        WHERE shop_id = ?
          AND category = ?
          AND sub_category = ?
        ORDER BY updated_at DESC, id DESC
        LIMIT 1`,
      [shop_id, category, subCategory]
    );
    if (rows && rows.length) return rows[0];
  }

  return null;
}

async function searchProducts(shop_id, products) {
  const found = [];
  const notFound = [];

  for (let i = 0; i < products.length; i++) {
    const req = products[i];
    const row = await findBestProductForRequest(shop_id, req);
    if (row) {
      const n = Number(req?.amount);
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
      });
    } else {
      const n = Number(req?.amount);
      notFound.push({
        originalIndex: i,
        requested_name: req?.name || null,
        requested_amount: Number.isFinite(n) ? n : 1,
        category: req?.category || null,
        sub_category: req?.["sub-category"] || req?.sub_category || null,
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
  limit = 3
) {
  if (!category && !subCategory) return [];
  const params = [shop_id];
  let sql = `
    SELECT id, name, price, stock_amount, category, sub_category
    FROM product
    WHERE shop_id = ?`;
  if (category) {
    sql += ` AND category = ?`;
    params.push(category);
  }
  if (subCategory) {
    sql += ` AND sub_category = ?`;
    params.push(subCategory);
  }
  if (excludeIds.length) {
    sql += ` AND id NOT IN (${excludeIds.map(() => "?").join(",")})`;
    params.push(...excludeIds);
  }
  sql += ` ORDER BY updated_at DESC, id DESC LIMIT ?`;
  params.push(limit);
  const [rows] = await db.query(sql, params);

  if ((!rows || !rows.length) && category && subCategory) {
    const params2 = [shop_id, category];
    let sql2 = `
      SELECT id, name, price, stock_amount, category, sub_category
      FROM product
      WHERE shop_id = ?
        AND category = ?`;
    if (excludeIds.length) {
      sql2 += ` AND id NOT IN (${excludeIds.map(() => "?").join(",")})`;
      params2.push(...excludeIds);
    }
    sql2 += ` ORDER BY updated_at DESC, id DESC LIMIT ?`;
    params2.push(limit);
    const [rows2] = await db.query(sql2, params2);
    return rows2 || [];
  }

  return rows || [];
}

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
  isEnglish
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
    const alts = await fetchAlternatives(shop_id, cat, sub, exclude, 3);
    if (!alts || !alts.length) continue;

    alts.forEach((a) => usedIds.add(a.id));
    alternativesMap[nf.originalIndex] = alts.map((a) => ({
      id: a.id,
      name: a.name,
      price: Number(a.price),
      stock_amount: Number(a.stock_amount),
      category: a.category,
      sub_category: a.sub_category,
    }));

    const names = alts.map((a) => a.name);
    const reqName =
      nf.requested_name || (isEnglish ? "the requested item" : "המוצר שביקשת");
    const questionText = pickAltTemplate(isEnglish, t++)(reqName, names);

    altQuestions.push({
      name: nf.requested_name || null,
      question: questionText,
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
    const qty = Number(it.amount);
    const unit = Number(it.price);
    const name = it.outputName || it.name;
    if (!name) continue;
    if (qty === 1) {
      lines.push(`• ${name} - ₪${unit.toFixed(2)}`);
    } else {
      const lineTotal = Number((qty * unit).toFixed(2));
      lines.push(
        `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)} (₪${unit.toFixed(
          2
        )} ליח')`
      );
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

module.exports = {
  // JSON parsing
  safeParseJson,
  parseModelAnswer,

  // Language & questions
  isEnglishSummary,
  normalizeIncomingQuestions,

  // Product search & alternatives
  findBestProductForRequest,
  searchProducts,
  fetchAlternatives,
  buildAlternativeQuestions,

  pickAltTemplate,

  buildItemsBlock,
  buildQuestionsBlock,
};

const db = require("../config/db");
const { normalizeIncomingQuestions } = require("./normalize");
const MIN_PARTIAL_COVERAGE = 0.65;

function tokenizeName(str) {
  if (!str) return [];
  return String(str)
    .toLowerCase()
    .replace(/[^\w\u0590-\u05FF]+/g, " ")
    .split(/\s+/)
    .map((t) => t.trim())
    .filter(Boolean);
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

async function findBestProductForRequest(shop_id, req) {
  const category = (req?.category || "").trim();
  const subCategory = (req?.["sub-category"] || req?.sub_category || "").trim();
  const nameRaw = (req?.name || "").trim();

  const reqTokens = tokenizeName(nameRaw);
  if (!reqTokens.length) {
    //there is no name, category + sub_category
    if (category && subCategory) {
      const [rows] = await db.query(
        `SELECT id, name, display_name_en, price, stock_amount, category, sub_category
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

  // category + sub_category
  if (category && subCategory) {
    //all the tokens in the product name
    {
      let sql = `
        SELECT id, name, display_name_en, price, stock_amount, category, sub_category
        FROM product
        WHERE shop_id = ?
          AND category = ?
          AND sub_category = ?
      `;
      const params = [shop_id, category, subCategory];

      for (const t of reqTokens) {
        sql += ` AND (name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
                 OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%'))`;
        params.push(t, t);
      }

      const [rows] = await db.query(sql, params);

      if (rows && rows.length) {
        let best = null;
        for (const r of rows) {
          const candTokens = tokenizeName(
            (r.name || "") + " " + (r.display_name_en || "")
          );
          const wordCount = candTokens.length || 9999;
          if (
            !best ||
            wordCount < best.wordCount ||
            (wordCount === best.wordCount && r.id > best.row.id)
          ) {
            best = { row: r, wordCount };
          }
        }
        if (best) return best.row;
      }
    }

    // MIN_PARTIAL_COVERAGE
    {
      const [rows] = await db.query(
        `
        SELECT id, name, display_name_en, price, stock_amount, category, sub_category
        FROM product
        WHERE shop_id = ?
          AND category = ?
          AND sub_category = ?
      `,
        [shop_id, category, subCategory]
      );

      if (rows && rows.length) {
        const scored = [];
        const reqSet = new Set(reqTokens);

        for (const r of rows) {
          const candTokens = tokenizeName(
            (r.name || "") + " " + (r.display_name_en || "")
          );
          if (!candTokens.length) continue;

          let common = 0;
          for (const t of reqSet) {
            if (candTokens.includes(t)) common++;
          }
          const coverage = common / reqTokens.length;

          if (coverage >= MIN_PARTIAL_COVERAGE) {
            scored.push({
              row: r,
              coverage,
              wordCount: candTokens.length,
            });
          }
        }

        if (scored.length) {
          scored.sort(
            (a, b) =>
              b.coverage - a.coverage ||
              a.wordCount - b.wordCount ||
              b.row.id - a.row.id
          );
          return scored[0].row;
        }
      }
    }
    return null;
  }

  //all categories
  {
    let sql = `
      SELECT id, name, display_name_en, price, stock_amount, category, sub_category
      FROM product
      WHERE shop_id = ?
    `;
    const params = [shop_id];

    for (const t of reqTokens) {
      sql += ` AND (name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
               OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%'))`;
      params.push(t, t);
    }

    const [rows] = await db.query(sql, params);
    if (rows && rows.length) {
      let best = null;
      for (const r of rows) {
        const candTokens = tokenizeName(
          (r.name || "") + " " + (r.display_name_en || "")
        );
        const wordCount = candTokens.length || 9999;
        if (
          !best ||
          wordCount < best.wordCount ||
          (wordCount === best.wordCount && r.id > best.row.id)
        ) {
          best = { row: r, wordCount };
        }
      }
      if (best) return best.row;
    }
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
        matched_display_name_en: row.display_name_en,
      });
    } else {
      const n = Number(req?.amount);
      notFound.push({
        originalIndex: i,
        requested_name: req?.name || null,
        requested_output_name: req?.outputName || null,
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
  limit = 3,
  requestedName = null
) {
  if (!category && !subCategory) return [];

  const reqTokens = tokenizeName(requestedName || "");

  async function fetchByCatSub(cat, sub) {
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
    if (sub) {
      sql += ` AND sub_category = ?`;
      params.push(sub);
    }
    if (excludeIds.length) {
      sql += ` AND id NOT IN (${excludeIds.map(() => "?").join(",")})`;
      params.push(...excludeIds);
    }

    const [rows] = await db.query(sql, params);
    if (!rows || !rows.length) return [];

    // all tokens
    if (reqTokens.length) {
      const reqSet = new Set(reqTokens);
      const scored = rows.map((r) => {
        const candTokens = tokenizeName(
          (r.name || "") + " " + (r.display_name_en || "")
        );
        let common = 0;
        for (const t of reqSet) {
          if (candTokens.includes(t)) common++;
        }
        const score = reqTokens.length > 0 ? common / reqTokens.length : 0;
        return {
          row: r,
          score,
          wordCount: candTokens.length || 9999,
        };
      });

      const positive = scored
        .filter((s) => s.score > 0)
        .sort(
          (a, b) =>
            b.score - a.score ||
            a.wordCount - b.wordCount ||
            b.row.id - a.row.id
        )
        .map((s) => s.row);

      if (positive.length >= limit) return positive.slice(0, limit);

      const zero = scored.filter((s) => s.score === 0).map((s) => s.row);

      return [...positive, ...zero].slice(0, limit);
    }

    return rows.slice(0, limit);
  }

  //category + subCategory
  let rows = await fetchByCatSub(category, subCategory);
  if (rows && rows.length) return rows;

  //category
  if (category) {
    rows = await fetchByCatSub(category, null);
    if (rows && rows.length) return rows;
  }

  return [];
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
    const mainName = nf.requested_name || nf.requested_output_name || null;

    const alts = await fetchAlternatives(
      shop_id,
      cat,
      sub,
      exclude,
      3,
      mainName
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

    const questionText = pickAltTemplate(isEnglish, t++)(subject, names);

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
    const qty = Number(it.amount);
    const unit = Number(it.price);
    const name = it.name;
    if (!name) continue;

    if (qty === 1) {
      lines.push(`• ${name} - ₪${unit.toFixed(2)}`);
    } else {
      const lineTotal = Number((qty * unit).toFixed(2));
      const eachSuffix = isEnglish ? "each" : "ליח'";
      lines.push(
        `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)} (₪${unit.toFixed(
          2
        )} ${eachSuffix})`
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

  // Product search & alternatives
  findBestProductForRequest,
  searchProducts,
  fetchAlternatives,
  buildAlternativeQuestions,

  pickAltTemplate,

  buildItemsBlock,
  buildQuestionsBlock,
};

const db = require("../config/db");
const {
  buildQuestionsBlock,
  searchProducts,
  fetchAlternatives,
  tokenizeName,
  getSubCategoryCandidates,
  getExcludeTokensFromReq,
} = require("./products");
const { saveOpenQuestions } = require("../utilities/openQuestions");
const { normalizeIncomingQuestions } = require("../utilities/normalize");

const DEBUG = process.env.DEBUG_PRICE_AND_SALES !== "0";
function dlog(...args) {
  if (DEBUG) console.log("[INV-PRICE]", ...args);
}

function formatILS(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return null;
  return `₪${x.toFixed(2)}`;
}

function fmtQty(n, digits = 2) {
  const x = Number(n);
  if (!Number.isFinite(x)) return "";
  return x.toFixed(digits).replace(/\.?0+$/, "");
}

function nullify(v) {
  if (v === null || v === undefined) return null;

  if (typeof v === "string") {
    const s = v.trim();
    if (!s) return null;
    const low = s.toLowerCase();
    if (low === "null" || low === "undefined" || low === "none") return null;
    return s;
  }

  return v;
}

function normalizeToken(t) {
  return (
    String(t || "")
      .toLowerCase()
      // remove Hebrew geresh/gershayim + common quotes
      .replace(/[\u05F3\u05F4"'`]/g, "")
      .trim()
  );
}

const DASHES_RE = /[-‐-‒–—―\u05BE]/g;

function tokenVariants(rawTok) {
  const raw = String(rawTok || "").trim();
  if (!raw) return [];

  const vars = new Set();

  vars.add(raw);

  const noQuotes = raw.replace(/[\u05F3\u05F4"'`]/g, "").trim();
  if (noQuotes) vars.add(noQuotes);

  const asDoubleQuote = raw.replace(/\u05F4/g, '"').trim();
  if (asDoubleQuote) vars.add(asDoubleQuote);

  const noDashes = raw.replace(DASHES_RE, " ").replace(/\s+/g, " ").trim();
  if (noDashes) vars.add(noDashes);

  const parts = raw
    .split(DASHES_RE)
    .map((x) => x.trim())
    .filter(Boolean);
  for (const p of parts) vars.add(p);

  return Array.from(vars).filter(Boolean);
}

function buildCompareQtyText({ req, foundRow, isEnglish }) {
  const soldByWeight = req?.sold_by_weight === true;

  const amountRaw = Number(req?.amount ?? foundRow?.requested_amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  const unitsRaw = foundRow?.requested_units ?? req?.units ?? null;
  const unitsNum = Number(unitsRaw);
  const units = Number.isFinite(unitsNum) && unitsNum > 0 ? unitsNum : null;

  if (soldByWeight) {
    const kgTxt = fmtQty(amount);
    const approxPrefixHe = "כ~";
    const approxPrefixEn = "~";

    if (units) {
      return isEnglish
        ? `${units} ${units === 1 ? "unit" : "units"}, ~${kgTxt} kg`
        : `${units} יח׳, כ~${kgTxt} ק״ג`;
    }

    return isEnglish ? `${kgTxt} kg` : `${kgTxt} ק״ג`;
  }

  const u = Number.isFinite(amount) ? amount : 1;
  return isEnglish ? `${u} ${u === 1 ? "unit" : "units"}` : `${u} יח׳`;
}

function cleanQuestionsBlockHeader(block, isEnglish) {
  if (!block || typeof block !== "string") return "";
  const header = isEnglish ? "Questions:" : "שאלות:";
  const lines = block.split("\n");
  const cleaned = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (trimmed === header) continue;
    cleaned.push(line);
  }
  return cleaned.join("\n").trim();
}

function buildBudgetPickSubject(req, isEnglish) {
  const nameHe = nullify(req?.name);
  const nameEn = nullify(req?.outputName);

  if (isEnglish) return nameEn || nameHe || null;
  return nameHe || nameEn || null;
}

function buildPromoSubject(req, isEnglish) {
  const nameHe = nullify(req?.name);
  const nameEn = nullify(req?.outputName);
  const sub = nullify(req?.["sub-category"] || req?.sub_category);
  const cat = nullify(req?.category);

  if (isEnglish && nameEn) return nameEn;
  if (nameHe) return nameHe;
  if (nameEn) return nameEn;

  if (sub || cat) return isEnglish ? "that category" : "הקטגוריה שביקשת";

  return isEnglish ? "that" : "מה שביקשת";
}

function buildQuestionsTextSmart({ questions, isEnglish }) {
  if (!Array.isArray(questions) || !questions.length) return "";
  if (questions.length === 1) return String(questions[0].question || "").trim();

  const hasMultiline = questions.some((q) =>
    String(q?.question || "").includes("\n")
  );
  if (hasMultiline) {
    return questions
      .map((q) => String(q.question || "").trim())
      .filter(Boolean)
      .join("\n\n");
  }

  const qb = buildQuestionsBlock({ questions, isEnglish });
  return cleanQuestionsBlockHeader(qb, isEnglish) || qb;
}

function isOutOfStockFromFound(foundRow) {
  const stockRaw = Number(foundRow?.stock_amount);
  const stock = Number.isFinite(stockRaw) && stockRaw >= 0 ? stockRaw : null;
  return stock !== null && stock <= 0;
}

function isInStockRow(row) {
  const s = Number(row?.stock_amount);
  if (!Number.isFinite(s)) return true;
  return s > 0;
}

function getSubjectForAlt({ req, foundRow, isEnglish }) {
  const he = String(foundRow?.matched_name || req?.name || "").trim();
  const en = String(
    foundRow?.matched_display_name_en || req?.outputName || ""
  ).trim();

  if (isEnglish) return en || he || "this product";
  return he || en || "המוצר הזה";
}

function formatAltLineWithPrice({ altRow, req, isEnglish }) {
  const name = isEnglish
    ? String(altRow?.display_name_en || altRow?.name || "").trim()
    : String(altRow?.name || altRow?.display_name_en || "").trim();
  if (!name) return null;

  const p = Number(altRow?.price);
  if (!Number.isFinite(p)) return name;

  const soldByWeight = req?.sold_by_weight === true;
  const amountRaw = Number(req?.amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  if (!soldByWeight) {
    if (amount <= 1) return `${name} - ${formatILS(p)}`;
    const total = p * amount;
    return isEnglish
      ? `${name} - ${formatILS(p)} each (total ${formatILS(
          total
        )} for ${amount})`
      : `${name} - ${formatILS(p)} ליח׳ (סה״כ ${formatILS(
          total
        )} ל-${amount} יח׳)`;
  }

  // by weight (₪/kg)
  if (Math.abs(amount - 1) < 1e-9)
    return isEnglish
      ? `${name} - ${formatILS(p)} per kg`
      : `${name} - ${formatILS(p)} לק״ג`;

  const total = p * amount;
  return isEnglish
    ? `${name} - ${formatILS(p)} per kg (est. ${formatILS(
        total
      )} for ~${amount} kg)`
    : `${name} - ${formatILS(p)} לק״ג (מחיר משוערך ${formatILS(
        total
      )} ל~${amount} ק״ג)`;
}

function buildAltQuestionText({ reason, subject, altLines, isEnglish }) {
  const list = altLines.map((x) => `• ${x}`).join("\n");
  const s = String(subject || "").trim();

  if (isEnglish) {
    if (reason === "OOS") {
      return `Unfortunately it is currently out of stock, but we have these alternatives:\n${list}`;
    }
    // NOT_FOUND
    return `We don’t have ${s}, but we do have:\n${list}`;
  }

  if (reason === "OOS") {
    return `לצערנו הוא לא במלאי כרגע, אבל יש לנו את החלופות הבאות:\n${list}`;
  }

  // NOT_FOUND
  return `אין לנו ${s} אבל יש לנו:\n${list}`;
}

async function buildAltBlockAndQuestion({
  shop_id,
  reason,
  req,
  foundRow,
  category,
  sub_category,
  excludeTokens,
  usedIds,
  isEnglish,
}) {
  const subject = getSubjectForAlt({ req, foundRow, isEnglish });

  let alts = await fetchAlternatives(
    shop_id,
    category,
    sub_category,
    Array.from(usedIds),
    3,
    req?.name || req?.outputName || subject,
    excludeTokens
  );

  if (Array.isArray(alts) && alts.length) {
    const inStock = alts.filter(isInStockRow);
    if (inStock.length) alts = inStock;
  }

  if (Array.isArray(alts) && alts.length && usedIds && usedIds.size) {
    alts = alts.filter((a) => {
      const id = Number(a?.id);
      const pid = Number(a?.product_id);

      if (Number.isFinite(id) && usedIds.has(id)) return false;
      if (Number.isFinite(pid) && usedIds.has(pid)) return false;

      return true;
    });
  }

  if (!Array.isArray(alts) || !alts.length) {
    const q = {
      name: foundRow?.matched_name || req?.name || null,
      question: isEnglish
        ? `I couldn’t find ${
            reason === "OOS" ? "in-stock alternatives" : "close matches"
          } for ${subject}. Can you write it differently?`
        : `לא מצאתי ${
            reason === "OOS" ? "חלופות במלאי" : "מוצרים דומים"
          } עבור ${subject}. תוכל לכתוב את זה בצורה אחרת?`,
      options: [],
    };
    return { blockText: q.question, questionObj: q, altIds: [] };
  }

  const altLines = alts
    .map((a) => formatAltLineWithPrice({ altRow: a, req, isEnglish }))
    .filter(Boolean);

  const options = alts
    .map((a) =>
      isEnglish
        ? String(a?.display_name_en || a?.name || "").trim()
        : String(a?.name || a?.display_name_en || "").trim()
    )
    .filter(Boolean);

  const q = {
    name: foundRow?.matched_name || req?.name || null,
    question: buildAltQuestionText({ reason, subject, altLines, isEnglish }),
    options,
  };

  const altIds = alts
    .flatMap((a) => [Number(a?.id), Number(a?.product_id)])
    .filter(Number.isFinite);

  return { blockText: q.question, questionObj: q, altIds };
}

function buildFoundProductLine({ req, foundRow, isEnglish }) {
  const reqName = String(req?.name || "").trim();

  const matchedNameHe = String(foundRow?.matched_name || "").trim();
  const matchedNameEn = String(foundRow?.matched_display_name_en || "").trim();

  const displayName = isEnglish
    ? (
        matchedNameEn ||
        matchedNameHe ||
        req?.outputName ||
        reqName ||
        "this product"
      ).trim()
    : (matchedNameHe || reqName || "המוצר הזה").trim();

  const unitPrice = Number(foundRow?.price);
  const hasPrice = Number.isFinite(unitPrice);

  const soldByWeight = req?.sold_by_weight === true;
  const amountRaw = Number(foundRow?.requested_amount ?? req?.amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  const unitsRaw = foundRow?.requested_units ?? req?.units ?? null;
  const unitsNum = Number(unitsRaw);
  const units =
    soldByWeight && Number.isFinite(unitsNum) && unitsNum > 0 ? unitsNum : null;

  if (isEnglish) {
    if (!hasPrice)
      return `The product we found is: ${displayName}. We don’t have an updated price for it.`;

    if (!soldByWeight) {
      if (amount <= 1)
        return `The product we found is: ${displayName}. Price: ${formatILS(
          unitPrice
        )}.`;
      const total = unitPrice * amount;
      return `The product we found is: ${displayName}. Price: ${formatILS(
        unitPrice
      )} each (total ${formatILS(total)} for ${amount}).`;
    }

    const base = `The product we found is: ${displayName}. Price: ${formatILS(
      unitPrice
    )} per kg.`;
    if (!units && Math.abs(amount - 1) < 1e-9) return base;

    const total = unitPrice * amount;
    return `${base} Estimated total for ~${amount} kg${
      units ? ` (about ${units} units)` : ""
    }: ${formatILS(total)}.`;
  }

  if (!hasPrice)
    return `המוצר שמצאנו אצלנו הוא: ${displayName}. אין לנו מחיר מעודכן עליו כרגע.`;

  if (!soldByWeight) {
    if (amount <= 1)
      return `המוצר שמצאנו אצלנו הוא: ${displayName}. מחירו: ${formatILS(
        unitPrice
      )}.`;
    const total = unitPrice * amount;
    return `המוצר שמצאנו אצלנו הוא: ${displayName}. מחיר ליח׳: ${formatILS(
      unitPrice
    )} (סה״כ ${formatILS(total)} ל-${amount} יח׳).`;
  }

  const base = `המוצר שמצאנו אצלנו הוא: ${displayName}. מחירו: ${formatILS(
    unitPrice
  )} לק״ג.`;
  if (!units && Math.abs(amount - 1) < 1e-9) return base;

  const total = unitPrice * amount;
  return `${base} מחיר משוערך ל~${amount} ק״ג${
    units ? ` (בערך ${units} יח׳)` : ""
  }: ${formatILS(total)}.`;
}

async function saveFallbackOpenQuestion(botPayload, customer_id, shop_id) {
  await saveOpenQuestions({
    customer_id,
    shop_id,
    order_id: null,
    questions: [
      {
        name: null,
        question: botPayload,
        options: null,
      },
    ],
  });
}

function getCompareDisplayName({ req, foundRow, isEnglish }) {
  const he = String(foundRow?.matched_name || req?.name || "").trim();
  const en = String(
    foundRow?.matched_display_name_en || req?.outputName || ""
  ).trim();

  if (isEnglish) return en || he || "this product";
  return he || en || "המוצר הזה";
}

function calcCompareTotal({ req, unitPrice }) {
  const p = Number(unitPrice);
  if (!Number.isFinite(p)) return null;

  const amountRaw = Number(req?.amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  // total is always unitPrice * amount (unit-based: units count, weight-based: kg)
  return p * amount;
}

function formatCompareFoundLine({ req, foundRow, isEnglish }) {
  const name = getCompareDisplayName({ req, foundRow, isEnglish });

  const unitPrice = Number(foundRow?.price);
  const hasPrice = Number.isFinite(unitPrice);

  const outOfStock = isOutOfStockFromFound(foundRow);
  const oosSuffix = outOfStock
    ? isEnglish
      ? " (out of stock)"
      : " (חסר במלאי)"
    : "";

  if (!hasPrice) {
    return isEnglish
      ? `${name} - no updated price${oosSuffix}`
      : `${name} - אין מחיר מעודכן${oosSuffix}`;
  }

  const soldByWeight = req?.sold_by_weight === true;

  const amountRaw = Number(req?.amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  const unitsRaw = foundRow?.requested_units ?? req?.units ?? null;
  const unitsNum = Number(unitsRaw);
  const units = Number.isFinite(unitsNum) && unitsNum > 0 ? unitsNum : null;

  // unit-based
  if (!soldByWeight) {
    if (Math.abs(amount - 1) < 1e-9) {
      return `${name} - ${formatILS(unitPrice)}${oosSuffix}`;
    }
    const total = unitPrice * amount;
    return isEnglish
      ? `${name} - ${formatILS(unitPrice)} each (total ${formatILS(
          total
        )} for ${amount})${oosSuffix}`
      : `${name} - ${formatILS(unitPrice)} ליח׳ (סה״כ ${formatILS(
          total
        )} ל-${amount} יח׳)${oosSuffix}`;
  }

  // weight-based (₪/kg)
  if (Math.abs(amount - 1) < 1e-9 && !units) {
    return isEnglish
      ? `${name} - ${formatILS(unitPrice)} per kg${oosSuffix}`
      : `${name} - ${formatILS(unitPrice)} לק״ג${oosSuffix}`;
  }

  const total = unitPrice * amount;
  const kgTxt = fmtQty(amount);

  if (units) {
    return isEnglish
      ? `${name} - ${formatILS(unitPrice)} per kg (est. total for ${units} ${
          units === 1 ? "unit" : "units"
        }, ~${kgTxt} kg: ${formatILS(total)})${oosSuffix}`
      : `${name} - ${formatILS(
          unitPrice
        )} לק״ג (מחיר משוערך ל-${units} יח׳, כ~${kgTxt} ק״ג: ${formatILS(
          total
        )})${oosSuffix}`;
  }

  return isEnglish
    ? `${name} - ${formatILS(
        unitPrice
      )} per kg (est. total for ~${kgTxt} kg: ${formatILS(total)})${oosSuffix}`
    : `${name} - ${formatILS(
        unitPrice
      )} לק״ג (מחיר משוערך ל~${kgTxt} ק״ג: ${formatILS(total)})${oosSuffix}`;
}

function buildCompareResultLine({ best, second, isEnglish }) {
  const eps = 0.01;
  const diff = Number(second.total) - Number(best.total);
  if (!Number.isFinite(diff)) return "";

  const bestLabel =
    best.showQtyInSummary && best.qtyText
      ? `${best.name} (${best.qtyText})`
      : best.name;

  const secondLabel =
    second.showQtyInSummary && second.qtyText
      ? `${second.name} (${second.qtyText})`
      : second.name;

  if (Math.abs(diff) <= eps) {
    return isEnglish
      ? `No price difference for ${bestLabel} vs ${secondLabel} (both about ${formatILS(
          best.total
        )}).`
      : `אין הבדל במחיר עבור ${bestLabel} מול ${secondLabel} (שניהם בערך ${formatILS(
          best.total
        )}).`;
  }

  const cheaperNote = best.outOfStock
    ? isEnglish
      ? " (but it’s currently out of stock)"
      : " (אבל כרגע חסר במלאי)"
    : "";

  return isEnglish
    ? `${bestLabel}${cheaperNote} is cheaper than ${secondLabel} by ${formatILS(
        diff
      )}.`
    : `${bestLabel}${cheaperNote} זול יותר מ${secondLabel} ב${formatILS(
        diff
      )}.`;
}

function buildNeedSecondCompareQuestion({ firstName, isEnglish }) {
  return {
    name: null,
    question: isEnglish
      ? `I can compare prices, but I only understood one item (${firstName}). What do you want to compare it with?`
      : `אני יכול להשוות מחירים, אבל הבנתי רק מוצר אחד (${firstName}). מול מה להשוות אותו?`,
    options: [],
  };
}

function buildNeedQtyForMixedUnitsQuestion({ names, isEnglish }) {
  const joined = names.join(isEnglish ? " vs " : " מול ");
  return {
    name: null,
    question: isEnglish
      ? `To compare fairly (${joined}), please specify quantities (units or kg) for each item.`
      : `כדי להשוות בצורה הוגנת (${joined}), כתוב בבקשה כמות לכל אחד (יח׳ או ק״ג).`,
    options: [],
  };
}

async function answerPriceCompareFlow({
  shop_id,
  customer_id,
  isEnglish,
  compareReqs,
  baseQuestions,
}) {
  let reqs = Array.isArray(compareReqs) ? compareReqs : [];
  if (
    reqs.length >= 2 &&
    reqs.every((r) => !String(r?.compare_group || "").trim())
  ) {
    reqs = reqs.map((r) => ({ ...r, compare_group: "1" }));
  }

  const searchRequests = reqs.map((p) => {
    const n = Number(p?.amount);
    const amount = Number.isFinite(n) && n > 0 ? n : 1;
    return { ...p, amount };
  });

  const res = await searchProducts(shop_id, searchRequests);
  const found = res?.found || [];
  const notFound = res?.notFound || [];

  const foundByIndex = new Map();
  for (const f of found) foundByIndex.set(f.originalIndex, f);

  const usedIds = new Set();
  for (const f of found) {
    const pid = Number(f?.product_id);
    const id = Number(f?.id);

    if (Number.isFinite(pid)) usedIds.add(pid);
    if (Number.isFinite(id)) usedIds.add(id);
  }

  // group by compare_group
  const groups = new Map(); // groupKey -> [indexes]
  for (let i = 0; i < searchRequests.length; i++) {
    const g =
      String(searchRequests[i]?.compare_group || "").trim() || `__AUTO_${i}`;
    if (!groups.has(g)) groups.set(g, []);
    groups.get(g).push(i);
  }

  const blocks = [];
  const questionsToSave = [...(baseQuestions || [])];

  for (const [groupKey, idxs] of groups.entries()) {
    const groupLines = [];
    let groupHasAltOrClarify = false;
    const candidates = []; // { name, total, outOfStock, soldByWeight, amount }
    const missingNames = [];

    const sortedIdxs = [...idxs].sort(
      (a, b) =>
        Number(Boolean(foundByIndex.get(b))) -
        Number(Boolean(foundByIndex.get(a)))
    );

    for (const i of sortedIdxs) {
      const req = searchRequests[i] || {};
      const f = foundByIndex.get(i) || null;

      if (!f) {
        // not found -> try alternatives (if cat/sub exists)
        const nf = notFound.find((x) => x.originalIndex === i) || null;

        const category =
          String(nf?.category || req?.category || "").trim() || null;
        const sub_category =
          String(
            nf?.sub_category || req?.["sub-category"] || req?.sub_category || ""
          ).trim() || null;

        const excludeTokens =
          Array.isArray(nf?.exclude_tokens) && nf.exclude_tokens.length
            ? nf.exclude_tokens
            : Array.isArray(req.exclude_tokens)
            ? req.exclude_tokens
            : [];

        if (category || sub_category) {
          const { blockText, questionObj, altIds } =
            await buildAltBlockAndQuestion({
              shop_id,
              reason: "NOT_FOUND",
              req,
              foundRow: null,
              category,
              sub_category,
              excludeTokens,
              usedIds,
              isEnglish,
            });

          if (blockText) groupLines.push(blockText);
          if (questionObj) questionsToSave.push(questionObj);

          groupHasAltOrClarify = true;

          for (const id of altIds) usedIds.add(id);
        } else {
          const reqName = String(req?.name || "").trim() || null;
          const q = {
            name: reqName,
            question: isEnglish
              ? `I couldn’t find "${
                  reqName || "this product"
                }". Can you write it differently?`
              : `לא מצאתי "${
                  reqName || "המוצר"
                }". תוכל לכתוב את זה בצורה אחרת?`,
            options: [],
          };
          groupLines.push(q.question);
          questionsToSave.push(q);
          groupHasAltOrClarify = true;
        }

        missingNames.push(String(req?.name || "").trim() || null);
        continue;
      }

      // found: add bullet line
      groupLines.push(
        `• ${formatCompareFoundLine({ req, foundRow: f, isEnglish })}`
      );

      const unitPrice = Number(f.price);
      const total = calcCompareTotal({ req, unitPrice });
      if (total === null) continue;

      const name = getCompareDisplayName({ req, foundRow: f, isEnglish });
      const outOfStock = isOutOfStockFromFound(f);
      const qtyText = buildCompareQtyText({ req, foundRow: f, isEnglish });

      const amountNum = Number(req?.amount ?? 1);
      const isDefaultUnitQty =
        req?.sold_by_weight !== true &&
        Number.isFinite(amountNum) &&
        Math.abs(amountNum - 1) < 1e-9;

      candidates.push({
        name,
        total,
        outOfStock,
        soldByWeight: req?.sold_by_weight === true,
        amount: amountNum,
        qtyText,
        showQtyInSummary: !isDefaultUnitQty,
      });
    }

    // If we have <2 candidates with price -> ask what to compare against
    if (candidates.length < 2) {
      if (!groupHasAltOrClarify) {
        const firstIdx = idxs[0];
        const req0 = searchRequests[firstIdx] || {};
        const f0 = foundByIndex.get(firstIdx) || null;

        const firstName =
          candidates[0]?.name ||
          getCompareDisplayName({ req: req0, foundRow: f0, isEnglish }) ||
          (isEnglish ? "this item" : "המוצר הזה");

        const q = buildNeedSecondCompareQuestion({ firstName, isEnglish });
        groupLines.push(q.question);
        questionsToSave.push(q);
      }

      blocks.push(groupLines.filter(Boolean).join("\n"));
      continue;
    }

    // Mixed units (per kg vs per unit) with default amounts -> ask for quantities
    const hasWeight = candidates.some((c) => c.soldByWeight === true);
    const hasUnit = candidates.some((c) => c.soldByWeight !== true);

    const allDefaultAmounts = candidates.every((c) => {
      const a = Number(c.amount);
      return Number.isFinite(a) && Math.abs(a - 1) < 1e-9;
    });

    if (hasWeight && hasUnit && allDefaultAmounts) {
      const q = buildNeedQtyForMixedUnitsQuestion({
        names: candidates.map((c) => c.name).slice(0, 3),
        isEnglish,
      });
      groupLines.push(q.question);
      questionsToSave.push(q);

      blocks.push(groupLines.filter(Boolean).join("\n"));
      continue;
    }

    // compute result
    const sorted = [...candidates].sort((a, b) => a.total - b.total);
    const best = sorted[0];
    const second = sorted[1];

    groupLines.push(buildCompareResultLine({ best, second, isEnglish }));

    blocks.push(groupLines.filter(Boolean).join("\n"));
  }

  const allQuestions = normalizeIncomingQuestions(questionsToSave, {
    preserveOptions: true,
  });

  if (allQuestions.length) {
    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id: null,
      questions: allQuestions,
    });
  }

  const body = blocks.filter(Boolean).join("\n\n");
  const tail = buildQuestionsTextSmart({ questions: baseQuestions, isEnglish });

  const finalMsg = [body, tail].filter((x) => x && x.trim()).join("\n\n");

  return finalMsg && finalMsg.trim()
    ? finalMsg
    : isEnglish
    ? "I couldn’t compare those prices. Can you rephrase?"
    : "לא הצלחתי להשוות את המחירים. תוכל לנסח שוב?";
}

function calcBudgetTotal({ req, unitPrice }) {
  const p = Number(unitPrice);
  if (!Number.isFinite(p)) return null;

  const amountRaw = Number(req?.amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  // For unit items: amount = units count
  // For weight items: amount = kg
  return p * amount;
}

function formatBudgetPickLine({ row, req, isEnglish }) {
  const name = isEnglish
    ? String(row?.display_name_en || row?.name || "").trim()
    : String(row?.name || row?.display_name_en || "").trim();
  if (!name) return null;

  const unitPrice = Number(row?.price);
  if (!Number.isFinite(unitPrice)) return name;

  const soldByWeight = req?.sold_by_weight === true;
  const total = calcBudgetTotal({ req, unitPrice });

  const outOfStock = !isInStockRow(row);
  const oosSuffix = outOfStock
    ? isEnglish
      ? " (out of stock)"
      : " (חסר במלאי)"
    : "";

  const amountRaw = Number(req?.amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  if (!soldByWeight) {
    if (amount <= 1) {
      return `${name} - ${formatILS(unitPrice)}${oosSuffix}`;
    }

    return isEnglish
      ? `${name} - ${formatILS(unitPrice)} each (total ${formatILS(
          total
        )} for ${amount})${oosSuffix}`
      : `${name} - ${formatILS(unitPrice)} ליח׳ (סה״כ ${formatILS(
          total
        )} ל-${amount} יח׳)${oosSuffix}`;
  }

  // weight-based price (₪/kg)
  if (Math.abs(amount - 1) < 1e-9) {
    return isEnglish
      ? `${name} - ${formatILS(unitPrice)} per kg${oosSuffix}`
      : `${name} - ${formatILS(unitPrice)} לק״ג${oosSuffix}`;
  }

  return isEnglish
    ? `${name} - ${formatILS(unitPrice)} per kg (est. ${formatILS(
        total
      )} for ~${amount} kg)${oosSuffix}`
    : `${name} - ${formatILS(unitPrice)} לק״ג (מחיר משוערך ${formatILS(
        total
      )} ל~${amount} ק״ג)${oosSuffix}`;
}

async function queryBudgetPickRows({
  shop_id,
  category,
  sub_category,
  reqTokens,
  excludeTokens,
  usedIds,
  amount,
  budget,
  limit = 6,
  applyTokenFilter = true,
}) {
  const params = [shop_id];

  let sql = `
    SELECT id, name, display_name_en, price, stock_amount, category, sub_category
      FROM product
     WHERE shop_id = ?
       AND price IS NOT NULL
       AND price >= 0
  `;

  if (category) {
    sql += ` AND category = ?`;
    params.push(category);
  }

  if (sub_category) {
    sql += ` AND sub_category = ?`;
    params.push(sub_category);
  }

  const exIds = Array.isArray(usedIds)
    ? usedIds
    : usedIds && typeof usedIds[Symbol.iterator] === "function"
    ? Array.from(usedIds)
    : [];

  if (exIds.length) {
    sql += ` AND id NOT IN (${exIds.map(() => "?").join(",")})`;
    params.push(...exIds);
  }

  // budget is for total of requested quantity (price * amount)
  sql += ` AND (price * ?) <= ?`;
  params.push(amount, budget);

  // exclude tokens
  if (Array.isArray(excludeTokens) && excludeTokens.length) {
    for (const t of excludeTokens) {
      if (!t) continue;
      sql += `
        AND (
          name COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
          AND display_name_en COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
        )
      `;
      params.push(t, t);
    }
  }

  // token filter
  if (applyTokenFilter && Array.isArray(reqTokens) && reqTokens.length) {
    for (const t of reqTokens) {
      if (!t) continue;
      sql += `
        AND (
          name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
          OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
        )
      `;
      params.push(t, t);
    }
  }

  const lim = Math.max(1, Math.min(20, Number(limit) || 6));

  sql += `
    ORDER BY
      CASE WHEN stock_amount IS NOT NULL AND stock_amount <= 0 THEN 1 ELSE 0 END ASC,
      (price * ?) ASC,
      price ASC,
      id ASC
    LIMIT ${lim}
  `;
  params.push(amount);

  const [rows] = await db.query(sql, params);
  return Array.isArray(rows) ? rows : [];
}

async function pickBudgetCandidates({
  shop_id,
  req,
  category,
  sub_category,
  budget,
  usedIds,
  limit,
}) {
  const reqName = nullify(req?.name) || nullify(req?.outputName) || "";
  const reqTokens = tokenizeName(reqName);

  const amountRaw = Number(req?.amount ?? 1);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  const excludeTokens = getExcludeTokensFromReq(req);

  const hasCat = Boolean(String(category || "").trim());
  const hasSub = Boolean(String(sub_category || "").trim());
  const tries = [];

  if (hasSub) {
    tries.push({
      category: hasCat ? category : null,
      sub_category,
      applyTokenFilter: true,
    });
    if (reqTokens.length) {
      tries.push({
        category: hasCat ? category : null,
        sub_category,
        applyTokenFilter: false,
      });
    }
  } else if (hasCat) {
    tries.push({ category, sub_category: null, applyTokenFilter: true });
    if (reqTokens.length) {
      tries.push({ category, sub_category: null, applyTokenFilter: false });
    }
  } else if (reqTokens.length) {
    tries.push({ category: null, sub_category: null, applyTokenFilter: true });
  }

  for (const t of tries) {
    const rows = await queryBudgetPickRows({
      shop_id,
      category: t.category,
      sub_category: t.sub_category,
      reqTokens,
      excludeTokens,
      usedIds,
      amount,
      budget,
      limit,
      applyTokenFilter: t.applyTokenFilter,
    });

    if (!rows.length) continue;

    const tokSet = new Set(
      reqTokens.map((x) => normalizeToken(x).toLowerCase()).filter(Boolean)
    );

    const scored = rows.map((r) => {
      const name = normalizeToken(r?.name).toLowerCase();
      const en = normalizeToken(r?.display_name_en).toLowerCase();
      let hit = 0;
      for (const tok of tokSet) {
        if (tok && (name.includes(tok) || en.includes(tok))) hit += 1;
      }
      const score = tokSet.size ? hit / tokSet.size : 0;

      const total = calcBudgetTotal({ req, unitPrice: Number(r?.price) });
      const totalScore = Number.isFinite(total)
        ? total
        : Number.POSITIVE_INFINITY;

      return { r, score, totalScore };
    });

    scored.sort(
      (a, b) =>
        b.score - a.score ||
        a.totalScore - b.totalScore ||
        Number(a.r.id) - Number(b.r.id)
    );

    return scored.slice(0, limit).map((x) => x.r);
  }

  return [];
}

function buildNoBudgetMatchesText({ subject, budget, isEnglish }) {
  const b = formatILS(budget);
  const s = String(subject || "").trim();
  return isEnglish
    ? `I couldn't find options for ${
        s || "that"
      } up to ${b}. You can raise the budget or describe the product differently.`
    : `לא מצאתי אפשרויות עבור ${
        s || "זה"
      } עד ${b}. אפשר להגדיל תקציב או לתאר את המוצר אחרת.`;
}

async function answerBudgetPickFlow({
  shop_id,
  customer_id,
  isEnglish,
  budgetReqs,
  baseQuestions,
}) {
  const reqs = Array.isArray(budgetReqs) ? budgetReqs : [];

  const blocks = [];
  const usedIds = new Set();

  for (let i = 0; i < reqs.length; i++) {
    const req = reqs[i] || {};

    const budgetRaw = Number(req?.budget_ils);
    const budget =
      Number.isFinite(budgetRaw) && budgetRaw > 0 ? budgetRaw : null;

    const category = String(req?.category || "").trim() || null;
    const sub_category =
      String(req?.["sub-category"] || req?.sub_category || "").trim() || null;

    const subject = buildBudgetPickSubject(req, isEnglish);

    if (!budget) continue;

    const hasMeaningfulType = Boolean(
      nullify(req?.name) || nullify(req?.outputName) || sub_category || category
    );

    if (!hasMeaningfulType) continue;

    const candidates = await pickBudgetCandidates({
      shop_id,
      req,
      category,
      sub_category,
      budget,
      usedIds: Array.from(usedIds),
      limit: 6,
    });

    if (!candidates.length) {
      blocks.push(buildNoBudgetMatchesText({ subject, budget, isEnglish }));
      continue;
    }

    for (const r of candidates) {
      const id = Number(r?.id);
      if (Number.isFinite(id)) usedIds.add(id);
    }

    const qtyText = buildCompareQtyText({ req, foundRow: null, isEnglish });

    const header = isEnglish
      ? `Options up to ${formatILS(budget)}${subject ? ` for ${subject}` : ""}${
          qtyText > 1 ? ` (${qtyText})` : ""
        }:`
      : `אפשרויות עד ${formatILS(budget)}${subject ? ` עבור ${subject}` : ""}${
          qtyText > 1 ? ` (${qtyText})` : ""
        }:`;

    const lines = candidates
      .map((r) => formatBudgetPickLine({ row: r, req, isEnglish }))
      .filter(Boolean)
      .map((x) => `• ${x}`);

    blocks.push([header, ...lines].join("\n"));
  }

  const modelQuestions = normalizeIncomingQuestions(baseQuestions || [], {
    preserveOptions: true,
  });

  if (modelQuestions.length) {
    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id: null,
      questions: modelQuestions,
    });
  }

  const body = blocks.filter(Boolean).join("\n\n");
  const tail = buildQuestionsTextSmart({ questions: baseQuestions, isEnglish });

  const finalMsg = [body, tail].filter((x) => x && x.trim()).join("\n\n");

  if (!finalMsg.trim()) {
    return isEnglish ? "What budget do you have?" : "מה התקציב?";
  }

  return finalMsg;
}

function round2(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return null;
  return Math.round(x * 100) / 100;
}

function pct(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return null;
  return round2(x);
}

function fmtDateTime(dt, isEnglish) {
  if (!dt) return null;
  const d = dt instanceof Date ? dt : new Date(dt);
  if (Number.isNaN(d.getTime())) return null;

  // Israel time
  const locale = isEnglish ? "en-GB" : "he-IL";
  const tz = "Asia/Jerusalem";

  const s = new Intl.DateTimeFormat(locale, {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(d);

  return s.replace(",", "");
}

async function searchPromotionsForRequest(shop_id, req, { limit = 50 } = {}) {
  const category = nullify(req?.category);
  const subCategory = nullify(req?.["sub-category"] || req?.sub_category);

  const nameRaw = nullify(req?.name || req?.outputName) || "";

  const STOP = new Set(["יח", "יחידה", "יחידות", "unit", "units"]);
  const rawTokens = tokenizeName(nameRaw).filter(Boolean);

  const tokenGroups = [];
  for (const rawTok of rawTokens) {
    let vars = tokenVariants(rawTok)
      .map((v) =>
        String(v || "")
          .replace(/\s+/g, " ")
          .trim()
      )
      .filter(Boolean);

    vars = vars.filter((v) => !STOP.has(normalizeToken(v)));

    const seen = new Set();
    const uniq = [];
    for (const v of vars) {
      const key = v.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      uniq.push(v);
    }

    if (uniq.length) tokenGroups.push(uniq.slice(0, 10));
  }

  const excludeTokens = getExcludeTokensFromReq(req);

  if (!category && !subCategory && tokenGroups.length === 0) {
    return { rows: [], total: 0 };
  }

  const subList = subCategory ? getSubCategoryCandidates(subCategory) : [];

  const params = [shop_id];
  let sql = `
    SELECT
      pr.id            AS promo_id,
      pr.kind          AS kind,
      pr.percent_off   AS percent_off,
      pr.amount_off    AS amount_off,
      pr.fixed_price   AS fixed_price,
      pr.bundle_buy_qty AS bundle_buy_qty,
      pr.bundle_pay_price AS bundle_pay_price,
      pr.description   AS description,
      pr.start_at      AS start_at,
      pr.end_at        AS end_at,
      pr.product_id    AS product_id,

      p.id             AS product_id2,
      p.name           AS name,
      p.display_name_en AS display_name_en,
      p.price          AS price,
      p.stock_amount   AS stock_amount,
      p.category       AS category,
      p.sub_category   AS sub_category
    FROM promotion pr
    JOIN product p
      ON p.id = pr.product_id
     AND p.shop_id = pr.shop_id
    WHERE pr.shop_id = ?
  `;

  if (category) {
    sql += ` AND p.category = ?`;
    params.push(category);
  }

  if (subList.length) {
    sql += ` AND p.sub_category IN (${subList.map(() => "?").join(",")})`;
    params.push(...subList);
  }

  for (const group of tokenGroups) {
    const ors = group
      .map(
        () => `
        (
          p.name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
          OR p.display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
        )
      `
      )
      .join(" OR ");

    sql += ` AND ( ${ors} )`;

    for (const v of group) {
      params.push(v, v);
    }
  }

  for (const t of excludeTokens) {
    const tt = String(t || "").trim();
    if (!tt) continue;
    sql += `
      AND (
        p.name COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
        AND p.display_name_en COLLATE utf8mb4_general_ci NOT LIKE CONCAT('%', ?, '%')
      )
    `;
    params.push(tt, tt);
  }

  sql += `
    AND (
      (
        (pr.start_at IS NULL OR pr.start_at <= NOW())
        AND (pr.end_at IS NULL OR pr.end_at >= NOW())
      )
      OR (pr.start_at > NOW())
      OR (
        pr.end_at IS NOT NULL
        AND pr.end_at < NOW()
        AND pr.end_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
      )
    )
  `;

  sql += `
    ORDER BY
      CASE
        WHEN ( (pr.start_at IS NULL OR pr.start_at <= NOW()) AND (pr.end_at IS NULL OR pr.end_at >= NOW()) ) THEN 0
        WHEN (pr.start_at > NOW()) THEN 1
        ELSE 2
      END,
      pr.start_at DESC,
      pr.end_at DESC,
      pr.id DESC
    LIMIT ?
  `;

  params.push(Math.min(Math.max(Number(limit) || 50, 1), 200));

  const [rows] = await db.query(sql, params);
  return { rows: rows || [], total: (rows || []).length };
}

function promotionStatus(promo, now = new Date()) {
  const start = promo?.start_at ? new Date(promo.start_at) : null;
  const end = promo?.end_at ? new Date(promo.end_at) : null;

  if (start && now < start) return "UPCOMING";
  if (!end) return "ACTIVE"; // no end -> active once started
  if (now <= end) return "ACTIVE";
  return "ENDED";
}

function calcPromotionPricing({
  promo,
  baseUnitPrice,
  reqAmount,
  soldByWeight,
}) {
  const base = Number(baseUnitPrice);
  const amountRaw = Number(reqAmount);
  const amount = Number.isFinite(amountRaw) && amountRaw > 0 ? amountRaw : 1;

  if (!Number.isFinite(base)) {
    return {
      baseUnit: null,
      newUnit: null,
      baseTotal: null,
      newTotal: null,
      savingsAmount: null,
      savingsPercent: null,
      amount,
    };
  }

  const kind = String(promo?.kind || "").toUpperCase();

  const baseTotal = base * amount;

  let newUnit = null;
  let newTotal = null;

  if (kind === "PERCENT_OFF") {
    const p = Number(promo?.percent_off);
    if (Number.isFinite(p)) {
      newUnit = base * (1 - p / 100);
      newTotal = newUnit * amount;
    }
  } else if (kind === "AMOUNT_OFF") {
    const off = Number(promo?.amount_off);
    if (Number.isFinite(off)) {
      newUnit = Math.max(0, base - off);
      newTotal = newUnit * amount;
    }
  } else if (kind === "FIXED_PRICE") {
    const fp = Number(promo?.fixed_price);
    if (Number.isFinite(fp)) {
      newUnit = Math.max(0, fp);
      newTotal = newUnit * amount;
    }
  } else if (kind === "BUNDLE") {
    // Usually unit-based. If weight-based, we’ll still display terms but calculations may be confusing.
    const buyQty = Number(promo?.bundle_buy_qty);
    const payPrice = Number(promo?.bundle_pay_price);

    if (
      Number.isFinite(buyQty) &&
      buyQty >= 2 &&
      Number.isFinite(payPrice) &&
      payPrice >= 0
    ) {
      // effective unit for info
      newUnit = payPrice / buyQty;

      if (!soldByWeight) {
        // if user asked N units, apply bundle for floor(N/buyQty)
        const N = Math.max(1, Math.floor(amount));
        const bundles = Math.floor(N / buyQty);
        const remainder = N - bundles * buyQty;

        newTotal = bundles * payPrice + remainder * base;
        // override baseTotal for integer units
        return finalizePromotionPricing({
          baseUnit: base,
          newUnit,
          baseTotal: base * N,
          newTotal,
          amount: N,
        });
      }

      // weight-based: keep per-unit effective only, totals stay null
      return finalizePromotionPricing({
        baseUnit: base,
        newUnit,
        baseTotal,
        newTotal: null,
        amount,
      });
    }
  }

  return finalizePromotionPricing({
    baseUnit: base,
    newUnit,
    baseTotal,
    newTotal,
    amount,
  });
}

function finalizePromotionPricing({
  baseUnit,
  newUnit,
  baseTotal,
  newTotal,
  amount,
}) {
  const bU = Number(baseUnit);
  const nU = Number(newUnit);
  const bT = Number(baseTotal);
  const nT = Number(newTotal);

  const hasBase = Number.isFinite(bU);
  const hasNew = Number.isFinite(nU);

  const savingsAmount =
    Number.isFinite(bT) && Number.isFinite(nT) ? Math.max(0, bT - nT) : null;

  const savingsPercent =
    Number.isFinite(bT) && Number.isFinite(nT) && bT > 0
      ? Math.max(0, (1 - nT / bT) * 100)
      : hasBase && hasNew && bU > 0
      ? Math.max(0, (1 - nU / bU) * 100)
      : null;

  return {
    amount,
    baseUnit: hasBase ? bU : null,
    newUnit: hasNew ? nU : null,
    baseTotal: Number.isFinite(bT) ? bT : null,
    newTotal: Number.isFinite(nT) ? nT : null,
    savingsAmount: Number.isFinite(savingsAmount) ? savingsAmount : null,
    savingsPercent: Number.isFinite(savingsPercent) ? savingsPercent : null,
  };
}

function humanKindLine(promo, isEnglish) {
  const kind = String(promo?.kind || "").toUpperCase();

  if (kind === "PERCENT_OFF") {
    const p = pct(promo?.percent_off);
    return isEnglish ? `${p}% off` : `${p}% הנחה`;
  }
  if (kind === "AMOUNT_OFF") {
    const a = round2(promo?.amount_off);
    return isEnglish ? `${formatILS(a)} off` : `${formatILS(a)} הנחה`;
  }
  if (kind === "FIXED_PRICE") {
    const fp = round2(promo?.fixed_price);
    return isEnglish
      ? `Fixed price: ${formatILS(fp)}`
      : `מחיר קבוע: ${formatILS(fp)}`;
  }
  if (kind === "BUNDLE") {
    const buyQty = Number(promo?.bundle_buy_qty);
    const pay = round2(promo?.bundle_pay_price);
    if (Number.isFinite(buyQty) && buyQty >= 2 && Number.isFinite(pay)) {
      return isEnglish
        ? `Buy ${buyQty} for ${formatILS(pay)}`
        : `${buyQty} יח׳ ב-${formatILS(pay)}`;
    }
    return isEnglish ? "Bundle deal" : "מבצע חבילה";
  }

  return isEnglish ? "Promotion" : "מבצע";
}

function buildPromotionBlock({ req, foundRow, promo, isEnglish }) {
  const name = getCompareDisplayName({ req, foundRow, isEnglish });
  const outOfStock = isOutOfStockFromFound(foundRow);

  const soldByWeight = req?.sold_by_weight === true;

  const unitPrice = Number(foundRow?.price);
  const hasPrice = Number.isFinite(unitPrice);

  const basePriceTxt = !hasPrice
    ? isEnglish
      ? "no updated price"
      : "אין מחיר מעודכן"
    : soldByWeight
    ? isEnglish
      ? `${formatILS(unitPrice)} per kg`
      : `${formatILS(unitPrice)} לק״ג`
    : `${formatILS(unitPrice)}`;

  // ---- NO PROMO ----
  if (!promo) {
    if (isEnglish) {
      return `No promotion right now for ${name}. Price: ${basePriceTxt}${
        outOfStock ? " (currently out of stock)" : ""
      }.`;
    }

    return `אין לנו מבצע כרגע על ${name}, המחיר שלו הוא ${basePriceTxt}${
      outOfStock ? " (כרגע חסר במלאי)" : ""
    }.`;
  }

  const kind = String(promo?.kind || "").toUpperCase();
  const now = new Date();
  const status = promotionStatus(promo, now);

  const startTxt = fmtDateTime(promo.start_at, isEnglish);
  const endTxt = promo.end_at ? fmtDateTime(promo.end_at, isEnglish) : null;

  const statusLine = (() => {
    if (isEnglish) {
      if (status === "UPCOMING")
        return `Status: not started yet (starts at ${startTxt}${
          endTxt ? `, ends at ${endTxt}` : ""
        })`;
      if (status === "ACTIVE")
        return `Status: active now (since ${startTxt}${
          endTxt ? `, until ${endTxt}` : ", no end date"
        })`;
      return `Status: ended (was valid until ${endTxt || startTxt})`;
    }

    if (status === "UPCOMING")
      return `סטטוס: עדיין לא התחיל (מתחיל ב-${startTxt}${
        endTxt ? `, עד ${endTxt}` : ""
      })`;
    if (status === "ACTIVE")
      return `סטטוס: פעיל עכשיו (מ-${startTxt}${
        endTxt ? `, עד ${endTxt}` : ", ללא תאריך סיום"
      })`;
    return `סטטוס: הסתיים (היה בתוקף עד ${endTxt || startTxt})`;
  })();

  if (kind === "BUNDLE") {
    const buyQty = Number(promo?.bundle_buy_qty);
    const pay = round2(promo?.bundle_pay_price);

    const hasDeal =
      Number.isFinite(buyQty) &&
      buyQty >= 2 &&
      Number.isFinite(pay) &&
      pay >= 0;

    if (hasDeal) {
      const oosNote = outOfStock
        ? isEnglish
          ? " (currently out of stock)"
          : " (כרגע חסר במלאי)"
        : "";

      // If we don't have a base price, we can still show the deal terms
      if (!hasPrice) {
        const line1 = isEnglish
          ? `Promotion for ${name}: buy ${buyQty} for ${formatILS(
              pay
            )}. Price: no updated price.${oosNote}.`
          : `יש מבצע על ${name}: ${buyQty} יח׳ ב-${formatILS(
              pay
            )}. אין מחיר מעודכן להשוואה.${oosNote}.`;

        return `${line1}\n${statusLine}`;
      }

      const dealUnit = pay / buyQty; // effective unit price in the bundle
      const baseDealTotal = unitPrice * buyQty;
      const savingsAmount = Math.max(0, baseDealTotal - pay);
      const savingsPercent =
        baseDealTotal > 0 ? (savingsAmount / baseDealTotal) * 100 : null;

      const sPct = savingsPercent === null ? null : pct(savingsPercent);

      const line1 = isEnglish
        ? `Promotion for ${name}: buy ${buyQty} for ${formatILS(
            pay
          )} (${formatILS(dealUnit)} each instead of ${formatILS(
            unitPrice
          )}), save ${formatILS(savingsAmount)}${
            sPct !== null ? ` (${sPct}%)` : ""
          }${oosNote}.`
        : `יש מבצע על ${name}: ${buyQty} יח׳ ב-${formatILS(pay)} (${formatILS(
            dealUnit
          )} ליח׳ במקום ${formatILS(unitPrice)}), חיסכון: ${formatILS(
            savingsAmount
          )}${sPct !== null ? ` (${sPct}%)` : ""}${oosNote}.`;

      const amountNum = Number(req?.amount ?? 1);
      const showReqTotals =
        Number.isFinite(amountNum) && Math.abs(amountNum - 1) > 1e-9;

      if (showReqTotals) {
        const pricing = calcPromotionPricing({
          promo,
          baseUnitPrice: unitPrice,
          reqAmount: amountNum,
          soldByWeight,
        });

        if (pricing?.baseTotal !== null && pricing?.newTotal !== null) {
          const totalsLine = isEnglish
            ? `Total for ${pricing.amount} units: ${formatILS(
                pricing.newTotal
              )} (instead of ${formatILS(pricing.baseTotal)})`
            : `סה״כ ל-${pricing.amount} יח׳: ${formatILS(
                pricing.newTotal
              )} (במקום ${formatILS(pricing.baseTotal)})`;

          return `${line1}\n${totalsLine}\n${statusLine}`;
        }
      }

      return `${line1}\n${statusLine}`;
    }
  }

  const promoShort = humanKindLine(promo, isEnglish);

  const pricing = calcPromotionPricing({
    promo,
    baseUnitPrice: unitPrice,
    reqAmount: req?.amount ?? 1,
    soldByWeight,
  });

  const newUnit = Number(pricing?.newUnit);
  const hasNewUnit = Number.isFinite(newUnit);

  const newPriceTxt = !hasNewUnit
    ? null
    : soldByWeight
    ? isEnglish
      ? `${formatILS(newUnit)} per kg`
      : `${formatILS(newUnit)} לק״ג`
    : `${formatILS(newUnit)}`;

  // savings (prefer per-unit when not bundle)
  const savingsPerUnit =
    kind !== "BUNDLE" && hasPrice && hasNewUnit
      ? Math.max(0, unitPrice - newUnit)
      : null;

  const savingsTxt = (() => {
    const sPct =
      pricing?.savingsPercent !== null && pricing?.savingsPercent !== undefined
        ? pct(pricing.savingsPercent)
        : null;

    if (savingsPerUnit !== null && Number.isFinite(savingsPerUnit)) {
      const amt = formatILS(savingsPerUnit);
      if (isEnglish) {
        return `Save: ${amt}${soldByWeight ? " per kg" : ""}`;
      }
      return `חיסכון: ${amt}${soldByWeight ? " לק״ג" : ""}`;
    }

    // fallback to total savings if we have totals (common for BUNDLE)
    if (
      pricing?.savingsAmount !== null &&
      pricing?.savingsAmount !== undefined
    ) {
      const amt = formatILS(pricing.savingsAmount);
      if (isEnglish) {
        return `Save: ${amt}${sPct !== null ? ` (${sPct}%)` : ""}`;
      }
      return `חיסכון: ${amt}${sPct !== null ? ` (${sPct}%)` : ""}`;
    }

    return null;
  })();

  // totals for requested qty (keep short + inline)
  const amountNum = Number(pricing?.amount ?? req?.amount ?? 1);
  const canShowTotals =
    pricing?.baseTotal !== null &&
    pricing?.newTotal !== null &&
    Number.isFinite(amountNum) &&
    (soldByWeight ? Math.abs(amountNum - 1) > 1e-9 : amountNum !== 1);

  const totalsTxt = (() => {
    if (!canShowTotals) return null;

    const qtyLabel = soldByWeight
      ? isEnglish
        ? `~${fmtQty(amountNum)} kg`
        : `~${fmtQty(amountNum)} ק״ג`
      : isEnglish
      ? `${amountNum} units`
      : `${amountNum} יח׳`;

    if (isEnglish) {
      return `Total for ${qtyLabel}: ${formatILS(
        pricing.newTotal
      )} (instead of ${formatILS(pricing.baseTotal)})`;
    }

    return `סה״כ ל-${qtyLabel}: ${formatILS(
      pricing.newTotal
    )} (במקום ${formatILS(pricing.baseTotal)})`;
  })();

  // bundle sentence slightly different (doesn't force "new price במקום old" per unit)
  const pricePart = (() => {
    if (!hasPrice)
      return isEnglish ? "Price: no updated price." : "אין מחיר מעודכן.";

    if (kind === "BUNDLE") {
      const buyQty = Number(promo?.bundle_buy_qty);
      const pay = round2(promo?.bundle_pay_price);
      if (Number.isFinite(buyQty) && Number.isFinite(pay)) {
        return isEnglish
          ? `Deal: buy ${buyQty} for ${formatILS(pay)} (regular ${formatILS(
              unitPrice
            )} each)`
          : `מחיר: ${buyQty} יח׳ ב-${formatILS(pay)} (במקום ${formatILS(
              unitPrice
            )} ליח׳)`;
      }
      // fallback
      return isEnglish ? `Price: ${basePriceTxt}` : `מחיר: ${basePriceTxt}`;
    }

    if (newPriceTxt) {
      return isEnglish
        ? `Price: ${newPriceTxt} instead of ${basePriceTxt}`
        : `מחיר: ${newPriceTxt} במקום ${basePriceTxt}`;
    }

    // promo exists but we couldn't compute new price
    return isEnglish ? `Price: ${basePriceTxt}` : `מחיר: ${basePriceTxt}`;
  })();

  const oosNote = outOfStock
    ? isEnglish
      ? " (currently out of stock)"
      : " (כרגע חסר במלאי)"
    : "";

  // If we have both base price and computed new unit price (non-bundle), print one clean line.
  const canPrintShort =
    kind !== "BUNDLE" && hasPrice && hasNewUnit && newUnit !== null;

  if (canPrintShort) {
    const promoNote = (() => {
      if (kind === "FIXED_PRICE") return null;
      if (kind === "PERCENT_OFF") {
        const p = pct(promo?.percent_off);
        return p !== null ? (isEnglish ? `${p}% off` : `${p}% הנחה`) : null;
      }
      if (kind === "AMOUNT_OFF") {
        const a = round2(promo?.amount_off);
        return Number.isFinite(a)
          ? isEnglish
            ? `${formatILS(a)} off`
            : `${formatILS(a)} הנחה`
          : null;
      }
      return null;
    })();

    const noteTxt = promoNote ? ` (${promoNote})` : "";

    const newUnitLabel = `${formatILS(newUnit)}`;

    const oldUnitLabel = soldByWeight
      ? isEnglish
        ? `${formatILS(unitPrice)} per kg`
        : `${formatILS(unitPrice)} לק״ג`
      : `${formatILS(unitPrice)}`;

    const saveLabel =
      savingsPerUnit !== null && Number.isFinite(savingsPerUnit)
        ? soldByWeight
          ? isEnglish
            ? `${formatILS(savingsPerUnit)} per kg`
            : `${formatILS(savingsPerUnit)} לק״ג`
          : `${formatILS(savingsPerUnit)}`
        : null;

    const lineShort = isEnglish
      ? `Promotion for ${name}${noteTxt}: ${newUnitLabel} instead of ${oldUnitLabel}${
          saveLabel ? `, save ${saveLabel}` : ""
        }${totalsTxt ? `. ${totalsTxt}` : ""}${oosNote}.`
      : `יש מבצע על ${name}${noteTxt}: ${newUnitLabel} במקום ${oldUnitLabel}${
          saveLabel ? `, חיסכון: ${saveLabel}` : ""
        }${totalsTxt ? `. ${totalsTxt}` : ""}${oosNote}.`;

    return `${lineShort}\n${statusLine}`;
  }

  const line1 = isEnglish
    ? `Promotion for ${name}: ${promoShort}. ${pricePart}${
        savingsTxt ? `, ${savingsTxt}` : ""
      }${totalsTxt ? `. ${totalsTxt}` : ""}${oosNote}.`
    : `יש מבצע על ${name}: ${promoShort}. ${pricePart}${
        savingsTxt ? `, ${savingsTxt}` : ""
      }${totalsTxt ? `. ${totalsTxt}` : ""}${oosNote}.`;

  return `${line1}\n${statusLine}`;
}

async function answerPromotionFlow({
  shop_id,
  customer_id,
  isEnglish,
  promotionReqs,
  baseQuestions,
}) {
  const reqs = Array.isArray(promotionReqs) ? promotionReqs : [];

  const blocks = [];
  const questionsToSave = [...(baseQuestions || [])];

  for (let i = 0; i < reqs.length; i++) {
    const req = reqs[i] || {};
    req.name = nullify(req.name);
    req.outputName = nullify(req.outputName);
    req["sub-category"] = nullify(req["sub-category"]);
    req.sub_category = nullify(req.sub_category);
    req.category = nullify(req.category);

    const hasName = Boolean(nullify(req?.name || req?.outputName));
    const limit = hasName ? 50 : 25;

    const { rows } = await searchPromotionsForRequest(shop_id, req, { limit });

    if (!rows.length) {
      const subject = buildPromoSubject(req, isEnglish);

      blocks.push(
        isEnglish
          ? `No promotions found (active/upcoming/ended this week) for ${subject}.`
          : `לא מצאתי מבצעים (פעילים/עתידיים/שהסתיימו השבוע) עבור ${subject}.`
      );
      continue;
    }

    const promoBlocks = rows.map((r) => {
      const foundRow = {
        product_id: Number(r.product_id),
        price: Number(r.price),
        stock_amount: Number(r.stock_amount),
        matched_name: r.name,
        matched_display_name_en: r.display_name_en,
      };

      const promo = {
        id: r.promo_id,
        shop_id,
        product_id: r.product_id,
        kind: r.kind,
        percent_off: r.percent_off,
        amount_off: r.amount_off,
        fixed_price: r.fixed_price,
        bundle_buy_qty: r.bundle_buy_qty,
        bundle_pay_price: r.bundle_pay_price,
        description: r.description,
        start_at: r.start_at,
        end_at: r.end_at,
      };

      return buildPromotionBlock({ req, foundRow, promo, isEnglish });
    });

    blocks.push(promoBlocks.filter(Boolean).join("\n\n"));
  }

  const allQuestions = normalizeIncomingQuestions(questionsToSave, {
    preserveOptions: true,
  });

  if (allQuestions.length) {
    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id: null,
      questions: allQuestions,
    });
  }

  const body = blocks.filter(Boolean).join("\n\n");
  const tail = buildQuestionsTextSmart({ questions: baseQuestions, isEnglish });

  const finalMsg = [body, tail].filter((x) => x && x.trim()).join("\n\n");

  return finalMsg && finalMsg.trim()
    ? finalMsg
    : isEnglish
    ? "I couldn’t answer that promotion question. Can you rephrase?"
    : "לא הצלחתי לענות על שאלת המבצע. תוכל לנסח שוב?";
}

function buildCheaperAltQuestionText({ subject, altLines, isEnglish }) {
  const list = altLines.map((x) => `• ${x}`).join("\n");
  return isEnglish
    ? `Cheaper alternatives for ${subject}:\n${list}`
    : `חלופות זולות יותר עבור ${subject}:\n${list}`;
}

async function buildCheaperAltBlockAndQuestion({
  shop_id,
  req,
  foundRow,
  baseUnitPrice,
  category,
  sub_category,
  excludeTokens,
  usedIds,
  isEnglish,
}) {
  const subject = getSubjectForAlt({ req, foundRow, isEnglish });
  let alts = await fetchAlternatives(
    shop_id,
    category,
    sub_category,
    Array.from(usedIds),
    50,
    req?.name || req?.outputName || subject,
    excludeTokens
  );

  if (Array.isArray(alts) && alts.length) {
    const cat = String(category || "").trim();
    if (cat) {
      alts = alts.filter((a) => String(a?.category || "").trim() === cat);
    }

    const sub = String(sub_category || "").trim();
    if (sub) {
      const subList = getSubCategoryCandidates(sub);
      if (subList.length) {
        alts = alts.filter((a) =>
          subList.includes(String(a?.sub_category || "").trim())
        );
      } else {
        alts = alts.filter((a) => String(a?.sub_category || "").trim() === sub);
      }
    }
  }

  const eps = 1e-9;
  const cheaper = (alts || [])
    .filter((a) => {
      const p = Number(a?.price);
      return (
        Number.isFinite(p) &&
        Number.isFinite(baseUnitPrice) &&
        p + eps < baseUnitPrice
      );
    })
    .sort(
      (a, b) => Number(a.price) - Number(b.price) || Number(b.id) - Number(a.id)
    )
    .slice(0, 3);

  if (!cheaper.length) {
    const msg = isEnglish
      ? `I couldn’t find cheaper alternatives for ${subject}.`
      : `לא מצאתי חלופות זולות יותר עבור ${subject}.`;
    return { blockText: msg, questionObj: null, altIds: [] };
  }

  const altLines = cheaper
    .map((a) => formatAltLineWithPrice({ altRow: a, req, isEnglish }))
    .filter(Boolean);

  const options = cheaper
    .map((a) =>
      isEnglish
        ? String(a?.display_name_en || a?.name || "").trim()
        : String(a?.name || a?.display_name_en || "").trim()
    )
    .filter(Boolean);

  const q = {
    name: foundRow?.matched_name || req?.name || null,
    question: buildCheaperAltQuestionText({ subject, altLines, isEnglish }),
    options,
  };

  const altIds = cheaper
    .flatMap((a) => [Number(a?.id), Number(a?.product_id)])
    .filter(Number.isFinite);

  return { blockText: q.question, questionObj: q, altIds };
}

async function answerCheaperAltFlow({
  shop_id,
  customer_id,
  isEnglish,
  cheaperAltReqs,
  baseQuestions,
}) {
  const reqs = Array.isArray(cheaperAltReqs) ? cheaperAltReqs : [];

  const searchRequests = reqs.map((p) => {
    const n = Number(p?.amount);
    const amount = Number.isFinite(n) && n > 0 ? n : 1;
    return { ...p, amount };
  });

  const res = await searchProducts(shop_id, searchRequests);
  const found = res?.found || [];
  const notFound = res?.notFound || [];

  const foundByIndex = new Map();
  for (const f of found) foundByIndex.set(f.originalIndex, f);

  const usedIds = new Set();
  for (const f of found) {
    const pid = Number(f?.product_id);
    if (Number.isFinite(pid)) usedIds.add(pid);
  }

  const blocks = [];
  const questionsToSave = [...(baseQuestions || [])];

  for (let i = 0; i < searchRequests.length; i++) {
    const req = searchRequests[i] || {};
    const f = foundByIndex.get(i) || null;

    if (!f) {
      const nf = notFound.find((x) => x.originalIndex === i) || null;

      const category =
        String(nf?.category || req?.category || "").trim() || null;
      const sub_category =
        String(
          nf?.sub_category || req?.["sub-category"] || req?.sub_category || ""
        ).trim() || null;

      const excludeTokens =
        Array.isArray(nf?.exclude_tokens) && nf.exclude_tokens.length
          ? nf.exclude_tokens
          : Array.isArray(req.exclude_tokens)
          ? req.exclude_tokens
          : [];

      if (category || sub_category) {
        const { blockText, questionObj, altIds } =
          await buildAltBlockAndQuestion({
            shop_id,
            reason: "NOT_FOUND",
            req,
            foundRow: null,
            category,
            sub_category,
            excludeTokens,
            usedIds,
            isEnglish,
          });

        if (blockText) blocks.push(blockText);
        if (questionObj) questionsToSave.push(questionObj);
        for (const id of altIds) usedIds.add(id);
      } else {
        const reqName = String(req?.name || "").trim() || null;
        const q = {
          name: reqName,
          question: isEnglish
            ? `I couldn’t find "${
                reqName || "this product"
              }". Can you write it differently?`
            : `לא מצאתי "${reqName || "המוצר"}". תוכל לכתוב את זה בצורה אחרת?`,
          options: [],
        };
        blocks.push(q.question);
        questionsToSave.push(q);
      }
      continue;
    }

    const baseUnitPrice = Number(f?.price);
    if (!Number.isFinite(baseUnitPrice)) {
      const subject = getSubjectForAlt({ req, foundRow: f, isEnglish });
      blocks.push(
        isEnglish
          ? `I found ${subject}, but I don’t have an updated price so I can’t guarantee what’s cheaper.`
          : `מצאתי ${subject}, אבל אין לי מחיר מעודכן ולכן אני לא יכול להבטיח מה זול יותר.`
      );
      continue;
    }

    const category = String(f?.category || req?.category || "").trim() || null;
    const sub_category =
      String(
        f?.sub_category || req?.["sub-category"] || req?.sub_category || ""
      ).trim() || null;

    const excludeTokens = getExcludeTokensFromReq(req);

    const baseId = Number(f?.product_id);
    if (Number.isFinite(baseId)) usedIds.add(baseId);

    const { blockText, questionObj, altIds } =
      await buildCheaperAltBlockAndQuestion({
        shop_id,
        req,
        foundRow: f,
        baseUnitPrice,
        category,
        sub_category,
        excludeTokens,
        usedIds,
        isEnglish,
      });

    if (blockText) blocks.push(blockText);
    if (questionObj) questionsToSave.push(questionObj);
    for (const id of altIds) usedIds.add(id);
  }

  const allQuestions = normalizeIncomingQuestions(questionsToSave, {
    preserveOptions: true,
  });

  if (allQuestions.length) {
    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id: null,
      questions: allQuestions,
    });
  }

  const body = blocks.filter(Boolean).join("\n\n");
  const tail = buildQuestionsTextSmart({ questions: baseQuestions, isEnglish });

  const finalMsg = [body, tail].filter((x) => x && x.trim()).join("\n\n");

  return finalMsg && finalMsg.trim()
    ? finalMsg
    : isEnglish
    ? "I couldn’t find cheaper alternatives. Can you rephrase?"
    : "לא הצלחתי למצוא חלופות זולות יותר. תוכל לנסח שוב?";
}

module.exports = {
  answerPriceCompareFlow,
  buildQuestionsTextSmart,
  buildFoundProductLine,
  buildAltBlockAndQuestion,
  getSubjectForAlt,
  isOutOfStockFromFound,
  saveFallbackOpenQuestion,
  answerPromotionFlow,
  searchPromotionsForRequest,
  answerCheaperAltFlow,
  answerBudgetPickFlow,
};

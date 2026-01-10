const {
  buildQuestionsBlock,
  searchProducts,
  fetchAlternatives,
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

    // no units => compare by kg
    return isEnglish ? `${kgTxt} kg` : `${kgTxt} ק״ג`;
  }

  // unit-based
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
  // treat NULL as "unknown" -> allow it as in-stock
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
    // collect candidates for result
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

module.exports = {
  answerPriceCompareFlow,
  buildQuestionsTextSmart,
  buildFoundProductLine,
  buildAltBlockAndQuestion,
  getSubjectForAlt,
  isOutOfStockFromFound,
  saveFallbackOpenQuestion,
};


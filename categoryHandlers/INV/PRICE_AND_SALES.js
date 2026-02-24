const { chat } = require("../../config/openai");
const { getPromptFromDB } = require("../../repositories/prompt");
const { searchProducts } = require("../../services/products");
const {
  answerPriceCompareFlow,
  answerCheaperAltFlow,
  answerBudgetPickFlow,
  buildQuestionsTextSmart,
  buildFoundProductLine,
  buildAltBlockAndQuestion,
  getSubjectForAlt,
  isOutOfStockFromFound,
  saveFallbackOpenQuestion,
  answerPromotionFlow,
} = require("../../services/priceAndSales");
const { saveOpenQuestions } = require("../../utilities/openQuestions");
const { normalizeIncomingQuestions } = require("../../utilities/normalize");
const {
  buildInvPriceAndSalesSchema
} = require("./schemas/priceAndSales.schema");
const { parseModelAnswer } = require("../../utilities/jsonParse");

const PROMPT_CAT = "INV";
const PROMPT_SUB = "PRICE_AND_SALES";

const DEBUG = process.env.DEBUG_PRICE_AND_SALES !== "0";
function dlog(...args) {
  if (DEBUG) console.log("[INV-PRICE]", ...args);
}

async function answerPriceAndSales({
  message,
  customer_id,
  shop_id,
  history = [],
  isEnglish = false,
}) {
  if (typeof message !== "string" || !customer_id || !shop_id) {
    throw new Error(
      "answerPriceAndSales: missing or invalid message/customer_id/shop_id",
    );
  }

  const systemPrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);
  const answer = await chat({
    message,
    history,
    systemPrompt,
    response_format: {
      type: "json_schema",
      json_schema: await buildInvPriceAndSalesSchema(),
    },
    prompt_cache_key: "inv_price_and_sales_v1",
  });

  let parsed;
  try {
    parsed = JSON.parse(answer);
  } catch (e1) {
    try {
      parsed = parseModelAnswer(answer);
    } catch (e2) {
      console.error(
        "[INV-PRICE][ERR] Failed to parse model JSON:",
        e2?.message,
        answer,
      );
      const botPayload = isEnglish
        ? "Sorry, there was a problem understanding your pricing question. Can you rephrase it?"
        : "מצטערים, הייתה תקלה בהבנת שאלת המחיר. אפשר לנסח שוב?";
      await saveFallbackOpenQuestion(botPayload, customer_id, shop_id);
      return botPayload;
    }
  }

  dlog("Parsed JSON", JSON.stringify(parsed, null, 2));

  const productRequests = Array.isArray(parsed.products) ? parsed.products : [];
  const clarifyQuestionsFromModel = Array.isArray(parsed.questions)
    ? parsed.questions
    : [];

  const hasProducts = productRequests.length > 0;
  const hasClarify = clarifyQuestionsFromModel.length > 0;

  if (!hasProducts && !hasClarify) {
    const botPayload = isEnglish
      ? "I couldn’t identify which product you’re asking about. Can you write the product name?"
      : "לא הצלחתי לזהות על איזה מוצר אתה שואל. תוכל לכתוב את שם המוצר?";
    await saveFallbackOpenQuestion(botPayload, customer_id, shop_id);
    return botPayload;
  }

  const baseQuestions = normalizeIncomingQuestions(clarifyQuestionsFromModel, {
    preserveOptions: true,
  });

  const compareReqs = productRequests.filter(
    (p) =>
      String(p?.price_intent || "")
        .trim()
        .toUpperCase() === "PRICE_COMPARE",
  );

  if (compareReqs.length) {
    return await answerPriceCompareFlow({
      shop_id,
      customer_id,
      isEnglish,
      compareReqs,
      baseQuestions,
    });
  }

  const promoReqs = productRequests.filter(
    (p) =>
      String(p?.price_intent || "")
        .trim()
        .toUpperCase() === "PROMOTION",
  );

  if (promoReqs.length) {
    return await answerPromotionFlow({
      shop_id,
      customer_id,
      isEnglish,
      promotionReqs: promoReqs,
      baseQuestions,
    });
  }

  const cheaperAltReqs = productRequests.filter(
    (p) =>
      String(p?.price_intent || "")
        .trim()
        .toUpperCase() === "CHEAPER_ALT",
  );

  if (cheaperAltReqs.length) {
    return await answerCheaperAltFlow({
      shop_id,
      customer_id,
      isEnglish,
      cheaperAltReqs,
      baseQuestions,
    });
  }

  const budgetReqs = productRequests.filter(
    (p) =>
      String(p?.price_intent || "")
        .trim()
        .toUpperCase() === "BUDGET_PICK",
  );

  if (budgetReqs.length) {
    return await answerBudgetPickFlow({
      shop_id,
      customer_id,
      isEnglish,
      budgetReqs,
      baseQuestions,
    });
  }

  const priceReqs = productRequests.filter(
    (p) =>
      String(p?.price_intent || "")
        .trim()
        .toUpperCase() === "PRICE",
  );

  if (!priceReqs.length) {
    if (baseQuestions.length) {
      await saveOpenQuestions({
        customer_id,
        shop_id,
        order_id: null,
        questions: baseQuestions,
      });
      return buildQuestionsTextSmart({ questions: baseQuestions, isEnglish });
    }

    return isEnglish
      ? "Right now I can answer only simple price questions like: “How much is X?”. Can you ask that way?"
      : 'כרגע אני מטפל רק בשאלות מחיר פשוטות כמו: "כמה עולה X?". תוכל לשאול ככה?';
  }

  const searchRequests = priceReqs.map((p) => {
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

  const blocks = [];
  const questionsToSave = [...baseQuestions];

  for (let i = 0; i < searchRequests.length; i++) {
    const req = searchRequests[i] || {};
    const f = foundByIndex.get(i) || null;

    const blockLines = [];

    if (f) {
      blockLines.push(buildFoundProductLine({ req, foundRow: f, isEnglish }));

      if (isOutOfStockFromFound(f)) {
        const category = (f.category || req.category || "").trim() || null;
        const sub_category =
          (
            f.sub_category ||
            req["sub-category"] ||
            req.sub_category ||
            ""
          ).trim() || null;

        const excludeTokens = Array.isArray(req.exclude_tokens)
          ? req.exclude_tokens
          : [];

        if (category || sub_category) {
          const { blockText, questionObj, altIds } =
            await buildAltBlockAndQuestion({
              shop_id,
              reason: "OOS",
              req,
              foundRow: f,
              category,
              sub_category,
              excludeTokens,
              usedIds,
              isEnglish,
            });

          if (blockText) blockLines.push(blockText);

          if (questionObj) questionsToSave.push(questionObj);
          for (const id of altIds) usedIds.add(id);
        } else {
          const subject = getSubjectForAlt({ req, foundRow: f, isEnglish });
          const q = {
            name: f.matched_name || req.name || null,
            question: isEnglish
              ? `I couldn’t suggest alternatives for ${subject}. Can you add a bit more detail?`
              : `אני לא מצליח להציע חלופות עבור ${subject}. תוכל להוסיף עוד קצת פרטים?`,
            options: [],
          };
          blockLines.push(q.question);
          questionsToSave.push(q);
        }
      }

      blocks.push(blockLines.join("\n"));
      continue;
    }

    const nf = notFound.find((x) => x.originalIndex === i) || null;

    const category = String(nf?.category || req?.category || "").trim() || null;
    const sub_category =
      String(
        nf?.sub_category || req?.["sub-category"] || req?.sub_category || "",
      ).trim() || null;

    const excludeTokens =
      Array.isArray(nf?.exclude_tokens) && nf.exclude_tokens.length
        ? nf.exclude_tokens
        : Array.isArray(req.exclude_tokens)
          ? req.exclude_tokens
          : [];

    if (category || sub_category) {
      const { blockText, questionObj, altIds } = await buildAltBlockAndQuestion(
        {
          shop_id,
          reason: "NOT_FOUND",
          req,
          foundRow: null,
          category,
          sub_category,
          excludeTokens,
          usedIds,
          isEnglish,
        },
      );

      if (blockText) blockLines.push(blockText);
      if (questionObj) questionsToSave.push(questionObj);
      for (const id of altIds) usedIds.add(id);

      blocks.push(blockLines.join("\n"));
      continue;
    }

    const reqName = String(req?.name || "").trim() || null;
    const q = {
      name: reqName,
      question: isEnglish
        ? `I couldn’t find "${
            reqName || "this product"
          }" in the system. Can you write it differently (or add a bit more detail)?`
        : `לא מצאתי "${
            reqName || "המוצר"
          }" במערכת. תוכל לכתוב את זה בצורה אחרת (או להוסיף עוד קצת פרטים)?`,
      options: [],
    };

    blockLines.push(q.question);
    questionsToSave.push(q);
    blocks.push(blockLines.join("\n"));
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

  if (!finalMsg.trim()) {
    return isEnglish
      ? "I couldn’t answer that price question. Can you rephrase?"
      : "לא הצלחתי לענות על שאלת המחיר. תוכל לנסח שוב?";
  }

  return finalMsg;
}

module.exports = {
  answerPriceAndSales,
};

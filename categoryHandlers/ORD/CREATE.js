const { chat } = require("../../config/openai");
const db = require("../../config/db");
const { getPromptFromDB } = require("../../repositories/prompt");
const { createOrderWithStockReserve } = require("../../utilities/orders");
const { isEnglishMessage } = require("../../utilities/lang");
const { addMoney, roundTo } = require("../../utilities/decimal");
const {
  searchProducts,
  buildAlternativeQuestions,
} = require("../../services/products");
const {
  saveOpenQuestions,
  closeQuestionsByIds,
  deleteQuestionsByIds,
} = require("../../utilities/openQuestions");
const { normalizeIncomingQuestions } = require("../../utilities/normalize");
const { buildCreateOrderSchema } = require("./schemas/create.schema");
const { parseModelAnswer } = require("../../utilities/jsonParse");
const {
  buildItemsBlock,
  buildQuestionsBlock,
} = require("../../utilities/messageBuilders");

const PROMPT_CAT = "ORD";
const PROMPT_SUB = "CREATE";

module.exports = {
  async orderProducts({
    message,
    customer_id,
    shop_id,
    history,
    openQsCtx = [],
    maxPerProduct,
  }) {
    if (typeof message !== "string" || !customer_id || !shop_id) {
      throw new Error(
        "orderProducts: missing or invalid message/customer_id/shop_id",
      );
    }

    const systemPrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);

    const openQsCtxToPrompt = openQsCtx.length ? JSON.stringify(openQsCtx) : "";

    const userContext = openQsCtxToPrompt || null;

    const answer = await chat({
      message,
      history,
      systemPrompt,
      userContext,
      response_format: {
        type: "json_schema",
        json_schema: await buildCreateOrderSchema(),
      },
      prompt_cache_key: "ord_create_v1",
    });
    console.log({ answer });
    let parsed;
    try {
      parsed = JSON.parse(answer);
    } catch (e1) {
      try {
        parsed = parseModelAnswer(answer);
      } catch (e2) {
        console.error("Failed to parse model JSON:", e2?.message, answer);
        return {
          reply:
            "מצטערים, הייתה תקלה בעיבוד ההזמנה. אפשר לנסח שוב בקצרה מה תרצה להזמין?",
          raw: answer,
        };
      }
    }

    const qUpdates = parsed?.question_updates || {};
    if (Array.isArray(qUpdates.close_ids) && qUpdates.close_ids.length) {
      await closeQuestionsByIds(qUpdates.close_ids);
    }
    if (Array.isArray(qUpdates.delete_ids) && qUpdates.delete_ids.length) {
      await deleteQuestionsByIds(qUpdates.delete_ids);
    }

    const reqProducts = Array.isArray(parsed?.products) ? parsed.products : [];
    const isEnglish = isEnglishMessage(message);

    if (!reqProducts.length) {
      const normalizedQs = normalizeIncomingQuestions(parsed?.questions, {
        preserveOptions: true,
      });

      const curated = { ...parsed, questions: normalizedQs };

      const emptyOrder = await createOrderWithStockReserve({
        shop_id,
        customer_id,
        lineItems: [], //empty
        status: "pending",
        payment_method: "other",
        delivery_address: null,
      });

      const hasQuestions = curated.questions && curated.questions.length > 0;
      const orderIdPart = emptyOrder?.order_id
        ? isEnglish
          ? `(Order: #${emptyOrder.order_id})`
          : `(הזמנה מספר: #${emptyOrder.order_id})`
        : "";

      let summaryLine;
      if (isEnglish) {
        if (hasQuestions) {
          summaryLine =
            `Your order is currently empty ${orderIdPart}.` +
            `\nTo build an order that fits what you want, I need your answers to a few questions:`;
        } else {
          summaryLine = `Your order is currently empty ${orderIdPart}.`;
        }
      } else {
        if (hasQuestions) {
          summaryLine =
            `ההזמנה שלך כרגע ריקה ${orderIdPart}.` +
            `\nכדי שאוכל לבנות עבורך הזמנה שמתאימה לך, אני צריך תשובה לכמה שאלות:`;
        } else {
          summaryLine = `ההזמנה שלך כרגע ריקה ${orderIdPart}.`;
        }
      }

      const questionsLines = (curated.questions || [])
        .map((q) => `• ${q.question}`)
        .join("\n");

      await saveOpenQuestions({
        customer_id,
        shop_id,
        order_id: emptyOrder?.order_id || null,
        questions: curated.questions,
      });

      const finalMessage = [summaryLine, questionsLines]
        .filter(Boolean)
        .join("\n\n");

      return finalMessage;
    }

    const { found, notFound } = await searchProducts(shop_id, reqProducts);

    const modelQuestions = normalizeIncomingQuestions(parsed?.questions, {
      preserveOptions: true,
    });

    const notFoundNameSet = new Set(
      notFound
        .map((nf) =>
          typeof nf.requested_name === "string" ? nf.requested_name.trim() : "",
        )
        .filter(Boolean),
    );

    const filteredModelQuestions = modelQuestions.filter((q) => {
      const nm = typeof q?.name === "string" ? q.name.trim() : "";
      return !nm || !notFoundNameSet.has(nm);
    });

    const cappedWarnings = [];
    const fractionalWarnings = [];

    const cappedFound = found.map((f) => {
      let amt = Number(f.requested_amount) || 0;
      const isWeight = f.sold_by_weight === true;

      if (
        !isWeight &&
        Number.isFinite(amt) &&
        Math.abs(amt - Math.round(amt)) > 1e-9
      ) {
        const rounded = Math.ceil(amt);

        const nameForWarning = isEnglish
          ? (f.matched_display_name_en && f.matched_display_name_en.trim()) ||
            f.matched_name
          : f.matched_name;

        fractionalWarnings.push({
          name: nameForWarning,
          original: amt,
          rounded,
        });

        amt = rounded;
      }
      if (amt > maxPerProduct) {
        const nameForWarning = isEnglish
          ? (f.matched_display_name_en && f.matched_display_name_en.trim()) ||
            f.matched_name
          : f.matched_name;

        cappedWarnings.push({
          name: nameForWarning,
          original: amt,
          capped: maxPerProduct,
        });

        amt = maxPerProduct;
      }

      return {
        ...f,
        requested_amount: amt,
      };
    });

    const foundIdsSet = new Set(cappedFound.map((f) => f.product_id));

    const orderInputLineItems = cappedFound.map((f) => ({
      product_id: f.product_id,
      amount: f.requested_amount,
      requested_name: f.requested_name || null,
      sold_by_weight: f.sold_by_weight === true,
      requested_units:
        f.sold_by_weight === true && Number.isFinite(Number(f.requested_units))
          ? Number(f.requested_units)
          : null,
    }));

    const orderRes = await createOrderWithStockReserve({
      shop_id,
      customer_id,
      lineItems: orderInputLineItems,
      status: "pending",
      payment_method: "other",
      delivery_address: null,
    });

    const insufficientCount = Array.isArray(orderRes.insufficient)
      ? orderRes.insufficient.length
      : 0;

    const notFoundEligibleCount = notFound.filter((nf) => {
      const cat = (nf.category || "").trim();
      const sub = (nf.sub_category || "").trim();
      return !!cat || !!sub;
    }).length;

    const baseQuestionsCount = filteredModelQuestions.length;
    const forceShort =
      baseQuestionsCount + notFoundEligibleCount + insufficientCount > 3;

    const altLimit = forceShort ? 2 : 3;

    const { altQuestions, alternativesMap } = await buildAlternativeQuestions(
      shop_id,
      notFound,
      foundIdsSet,
      isEnglish,
      "",
      {
        baseQuestionsCount,
        forceShort,
        threshold: 3,
        shortLimit: 2,
        longLimit: 3,
      },
    );

    // questions about the stock
    const stockAltQuestions = [];
    if (Array.isArray(orderRes.insufficient) && orderRes.insufficient.length) {
      for (const miss of orderRes.insufficient) {
        const reqName = (
          isEnglish
            ? miss.requested_output_name ||
              miss.matched_display_name_en ||
              miss.requested_name ||
              miss.matched_name
            : miss.requested_name ||
              miss.matched_name ||
              miss.matched_display_name_en
        ).trim();

        const altNames = (miss.alternatives || [])
          .slice(0, altLimit)
          .map((a) =>
            isEnglish
              ? (a.display_name_en && a.display_name_en.trim()) || a.name
              : a.name,
          );
        if (isEnglish) {
          stockAltQuestions.push({
            name: reqName,
            question:
              altNames.length > 0
                ? `${reqName ?? "The item"} is short on stock (requested ${
                    miss.requested_amount
                  }, available ${miss.in_stock}). Would ${altNames.join(
                    " / ",
                  )} work instead?`
                : `${reqName ?? "The item"} is short on stock (requested ${
                    miss.requested_amount
                  }, available ${
                    miss.in_stock
                  }). Would you like a replacement or should I skip it?`,
            options: altNames,
          });
        } else {
          stockAltQuestions.push({
            name: reqName,
            question:
              altNames.length > 0
                ? `${reqName ?? "המוצר"} חסר במלאי (התבקשה כמות ${
                    miss.requested_amount
                  }, זמינות ${miss.in_stock}). האם יתאים ${altNames.join(
                    " / ",
                  )} במקום?`
                : `${reqName ?? "המוצר"} חסר במלאי (התבקשה כמות ${
                    miss.requested_amount
                  }, זמינות ${miss.in_stock}). להציע חלופה או לדלג?`,
            options: altNames,
          });
        }
      }
    }

    const combinedQuestions = [
      ...filteredModelQuestions,
      ...altQuestions,
      ...stockAltQuestions,
    ];

    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id: orderRes?.order_id || null,
      questions: combinedQuestions,
    });

    const hasItems = Array.isArray(orderRes.items) && orderRes.items.length > 0;

    if (!hasItems) {
      const hasQuestions = combinedQuestions.length > 0;

      const orderIdPart = orderRes?.order_id
        ? isEnglish
          ? `(Order: #${orderRes.order_id})`
          : `(הזמנה מספר: #${orderRes.order_id})`
        : "";

      let summaryLine;
      if (isEnglish) {
        if (hasQuestions) {
          summaryLine =
            `Your order is currently empty ${orderIdPart}.` +
            `\nTo build an order that fits what you want, I need your answers to a few questions:`;
        } else {
          summaryLine = `Your order is currently empty ${orderIdPart}.`;
        }
      } else {
        if (hasQuestions) {
          summaryLine =
            `ההזמנה שלך כרגע ריקה ${orderIdPart}.` +
            `\nכדי שאוכל לבנות עבורך הזמנה שמתאימה לך, אני צריך תשובה לכמה שאלות:`;
        } else {
          summaryLine = `ההזמנה שלך כרגע ריקה ${orderIdPart}.`;
        }
      }

      const questionsLines = (combinedQuestions || [])
        .map((q) => `• ${q.question}`)
        .join("\n");

      const finalMessage = [summaryLine, questionsLines]
        .filter(Boolean)
        .join("\n\n");

      return finalMessage;
    }

    const [rows] = await db.query(
      `SELECT
     oi.product_id,
     oi.amount,
     oi.sold_by_weight,
     oi.requested_units,
     oi.price AS line_total,
     oi.promo_id,

     pr.kind AS promo_kind,
     pr.percent_off,
     pr.amount_off,
     pr.fixed_price,
     pr.bundle_buy_qty,
     pr.bundle_pay_price,

     p.price AS unit_price,
     p.name AS name_he,
     p.display_name_en
      FROM order_item oi
      JOIN product p ON p.id = oi.product_id
      LEFT JOIN promotion pr ON pr.id = oi.promo_id
      WHERE oi.order_id = ?`,
      [orderRes.order_id],
    );

    const totalWithPromos = Number(orderRes.totalPrice ?? 0);

    let totalNoPromos = 0;
    for (const r of rows || []) {
      const unit = Number(r.unit_price);
      const qty = Number(r.amount);
      if (!Number.isFinite(unit) || !Number.isFinite(qty)) continue;
      totalNoPromos = addMoney(totalNoPromos, roundTo(unit * qty, 2));
    }
    const savings = roundTo(totalNoPromos - totalWithPromos, 2);
    const hasSavings = Number.isFinite(savings) && savings >= 0.01;

    const productsForDisplay = rows.map((r) => {
      const units = Number(r.requested_units);
      const hasUnits = Number.isFinite(units) && units > 0;

      const promoId = r.promo_id ? Number(r.promo_id) : null;

      return {
        name: isEnglish
          ? (r.display_name_en && r.display_name_en.trim()) || r.name_he
          : r.name_he,
        amount: Number(r.amount),

        unit_price: Number(r.unit_price),
        line_total: Number(r.line_total),

        promo_id: promoId,
        promo:
          promoId && r.promo_kind
            ? {
                kind: r.promo_kind,
                percent_off: r.percent_off,
                amount_off: r.amount_off,
                fixed_price: r.fixed_price,
                bundle_buy_qty: r.bundle_buy_qty,
                bundle_pay_price: r.bundle_pay_price,
              }
            : null,

        ...(r.sold_by_weight ? { sold_by_weight: true } : {}),
        ...(hasUnits ? { units } : {}),
      };
    });

    const hasQuestions =
      Array.isArray(combinedQuestions) && combinedQuestions.length > 0;

    const summaryLine = hasQuestions
      ? isEnglish
        ? "To complete your order, I need a few clarifications:"
        : "כדי להשלים את ההזמנה חסרות כמה הבהרות:"
      : isEnglish
        ? "Great, here’s the order I understood from you:"
        : "יופי, זאת ההזמנה שהבנתי ממך:";

    const headerBlock = isEnglish
      ? [
          orderRes?.order_id ? `Order: #${orderRes.order_id}` : null,
          hasSavings
            ? `Subtotal: *₪${totalWithPromos.toFixed(
                2,
              )}* instead of ₪${totalNoPromos.toFixed(2)}`
            : `Subtotal: *₪${totalWithPromos.toFixed(2)}*`,
        ]
          .filter(Boolean)
          .join("\n")
      : [
          orderRes?.order_id ? `מספר הזמנה: #${orderRes.order_id}` : null,
          hasSavings
            ? `סה״כ ביניים: *₪${totalWithPromos.toFixed(
                2,
              )}* במקום ₪${totalNoPromos.toFixed(2)}`
            : `סה״כ ביניים: *₪${totalWithPromos.toFixed(2)}*`,
        ]
          .filter(Boolean)
          .join("\n");

    let limitWarningsBlock = "";
    if (cappedWarnings.length) {
      if (isEnglish) {
        limitWarningsBlock = `Note: you can order up to ${maxPerProduct} units per product.`;
      } else {
        limitWarningsBlock = `שימו לב: ניתן להזמין עד ${maxPerProduct} יחידות מכל מוצר.`;
      }
    }

    let fractionalWarningsBlock = "";
    if (fractionalWarnings.length) {
      if (isEnglish) {
        fractionalWarningsBlock =
          "Note: some items can only be ordered in whole units, so I rounded up:\n" +
          fractionalWarnings
            .map((w) => `• ${w.name}: ${w.original} → ${w.rounded}`)
            .join("\n");
      } else {
        fractionalWarningsBlock =
          "שימו לב: יש מוצרים שנמכרים ביחידות שלמות בלבד, לכן עיגלתי למעלה:\n" +
          fractionalWarnings
            .map((w) => `• ${w.name}: ${w.original} → ${w.rounded}`)
            .join("\n");
      }
    }

    const itemsBlock = buildItemsBlock({
      items: productsForDisplay,
      isEnglish,
      mode: "create",
    });
    const questionsBlock = buildQuestionsBlock({
      questions: combinedQuestions,
      isEnglish,
    });

    const finalMessage = [
      summaryLine,
      limitWarningsBlock,
      fractionalWarningsBlock,
      itemsBlock,
      " ",
      headerBlock,
      questionsBlock,
    ]
      .filter(Boolean)
      .join("\n");

    console.log("[ORD-CREATE] Order created/reserved:", {
      order_id: orderRes?.order_id,
      total: orderRes?.totalPrice,
      itemsCount: (orderRes?.items || []).length,
    });
    console.log(
      "[ORD-CREATE] Not found (no product matched):",
      JSON.stringify(notFound, null, 2),
    );
    return finalMessage;
  },
};

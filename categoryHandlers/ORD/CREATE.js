const { chat } = require("../../config/openai");
const db = require("../../config/db");
const { getPromptFromDB } = require("../../repositories/prompt");
const { createOrderWithStockReserve } = require("../../utilities/orders");
const { isEnglishSummary } = require("../../utilities/lang");
const {
  parseModelAnswer,
  searchProducts,
  buildAlternativeQuestions,
  buildItemsBlock,
  buildQuestionsBlock,
} = require("../../services/products");
const {
  saveOpenQuestions,
  closeQuestionsByIds,
  deleteQuestionsByIds,
} = require("../../utilities/openQuestions");
const { normalizeIncomingQuestions } = require("../../utilities/normalize");
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
        "orderProducts: missing or invalid message/customer_id/shop_id"
      );
    }

    const basePrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);

    const systemPrompt = [
      basePrompt,
      "",
      "=== STRUCTURED CONTEXT ===",
      openQsCtx,
    ].join("\n");

    const answer = await chat({ message, history, systemPrompt });

    let parsed;
    try {
      parsed = parseModelAnswer(answer);
    } catch (e) {
      console.error("Failed to parse model JSON:", e?.message, answer);
      return {
        reply:
          "מצטערים, הייתה תקלה בעיבוד ההזמנה. אפשר לנסח שוב בקצרה מה תרצה להזמין?",
        raw: answer,
      };
    }

    const qUpdates = parsed?.question_updates || {};
    if (Array.isArray(qUpdates.close_ids) && qUpdates.close_ids.length) {
      await closeQuestionsByIds(qUpdates.close_ids);
    }
    if (Array.isArray(qUpdates.delete_ids) && qUpdates.delete_ids.length) {
      await deleteQuestionsByIds(qUpdates.delete_ids);
    }

    const reqProducts = Array.isArray(parsed?.products) ? parsed.products : [];
    const isEnglish = isEnglishSummary(parsed?.summary_line);

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

    const cappedWarnings = [];
    const cappedFound = found.map((f) => {
      const origAmount = Number(f.requested_amount) || 0;
      if (origAmount > maxPerProduct) {
        const nameForWarning = isEnglish
          ? (f.matched_display_name_en && f.matched_display_name_en.trim()) ||
            f.matched_name
          : f.matched_name;

        cappedWarnings.push({
          name: nameForWarning,
          original: origAmount,
          capped: maxPerProduct,
        });

        return {
          ...f,
          requested_amount: maxPerProduct,
        };
      }
      return f;
    });

    const unitsByProductId = new Map();
    const weightFlagByProductId = new Map();

    for (const f of cappedFound) {
      const pid = Number(f.product_id);

      const u = Number(f.requested_units);
      if (Number.isFinite(u) && u > 0) {
        unitsByProductId.set(pid, (unitsByProductId.get(pid) || 0) + u);
      }

      if (f.sold_by_weight === true) {
        weightFlagByProductId.set(pid, true);
      }
    }

    const foundIdsSet = new Set(cappedFound.map((f) => f.product_id));
    const { altQuestions, alternativesMap } = await buildAlternativeQuestions(
      shop_id,
      notFound,
      foundIdsSet,
      isEnglish
    );

    const notFoundNameSet = new Set(
      notFound
        .map((nf) =>
          typeof nf.requested_name === "string" ? nf.requested_name.trim() : ""
        )
        .filter(Boolean)
    );

    const modelQuestions = normalizeIncomingQuestions(parsed?.questions, {
      preserveOptions: true,
    });

    const filteredModelQuestions = modelQuestions.filter((q) => {
      const nm = typeof q?.name === "string" ? q.name.trim() : "";
      return !nm || !notFoundNameSet.has(nm);
    });

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

        const altNames = (miss.alternatives || []).map((a) =>
          isEnglish
            ? (a.display_name_en && a.display_name_en.trim()) || a.name
            : a.name
        );
        if (isEnglish) {
          stockAltQuestions.push({
            name: reqName,
            question:
              altNames.length > 0
                ? `${reqName ?? "The item"} is short on stock (requested ${
                    miss.requested_amount
                  }, available ${miss.in_stock}). Would ${altNames.join(
                    " / "
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
                    " / "
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
     p.price,
     p.name AS name_he,
     p.display_name_en
      FROM order_item oi
      JOIN product p ON p.id = oi.product_id
      WHERE oi.order_id = ?`,
      [orderRes.order_id]
    );

    const productsForDisplay = rows.map((r) => {
      const units = Number(r.requested_units);
      const hasUnits = Number.isFinite(units) && units > 0;

      return {
        name: isEnglish
          ? (r.display_name_en && r.display_name_en.trim()) || r.name_he
          : r.name_he,
        amount: Number(r.amount),
        price: Number(r.price),
        ...(r.sold_by_weight ? { sold_by_weight: true } : {}),
        ...(hasUnits ? { units } : {}),
      };
    });

    const summaryLine =
      typeof parsed?.summary_line === "string" && parsed.summary_line.trim()
        ? parsed.summary_line.trim()
        : isEnglish
        ? "Great, here’s the order I understood from you:"
        : "יופי, זאת ההזמנה שהבנתי ממך:";

    const headerBlock = isEnglish
      ? [
          orderRes?.order_id ? `Order: #${orderRes.order_id}` : null,
          `Subtotal: *₪${(orderRes.totalPrice ?? 0).toFixed(2)}*`,
        ]
          .filter(Boolean)
          .join("\n")
      : [
          orderRes?.order_id ? `מספר הזמנה: #${orderRes.order_id}` : null,
          `סה״כ ביניים: *₪${(orderRes.totalPrice ?? 0).toFixed(2)}*`,
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
      "[ORD-CREATE] Items actually added to order:",
      JSON.stringify(orderRes.items || [], null, 2)
    );
    console.log(
      "[ORD-CREATE] Not found (no product matched):",
      JSON.stringify(notFound, null, 2)
    );
    console.log(
      "[ORD-CREATE] Alternatives for NOT-FOUND items (alternativesMap):",
      JSON.stringify(alternativesMap, null, 2)
    );
    console.log(
      "[ORD-CREATE] Insufficient items (with STOCK alternatives):",
      JSON.stringify(orderRes.insufficient || [], null, 2)
    );
    return finalMessage;
  },
};

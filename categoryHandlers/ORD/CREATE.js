const { chat } = require("../../config/openai");
const db = require("../../config/db");
const { getPromptFromDB } = require("../../repositories/prompt");
const { createOrderWithStockReserve } = require("../../utilities/orders");
const {
  parseModelAnswer,
  isEnglishSummary,
  normalizeIncomingQuestions,
  searchProducts,
  buildAlternativeQuestions,
  buildItemsBlock,
  buildQuestionsBlock,
} = require("../../utilities/products");

const PROMPT_CAT = "ORD";
const PROMPT_SUB = "CREATE";

async function getUnclassifiedHistory(customer_id, shop_id) {
  let [rows] = await db.query(
    `SELECT sender, status, message
       FROM chat
      WHERE customer_id = ? AND shop_id = ?
      ORDER BY created_at DESC, id DESC
      LIMIT 20`,
    [customer_id, shop_id]
  );

  const chunk = [];
  if (rows && rows.length && rows[0].status === "classified") {
    rows = rows.slice(1);
  }
  for (const r of rows) {
    if (r.status !== "unclassified") break;
    chunk.push(r);
  }
  chunk.reverse();

  const history = [];
  for (const r of chunk) {
    const content = (r.message || "").trim();
    if (!content) continue;
    if (r.sender === "customer") history.push({ role: "user", content });
    else if (r.sender === "bot") history.push({ role: "assistant", content });
  }
  return history;
}

module.exports = {
  async orderProducts({ message, customer_id, shop_id }) {
    if (typeof message !== "string" || !customer_id || !shop_id) {
      throw new Error(
        "orderProducts: missing or invalid message/customer_id/shop_id"
      );
    }

    const history = await getUnclassifiedHistory(customer_id, shop_id);
    const systemPrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);

    const answer = await chat({ message, history, systemPrompt });
    console.log("[model answer]", answer);

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

    const reqProducts = Array.isArray(parsed?.products) ? parsed.products : [];
    const isEnglish = isEnglishSummary(parsed?.summary_line);
      
    if (!reqProducts.length) {
      const normalizedQs = normalizeIncomingQuestions(parsed?.questions);
      const curated = { ...parsed, questions: normalizedQs };

      const emptyOrder = await createOrderWithStockReserve({
        shop_id,
        customer_id,
        lineItems: [], //empty
        status: "pending",
        payment_method: "other",
        delivery_address: null,
      });

      const summaryLine =
        typeof parsed?.summary_line === "string" && parsed.summary_line.trim()
          ? parsed.summary_line.trim()
          : isEnglish
          ? "To complete your order, I need a few clarifications:"
          : "כדי להשלים את ההזמנה חסרות כמה הבהרות:";

      const itemsBlock = "";
      const headerBlock = [
        emptyOrder?.order_id ? `מספר הזמנה: #${emptyOrder.order_id}` : null,
        "הזמנה נפתחה ללא פריטים בשלב זה.",
        `סה״כ ביניים: ₪${(emptyOrder.totalPrice ?? 0).toFixed(2)}`,
      ]
        .filter(Boolean)
        .join("\n");

      const questionsBlock = buildQuestionsBlock({
        questions: curated.questions,
        isEnglish,
      });

      const finalMessage = [
        summaryLine,
        itemsBlock,
        "",
        headerBlock,
        questionsBlock,
      ]
        .filter(Boolean)
        .join("\n");

      return finalMessage;
    }

    const { found, notFound } = await searchProducts(shop_id, reqProducts);

    const foundIdsSet = new Set(found.map((f) => f.product_id));
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

    const modelQuestions = normalizeIncomingQuestions(parsed?.questions);
    const filteredModelQuestions = modelQuestions.filter((q) => {
      const nm = typeof q?.name === "string" ? q.name.trim() : "";
      return !nm || !notFoundNameSet.has(nm);
    });

    const orderInputLineItems = found.map((f) => ({
      product_id: f.product_id,
      amount: f.requested_amount,
      requested_name: f.requested_name || null,
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
        const reqName = miss.requested_name || miss.matched_name || null;
        const altNames = (miss.alternatives || []).map((a) => a.name);
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
          });
        }
      }
    }

    const combinedQuestions = [
      ...filteredModelQuestions,
      ...altQuestions,
      ...stockAltQuestions,
    ];

    const productsForDisplay = (orderRes.items || []).map((it) => ({
      name: it.name,
      amount: it.amount,
      outputName: it.name,
      price: Number(it.price),
    }));

    const summaryLine =
      typeof parsed?.summary_line === "string" && parsed.summary_line.trim()
        ? parsed.summary_line.trim()
        : isEnglish
        ? "Great, here’s the order I understood from you:"
        : "יופי, זאת ההזמנה שהבנתי ממך:";

    const itemsBlock = buildItemsBlock({
      items: productsForDisplay,
      isEnglish,
      mode: "create",
    });

    const headerBlock = [
      orderRes?.order_id ? `מספר הזמנה: #${orderRes.order_id}` : null,
      `סה״כ ביניים: ₪${(orderRes.totalPrice ?? 0).toFixed(2)}`,
    ]
      .filter(Boolean)
      .join("\n");

    const questionsBlock = buildQuestionsBlock({
      questions: combinedQuestions,
      isEnglish,
    });

    const finalMessage = [
      summaryLine,
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

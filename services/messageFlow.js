const {
  routeByCategory,
  isValidCategorySub,
} = require("../categoryHandlers/router");
const { classifyIncoming } = require("../services/classifier");
const { wasSentBefore, saveChat } = require("../repositories/chat");
const { ensureCustomer } = require("../repositories/customer");
const {
  fetchOpenQuestions,
  fetchRecentClosedQuestions,
  buildOpenQuestionsContextForPrompt,
} = require("../utilities/openQuestions");
const { normalizeOutboundMessage } = require("../utilities/normalize");
const {
  getActiveOrder,
  getOrderItems,
  buildActiveOrderSignals,
} = require("../utilities/orders");
const { detectIsEnglish } = require("../utilities/lang");
const { checkIfToCancelOrder } = require("../categoryHandlers/ORD/CANCEL")
const maxPerProduct = 10;

async function processMessage(message, phone_number, shop_id) {
  const customer_id = await ensureCustomer(shop_id, phone_number);

  const wasSent = await wasSentBefore(customer_id, shop_id, message);
  if (wasSent) {
    return { skipSend: true };
  }

  const activeOrder = await getActiveOrder(customer_id, shop_id);
  const order_id = activeOrder ? activeOrder.id : null;
  const items = activeOrder ? await getOrderItems(activeOrder.id) : [];
  const sig = buildActiveOrderSignals(activeOrder, items);

  const cancelReply = await checkIfToCancelOrder({
    activeOrder,
    message,
    customer_id,
    shop_id,
    saveChat,
  });

  if (cancelReply) return cancelReply;

  const openQs = await fetchOpenQuestions(customer_id, shop_id, 7);
  const closedQs = await fetchRecentClosedQuestions(customer_id, shop_id, 5);

  const { parsed, replyText, history } = await classifyIncoming({
    message,
    customer_id,
    shop_id,
    sig,
    openQs,
    closedQs,
  });

  if (parsed.type === "clarify") {
    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "unclassified",
      message,
    });

    //bot's question
    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "unclassified",
      message: parsed.text,
    });
    return parsed.text || "לא התקבלה תשובה מהמודל.";
  } else if (parsed.type === "classified") {
    const { category, subcategory } = parsed;
    console.log(
      `[classification] category=${category}, subcategory=${subcategory}`
    );

    if (!isValidCategorySub(category, subcategory)) {
      const apology =
        "מצטערים, לא הצלחנו להבין את הבקשה. נשמח אם תנוסח שוב בקצרה";
      await saveChat({
        customer_id,
        shop_id,
        sender: "customer",
        status: "unclassified",
        message,
      });

      await saveChat({
        customer_id,
        shop_id,
        sender: "bot",
        status: "unclassified",
        message: apology,
      });
      return apology;
    }

    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message,
    });

    const openQsCtx = buildOpenQuestionsContextForPrompt(openQs);
    const isEnglish = detectIsEnglish(message);
    const ctx = {
      message,
      customer_id,
      shop_id,
      history,
      openQsCtx,
      activeOrder,
      order_id,
      items,
      isEnglish,
      maxPerProduct,
    };
    let botPayload = null;

    try {
      botPayload = await routeByCategory(category, subcategory, ctx);
    } catch (err) {
      console.error("[routeByCategory error]", err);
    }

    if (botPayload == null) {
      await saveChat({
        customer_id,
        shop_id,
        sender: "server",
        status: "close",
        message: "",
      });

      const fallback = "כרגע אין לנו תמיכה בבקשות מסוג זה";

      return fallback;
    }

    const botText = normalizeOutboundMessage(botPayload);
    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: botText,
    });

    return botPayload;
  } else {
    //if not classified and not clarify
    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "unclassified",
      message,
    });
    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "unclassified",
      message: replyText || "",
    });
    return replyText || "לא התקבלה תשובה מהמודל.";
  }
}

module.exports = {
  processMessage,
};

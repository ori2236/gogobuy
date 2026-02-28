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
const { checkIfToCancelOrder } = require("../categoryHandlers/ORD/CANCEL");
const { checkIfToCheckoutOrder } = require("../categoryHandlers/ORD/CHECKOUT");
const {
  sendWhatsAppText,
  sendWhatsAppTypingIndicator,
  sendWhatsAppMarkAsRead,
} = require("../config/whatsapp");
const {
  isSlowIntent,
  pickProgressText,
} = require("../services/sendProgressionMessage");

const maxPerProduct = 10;

async function processMessage(
  message,
  phone_number,
  shop_id,
  waMessageId = "",
  receivedAt = Date.now(),
) {
  const customer_id = await ensureCustomer(shop_id, phone_number);

  const wasSent = await wasSentBefore(customer_id, shop_id, message);
  if (wasSent) {
    return { skipSend: true };
  }

  if (waMessageId) {
    setTimeout(() => {
      sendWhatsAppMarkAsRead(waMessageId).catch((e) =>
        console.error("[wa markAsRead]", e?.response?.data || e),
      );
    }, 700);
  }

  const activeOrder = await getActiveOrder(customer_id, shop_id);
  const order_id = activeOrder ? activeOrder.id : null;
  const items = activeOrder ? await getOrderItems(activeOrder.id) : [];
  const sig = buildActiveOrderSignals(activeOrder, items);

  const checkoutReply = await checkIfToCheckoutOrder({
    activeOrder,
    message,
    customer_id,
    shop_id,
    saveChat,
  });

  if (checkoutReply) return checkoutReply;

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
      `[classification] category=${category}, subcategory=${subcategory}`,
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

    const slow = isSlowIntent(category, subcategory);

    let typingTimer = null;
    let progressTimer = null;
    let typingInterval = null;

    if (slow) {
      const elapsed = Date.now() - receivedAt;

      const pokeTyping = () => {
        if (!waMessageId) return;
        sendWhatsAppTypingIndicator(waMessageId).catch((e) =>
          console.error("[wa typing]", e?.response?.data || e),
        );
      };

      const startTypingLoop = () => {
        if (!waMessageId) return;
        pokeTyping();

        if (typingInterval) return;
        typingInterval = setInterval(pokeTyping, 20000);
      };

      const typingDelay = Math.max(0, 3000 - elapsed);
      typingTimer = setTimeout(startTypingLoop, typingDelay);

      const progressDelay = Math.max(0, 8000 - elapsed);

      progressTimer = setTimeout(() => {
        const progressText = pickProgressText(category, subcategory, isEnglish);

        sendWhatsAppText(phone_number, progressText)
          .catch((e) =>
            console.error("[wa progress text]", e?.response?.data || e),
          )
          .finally(() => {
            startTypingLoop();
          });
      }, progressDelay);
    }

    let botPayload = null;

    try {
      botPayload = await routeByCategory(category, subcategory, ctx);
    } catch (err) {
      console.error("[routeByCategory error]", err);
    } finally {
      if (typingTimer) clearTimeout(typingTimer);
      if (progressTimer) clearTimeout(progressTimer);
      if (typingInterval) clearInterval(typingInterval);
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

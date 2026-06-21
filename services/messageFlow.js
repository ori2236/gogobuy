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
const { botText } = require("../utilities/i18n");
const { checkIfToCancelOrder } = require("../categoryHandlers/ORD/CANCEL");
const { checkIfToCheckoutOrder } = require("../categoryHandlers/ORD/CHECKOUT");
const { answerOrderStatus } = require("../categoryHandlers/ORD/STATUS");
const { sendWhatsAppMarkAsRead } = require("../utilities/whatsapp");
const { startSlowProgression } = require("./sendProgressionMessage");
const { buildConversationGreetingPrefix } = require("./conversationGreeting");
const { handleSuggestionReply } = require("./orderSuggestions");
const { handleFulfillmentReply } = require("./fulfillment");
const {
  isMarketDayRequest,
  buildMarketDayPromotionsReply,
} = require("./marketDayPromotions");
const {
  handlePendingCustomerName,
  requestFullNameBeforeOrder,
  shouldRequireNameBeforeOrder,
} = require("./customerOnboarding");
const {
  handleCheckoutNudgeReply,
  attachCheckoutNudgeIfNeeded,
} = require("../utilities/checkoutNudge");
const db = require("../config/db");

const DEFAULT_MAX_PER_PRODUCT = 10;
let maxPerProductSchemaReadyPromise = null;

async function ensureMaxPerProductSchema() {
  if (!maxPerProductSchemaReadyPromise) {
    maxPerProductSchemaReadyPromise = (async () => {
      const [rows] = await db.query(
        `
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'shop'
          AND COLUMN_NAME = 'max_order_quantity_per_product'
        LIMIT 1
        `,
      );
      if (!rows.length) {
        await db.query(
          `ALTER TABLE shop ADD COLUMN max_order_quantity_per_product INT UNSIGNED NOT NULL DEFAULT 10`,
        );
      }
    })().catch((err) => {
      maxPerProductSchemaReadyPromise = null;
      throw err;
    });
  }
  return maxPerProductSchemaReadyPromise;
}

async function getMaxPerProductForShop(shop_id) {
  try {
    await ensureMaxPerProductSchema();
    const [[row]] = await db.query(
      `SELECT max_order_quantity_per_product FROM shop WHERE id = ? LIMIT 1`,
      [shop_id],
    );
    const n = Number(row?.max_order_quantity_per_product);
    return Number.isFinite(n) && n >= DEFAULT_MAX_PER_PRODUCT
      ? Math.floor(n)
      : DEFAULT_MAX_PER_PRODUCT;
  } catch (err) {
    console.error("[messageFlow.getMaxPerProductForShop]", err?.message || err);
    return DEFAULT_MAX_PER_PRODUCT;
  }
}

function isOrderStatusQuestion(message) {
  const raw = String(message || "").trim().toLowerCase();
  if (!raw) return false;

  const asksCartContent = /(מה\s+יש\s+(בסל|בעגלה|בהזמנה)|תראה\s+לי\s+(את\s+)?(הסל|העגלה)|סיכום\s+הזמנה|cart|basket|order summary)/i.test(raw);
  if (asksCartContent) return false;

  const asksGeneralDeliveryTiming = /(עד\s+איזה\s+שעה|שעת\s+קבלת|שעות\s+(הגעה|משלוחים)|באיזה\s+שעות\s+.*משלוחים|מתי\s+משלוחים\s+מגיעים|delivery\s+(hours|window|cutoff)|same[-\s]?day\s+delivery)/i.test(raw);
  if (asksGeneralDeliveryTiming) return false;

  return /(מה\s+עם\s+ההזמנה|איפה\s+ההזמנה|סטטוס\s+הזמנה|מה\s+קורה\s+עם\s+ההזמנה|המשלוח\s+יצא|שליח\s+בדרך|ההזמנה\s+מוכנה|מתי\s+מגיע|מתי\s+מוכן|נשלחה|נאספה|status|tracking|where\s+is\s+my\s+order|is\s+my\s+order\s+ready|delivery\s+sent)/i.test(raw);
}


function isWrongOpenOrderClarification(text) {
  const raw = String(text || "").toLowerCase();
  if (!raw) return false;
  return (
    raw.includes("יש כבר הזמנה פתוחה") ||
    raw.includes("כבר יש הזמנה פתוחה") ||
    raw.includes("already have an open order") ||
    raw.includes("there is already an open order")
  );
}

function buildNoActiveOrderReply(message) {
  return botText("noActiveOrder", detectIsEnglish(message));
}

async function saveAndReturnUnclassifiedReply({
  customer_id,
  shop_id,
  message,
  reply,
  greetingPrefix = "",
}) {
  const finalReply = prependTextToBotPayload(reply, greetingPrefix);
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
    message: normalizeOutboundMessage(finalReply),
  });

  return finalReply;
}

function prependTextToBotPayload(payload, prefix) {
  const cleanPrefix = String(prefix || "").trim();
  if (!cleanPrefix) return payload;

  const currentText = normalizeOutboundMessage(payload);
  const mergedText = currentText ? `${cleanPrefix}\n\n${currentText}` : cleanPrefix;

  if (!payload || typeof payload === "string") return mergedText;
  if (typeof payload !== "object") return mergedText;

  if (typeof payload.reply === "string") {
    return { ...payload, reply: mergedText };
  }

  if (typeof payload.message === "string") {
    return { ...payload, message: mergedText };
  }

  if (payload.message && typeof payload.message.reply === "string") {
    return {
      ...payload,
      message: {
        ...payload.message,
        reply: mergedText,
      },
    };
  }

  return { ...payload, reply: mergedText };
}

function buildGreetingAwareSaveChat(greetingPrefix) {
  let prefixedFirstBotMessage = false;

  return async function saveChatWithGreeting(args) {
    if (
      greetingPrefix &&
      !prefixedFirstBotMessage &&
      args &&
      args.sender === "bot" &&
      typeof args.message === "string" &&
      args.message.trim()
    ) {
      prefixedFirstBotMessage = true;
      return saveChat({
        ...args,
        message: normalizeOutboundMessage(
          prependTextToBotPayload(args.message, greetingPrefix),
        ),
      });
    }

    return saveChat(args);
  };
}

async function processMessage(
  message,
  phone_number,
  shop_id,
  waMessageId = "",
  receivedAt = Date.now(),
  businessPhoneNumberId = "",
) {
  const maxPerProduct = await getMaxPerProductForShop(shop_id);
  const customer_id = await ensureCustomer(shop_id, phone_number);

  const wasSent = await wasSentBefore(customer_id, shop_id, message);
  if (wasSent) {
    return { skipSend: true };
  }

  const conversationGreetingPrefix = await buildConversationGreetingPrefix({
    customer_id,
    shop_id,
    message,
  });
  const saveChatWithGreeting = buildGreetingAwareSaveChat(conversationGreetingPrefix);

  if (waMessageId) {
    setTimeout(() => {
      sendWhatsAppMarkAsRead(waMessageId, businessPhoneNumberId).catch((e) =>
        console.error("[wa markAsRead]", e?.response?.data || e),
      );
    }, 800);
  }

  if (isMarketDayRequest(message)) {
    const reply = await buildMarketDayPromotionsReply(shop_id);

    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message,
    });

    const finalReply = prependTextToBotPayload(reply, conversationGreetingPrefix);

    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: normalizeOutboundMessage(finalReply),
    });

    return finalReply;
  }

  let effectiveMessage = message;
  let onboardingPrefix = "";

  const pendingNameResult = await handlePendingCustomerName({
    customer_id,
    message,
  });

  if (pendingNameResult?.type === "need_name_reply") {
    return saveAndReturnUnclassifiedReply({
      customer_id,
      shop_id,
      message,
      reply: pendingNameResult.reply,
      greetingPrefix: conversationGreetingPrefix,
    });
  }

  if (pendingNameResult?.type === "name_saved") {
    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message,
    });

    effectiveMessage = pendingNameResult.pendingMessage || message;
    onboardingPrefix = pendingNameResult.prefix || "";
  }

  const activeOrder = await getActiveOrder(customer_id, shop_id);
  const order_id = activeOrder ? activeOrder.id : null;
  const items = activeOrder ? await getOrderItems(activeOrder.id) : [];
  const sig = buildActiveOrderSignals(activeOrder, items);
  const openQs = await fetchOpenQuestions(customer_id, shop_id, 20);

  const fulfillmentReply = await handleFulfillmentReply({
    message: effectiveMessage,
    customer_id,
    shop_id,
    activeOrder,
    openQs,
    saveChat: saveChatWithGreeting,
    maxPerProduct,
  });

  if (fulfillmentReply) {
    return prependTextToBotPayload(fulfillmentReply, conversationGreetingPrefix);
  }

  const checkoutReply = await checkIfToCheckoutOrder({
    activeOrder,
    message: effectiveMessage,
    customer_id,
    shop_id,
    saveChat: saveChatWithGreeting,
  });

  if (checkoutReply) {
    return prependTextToBotPayload(checkoutReply, conversationGreetingPrefix);
  }

  const cancelReply = await checkIfToCancelOrder({
    activeOrder,
    message: effectiveMessage,
    customer_id,
    shop_id,
    saveChat: saveChatWithGreeting,
  });

  if (cancelReply) {
    return prependTextToBotPayload(cancelReply, conversationGreetingPrefix);
  }

  const checkoutNudgeReply = await handleCheckoutNudgeReply({
    message: effectiveMessage,
    customer_id,
    shop_id,
    activeOrder,
    openQs,
    saveChat: saveChatWithGreeting,
  });

  if (checkoutNudgeReply) {
    return prependTextToBotPayload(checkoutNudgeReply, conversationGreetingPrefix);
  }

  const suggestionReply = await handleSuggestionReply({
    message: effectiveMessage,
    customer_id,
    shop_id,
    activeOrder,
    openQs,
    maxPerProduct,
  });

  if (suggestionReply) {
    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message: effectiveMessage,
    });

    const finalSuggestionReply = prependTextToBotPayload(
      suggestionReply,
      conversationGreetingPrefix,
    );

    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: normalizeOutboundMessage(finalSuggestionReply),
    });

    return finalSuggestionReply;
  }

  if (isOrderStatusQuestion(effectiveMessage)) {
    const statusReply = await answerOrderStatus({
      message: effectiveMessage,
      customer_id,
      shop_id,
      isEnglish: detectIsEnglish(effectiveMessage),
    });

    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message: effectiveMessage,
    });

    const finalStatusReply = prependTextToBotPayload(
      statusReply,
      conversationGreetingPrefix,
    );

    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: normalizeOutboundMessage(finalStatusReply),
    });

    return finalStatusReply;
  }

  const closedQs = await fetchRecentClosedQuestions(customer_id, shop_id, 5);

  const { parsed, replyText, history } = await classifyIncoming({
    message: effectiveMessage,
    customer_id,
    shop_id,
    sig,
    openQs,
    closedQs,
  });

  if (parsed.type === "clarify") {
    let clarifyText = parsed.text || botText("modelNoReply", detectIsEnglish(effectiveMessage));

    // Defensive state guard: the classifier prompt contains a clarification that is valid
    // only when ACTIVE_ORDER_EXISTS=true. In vague follow-ups after an automatic expiry,
    // the model may still output that text. Never tell the customer there is an open
    // order when the DB says there is no active order.
    if (!activeOrder && isWrongOpenOrderClarification(clarifyText)) {
      clarifyText = buildNoActiveOrderReply(effectiveMessage);
    }

    return saveAndReturnUnclassifiedReply({
      customer_id,
      shop_id,
      message: effectiveMessage,
      reply: clarifyText,
      greetingPrefix: conversationGreetingPrefix,
    });
  } else if (parsed.type === "classified") {
    const { category, subcategory } = parsed;
    console.log(
      `[classification] category=${category}, subcategory=${subcategory}`,
    );

    if (!activeOrder && category === "ORD" && subcategory === "MODIFY") {
      return saveAndReturnUnclassifiedReply({
        customer_id,
        shop_id,
        message: effectiveMessage,
        reply: buildNoActiveOrderReply(effectiveMessage),
        greetingPrefix: conversationGreetingPrefix,
      });
    }

    if (!isValidCategorySub(category, subcategory)) {
      const apology = botText("invalidIntent", detectIsEnglish(effectiveMessage));
      await saveChat({
        customer_id,
        shop_id,
        sender: "customer",
        status: "unclassified",
        message: effectiveMessage,
      });

      const finalApology = prependTextToBotPayload(
        apology,
        conversationGreetingPrefix,
      );

      await saveChat({
        customer_id,
        shop_id,
        sender: "bot",
        status: "unclassified",
        message: normalizeOutboundMessage(finalApology),
      });
      return finalApology;
    }

    if (category === "ORD" && subcategory === "CREATE") {
      const nameRequired = await shouldRequireNameBeforeOrder(customer_id);

      if (nameRequired) {
        const nameRequestReply = await requestFullNameBeforeOrder({
          customer_id,
          shop_id,
          message: effectiveMessage,
        });

        return saveAndReturnUnclassifiedReply({
          customer_id,
          shop_id,
          message: effectiveMessage,
          reply: nameRequestReply,
          greetingPrefix: conversationGreetingPrefix,
        });
      }
    }

    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "classified",
      message: effectiveMessage,
    });

    const openQsCtx = buildOpenQuestionsContextForPrompt(openQs);
    const isEnglish = detectIsEnglish(effectiveMessage);
    const ctx = {
      message: effectiveMessage,
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

    const stopProgression = startSlowProgression({
      category,
      subcategory,
      isEnglish,
      phone_number,
      waMessageId,
      receivedAt,
      typingAtMs: 2000,
      progressEveryMs: 8000,
      businessPhoneNumberId,
    });

    let botPayload = null;

    try {
      botPayload = await routeByCategory(category, subcategory, ctx);
    } catch (err) {
      console.error("[routeByCategory error]", err);
    } finally {
      stopProgression();
    }

    if (botPayload == null) {
      await saveChat({
        customer_id,
        shop_id,
        sender: "server",
        status: "close",
        message: "",
      });

      const fallback = prependTextToBotPayload(
        botText("unsupportedRequest", detectIsEnglish(effectiveMessage)),
        conversationGreetingPrefix,
      );
      return fallback;
    }

    botPayload = await attachCheckoutNudgeIfNeeded({
      botPayload,
      category,
      subcategory,
      customer_id,
      shop_id,
      isEnglish,
    });

    botPayload = prependTextToBotPayload(botPayload, onboardingPrefix);
    botPayload = prependTextToBotPayload(botPayload, conversationGreetingPrefix);

    const outboundText = normalizeOutboundMessage(botPayload);
    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message: outboundText,
    });

    if (botPayload && Array.isArray(botPayload.followUpMessages)) {
      for (const followUp of botPayload.followUpMessages) {
        const msg = typeof followUp === "string" ? followUp.trim() : "";
        if (!msg) continue;
        await saveChat({
          customer_id,
          shop_id,
          sender: "bot",
          status: "classified",
          message: msg,
        });
      }
    }

    return botPayload;
  } else {
    //if not classified and not clarify
    await saveChat({
      customer_id,
      shop_id,
      sender: "customer",
      status: "unclassified",
      message: effectiveMessage,
    });
    const finalUnclassifiedReply = prependTextToBotPayload(
      replyText || botText("modelNoReply", detectIsEnglish(effectiveMessage)),
      conversationGreetingPrefix,
    );

    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "unclassified",
      message: normalizeOutboundMessage(finalUnclassifiedReply),
    });
    return finalUnclassifiedReply;
  }
}

module.exports = {
  processMessage,
};

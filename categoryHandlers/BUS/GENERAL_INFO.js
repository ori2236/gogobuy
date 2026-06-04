const { chat } = require("../../config/openai");
const { getPromptFromDB } = require("../../repositories/prompt");
const {
  getShopInfo,
  getRegularHours,
  getSpecialHours,
  buildGeneralInfoContext,
} = require("../../repositories/shopInfo");

const PROMPT_CAT = "BUS";
const PROMPT_SUB = "GENERAL_INFO";

function getIsraelNow() {
  const now = new Date();

  const isoDate = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Jerusalem",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);

  const weekday = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Jerusalem",
    weekday: "long",
  }).format(now);

  return { isoDate, weekday };
}

module.exports = {
  async answerGeneralInfo({
    message,
    shop_id,
    history = [],
    isEnglish = false,
  }) {
    if (typeof message !== "string" || !message.trim()) {
      throw new Error("BUS.GENERAL_INFO: message is required");
    }
    if (!shop_id) {
      throw new Error("BUS.GENERAL_INFO: shop_id is required");
    }

    const dbPrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);
    const deliveryTimingPrompt = `

DELIVERY TIMING RULES
- The context may include DELIVERY_TIMING. Use it to answer questions about delivery cutoff time, whether an order can still arrive today, estimated delivery date, and customer arrival window.
- order_same_day_cutoff_time / cutoff_time means: delivery orders confirmed up to and including this time are intended for same-day delivery, if today is a business delivery day.
- Orders confirmed after the cutoff are scheduled for the next business delivery day.
- Friday and Saturday are never delivery days, even if the supermarket is open. Skip them when calculating the next delivery day.
- delivery_arrival_start_time and delivery_arrival_end_time are the estimated delivery arrival window for customers.
- If DELIVERY_TIMING contains message_he or message_en in the customer's language, you may use that wording directly.
- If the delivery timing fields are missing, clearly say the delivery timing information is unavailable.`;
    const systemPrompt = `${dbPrompt || "You answer branch/store information questions using only the provided context."}${deliveryTimingPrompt}`;

    const [info, regularHours, specialHours] = await Promise.all([
      getShopInfo(shop_id),
      getRegularHours(shop_id),
      getSpecialHours(shop_id),
    ]);

    if (!info) {
      return isEnglish
        ? "ℹ️ Sorry, I couldn't find branch information for this store."
        : "ℹ️ מצטערים, לא מצאתי מידע על הסניף הזה.";
    }

    const userContext = buildGeneralInfoContext({
      info,
      regularHours,
      specialHours,
      now: getIsraelNow(),
    });

    const answer = await chat({
      message,
      history,
      systemPrompt,
      userContext,
      prompt_cache_key: "bus_general_info_v1",
    });

    const replyText =
      typeof answer === "string"
        ? answer.trim()
        : answer && typeof answer.message === "string"
        ? answer.message.trim()
        : "";

    if (replyText) return replyText;

    return isEnglish
      ? "ℹ️ Sorry, I couldn't generate a branch information answer."
      : "ℹ️ מצטערים, לא הצלחתי לייצר תשובה לגבי פרטי הסניף.";
  },
};
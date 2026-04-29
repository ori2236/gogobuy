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

    const systemPrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);

    const [info, regularHours, specialHours] = await Promise.all([
      getShopInfo(shop_id),
      getRegularHours(shop_id),
      getSpecialHours(shop_id),
    ]);

    if (!info) {
      return isEnglish
        ? "Sorry, I couldn't find branch information for this store."
        : "מצטערים, לא מצאתי מידע על הסניף הזה.";
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
      ? "Sorry, I couldn't generate a branch information answer."
      : "מצטערים, לא הצלחתי לייצר תשובה לגבי פרטי הסניף.";
  },
};
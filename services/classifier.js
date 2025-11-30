const { chat } = require("../config/openai");
const {
  getPromptFromDB,
  buildClassifierContextHeader,
  buildOpenQuestionsContext,
} = require("../repositories/prompt");
const { getHistory } = require("../repositories/chat");

const CLASSIFIER_PROMPT_CAT = "initial";
const CLASSIFIER_PROMPT_SUB = "initial-classification";

function parseModelMessage(raw) {
  if (!raw || typeof raw !== "string") return { type: "raw", text: "" };

  const parts = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  // 0, <question>
  if (parts[0] === "0") {
    const rest = raw.slice(raw.indexOf(",") + 1).trim();
    return { type: "clarify", text: rest };
  }

  if (parts[0] === "1") {
    let category = "";
    let subcategory = "";

    if (parts.length >= 3) {
      category = (parts[1] || "").toUpperCase().trim();
      const subRaw = (parts[2] || "").trim();
      subcategory = subRaw.split(".").pop().toUpperCase().trim();
    } else if (parts.length === 2) {
      const m = parts[1].match(/^([A-Za-z]+)\s*[\.\-\/]\s*([A-Za-z_.]+)$/);
      if (m) {
        category = m[1].toUpperCase().trim();
        subcategory = m[2].toUpperCase().split(".").pop().trim();
      } else {
        category = parts[1].toUpperCase().trim();
        subcategory = "";
      }
    }

    return { type: "classified", category, subcategory };
  }

  return { type: "raw", text: raw };
}

async function classifyIncoming({
  message,
  customer_id,
  shop_id,
  sig,
  openQs,
  closedQs,
  maxHistoryMsgs = 7,
}) {
  let systemPromptBase = await getPromptFromDB(
    CLASSIFIER_PROMPT_CAT,
    CLASSIFIER_PROMPT_SUB
  );
  const contextHeader = buildClassifierContextHeader({ sig });
  const openQuestionsCtx = buildOpenQuestionsContext({ openQs, closedQs });
  const systemPrompt = [
    systemPromptBase,
    "",
    "=== STRUCTURED CONTEXT ===",
    contextHeader,
    openQuestionsCtx,
  ].join("\n");

  let history = await getHistory(customer_id, shop_id, 7);
  console.log("history:", history);
  const answer = await chat({ message, history, systemPrompt });
  const replyText =
    typeof answer === "string"
      ? answer
      : answer && typeof answer.message === "string"
      ? answer.message
      : "";

  const parsed = parseModelMessage(replyText);

  return { parsed, replyText, history };
}

module.exports = {
  classifyIncoming,
};

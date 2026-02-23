const { chat } = require("../config/openai");
const {
  getPromptFromDB,
  buildClassifierContextHeader,
  buildOpenQuestionsContext,
} = require("../repositories/prompt");
const { getHistory } = require("../repositories/chat");
const {
  buildIntentRouterSchema,
} = require("../categoryHandlers/ORD/schemas/intentRouter.schema");

// const CLASSIFIER_PROMPT_CAT = "initial";
// const CLASSIFIER_PROMPT_SUB = "initial-classification";

const CLASSIFIER_PROMPT_CAT = "IntentRouter";
const CLASSIFIER_PROMPT_SUB = "IntentRouter";

function parseModelMessage(raw) {
  if (!raw || typeof raw !== "string") return { type: "raw", text: "" };

  const line = raw.trim();

  // 0, <question>
  if (line.startsWith("0")) {
    const idx = line.indexOf(",");
    const rest = idx >= 0 ? line.slice(idx + 1).trim() : "";
    return { type: "clarify", text: rest };
  }

  // 1, <LABEL>
  if (line.startsWith("1")) {
    const parts = line
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    let category = "";
    let subcategory = "";

    if (parts.length === 2) {
      const label = parts[1].toUpperCase().trim(); // e.g. ORD.CREATE
      const m = label.match(/^([A-Z]+)\.([A-Z_]+)$/);
      if (m) {
        category = m[1];
        subcategory = m[2];
        return { type: "classified", category, subcategory };
      }

      category = label.replace(/[^A-Z]/g, "");
      return { type: "classified", category, subcategory: "" };
    }
    return { type: "raw", text: line };
  }

  return { type: "raw", text: line };
}

async function classifyIncoming({
  message,
  customer_id,
  shop_id,
  sig,
  openQs,
  closedQs,
  promptPayload,
}) {
  // const contextHeader = buildClassifierContextHeader({ sig });
  // const openQuestionsCtx = buildOpenQuestionsContext({ openQs, closedQs });

  let systemPromptBase = await getPromptFromDB(
    CLASSIFIER_PROMPT_CAT,
    CLASSIFIER_PROMPT_SUB,
  );

  const systemPrompt = systemPromptBase;

  let history = await getHistory(customer_id, shop_id);

  const answer = await chat({
    message: JSON.stringify(
      promptPayload || {
        current_user_message: message || "",
        active_order_exists: false,
        open_questions: [],
      },
    ),
    history,
    systemPrompt,
    response_format: {
      type: "json_schema",
      json_schema: await buildIntentRouterSchema(),
    },
  });

  console.log({answer})
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

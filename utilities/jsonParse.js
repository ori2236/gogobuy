function safeParseJson(txt) {
  if (typeof txt !== "string") throw new Error("safeParseJson expects string");
  try {
    return JSON.parse(txt);
  } catch {}
  const fenced = txt.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) {
    try {
      return JSON.parse(fenced[1]);
    } catch {}
  }
  const i = txt.indexOf("{");
  const j = txt.lastIndexOf("}");
  if (i !== -1 && j !== -1 && j > i) {
    const slice = txt.slice(i, j + 1);
    try {
      return JSON.parse(slice);
    } catch {}
  }
  throw new Error("Not valid JSON");
}

function parseModelAnswer(answer) {
  if (!answer) throw new Error("Empty model answer");

  if (
    typeof answer === "object" &&
    ("products" in answer || "summary_line" in answer || "questions" in answer)
  ) {
    return answer;
  }

  const content =
    (typeof answer?.choices?.[0]?.message?.content === "string" &&
      answer.choices[0].message.content) ||
    (typeof answer?.message === "string" && answer.message) ||
    (typeof answer?.content === "string" && answer.content) ||
    (typeof answer === "string" ? answer : null);

  if (!content) {
    if (typeof answer?.choices?.[0]?.message?.content === "object")
      return answer.choices[0].message.content;
    if (typeof answer?.content === "object") return answer.content;
    throw new Error("Unknown model answer shape");
  }
  return safeParseJson(content);
}

module.exports = {
  parseModelAnswer,
};
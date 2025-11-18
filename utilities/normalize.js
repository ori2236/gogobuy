function normalizeIncomingQuestions(qs, { preserveOptions = false } = {}) {
  if (!Array.isArray(qs)) return [];
  const out = [];
  for (const q of qs) {
    if (!q) continue;
    if (typeof q === "string" && q.trim()) {
      const base = { name: null, question: q.trim() };
      out.push(base);
    } else if (
      typeof q === "object" &&
      typeof q.question === "string" &&
      q.question.trim()
    ) {
      const item = {
        name: typeof q.name === "string" ? q.name : null,
        question: q.question.trim(),
      };
      if (preserveOptions && Array.isArray(q.options)) {
        item.options = q.options.map((s) => String(s).trim()).filter(Boolean);
      }
      out.push(item);
    }
  }
  return out;
}

function normalizeOutboundMessage(payload) {
  if (!payload) return "";
  if (typeof payload === "string") return payload.trim();

  if (payload && typeof payload.reply === "string") {
    return payload.reply.trim();
  }
  if (payload && typeof payload.message === "string") {
    return payload.message.trim();
  }
  if (payload && payload.message && typeof payload.message.reply === "string") {
    return payload.message.reply.trim();
  }

  try {
    return JSON.stringify(payload);
  } catch {
    return String(payload);
  }
}

module.exports = {
  normalizeIncomingQuestions,
  normalizeOutboundMessage,
};
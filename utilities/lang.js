function isEnglishMessage(summaryLine) {
  if (typeof summaryLine !== "string") return false;
  const hasLatin = /[A-Za-z]/.test(summaryLine);
  const hasHeb = /[\u0590-\u05FF]/.test(summaryLine);
  if (hasLatin && !hasHeb) return true;
  if (
    summaryLine.startsWith("Great, here’s") ||
    summaryLine.startsWith("To complete your order")
  )
    return true;
  return false;
}

function detectIsEnglish(text) {
  if (!text) return false;
  const hasHeb = /[\u0590-\u05FF]/.test(text);
  const hasLat = /[A-Za-z]/.test(text);
  if (hasHeb && !hasLat) return false;
  if (hasLat && !hasHeb) return true;

  if (hasHeb && hasLat) {
    const heCount = (text.match(/[\u0590-\u05FF]/g) || []).length;
    const enCount = (text.match(/[A-Za-z]/g) || []).length;
    return enCount > heCount;
  }
  //default - hebrew
  return false;
}

module.exports = {
  isEnglishMessage,
  detectIsEnglish,
};

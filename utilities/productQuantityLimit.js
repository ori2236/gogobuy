const { boldProductName } = require("./productMessaging");

function normalizeMaxPerProduct(maxPerProduct, fallback = 10) {
  const n = Number(maxPerProduct);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function formatQuantity(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value ?? "");
  return n.toFixed(3).replace(/\.?0+$/, "");
}

function getQuantityUnitLabel({ soldByWeight, isEnglish }) {
  if (soldByWeight) return isEnglish ? "kg" : "ק״ג";
  return isEnglish ? "units" : "יח׳";
}

function buildQuantityLimitWarningBlock({ warnings, isEnglish }) {
  const rows = (Array.isArray(warnings) ? warnings : []).filter(
    (w) => w && Number(w.capped) > 0,
  );
  if (!rows.length) return "";

  if (isEnglish) {
    return [
      "⚠️ I adjusted the quantity to the branch limit:",
      ...rows.map((w) => {
        const unit = getQuantityUnitLabel({
          soldByWeight: !!w.sold_by_weight,
          isEnglish: true,
        });
        return `• ${boldProductName(w.name)}: you asked for ${formatQuantity(w.original)} ${unit}, so I added ${formatQuantity(w.capped)} ${unit}.`;
      }),
    ].join("\n");
  }

  return [
    "⚠️ עדכנתי את הכמות למקסימום שמותר בסניף הזה:",
    ...rows.map((w) => {
      const unit = getQuantityUnitLabel({
        soldByWeight: !!w.sold_by_weight,
        isEnglish: false,
      });
      return `• ${boldProductName(w.name)}: ביקשת ${formatQuantity(w.original)} ${unit}, שמתי ${formatQuantity(w.capped)} ${unit}.`;
    }),
  ].join("\n");
}

module.exports = {
  normalizeMaxPerProduct,
  formatQuantity,
  getQuantityUnitLabel,
  buildQuantityLimitWarningBlock,
};

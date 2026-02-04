const { normalizeIncomingQuestions } = require("../utilities/normalize");
const { promoToShortText } = require("../utilities/promotions");

const bold = (s) => (s ? `*${s}*` : "");

function buildItemsBlock({ items, isEnglish, mode }) {
  if (!Array.isArray(items) || !items.length) return "";

  const lines = [];
  lines.push(
    isEnglish
      ? mode === "create"
        ? "Items added to your order:"
        : "Items in your order now:"
      : mode === "create"
        ? "המוצרים שהוספתי להזמנה:"
        : "המוצרים שכעת בהזמנה:",
  );

  for (const it of items) {
    if (!it || !it.name) continue;

    const name = it.name;
    const qty = Number(it.amount);
    if (!Number.isFinite(qty) || qty <= 0) continue;

    const soldByWeightRaw = it.sold_by_weight;
    const isWeight =
      soldByWeightRaw === true ||
      soldByWeightRaw === 1 ||
      soldByWeightRaw === "1";

    const unitsRaw = it.units ?? it.requested_units ?? it.requestedUnits;
    const unitsNum = Number(unitsRaw);
    const units =
      isWeight && Number.isFinite(unitsNum) && unitsNum > 0 ? unitsNum : null;

    const unitPrice = Number(it.unit_price ?? it.price);
    const hasUnitPrice = Number.isFinite(unitPrice);

    const lineTotalRaw = Number(it.line_total);
    const hasLineTotal = Number.isFinite(lineTotalRaw);

    const lineTotal = hasLineTotal
      ? lineTotalRaw
      : hasUnitPrice
        ? Number((qty * unitPrice).toFixed(2))
        : null;

    if (!Number.isFinite(lineTotal)) continue;

    const hasPromo = it.promo_id != null;
    const promo = it.promo || null;
    const promoText = promoToShortText({
      promo,
      unitPrice,
      isEnglish,
      isWeight,
    });
    const promoBadge = promoText ? ` ${bold(promoText)}` : "";

    if (!isWeight) {
      // unit items
      if (qty === 1) {
        // show final line price (maybe promo)
        lines.push(`• ${name} - ₪${lineTotal.toFixed(2)}${promoBadge}`);
      } else {
        const eachSuffix = isEnglish ? "each" : "ליח'";

        // If promo exists, the effective per-unit can differ (bundle etc.)
        const effectiveEach = lineTotal / qty;
        const eachText = hasPromo
          ? isEnglish
            ? `avg ₪${effectiveEach.toFixed(2)} ${eachSuffix}`
            : `ממוצע ₪${effectiveEach.toFixed(2)} ${eachSuffix}`
          : `₪${unitPrice.toFixed(2)} ${eachSuffix}`;

        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(
            2,
          )} (${eachText})${promoBadge}`,
        );
      }
      continue;
    }

    // weight items (kg)
    if (units) {
      if (isEnglish) {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)}${
            hasUnitPrice
              ? ` (₪${unitPrice.toFixed(2)} per kg, approx for ${units} units)`
              : ""
          }${promoBadge}`,
        );
      } else {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)}${
            hasUnitPrice
              ? ` (₪${unitPrice.toFixed(2)} לק"ג, מחיר משוערך ל${units} יחידות)`
              : ""
          }${promoBadge}`,
        );
      }
    } else {
      if (isEnglish) {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)}${
            hasUnitPrice ? ` (₪${unitPrice.toFixed(2)} per kg)` : ""
          }${promoBadge}`,
        );
      } else {
        lines.push(
          `• ${name} × ${qty} - ₪${lineTotal.toFixed(2)}${
            hasUnitPrice ? ` (₪${unitPrice.toFixed(2)} לק"ג)` : ""
          }${promoBadge}`,
        );
      }
    }
  }

  return lines.join("\n");
}

function buildQuestionsBlock({ questions, isEnglish }) {
  const qs = normalizeIncomingQuestions(questions);
  if (!qs.length) return "";
  const lines = [];
  lines.push("");
  lines.push(isEnglish ? "Questions:" : "שאלות:");
  for (const q of qs) lines.push(`• ${q.question}`);
  return lines.join("\n");
}

module.exports = {
  buildItemsBlock,
  buildQuestionsBlock,
};

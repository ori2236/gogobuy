function round2(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return null;
  return Math.round(x * 100) / 100;
}

function normalizePositiveQty(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function normalizePositiveInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function splitQtyByPromoLimit(qty, promo, kind) {
  const totalQty = normalizePositiveQty(qty);
  if (!totalQty) return { promoQty: 0, regularQty: 0 };

  // max_discounted_qty is kept as the DB column for backwards compatibility,
  // but the business meaning is "maximum promotion applications / uses".
  // For bundle deals, one use covers bundle_buy_qty units.
  // For single-unit discounts (percent/amount/fixed price), one use covers one unit/quantity.
  const maxUses = normalizePositiveInt(promo?.max_discounted_qty);
  if (!maxUses) return { promoQty: totalQty, regularQty: 0 };

  let qtyPerUse = 1;
  if (String(kind || "").toUpperCase() === "BUNDLE") {
    const buyQty = normalizePositiveQty(promo?.bundle_buy_qty);
    if (buyQty) qtyPerUse = buyQty;
  }

  const limitQty = maxUses * qtyPerUse;
  const promoQty = Math.min(totalQty, limitQty);
  return {
    promoQty,
    regularQty: Math.max(0, totalQty - promoQty),
  };
}

function calcLineTotalWithPromo({ unitPrice, amount, soldByWeight, promo }) {
  const base = Number(unitPrice);
  const qty = Number(amount);

  if (!Number.isFinite(base) || !Number.isFinite(qty) || qty <= 0) {
    return { lineTotal: null, promo_id: null };
  }

  if (!promo) {
    return { lineTotal: round2(base * qty), promo_id: null };
  }

  const kind = String(promo.kind || "").toUpperCase();
  const { promoQty, regularQty } = splitQtyByPromoLimit(qty, promo, kind);
  const regularTotal = regularQty * base;

  if (kind === "PERCENT_OFF") {
    const p = Number(promo.percent_off);
    if (!Number.isFinite(p)) {
      return { lineTotal: round2(base * qty), promo_id: promo.id };
    }
    const newUnit = base * (1 - p / 100);
    return { lineTotal: round2(newUnit * promoQty + regularTotal), promo_id: promo.id };
  }

  if (kind === "AMOUNT_OFF") {
    const off = Number(promo.amount_off);
    if (!Number.isFinite(off)) {
      return { lineTotal: round2(base * qty), promo_id: promo.id };
    }
    const newUnit = Math.max(0, base - off);
    return { lineTotal: round2(newUnit * promoQty + regularTotal), promo_id: promo.id };
  }

  if (kind === "FIXED_PRICE") {
    const fp = Number(promo.fixed_price);
    if (!Number.isFinite(fp)) {
      return { lineTotal: round2(base * qty), promo_id: promo.id };
    }
    const newUnit = Math.max(0, fp);
    return { lineTotal: round2(newUnit * promoQty + regularTotal), promo_id: promo.id };
  }

  if (kind === "BUNDLE") {
    const buyQty = Number(promo.bundle_buy_qty);
    const pay = Number(promo.bundle_pay_price);

    if (
      !Number.isFinite(buyQty) ||
      buyQty <= 0 ||
      !Number.isFinite(pay) ||
      pay < 0
    ) {
      return { lineTotal: round2(base * qty), promo_id: promo.id };
    }

    if (soldByWeight) {
      const bundles = Math.floor(promoQty / buyQty);
      const remainder = promoQty - bundles * buyQty;
      const total = bundles * pay + remainder * base + regularTotal;
      return { lineTotal: round2(total), promo_id: promo.id };
    }

    const N = Math.max(1, Math.ceil(promoQty));
    const bundles = Math.floor(N / buyQty);
    const remainder = N - bundles * buyQty;
    const total = bundles * pay + remainder * base + regularTotal;
    return { lineTotal: round2(total), promo_id: promo.id };
  }

  return { lineTotal: round2(base * qty), promo_id: promo.id };
}

module.exports = {
  round2,
  calcLineTotalWithPromo,
};

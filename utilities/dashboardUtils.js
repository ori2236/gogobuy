function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function clampInt(n, min, max, fallback) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(x)));
}

function parseShopId(req) {
  const q = Number(req.query.shop_id);
  const b = Number(req.body?.shop_id);
  const shopId =
    Number.isFinite(q) && q > 0 ? q : Number.isFinite(b) && b > 0 ? b : 1;
  return shopId;
}

function normalizeWaNumber(phone) {
  let digits = String(phone || "").replace(/[^\d]/g, "");
  if (!digits) return null;

  digits = digits.replace(/^0+/, "0");

  if (digits.startsWith("0") && digits.length === 10) {
    return "972" + digits.slice(1);
  }

  if (digits.startsWith("972") && digits.length >= 11) {
    return digits;
  }

  if (digits.length === 9 && digits.startsWith("5")) {
    return "972" + digits;
  }

  return digits;
}

module.exports = {
  isNonEmptyString,
  clampInt,
  parseShopId,
  normalizeWaNumber,
};

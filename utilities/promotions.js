const fmtMoney = (n) => {
  const x = Number(n);
  return Number.isFinite(x) ? x.toFixed(2) : null;
};

const fmtQtyShort = (n) => {
  const x = Number(n);
  if (!Number.isFinite(x)) return null;
  const s = x.toFixed(3).replace(/\.?0+$/, "");
  return s;
};

function promoToShortText({ promo, unitPrice, isEnglish, isWeight }) {
  if (!promo || !promo.kind) return "";

  if (promo.kind === "BUNDLE") {
    const buy = fmtQtyShort(promo.bundle_buy_qty);
    const pay = fmtMoney(promo.bundle_pay_price);
    if (!buy || !pay) return "";
    if (isEnglish) return `${buy} for ₪${pay}`;
    return isWeight ? `${buy} ק״ג ב-₪${pay}` : `${buy} ב-₪${pay}`;
  }

  if (promo.kind === "FIXED_PRICE") {
    const up = fmtMoney(unitPrice);
    if (isEnglish) return up ? `instead of ₪${up} each` : ``;
    return up ? `במקום ₪${up} ליח'` : ``;
  }

  if (promo.kind === "PERCENT_OFF") {
    const pct = fmtQtyShort(promo.percent_off);
    if (!pct) return "";
    return isEnglish ? `${pct}% off` : `${pct}% הנחה`;
  }

  if (promo.kind === "AMOUNT_OFF") {
    const off = fmtMoney(promo.amount_off);
    if (!off) return "";
    return isEnglish ? `₪${off} off each` : `הנחה ₪${off} ליח'`;
  }

  return isEnglish ? "promotion" : "מבצע";
}

module.exports = {
  promoToShortText
};

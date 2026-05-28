const { roundTo } = require("./decimal");
const { buildQuestionsBlock } = require("./messageBuilders");

const DEFAULT_EMOJI = "🛒";
const PROMO_INDENT = "       ";

const STATUS_EMOJIS = {
  pending: "🎒",
  checkout_pending: "🧾",
  confirmed: "✅",
  preparing: "👨‍🍳",
  ready: "📦",
  delivering: "🛵",
  completed: "🎉",
  cancel_pending: "⚠️",
};

const GREETINGS_HE = [
  "שמחים שזכינו לשרת אותך 😊",
  "תודה שבחרת להזמין אצלנו 🙏",
  "כיף שבחרת בנו לקנייה שלך 😊",
  "אנחנו כבר דואגים להכין לך הכול בצורה הטובה ביותר 🛍️",
  "שמחים להכין עבורך את ההזמנה 🧡",
];

const GREETINGS_EN = [
  "Happy to serve you 😊",
  "Thank you for choosing us 🙏",
  "We’re glad you ordered with us 🛍️",
  "We’ll make sure your order is handled with care 😊",
  "Thanks for shopping with us 🧡",
];

const lastGreetingIndexByLang = {
  he: null,
  en: null,
};

function cleanEmoji(value) {
  const s = String(value ?? "").trim();
  if (!s || /^[?\uFFFD\s]+$/u.test(s)) return DEFAULT_EMOJI;
  return Array.from(s).slice(0, 8).join("");
}

function fmtMoney(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(2) : "0.00";
}

function fmtQty(value, maxDigits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return n
    .toFixed(maxDigits)
    .replace(/\.0+$/, "")
    .replace(/(\.\d*?)0+$/, "$1");
}


function formatOrderStatus(status, isEnglish) {
  const s = String(status || "").toLowerCase();

  const mapEn = {
    pending: "Pending",
    checkout_pending: "Waiting for checkout confirmation",
    confirmed: "Confirmed",
    preparing: "Preparing",
    ready: "Ready",
    delivering: "Delivering",
    completed: "Completed",
    cancel_pending: "Waiting for cancel confirmation",
  };

  const mapHe = {
    pending: "פתוחה",
    checkout_pending: "בהמתנה לאישור סיום הזמנה",
    confirmed: "אושרה",
    preparing: "בהכנה",
    ready: "מוכנה",
    delivering: "במשלוח",
    completed: "הושלמה",
    cancel_pending: "בהמתנה לאישור ביטול",
  };

  const txt = (isEnglish ? mapEn : mapHe)[s];
  return txt || (isEnglish ? s.toUpperCase() : s);
}

function pickGreeting(isEnglish) {
  const lang = isEnglish ? "en" : "he";
  const greetings = isEnglish ? GREETINGS_EN : GREETINGS_HE;

  if (greetings.length === 1) return greetings[0];

  const last = lastGreetingIndexByLang[lang];
  let idx = Math.floor(Math.random() * greetings.length);

  if (idx === last) {
    idx = (idx + 1) % greetings.length;
  }

  lastGreetingIndexByLang[lang] = idx;
  return greetings[idx];
}

function getStatusEmoji(status) {
  const key = String(status || "").toLowerCase();
  return STATUS_EMOJIS[key] || "📌";
}

function buildQuantitySuffix(item, isEnglish) {
  const amount = Number(item?.amount);
  if (!Number.isFinite(amount) || amount <= 0) return "";

  const isWeight =
    item?.sold_by_weight === true ||
    item?.sold_by_weight === 1 ||
    item?.sold_by_weight === "1";

  const unitsRaw = item?.units ?? item?.requested_units ?? item?.requestedUnits;
  const unitsNum = Number(unitsRaw);
  const units =
    isWeight && Number.isFinite(unitsNum) && unitsNum > 0 ? unitsNum : null;

  if (isWeight) {
    const kg = fmtQty(amount, 3);
    if (!kg) return "";

    if (units) {
      const unitsText = fmtQty(units, 2);
      return isEnglish
        ? ` (${kg} kg / ${unitsText} ${units === 1 ? "unit" : "units"})`
        : ` (${kg} ק״ג / ${unitsText} יח׳)`;
    }

    return isEnglish ? ` (${kg} kg)` : ` (${kg} ק״ג)`;
  }

  if (amount === 1) return "";

  const qty = Number.isInteger(amount) ? String(amount) : fmtQty(amount, 3);
  return isEnglish ? ` (${qty} units)` : ` (${qty} יח׳)`;
}

function buildPromoLine({ promo, isEnglish, isWeight }) {
  if (!promo || !promo.kind) return "";

  const kind = String(promo.kind || "").toUpperCase();

  if (kind === "PERCENT_OFF") {
    const pct = fmtQty(promo.percent_off, 2);
    if (!pct) return "";
    return isEnglish
      ? `${PROMO_INDENT}🏷️ Purchased with ${pct}% off!`
      : `${PROMO_INDENT}🏷️ נרכש ב-${pct}% הנחה!`;
  }

  if (kind === "AMOUNT_OFF") {
    const off = Number(promo.amount_off);
    if (!Number.isFinite(off)) return "";
    return isEnglish
      ? `${PROMO_INDENT}🏷️ Promotion applied: ₪${fmtMoney(off)} off per unit!`
      : `${PROMO_INDENT}🏷️ נרכש במבצע: ₪${fmtMoney(off)} הנחה ליח׳!`;
  }

  if (kind === "FIXED_PRICE") {
    const fixed = Number(promo.fixed_price);
    if (!Number.isFinite(fixed)) return "";
    const unitLabel = isEnglish
      ? isWeight
        ? "per kg"
        : "per unit"
      : isWeight
        ? "לק״ג"
        : "ליח׳";
    return isEnglish
      ? `${PROMO_INDENT}🏷️ Promotion applied: fixed price ₪${fmtMoney(fixed)} ${unitLabel}!`
      : `${PROMO_INDENT}🏷️ נרכש במבצע: מחיר קבוע ₪${fmtMoney(fixed)} ${unitLabel}!`;
  }

  if (kind === "BUNDLE") {
    const buyQty = fmtQty(promo.bundle_buy_qty, 3);
    const pay = Number(promo.bundle_pay_price);
    if (!buyQty || !Number.isFinite(pay)) return "";
    const qtyLabel = isEnglish
      ? isWeight
        ? "kg"
        : "units"
      : isWeight
        ? "ק״ג"
        : "יח׳";
    return isEnglish
      ? `${PROMO_INDENT}🏷️ Promotion applied: ${buyQty} ${qtyLabel} for ₪${fmtMoney(pay)}!`
      : `${PROMO_INDENT}🏷️ נרכש במבצע: ${buyQty} ${qtyLabel} ב-₪${fmtMoney(pay)}!`;
  }

  return isEnglish
    ? `${PROMO_INDENT}🏷️ Promotion applied!`
    : `${PROMO_INDENT}🏷️ נרכש במבצע!`;
}

function normalizeOrderItemForSummary(item) {
  if (!item || !item.name) return null;

  const amount = Number(item.amount);
  const lineTotal = Number(item.line_total ?? item.price_total ?? item.total);

  if (!Number.isFinite(amount) || amount <= 0) return null;
  if (!Number.isFinite(lineTotal)) return null;

  const isWeight =
    item.sold_by_weight === true ||
    item.sold_by_weight === 1 ||
    item.sold_by_weight === "1";

  return {
    ...item,
    amount,
    line_total: lineTotal,
    unit_price: Number(item.unit_price ?? item.price),
    sold_by_weight: isWeight,
    emoji: cleanEmoji(item.emoji),
  };
}

function calcTotalsFromItems(items) {
  let totalWithPromos = 0;
  let totalNoPromos = 0;

  for (const item of items) {
    const lineTotal = Number(item.line_total);
    if (Number.isFinite(lineTotal)) {
      totalWithPromos = roundTo(totalWithPromos + lineTotal, 2);
    }

    const unitPrice = Number(item.unit_price ?? item.price);
    const amount = Number(item.amount);
    if (Number.isFinite(unitPrice) && Number.isFinite(amount)) {
      totalNoPromos = roundTo(totalNoPromos + roundTo(unitPrice * amount, 2), 2);
    }
  }

  const savings = roundTo(totalNoPromos - totalWithPromos, 2);

  return {
    totalWithPromos,
    totalNoPromos,
    savings: Number.isFinite(savings) && savings >= 0.01 ? savings : 0,
  };
}

function buildOrderSummaryMessage({
  orderId,
  status = "pending",
  items = [],
  isEnglish = false,
  shopName = process.env.ORDER_SUMMARY_SHOP_NAME || "סופר גלסנר",
  totalWithPromos = null,
  totalNoPromos = null,
  savings = null,
  questions = [],
  greeting = null,
  fulfillmentMethod = null,
  deliveryAddress = null,
  deliveryFee = null,
} = {}) {
  const normalizedItems = (Array.isArray(items) ? items : [])
    .map(normalizeOrderItemForSummary)
    .filter(Boolean);

  const calculated = calcTotalsFromItems(normalizedItems);

  const hasExplicitTotal =
    totalWithPromos !== null &&
    totalWithPromos !== undefined &&
    Number.isFinite(Number(totalWithPromos));

  const hasExplicitNoPromosTotal =
    totalNoPromos !== null &&
    totalNoPromos !== undefined &&
    Number.isFinite(Number(totalNoPromos));

  const hasExplicitSavings =
    savings !== null && savings !== undefined && Number.isFinite(Number(savings));

  const finalTotal = hasExplicitTotal
    ? Number(totalWithPromos)
    : calculated.totalWithPromos;

  const finalNoPromos = hasExplicitNoPromosTotal
    ? Number(totalNoPromos)
    : calculated.totalNoPromos;

  const finalSavingsRaw = hasExplicitSavings
    ? Number(savings)
    : roundTo(finalNoPromos - finalTotal, 2);

  const finalSavings =
    Number.isFinite(finalSavingsRaw) && finalSavingsRaw >= 0.01
      ? finalSavingsRaw
      : 0;

  const lines = [];

  lines.push(
    isEnglish
      ? `🛒 Your order summary from ${shopName}:`
      : `🛒 סיכום ההזמנה שלך מ${shopName}:`,
  );
  lines.push(greeting || pickGreeting(isEnglish));
  lines.push("");
  lines.push(isEnglish ? "📦 Products in your cart:" : "📦 המוצרים בסל שלך:");
  lines.push("");

  for (const item of normalizedItems) {
    const name = String(item.name || "").trim();
    if (!name) continue;

    const emoji = cleanEmoji(item.emoji);
    const qtySuffix = buildQuantitySuffix(item, isEnglish);
    lines.push(`${emoji} ${name}${qtySuffix} — ₪${fmtMoney(item.line_total)}`);

    const promoLine = buildPromoLine({
      promo: item.promo,
      isEnglish,
      isWeight: item.sold_by_weight === true,
    });
    if (promoLine) lines.push(promoLine);
  }

  lines.push("");

  const normalizedFulfillment = String(fulfillmentMethod || "").toLowerCase();
  const normalizedDeliveryFee = Number(deliveryFee || 0);
  const hasDeliveryFee =
    normalizedFulfillment === "delivery" &&
    Number.isFinite(normalizedDeliveryFee) &&
    normalizedDeliveryFee > 0;

  if (normalizedFulfillment === "delivery") {
    const productsSubtotal = hasDeliveryFee
      ? Math.max(0, roundTo(finalTotal - normalizedDeliveryFee, 2))
      : calculated.totalWithPromos;

    lines.push(isEnglish ? "📦 Receiving method: home delivery" : "📦 אופן קבלה: משלוח עד הבית");
    if (deliveryAddress) {
      lines.push(
        isEnglish
          ? `📍 Delivery address: ${deliveryAddress}`
          : `📍 כתובת למשלוח: ${deliveryAddress}`,
      );
    }
    lines.push(
      isEnglish
        ? `🧾 Products subtotal: ₪${fmtMoney(productsSubtotal)}`
        : `🧾 סה״כ מוצרים: ₪${fmtMoney(productsSubtotal)}`,
    );
    if (hasDeliveryFee) {
      lines.push(
        isEnglish
          ? `🏍️ Delivery fee: ₪${fmtMoney(normalizedDeliveryFee)}`
          : `🏍️ דמי משלוח: ₪${fmtMoney(normalizedDeliveryFee)}`,
      );
    }
  } else if (normalizedFulfillment === "pickup") {
    lines.push(isEnglish ? "🛍️ Receiving method: store pickup" : "🛍️ אופן קבלה: איסוף עצמי מהחנות");
  }

  lines.push(
    isEnglish
      ? `💰 Total to pay: ₪${fmtMoney(finalTotal)}`
      : `💰 סה״כ לתשלום: ₪${fmtMoney(finalTotal)}`,
  );

  if (finalSavings >= 0.01) {
    lines.push(
      isEnglish
        ? `(You saved ₪${fmtMoney(finalSavings)} on promotions! 🎉)`
        : `(חסכת ₪${fmtMoney(finalSavings)} במבצעים! 🎉)`,
    );
  }

  lines.push("");

  if (orderId) {
    const statusText = formatOrderStatus(status, isEnglish);
    const statusEmoji = getStatusEmoji(status);
    lines.push(
      isEnglish
        ? `${statusEmoji} Order #${orderId} | Status: ${statusText}`
        : `${statusEmoji} הזמנה #${orderId} | סטטוס: ${statusText}`,
    );
  }

  const summary = lines.join("\n").trim();
  const questionsBlock = buildQuestionsBlock({ questions, isEnglish });

  return [summary, questionsBlock].filter(Boolean).join("\n");
}

module.exports = {
  STATUS_EMOJIS,
  buildOrderSummaryMessage,
  buildPromoLine,
  pickGreeting,
};

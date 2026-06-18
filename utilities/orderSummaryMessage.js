const { roundTo } = require("./decimal");
const { buildQuestionsBlock } = require("./messageBuilders");
const { boldProductName } = require("./productMessaging");

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

function fmtMoneyCompact(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(2).replace(/\.00$/, "") : "0";
}

function fmtQty(value, maxDigits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return n
    .toFixed(maxDigits)
    .replace(/\.0+$/, "")
    .replace(/(\.\d*?)0+$/, "$1");
}



function parseJsonMaybe(raw) {
  if (!raw) return {};
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
}

function itemKey(item) {
  const orderItemId = Number(item?.order_item_id ?? item?.orderItemId);
  if (Number.isInteger(orderItemId) && orderItemId > 0) return `oi:${orderItemId}`;
  const productId = Number(item?.product_id ?? item?.productId);
  if (Number.isInteger(productId) && productId > 0) return `p:${productId}`;
  return null;
}

function fmtGroupPromoLine(app, isEnglish) {
  const meta = parseJsonMaybe(app?.metadata);
  const title = String(app?.title || (isEnglish ? "Product group promotion" : "מבצע קבוצת מוצרים")).trim();
  const buyQty = fmtQty(app?.bundle_buy_qty ?? meta.bundle_buy_qty, 3);
  const price = Number(app?.bundle_pay_price ?? meta.bundle_pay_price);
  const appliedCount = Number(app?.applied_count || 0);
  const appliedText = appliedCount > 1 ? ` × ${appliedCount}` : "";
  if (!buyQty || !Number.isFinite(price)) return title ? `${PROMO_INDENT}🏷️ ${title}` : "";
  return isEnglish
    ? `${PROMO_INDENT}🏷️ ${title}: ${buyQty} for ₪${fmtMoney(price)}${appliedText}`
    : `${PROMO_INDENT}🏷️ ${title}: ${buyQty} ב-₪${fmtMoney(price)}${appliedText}`;
}

function buildGroupPromoHintLine(groupPromo, isEnglish) {
  const group = groupPromo || null;
  if (!group) return "";
  const buyQty = fmtQty(group.bundle_buy_qty, 3);
  const price = Number(group.bundle_pay_price);
  if (!buyQty || !Number.isFinite(price)) return "";
  return isEnglish
    ? `${PROMO_INDENT}(🏷️ Existing promotion: ${buyQty} for ₪${fmtMoneyCompact(price)})`
    : `${PROMO_INDENT}(🏷️ קיים מבצע: ${buyQty} יח׳ ב-₪${fmtMoneyCompact(price)})`;
}

function collectGroupPromotionBlocks(items, applications, isEnglish) {
  const normalizedApps = Array.isArray(applications) ? applications : [];
  if (!normalizedApps.length || !items.length) {
    return { blocksByStartIndex: new Map(), consumedByKey: new Map() };
  }

  const byOrderItemId = new Map();
  const byProductId = new Map();
  const indexByKey = new Map();

  items.forEach((item, index) => {
    const key = itemKey(item);
    if (key) indexByKey.set(key, index);

    const orderItemId = Number(item.order_item_id ?? item.orderItemId);
    if (Number.isInteger(orderItemId) && orderItemId > 0) byOrderItemId.set(orderItemId, item);

    const productId = Number(item.product_id ?? item.productId);
    if (Number.isInteger(productId) && productId > 0 && !byProductId.has(productId)) {
      byProductId.set(productId, item);
    }
  });

  const consumedByKey = new Map();
  const blocks = [];

  for (const app of normalizedApps) {
    const meta = parseJsonMaybe(app?.metadata);
    const bundles = Array.isArray(meta.bundles) ? meta.bundles : [];
    const counts = new Map();

    for (const bundle of bundles) {
      const orderItemIds = Array.isArray(bundle.order_item_ids)
        ? bundle.order_item_ids
        : Array.isArray(bundle.item_ids)
          ? bundle.item_ids
          : [];

      if (orderItemIds.length) {
        for (const rawId of orderItemIds) {
          const id = Number(rawId);
          const item = byOrderItemId.get(id);
          if (!item) continue;
          const key = itemKey(item);
          if (!key) continue;
          counts.set(key, (counts.get(key) || 0) + 1);
        }
        continue;
      }

      const productIds = Array.isArray(bundle.product_ids) ? bundle.product_ids : [];
      for (const rawId of productIds) {
        const id = Number(rawId);
        const item = byProductId.get(id);
        if (!item) continue;
        const key = itemKey(item);
        if (!key) continue;
        counts.set(key, (counts.get(key) || 0) + 1);
      }
    }

    if (!counts.size) continue;

    const entries = Array.from(counts.entries())
      .map(([key, count]) => {
        const index = indexByKey.has(key) ? indexByKey.get(key) : Number.MAX_SAFE_INTEGER;
        const item = items[index];
        const maxAmount = Math.floor(Number(item?.amount || 0));
        const safeCount = Math.min(Math.max(0, count), Math.max(0, maxAmount));
        return safeCount > 0 ? { key, item, index, amount: safeCount } : null;
      })
      .filter(Boolean)
      .sort((a, b) => a.index - b.index);

    if (!entries.length) continue;

    for (const entry of entries) {
      consumedByKey.set(entry.key, (consumedByKey.get(entry.key) || 0) + entry.amount);
    }

    const startIndex = Math.min(...entries.map((entry) => entry.index));
    blocks.push({
      startIndex,
      entries,
      promoLine: fmtGroupPromoLine(app, isEnglish),
    });
  }

  const blocksByStartIndex = new Map();
  blocks
    .sort((a, b) => a.startIndex - b.startIndex)
    .forEach((block) => {
      if (!blocksByStartIndex.has(block.startIndex)) blocksByStartIndex.set(block.startIndex, []);
      blocksByStartIndex.get(block.startIndex).push(block);
    });

  return { blocksByStartIndex, consumedByKey };
}

function buildItemLine({ item, isEnglish, showPrice = true }) {
  const name = String(item.name || "").trim();
  if (!name) return "";
  const emoji = cleanEmoji(item.emoji);
  const qtySuffix = buildQuantitySuffix(item, isEnglish);

  if (!showPrice) return `${emoji} ${boldProductName(name)}${qtySuffix}`;

  return `${emoji} ${boldProductName(name)}${qtySuffix} - ₪${fmtMoney(item.line_total)}`;
}

function isPromoAppliedToItem(item) {
  if (!item?.promo) return false;
  const unitPrice = Number(item.unit_price ?? item.price);
  const amount = Number(item.amount);
  const lineTotal = Number(item.line_total);
  if (!Number.isFinite(unitPrice) || !Number.isFinite(amount) || !Number.isFinite(lineTotal)) {
    return true;
  }
  const regular = roundTo(unitPrice * amount, 2);
  return lineTotal <= regular - 0.01;
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

function buildQuickCheckoutHint({ status, isEnglish }) {
  const normalizedStatus = String(status || "").toLowerCase();

  if (normalizedStatus !== "pending") return "";

  return isEnglish
    ? '⭐ *To finish your order: send finish*'
    : '⭐ *לסיום ההזמנה: שלח סיים*';
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

function buildPromoLine({ promo, isEnglish, isWeight, applied = true }) {
  if (!promo || !promo.kind) return "";

  const kind = String(promo.kind || "").toUpperCase();
  const prefix = applied
    ? isEnglish
      ? `${PROMO_INDENT}🏷️ Promotion applied: `
      : `${PROMO_INDENT}🏷️ נרכש במבצע: `
    : isEnglish
      ? `${PROMO_INDENT}(🏷️ Existing promotion: `
      : `${PROMO_INDENT}(🏷️ קיים מבצע: `;
  const suffix = applied ? "!" : ")";

  if (kind === "PERCENT_OFF") {
    const pct = fmtQty(promo.percent_off, 2);
    if (!pct) return "";
    return applied
      ? isEnglish
        ? `${PROMO_INDENT}🏷️ Purchased with ${pct}% off!`
        : `${PROMO_INDENT}🏷️ נרכש ב-${pct}% הנחה!`
      : isEnglish
        ? `${prefix}${pct}% off${suffix}`
        : `${prefix}${pct}% הנחה${suffix}`;
  }

  if (kind === "AMOUNT_OFF") {
    const off = Number(promo.amount_off);
    if (!Number.isFinite(off)) return "";
    return isEnglish
      ? `${prefix}₪${fmtMoneyCompact(off)} off per unit${suffix}`
      : `${prefix}₪${fmtMoneyCompact(off)} הנחה ליח׳${suffix}`;
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
      ? `${prefix}fixed price ₪${fmtMoneyCompact(fixed)} ${unitLabel}${suffix}`
      : `${prefix}מחיר קבוע ₪${fmtMoneyCompact(fixed)} ${unitLabel}${suffix}`;
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
      ? `${prefix}${buyQty} ${qtyLabel} for ₪${fmtMoneyCompact(pay)}${suffix}`
      : `${prefix}${buyQty} ${qtyLabel} ב-₪${fmtMoneyCompact(pay)}${suffix}`;
  }

  return applied
    ? isEnglish
      ? `${PROMO_INDENT}🏷️ Promotion applied!`
      : `${PROMO_INDENT}🏷️ נרכש במבצע!`
    : isEnglish
      ? `${PROMO_INDENT}(🏷️ Existing promotion)`
      : `${PROMO_INDENT}(🏷️ קיים מבצע)`;
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
    is_gift:
      item.is_gift === true || item.is_gift === 1 || item.is_gift === "1",
    cart_promotion_rule_id: item.cart_promotion_rule_id || null,
    order_item_id: item.order_item_id ?? item.orderItemId ?? null,
    product_id: item.product_id ?? item.productId ?? null,
    emoji: cleanEmoji(item.emoji),
    group_promo: item.group_promo || item.group_promo_hint || null,
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

    if (item.is_gift) continue;

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
  cartPromotionLines = [],
  productGroupPromotionApplications = [],
  showQuickCheckoutHint = false,
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

  const groupDisplay = collectGroupPromotionBlocks(
    normalizedItems,
    productGroupPromotionApplications,
    isEnglish,
  );

  for (let itemIndex = 0; itemIndex < normalizedItems.length; itemIndex += 1) {
    const blocks = groupDisplay.blocksByStartIndex.get(itemIndex) || [];
    for (const block of blocks) {
      for (const entry of block.entries) {
        const blockItem = {
          ...entry.item,
          amount: entry.amount,
          line_total: 0,
        };
        const line = buildItemLine({ item: blockItem, isEnglish, showPrice: false });
        if (line) lines.push(line);
      }
      if (block.promoLine) lines.push(block.promoLine);
    }

    const item = normalizedItems[itemIndex];
    const name = String(item.name || "").trim();
    if (!name) continue;

    const key = itemKey(item);
    const consumedQty = key ? Number(groupDisplay.consumedByKey.get(key) || 0) : 0;
    const remainingQty = Math.max(0, Number(item.amount) - consumedQty);
    if (remainingQty <= 0) continue;

    const displayItem = remainingQty < Number(item.amount)
      ? {
          ...item,
          amount: remainingQty,
          line_total: roundTo(Number(item.unit_price || 0) * remainingQty, 2),
        }
      : item;

    if (displayItem.is_gift) {
      const emoji = cleanEmoji(displayItem.emoji);
      const qtySuffix = buildQuantitySuffix(displayItem, isEnglish);
      lines.push(
        isEnglish
          ? `${emoji} ${boldProductName(name)}${qtySuffix} - gift 🎁`
          : `${emoji} ${boldProductName(name)}${qtySuffix} - מתנה 🎁`,
      );
      continue;
    }

    const line = buildItemLine({ item: displayItem, isEnglish, showPrice: true });
    if (line) lines.push(line);

    const promoLine = buildPromoLine({
      promo: displayItem.promo,
      isEnglish,
      isWeight: displayItem.sold_by_weight === true,
      applied: isPromoAppliedToItem(displayItem),
    });
    if (promoLine) lines.push(promoLine);
    else {
      const groupHintLine = buildGroupPromoHintLine(displayItem.group_promo, isEnglish);
      if (groupHintLine) lines.push(groupHintLine);
    }
  }

  const normalizedCartPromotionLines = (Array.isArray(cartPromotionLines) ? cartPromotionLines : [])
    .map((line) => String(line || "").trim())
    .filter(Boolean);

  if (normalizedCartPromotionLines.length) {
    lines.push("");
    lines.push(isEnglish ? "🎁 Basket promotions:" : "🎁 מבצעי סל:");
    for (const line of normalizedCartPromotionLines) lines.push(`• ${line}`);
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

    const hasInlineQuestions = Array.isArray(questions) && questions.length > 0;

    if (
      showQuickCheckoutHint &&
      normalizedItems.length > 0 &&
      !hasInlineQuestions
    ) {
      const quickCheckoutHint = buildQuickCheckoutHint({
        status,
        isEnglish,
      });
      if (quickCheckoutHint) lines.push(quickCheckoutHint);
    }
  }

  const summary = lines.join("\n").trim();
  const questionsBlock = buildQuestionsBlock({ questions, isEnglish });

  return [summary, questionsBlock].filter(Boolean).join("\n");
}

module.exports = {
  STATUS_EMOJIS,
  buildOrderSummaryMessage,
  buildPromoLine,
  buildQuickCheckoutHint,
  pickGreeting,
};

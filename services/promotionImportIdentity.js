const crypto = require("crypto");

const MARKET_DAY_DESCRIPTION = String(
  process.env.MARKET_DAY_DESCRIPTION || "מבצע יום השוק",
).trim();
const MARKET_DAY_TIMEZONE = String(
  process.env.MARKET_DAY_TIMEZONE || "Asia/Jerusalem",
).trim();

function normalizeText(value) {
  let text = String(value ?? "").toLowerCase();
  text = text.normalize("NFKD").replace(/[\u0591-\u05C7]/g, "");
  const finalLetters = { ך: "כ", ם: "מ", ן: "נ", ף: "פ", ץ: "צ" };
  text = text.replace(/[ךםןףץ]/g, (char) => finalLetters[char] || char);
  text = text.replace(/[״”“,׳’‘'"`´]/g, "");
  text = text.replace(/[־–—\-&+₪%.,:;()[\]{}\\/|]/g, " ");
  return text.replace(/\s+/g, " ").trim();
}

function stripMarketDayPrefix(value) {
  const title = String(value ?? "").trim();
  const marketDayPrefix = /^\s*[~*#•]*\s*שוק(?=\s|[-:־–—]|$)[\s\-:־–—]*/u;
  const isMarketDay = marketDayPrefix.test(title);
  if (!isMarketDay) return { title, is_market_day: false };
  return {
    title: title.replace(marketDayPrefix, "").trim(),
    is_market_day: true,
  };
}

function isMarketDayEntity(entity) {
  return Boolean(
    entity?.is_market_day ||
    String(entity?.description ?? "").trim() === MARKET_DAY_DESCRIPTION,
  );
}

function localDateParts(now = new Date(), timeZone = MARKET_DAY_TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    weekday: "short",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const weekdayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  return {
    year: Number(byType.year),
    month: Number(byType.month),
    day: Number(byType.day),
    weekday: weekdayMap[byType.weekday] ?? 0,
  };
}

function addDaysToYmd({ year, month, day }, days) {
  const date = new Date(Date.UTC(year, month - 1, day + Number(days || 0), 12, 0, 0));
  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}

function marketDayWindow(now = new Date()) {
  const local = localDateParts(now);
  const daysUntilTuesday = (2 - local.weekday + 7) % 7;
  const target = addDaysToYmd(local, daysUntilTuesday);
  const date = `${target.year}-${String(target.month).padStart(2, "0")}-${String(target.day).padStart(2, "0")}`;
  return {
    date,
    start_at: `${date} 00:00:00`,
    end_at: `${date} 23:59:59`,
  };
}

function cleanTitleForComparison(value) {
  return normalizeText(String(value ?? "")
    .replace(/בקני(?:י|)ה\s+מעל\s*\d+(?:\.\d+)?/gi, " ")
    .replace(/\d+(?:\.\d+)?\s*ב\s*-?\s*\d+(?:\.\d+)?/gi, " ")
    .replace(/רק\s*ב\s*-?\s*\d+(?:\.\d+)?/gi, " ")
    .replace(/ב\s*-?\s*\d+(?:\.\d+)?\s*(?:₪|שח|ש״ח)?/gi, " "));
}

function regexNumberPattern(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  const normalized = String(number);
  const [whole, decimal] = normalized.split(".");
  if (!decimal) return `${whole}(?:[\.,]0+)?`;
  return `${whole}[\.,]${decimal}0*`;
}

function cleanProductPromotionTitle(value, bundleBuyQty = null, bundlePayPrice = null) {
  const marketDay = stripMarketDayPrefix(value);
  const original = marketDay.title.replace(/\s+/g, " ").trim();
  if (!original) return "";

  const qtyPattern = regexNumberPattern(bundleBuyQty);
  const pricePattern = regexNumberPattern(bundlePayPrice);
  let cleaned = original;

  if (qtyPattern && pricePattern) {
    const dealSuffix = new RegExp(
      `\\s+${qtyPattern}\\s*(?:יח(?:ידות|['׳])?\\s*)?ב\\s*-?\\s*(?:₪\\s*)?${pricePattern}\\s*(?:₪|ש\\s*["״'׳]?\\s*ח)?\\s*$`,
      "iu",
    );
    cleaned = cleaned.replace(dealSuffix, "").trim();
  }

  if (cleaned === original) {
    cleaned = cleaned
      .replace(/\s+\d+(?:[\.,]\d+)?\s*(?:יח(?:ידות|['׳])?\s*)?ב\s*-?\s*(?:₪\s*)?\d+(?:[\.,]\d+)?\s*(?:₪|ש\s*["״'׳]?\s*ח)?\s*$/iu, "")
      .trim();
  }

  cleaned = cleaned.replace(/[\s\-–—:]+$/u, "").trim();
  return cleaned || original;
}

function parseDealText(value) {
  const match = String(value ?? "").match(/(\d+(?:\.\d+)?)\s*ב\s*(\d+(?:\.\d+)?)/);
  if (!match) return null;
  const qty = Number(match[1]);
  const price = Number(match[2]);
  if (!Number.isFinite(qty) || !Number.isFinite(price) || qty <= 0 || price < 0) return null;
  return { qty, price: roundMoney(price) };
}

function roundMoney(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? Math.round(number * 100) / 100 : null;
}

function roundQty(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? Math.round(number * 1000) / 1000 : null;
}

function startDateTime(value) {
  const text = String(value ?? "").trim();
  return text ? `${text} 00:00:00` : null;
}

function endDateTime(value) {
  const text = String(value ?? "").trim();
  return text ? `${text} 23:59:59` : null;
}

function normalizeDateTime(value) {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value.toISOString().slice(0, 19).replace("T", " ");
  }
  const text = String(value).trim().replace("T", " ").replace(/\.\d+Z?$/, "");
  return text ? text.slice(0, 19) : null;
}

function dateMs(value, fallback) {
  if (!value) return fallback;
  const parsed = new Date(String(value).replace(" ", "T"));
  return Number.isFinite(parsed.getTime()) ? parsed.getTime() : fallback;
}

function intervalsOverlap(aStart, aEnd, bStart, bEnd) {
  const aStartMs = dateMs(aStart, Number.NEGATIVE_INFINITY);
  const bStartMs = dateMs(bStart, Number.NEGATIVE_INFINITY);
  const aEndMs = dateMs(aEnd, Number.POSITIVE_INFINITY);
  const bEndMs = dateMs(bEnd, Number.POSITIVE_INFINITY);
  return aStartMs <= bEndMs && bStartMs <= aEndMs;
}

function intervalContains(outerStart, outerEnd, innerStart, innerEnd) {
  const outerStartMs = dateMs(outerStart, Number.NEGATIVE_INFINITY);
  const outerEndMs = dateMs(outerEnd, Number.POSITIVE_INFINITY);
  const innerStartMs = dateMs(innerStart, Number.NEGATIVE_INFINITY);
  const innerEndMs = dateMs(innerEnd, Number.POSITIVE_INFINITY);
  return outerStartMs <= innerStartMs && outerEndMs >= innerEndMs;
}

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    return Object.keys(value)
      .sort()
      .reduce((result, key) => {
        if (value[key] !== undefined) result[key] = canonicalize(value[key]);
        return result;
      }, {});
  }
  return value;
}

function stableJson(value) {
  return JSON.stringify(canonicalize(value));
}

function sha256(value) {
  const input = typeof value === "string" || Buffer.isBuffer(value) ? value : stableJson(value);
  return crypto.createHash("sha256").update(input).digest("hex");
}

function productLookup(product) {
  return {
    chain_product_key: cleanNullable(product?.chain_product_key),
    barcode: cleanNullable(product?.barcode),
    name: cleanNullable(product?.name),
    expected_product_id: positiveInt(product?.id),
  };
}

function stableProductKey(product) {
  const chainKey = cleanNullable(product?.chain_product_key);
  if (chainKey) return `chain:${chainKey}`;
  const barcode = cleanNullable(product?.barcode);
  if (barcode) return `barcode:${barcode}`;
  const name = normalizeText(product?.name);
  return name ? `name:${name}` : null;
}

function cleanNullable(value) {
  const text = String(value ?? "").trim();
  return text || null;
}

function positiveInt(value) {
  const number = Number(value);
  return Number.isInteger(number) && number > 0 ? number : null;
}

function sourceSlotKey(entity) {
  if (entity.entity_type === "promotion") {
    return `product:${entity.product_key}`;
  }
  if (entity.entity_type === "product_group_promotion") {
    return `group:${sha256(entity.product_keys).slice(0, 24)}`;
  }
  if (entity.entity_type === "cart_promotion_rule") {
    const descriptor = identityDescriptor(entity);
    return `cart:${entity.rule_type}:${sha256(descriptor).slice(0, 24)}`;
  }
  throw new Error(`Unsupported entity_type for source slot: ${entity.entity_type}`);
}

function identityDescriptor(entity) {
  const marketDay = isMarketDayEntity(entity);
  if (entity.entity_type === "promotion") {
    return {
      entity_type: entity.entity_type,
      promotion_scope: marketDay ? "MARKET_DAY" : "STANDARD",
      product_key: entity.product_key,
      kind: entity.kind,
      percent_off: roundMoney(entity.percent_off),
      amount_off: roundMoney(entity.amount_off),
      fixed_price: roundMoney(entity.fixed_price),
      bundle_buy_qty: entity.bundle_buy_qty == null ? null : Number(entity.bundle_buy_qty),
      bundle_pay_price: roundMoney(entity.bundle_pay_price),
    };
  }

  if (entity.entity_type === "product_group_promotion") {
    return {
      entity_type: entity.entity_type,
      promotion_scope: marketDay ? "MARKET_DAY" : "STANDARD",
      product_keys: [...(entity.product_keys || [])].sort(),
      kind: "BUNDLE",
      bundle_buy_qty: Number(entity.bundle_buy_qty),
      bundle_pay_price: roundMoney(entity.bundle_pay_price),
    };
  }

  if (entity.entity_type === "cart_promotion_rule") {
    return {
      entity_type: entity.entity_type,
      promotion_scope: marketDay ? "MARKET_DAY" : "STANDARD",
      rule_type: entity.rule_type,
      threshold_amount: roundMoney(entity.threshold_amount),
      delivery_fee_override: roundMoney(entity.delivery_fee_override),
      reward_product_key: entity.reward_product_key || null,
      reward_qty: roundQty(entity.reward_qty),
      reward_fixed_price: roundMoney(entity.reward_fixed_price),
      reward_max_qty: roundQty(entity.reward_max_qty),
      threshold_base_mode: entity.threshold_base_mode || "ITEMS_SUBTOTAL",
    };
  }

  throw new Error(`Unsupported entity_type: ${entity.entity_type}`);
}

function contentDescriptor(entity) {
  return {
    identity: identityDescriptor(entity),
    max_discounted_qty: roundQty(entity.max_discounted_qty),
    start_at: normalizeDateTime(entity.start_at),
    end_at: normalizeDateTime(entity.end_at),
    is_active: entity.is_active === undefined ? null : Number(Boolean(entity.is_active)),
  };
}

function identityHash(entity) {
  return sha256(identityDescriptor(entity));
}

function contentHash(entity) {
  return sha256(contentDescriptor(entity));
}

function sameNullableNumber(a, b, precision = 3) {
  if (a === null || a === undefined || a === "") return b === null || b === undefined || b === "";
  if (b === null || b === undefined || b === "") return false;
  const factor = 10 ** precision;
  return Math.round(Number(a) * factor) === Math.round(Number(b) * factor);
}

module.exports = {
  MARKET_DAY_DESCRIPTION,
  MARKET_DAY_TIMEZONE,
  canonicalize,
  cleanNullable,
  cleanProductPromotionTitle,
  cleanTitleForComparison,
  contentDescriptor,
  contentHash,
  endDateTime,
  identityDescriptor,
  identityHash,
  intervalContains,
  intervalsOverlap,
  isMarketDayEntity,
  marketDayWindow,
  normalizeDateTime,
  normalizeText,
  parseDealText,
  positiveInt,
  productLookup,
  roundMoney,
  roundQty,
  sameNullableNumber,
  sha256,
  sourceSlotKey,
  stableJson,
  stableProductKey,
  startDateTime,
  stripMarketDayPrefix,
};

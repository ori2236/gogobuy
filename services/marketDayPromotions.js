const db = require("../config/db");
const { sendWhatsAppTemplate } = require("../utilities/whatsapp");
const { normalizeWaNumber } = require("../utilities/dashboardUtils");

const MARKET_DAY_DESCRIPTION = String(
  process.env.MARKET_DAY_DESCRIPTION || "מבצע יום השוק",
).trim();
const MARKET_DAY_TEMPLATE_NAME = String(
  process.env.MARKET_DAY_TEMPLATE_NAME || "market_day_sales",
).trim();
const MARKET_DAY_TEMPLATE_LANGUAGE = String(
  process.env.MARKET_DAY_TEMPLATE_LANGUAGE || "he",
).trim();
const MARKET_DAY_TIMEZONE = String(
  process.env.MARKET_DAY_TIMEZONE || "Asia/Jerusalem",
).trim();
const DEFAULT_ELIGIBLE_ORDER_STATUSES = [
  "confirmed",
  "preparing",
  "ready",
  "delivering",
  "completed",
];

let schemaReadyPromise = null;

function eligibleOrderStatuses() {
  const raw = String(process.env.MARKET_DAY_ELIGIBLE_ORDER_STATUSES || "").trim();
  const values = raw
    ? raw.split(",").map((x) => x.trim()).filter(Boolean)
    : DEFAULT_ELIGIBLE_ORDER_STATUSES;
  return values.length ? values : DEFAULT_ELIGIBLE_ORDER_STATUSES;
}

function isTruthy(value) {
  if (value === true || value === 1) return true;
  const s = String(value ?? "").trim().toLowerCase();
  return ["1", "true", "yes", "y", "on", "כן"].includes(s);
}

function hasExplicitMarketDayPayload(body) {
  return Boolean(
    body &&
      (Object.prototype.hasOwnProperty.call(body, "is_market_day") ||
        Object.prototype.hasOwnProperty.call(body, "isMarketDay") ||
        Object.prototype.hasOwnProperty.call(body, "market_day") ||
        Object.prototype.hasOwnProperty.call(body, "marketDay"))
  );
}

function isMarketDayPayload(body) {
  return isTruthy(
    body?.is_market_day ??
      body?.isMarketDay ??
      body?.market_day ??
      body?.marketDay,
  );
}

function isMarketDayDescription(value) {
  return String(value ?? "").trim() === MARKET_DAY_DESCRIPTION;
}

function localDateParts(now = new Date(), timeZone = MARKET_DAY_TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    weekday: "short",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);

  const byType = Object.fromEntries(parts.map((p) => [p.type, p.value]));
  const weekdayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

  return {
    year: Number(byType.year),
    month: Number(byType.month),
    day: Number(byType.day),
    weekday: weekdayMap[byType.weekday] ?? 0,
  };
}

function addDaysToYmd({ year, month, day }, days) {
  const dt = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day) + Number(days || 0), 12, 0, 0));
  return {
    year: dt.getUTCFullYear(),
    month: dt.getUTCMonth() + 1,
    day: dt.getUTCDate(),
  };
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function ymdString(parts) {
  return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)}`;
}

function nearestTuesdayWindow(now = new Date()) {
  const local = localDateParts(now);
  const daysUntilTuesday = (2 - local.weekday + 7) % 7;
  const target = addDaysToYmd(local, daysUntilTuesday);
  const date = ymdString(target);
  return {
    date,
    start_at: `${date} 00:00:00`,
    end_at: `${date} 23:59:59`,
  };
}
function currentMarketDayCycleStart(now = new Date()) {
  const local = localDateParts(now);
  const daysSinceTuesday = (local.weekday - 2 + 7) % 7;
  const target = addDaysToYmd(local, -daysSinceTuesday);
  const date = ymdString(target);
  return {
    date,
    start_at: `${date} 00:00:00`,
  };
}

function mysqlDateTimeForCompare(value) {
  if (!value) return "";
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value.toISOString().slice(0, 19).replace("T", " ");
  }
  const raw = String(value);
  const direct = raw.match(/^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2}))?/);
  if (direct) return `${direct[1]} ${direct[2] || "00:00:00"}`;
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 19).replace("T", " ");
  return raw;
}

function effectiveMarketDaySendStatus(row, now = new Date()) {
  const rawStatus = String(row?.last_send_status || row?.send_status || "not_sent").trim();
  if (rawStatus !== "sent" && rawStatus !== "failed") return "not_sent";
  const lastAttempt = mysqlDateTimeForCompare(row?.last_send_attempt_at || row?.last_send_at);
  if (!lastAttempt) return "not_sent";
  const cycleStart = currentMarketDayCycleStart(now).start_at;
  return lastAttempt >= cycleStart ? rawStatus : "not_sent";
}


function shouldForceMarketDay(body, existing = null) {
  if (hasExplicitMarketDayPayload(body)) return isMarketDayPayload(body);
  return isMarketDayDescription(existing?.description);
}

function reservedDescriptionError() {
  const err = new Error(`התיאור ${MARKET_DAY_DESCRIPTION} שמור רק למבצעים שסומנו כמבצעי יום השוק`);
  err.status = 400;
  return err;
}

function applyMarketDayOverrides(payload, body, existing = null) {
  const explicit = hasExplicitMarketDayPayload(body);
  const requestedMarketDay = isMarketDayPayload(body);

  if (explicit && requestedMarketDay) {
    if (isMarketDayDescription(existing?.description) && isMarketDayDescription(payload?.description)) {
      return {
        ...payload,
        description: MARKET_DAY_DESCRIPTION,
        start_at: existing.start_at || payload.start_at,
        end_at: existing.end_at || payload.end_at,
        is_market_day: true,
      };
    }

    const window = nearestTuesdayWindow();
    return {
      ...payload,
      description: MARKET_DAY_DESCRIPTION,
      start_at: window.start_at,
      end_at: window.end_at,
      is_market_day: true,
    };
  }

  if (explicit && !requestedMarketDay) {
    return {
      ...payload,
      description: isMarketDayDescription(payload?.description) ? null : payload.description,
      is_market_day: false,
    };
  }

  if (!explicit && isMarketDayDescription(existing?.description)) {
    return {
      ...payload,
      description: MARKET_DAY_DESCRIPTION,
      start_at: existing.start_at || payload.start_at,
      end_at: existing.end_at || payload.end_at,
      is_market_day: true,
    };
  }

  if (isMarketDayDescription(payload?.description)) {
    throw reservedDescriptionError();
  }

  return { ...payload, is_market_day: false };
}

function isMarketDayRequest(message) {
  const raw = String(message || "")
    .trim()
    .replace(/[\u200f\u200e]/g, "")
    .replace(/\s+/g, " ");
  if (!raw) return false;
  return raw === "מבצעי יום השוק" || raw === "מבצע יום השוק" || raw.includes("מבצעי יום השוק");
}

function money(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `₪${n.toFixed(2)}`;
}

function shortNumber(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(3).replace(/\.?0+$/, "");
}

function promoValueText(promo) {
  if (!promo) return "";
  if (promo.kind === "PERCENT_OFF") return `${shortNumber(promo.percent_off)}% הנחה`;
  if (promo.kind === "AMOUNT_OFF") return `${money(promo.amount_off)} הנחה`;
  if (promo.kind === "FIXED_PRICE") return `ב-${money(promo.fixed_price)}`;
  if (promo.kind === "BUNDLE") return `${shortNumber(promo.bundle_buy_qty)} יח׳ ב-${money(promo.bundle_pay_price)}`;
  if (promo.rule_type === "DELIVERY_FEE_OVERRIDE") {
    const fee = Number(promo.delivery_fee_override || 0);
    return fee <= 0 ? "משלוח חינם" : `משלוח ב-${money(fee)}`;
  }
  if (promo.rule_type === "GIFT_PRODUCT") {
    const qty = Number(promo.reward_qty || 1);
    const qtyText = Number.isFinite(qty) && qty > 1 ? `${shortNumber(qty)} × ` : "";
    return `${qtyText}${promo.reward_product_name || promo.gift_text || "מתנה"} מתנה`;
  }
  if (promo.rule_type === "THRESHOLD_PRODUCT_FIXED_PRICE") {
    return `${promo.reward_product_name || "מוצר"} ב-${money(promo.reward_fixed_price)}`;
  }
  return "";
}

function activeProductPromotionSql(alias = "pr") {
  return `(${alias}.description = ? AND ${alias}.start_at <= NOW() AND (${alias}.end_at IS NULL OR ${alias}.end_at > NOW()))`;
}

function activeGroupPromotionSql(alias = "pgp") {
  return `(${alias}.description = ? AND ${alias}.is_active = 1 AND (${alias}.start_at IS NULL OR ${alias}.start_at <= NOW()) AND (${alias}.end_at IS NULL OR ${alias}.end_at > NOW()))`;
}

function activeCartRuleSql(alias = "cpr") {
  return `(${alias}.description = ? AND ${alias}.is_active = 1 AND (${alias}.start_at IS NULL OR ${alias}.start_at <= NOW()) AND (${alias}.end_at IS NULL OR ${alias}.end_at > NOW()))`;
}

async function queryOrEmpty(sql, params) {
  try {
    const [rows] = await db.query(sql, params);
    return rows || [];
  } catch (err) {
    if (err?.code === "ER_NO_SUCH_TABLE" || err?.code === "ER_BAD_FIELD_ERROR") return [];
    throw err;
  }
}

async function tableColumnExists(tableName, columnName) {
  const [rows] = await db.query(
    `
    SELECT COUNT(*) AS cnt
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?
    `,
    [tableName, columnName],
  );
  return Number(rows?.[0]?.cnt || 0) > 0;
}

async function ensureTableColumn(tableName, columnName, definitionSql) {
  const exists = await tableColumnExists(tableName, columnName);
  if (exists) return;
  await db.query(`ALTER TABLE ${tableName} ADD COLUMN ${definitionSql}`);
}

function normalizeMarketDayRecipientPhone(phone) {
  return normalizeWaNumber(phone);
}

function isValidMarketDayRecipientPhone(phone) {
  const normalized = normalizeMarketDayRecipientPhone(phone);
  return /^9725\d{8}$/.test(String(normalized || ""));
}

function publicSendError(err) {
  const metaError = err?.response?.data?.error;
  const rawMessage = metaError?.message || err?.message || "Unknown error";
  const code = metaError?.code || err?.code || null;
  const text = String(rawMessage || "").toLowerCase();
  if (code === 131009 || text.includes("parameter value is not valid") || text.includes("invalid phone")) {
    return "מספר טלפון לא תקין";
  }
  return rawMessage;
}

async function markRecipientSendResult(shopId, recipientId, status, errorMessage = null) {
  await ensureMarketDayRecipientSchema();
  const normalizedStatus = status === "sent" ? "sent" : status === "failed" ? "failed" : "not_sent";
  await db.query(
    `
    UPDATE market_day_template_recipient
    SET
      last_send_status = ?,
      last_send_attempt_at = CURRENT_TIMESTAMP,
      last_send_at = CASE WHEN ? = 'sent' THEN CURRENT_TIMESTAMP ELSE last_send_at END,
      last_send_error = ?,
      updated_at = CURRENT_TIMESTAMP
    WHERE id = ? AND shop_id = ?
    LIMIT 1
    `,
    [normalizedStatus, normalizedStatus, normalizedStatus === "failed" ? String(errorMessage || "השליחה נכשלה") : null, Number(recipientId), Number(shopId)],
  );
}

function parseProductsJson(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (Buffer.isBuffer(value)) {
    try { return JSON.parse(value.toString("utf8")); } catch { return []; }
  }
  if (typeof value === "object") return Array.isArray(value) ? value : [];
  try {
    const parsed = JSON.parse(String(value));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function fetchMarketDayPromotions(shopId) {
  const productPromotions = await queryOrEmpty(
    `
    SELECT
      pr.id,
      pr.kind,
      pr.percent_off,
      pr.amount_off,
      pr.fixed_price,
      pr.bundle_buy_qty,
      pr.bundle_pay_price,
      pr.max_discounted_qty,
      pr.start_at,
      pr.end_at,
      p.name AS product_name,
      p.price AS product_price
    FROM promotion pr
    JOIN product p ON p.id = pr.product_id AND p.shop_id = pr.shop_id
    WHERE pr.shop_id = ? AND ${activeProductPromotionSql("pr")}
    ORDER BY p.name ASC, pr.id ASC
    LIMIT 100
    `,
    [Number(shopId), MARKET_DAY_DESCRIPTION],
  );

  const groupPromotions = await queryOrEmpty(
    `
    SELECT
      pgp.id,
      pgp.title,
      pgp.bundle_buy_qty,
      pgp.bundle_pay_price,
      pgp.max_discounted_qty,
      pgp.start_at,
      pgp.end_at,
      COALESCE(
        JSON_ARRAYAGG(
          CASE
            WHEN p.id IS NULL THEN NULL
            ELSE JSON_OBJECT('id', p.id, 'name', p.name)
          END
        ),
        JSON_ARRAY()
      ) AS products_json
    FROM product_group_promotion pgp
    LEFT JOIN product_group_promotion_item pgpi
      ON pgpi.group_promotion_id = pgp.id AND pgpi.shop_id = pgp.shop_id
    LEFT JOIN product p ON p.id = pgpi.product_id AND p.shop_id = pgp.shop_id
    WHERE pgp.shop_id = ? AND ${activeGroupPromotionSql("pgp")}
    GROUP BY pgp.id
    ORDER BY pgp.priority ASC, pgp.id ASC
    LIMIT 100
    `,
    [Number(shopId), MARKET_DAY_DESCRIPTION],
  );

  const cartPromotions = await queryOrEmpty(
    `
    SELECT
      cpr.*,
      p.name AS reward_product_name,
      p.price AS reward_product_price
    FROM cart_promotion_rule cpr
    LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = cpr.shop_id
    WHERE cpr.shop_id = ? AND ${activeCartRuleSql("cpr")}
    ORDER BY cpr.priority ASC, cpr.threshold_amount DESC, cpr.id ASC
    LIMIT 100
    `,
    [Number(shopId), MARKET_DAY_DESCRIPTION],
  );

  return { productPromotions, groupPromotions, cartPromotions };
}

function statusFromDates({ is_active, is_currently_active, is_upcoming, is_expired, start_at, end_at } = {}) {
  if (is_currently_active || is_active === 1 || is_active === true) return "active";
  if (is_upcoming === 1 || is_upcoming === true) return "upcoming";
  if (is_expired === 1 || is_expired === true) return "expired";
  const now = Date.now();
  const start = start_at ? new Date(start_at).getTime() : 0;
  const end = end_at ? new Date(end_at).getTime() : null;
  if (Number.isFinite(start) && start > now) return "upcoming";
  if (end && Number.isFinite(end) && end <= now) return "expired";
  return "inactive";
}

function mapDashboardProductPromo(row) {
  const item = {
    type: "product",
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    title: row.product_name || `#${row.product_id}`,
    subtitle: row.product_display_name_en || "",
    product_id: Number(row.product_id),
    product_name: row.product_name || null,
    product_price: row.product_price == null ? null : Number(row.product_price),
    kind: row.kind,
    percent_off: row.percent_off == null ? null : Number(row.percent_off),
    amount_off: row.amount_off == null ? null : Number(row.amount_off),
    fixed_price: row.fixed_price == null ? null : Number(row.fixed_price),
    bundle_buy_qty: row.bundle_buy_qty == null ? null : Number(row.bundle_buy_qty),
    bundle_pay_price: row.bundle_pay_price == null ? null : Number(row.bundle_pay_price),
    max_discounted_qty: row.max_discounted_qty == null ? null : Number(row.max_discounted_qty),
    description: row.description ?? null,
    start_at: row.start_at ?? null,
    end_at: row.end_at ?? null,
    is_active: Boolean(row.is_active),
    is_upcoming: Boolean(row.is_upcoming),
    is_expired: Boolean(row.is_expired),
  };
  item.value_text = promoValueText(item);
  item.status = statusFromDates(item);
  return item;
}

function mapDashboardGroupPromo(row) {
  const products = parseProductsJson(row.products_json).filter((p) => p && Number(p.id) > 0);
  const item = {
    type: "group",
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    title: row.title || `#${row.id}`,
    subtitle: products.map((p) => p.name).filter(Boolean).slice(0, 3).join(", "),
    products,
    bundle_buy_qty: row.bundle_buy_qty == null ? null : Number(row.bundle_buy_qty),
    bundle_pay_price: row.bundle_pay_price == null ? null : Number(row.bundle_pay_price),
    max_discounted_qty: row.max_discounted_qty == null ? null : Number(row.max_discounted_qty),
    priority: row.priority == null ? null : Number(row.priority),
    description: row.description ?? null,
    start_at: row.start_at ?? null,
    end_at: row.end_at ?? null,
    is_active_flag: Number(row.is_active || 0),
    is_currently_active: Boolean(row.is_currently_active),
    is_upcoming: Boolean(row.is_upcoming),
    is_expired: Boolean(row.is_expired),
  };
  item.value_text = `${shortNumber(item.bundle_buy_qty)} יח׳ ב-${money(item.bundle_pay_price)}`;
  item.status = statusFromDates(item);
  return item;
}

function mapDashboardCartRule(row) {
  const item = {
    type: "cart",
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    title: row.title || `#${row.id}`,
    subtitle: row.reward_product_name || row.gift_text || "",
    rule_type: row.rule_type,
    threshold_amount: row.threshold_amount == null ? null : Number(row.threshold_amount),
    delivery_fee_override: row.delivery_fee_override == null ? null : Number(row.delivery_fee_override),
    reward_product_id: row.reward_product_id == null ? null : Number(row.reward_product_id),
    reward_product_name: row.reward_product_name || null,
    reward_qty: row.reward_qty == null ? null : Number(row.reward_qty),
    reward_fixed_price: row.reward_fixed_price == null ? null : Number(row.reward_fixed_price),
    reward_max_qty: row.reward_max_qty == null ? null : Number(row.reward_max_qty),
    description: row.description ?? null,
    start_at: row.start_at ?? null,
    end_at: row.end_at ?? null,
    is_active_flag: Number(row.is_active || 0),
    is_currently_active: Boolean(row.is_currently_active),
    is_upcoming: Boolean(row.is_upcoming),
    is_expired: Boolean(row.is_expired),
  };
  item.value_text = `בקנייה מעל ${money(item.threshold_amount)}: ${promoValueText(item)}`;
  item.status = statusFromDates(item);
  return item;
}

async function fetchDashboardMarketDayPromotions(shopId) {
  const productPromotions = await queryOrEmpty(
    `
    SELECT
      pr.*,
      p.name AS product_name,
      p.display_name_en AS product_display_name_en,
      p.price AS product_price,
      ${activeProductPromotionSql("pr").replace("pr.description = ? AND ", "")} AS is_active,
      (pr.start_at > NOW()) AS is_upcoming,
      (pr.end_at IS NOT NULL AND pr.end_at <= NOW()) AS is_expired
    FROM promotion pr
    LEFT JOIN product p ON p.id = pr.product_id AND p.shop_id = pr.shop_id
    WHERE pr.shop_id = ? AND pr.description = ?
    ORDER BY pr.start_at DESC, pr.id DESC
    LIMIT 500
    `,
    [Number(shopId), MARKET_DAY_DESCRIPTION],
  );

  const groupPromotions = await queryOrEmpty(
    `
    SELECT
      pgp.*,
      (pgp.is_active = 1 AND (pgp.start_at IS NULL OR pgp.start_at <= NOW()) AND (pgp.end_at IS NULL OR pgp.end_at > NOW())) AS is_currently_active,
      (pgp.start_at IS NOT NULL AND pgp.start_at > NOW()) AS is_upcoming,
      (pgp.end_at IS NOT NULL AND pgp.end_at <= NOW()) AS is_expired,
      COALESCE(
        JSON_ARRAYAGG(
          CASE
            WHEN p.id IS NULL THEN NULL
            ELSE JSON_OBJECT('id', p.id, 'name', p.name)
          END
        ),
        JSON_ARRAY()
      ) AS products_json
    FROM product_group_promotion pgp
    LEFT JOIN product_group_promotion_item pgpi
      ON pgpi.group_promotion_id = pgp.id AND pgpi.shop_id = pgp.shop_id
    LEFT JOIN product p ON p.id = pgpi.product_id AND p.shop_id = pgp.shop_id
    WHERE pgp.shop_id = ? AND pgp.description = ?
    GROUP BY pgp.id
    ORDER BY pgp.start_at DESC, pgp.priority ASC, pgp.id DESC
    LIMIT 500
    `,
    [Number(shopId), MARKET_DAY_DESCRIPTION],
  );

  const cartPromotions = await queryOrEmpty(
    `
    SELECT
      cpr.*,
      p.name AS reward_product_name,
      p.price AS reward_product_price,
      (cpr.is_active = 1 AND (cpr.start_at IS NULL OR cpr.start_at <= NOW()) AND (cpr.end_at IS NULL OR cpr.end_at > NOW())) AS is_currently_active,
      (cpr.start_at IS NOT NULL AND cpr.start_at > NOW()) AS is_upcoming,
      (cpr.end_at IS NOT NULL AND cpr.end_at <= NOW()) AS is_expired
    FROM cart_promotion_rule cpr
    LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = cpr.shop_id
    WHERE cpr.shop_id = ? AND cpr.description = ?
    ORDER BY cpr.start_at DESC, cpr.priority ASC, cpr.id DESC
    LIMIT 500
    `,
    [Number(shopId), MARKET_DAY_DESCRIPTION],
  );

  const productItems = productPromotions.map(mapDashboardProductPromo);
  const groupItems = groupPromotions.map(mapDashboardGroupPromo);
  const cartItems = cartPromotions.map(mapDashboardCartRule);

  return {
    description: MARKET_DAY_DESCRIPTION,
    product_promotions: productItems,
    group_promotions: groupItems,
    cart_promotions: cartItems,
    items: [...productItems, ...groupItems, ...cartItems],
  };
}

function formatMarketDayPromotionsReply(data) {
  const products = data?.productPromotions || [];
  const groups = data?.groupPromotions || [];
  const cartRules = data?.cartPromotions || [];
  const total = products.length + groups.length + cartRules.length;

  if (!total) {
    return [
      "🌽 *מבצעי יום השוק*",
      "",
      "כרגע אין מבצעי יום השוק פעילים.",
      "שווה לבדוק שוב בהמשך 🙏",
    ].join("\n");
  }

  const lines = [
    "🌽 *מבצעי יום השוק*",
    "",
    "הנה כל המבצעים המיוחדים להיום 🛒",
  ];

  if (products.length) {
    lines.push("", "🥬 *מבצעי מוצרים:*");
    for (const promo of products) {
      const regular = promo.product_price == null ? "" : ` במקום ${money(promo.product_price)}`;
      lines.push(`• *${promo.product_name || "מוצר"}* - ${promoValueText(promo)}${regular}`);
    }
  }

  if (groups.length) {
    lines.push("", "🧺 *מבצעי קבוצות:*");
    for (const group of groups) {
      const productsList = parseProductsJson(group.products_json)
        .filter((p) => p && p.name)
        .map((p) => p.name);
      const shown = productsList.slice(0, 4).join(", ");
      const more = productsList.length > 4 ? ` ועוד ${productsList.length - 4}` : "";
      const productsText = shown ? ` (${shown}${more})` : "";
      lines.push(`• *${group.title || "מבצע קבוצה"}* - ${shortNumber(group.bundle_buy_qty)} יח׳ ב-${money(group.bundle_pay_price)}${productsText}`);
    }
  }

  if (cartRules.length) {
    lines.push("", "🎁 *מבצעי סל:*");
    for (const rule of cartRules) {
      const threshold = money(rule.threshold_amount);
      lines.push(`• *${rule.title || "מבצע סל"}* - בקנייה מעל ${threshold}: ${promoValueText(rule)}`);
    }
  }

  lines.push("", "אפשר פשוט לכתוב לי מה להוסיף להזמנה 😊");
  return lines.join("\n");
}

async function buildMarketDayPromotionsReply(shopId) {
  const data = await fetchMarketDayPromotions(shopId);
  return formatMarketDayPromotionsReply(data);
}

async function ensureMarketDayRecipientSchema() {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await db.query(`
        CREATE TABLE IF NOT EXISTS market_day_template_recipient (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          shop_id INT UNSIGNED NOT NULL,
          customer_id INT UNSIGNED DEFAULT NULL,
          name VARCHAR(255) DEFAULT NULL,
          phone VARCHAR(32) NOT NULL,
          is_active TINYINT(1) NOT NULL DEFAULT 1,
          orders_count INT UNSIGNED NOT NULL DEFAULT 0,
          last_order_at DATETIME DEFAULT NULL,
          source ENUM('order_history','manual') NOT NULL DEFAULT 'order_history',
          last_send_status ENUM('not_sent','sent','failed') NOT NULL DEFAULT 'not_sent',
          last_send_at DATETIME DEFAULT NULL,
          last_send_attempt_at DATETIME DEFAULT NULL,
          last_send_error TEXT DEFAULT NULL,
          created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uniq_market_day_recipient_shop_phone (shop_id, phone),
          KEY idx_market_day_recipient_shop_active (shop_id, is_active),
          KEY idx_market_day_recipient_customer (shop_id, customer_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      `);
      await ensureTableColumn(
        "market_day_template_recipient",
        "last_send_status",
        "last_send_status ENUM('not_sent','sent','failed') NOT NULL DEFAULT 'not_sent'",
      );
      await ensureTableColumn(
        "market_day_template_recipient",
        "last_send_at",
        "last_send_at DATETIME DEFAULT NULL",
      );
      await ensureTableColumn(
        "market_day_template_recipient",
        "last_send_attempt_at",
        "last_send_attempt_at DATETIME DEFAULT NULL",
      );
      await ensureTableColumn(
        "market_day_template_recipient",
        "last_send_error",
        "last_send_error TEXT DEFAULT NULL",
      );
    })().catch((err) => {
      schemaReadyPromise = null;
      throw err;
    });
  }
  return schemaReadyPromise;
}

async function getEligibleCustomers(shopId) {
  const statuses = eligibleOrderStatuses();
  const placeholders = statuses.map(() => "?").join(",");
  const rows = await queryOrEmpty(
    `
    SELECT
      c.id AS customer_id,
      COALESCE(NULLIF(c.name, ''), CONCAT('לקוח ', c.id)) AS name,
      c.phone,
      COUNT(DISTINCT o.id) AS orders_count,
      MAX(o.created_at) AS last_order_at
    FROM customer c
    JOIN orders o ON o.customer_id = c.id AND o.shop_id = c.shop_id
    WHERE c.shop_id = ?
      AND o.status IN (${placeholders})
      AND c.phone IS NOT NULL
      AND c.phone <> ''
    GROUP BY c.id, c.name, c.phone
    ORDER BY last_order_at DESC, c.id DESC
    `,
    [Number(shopId), ...statuses],
  );

  const byPhone = new Map();
  for (const row of rows || []) {
    const phone = normalizeWaNumber(row.phone);
    if (!phone) continue;
    const current = byPhone.get(phone);
    if (!current || Number(row.orders_count || 0) > Number(current.orders_count || 0)) {
      byPhone.set(phone, {
        customer_id: Number(row.customer_id),
        name: row.name || null,
        phone,
        orders_count: Number(row.orders_count || 0),
        last_order_at: row.last_order_at || null,
        source: "order_history",
      });
    }
  }
  return [...byPhone.values()];
}

async function syncEligibleRecipients(shopId) {
  await ensureMarketDayRecipientSchema();
  const eligible = await getEligibleCustomers(shopId);
  for (const row of eligible) {
    await db.query(
      `
      INSERT INTO market_day_template_recipient
        (shop_id, customer_id, name, phone, is_active, orders_count, last_order_at, source)
      VALUES (?, ?, ?, ?, 1, ?, ?, 'order_history')
      ON DUPLICATE KEY UPDATE
        customer_id = COALESCE(VALUES(customer_id), customer_id),
        name = COALESCE(NULLIF(VALUES(name), ''), name),
        orders_count = VALUES(orders_count),
        last_order_at = VALUES(last_order_at),
        updated_at = CURRENT_TIMESTAMP
      `,
      [Number(shopId), row.customer_id || null, row.name || null, row.phone, row.orders_count || 0, row.last_order_at || null],
    );
  }
  return eligible.length;
}

function mapRecipient(row) {
  const sendStatus = effectiveMarketDaySendStatus(row);
  return {
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    customer_id: row.customer_id == null ? null : Number(row.customer_id),
    name: row.name || "",
    phone: row.phone || "",
    is_active: Boolean(row.is_active),
    orders_count: Number(row.orders_count || 0),
    last_order_at: row.last_order_at || null,
    source: row.source || "manual",
    send_status: sendStatus,
    last_send_status: sendStatus,
    raw_last_send_status: row.last_send_status || row.send_status || "not_sent",
    last_send_at: row.last_send_at || null,
    last_send_attempt_at: row.last_send_attempt_at || null,
    last_send_error: sendStatus === "failed" ? (row.last_send_error || null) : null,
    phone_is_valid: isValidMarketDayRecipientPhone(row.phone),
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
  };
}

async function listMarketDayRecipients(shopId, { sync = true, includeInactive = false } = {}) {
  await ensureMarketDayRecipientSchema();
  const synced_count = sync ? await syncEligibleRecipients(shopId) : 0;
  const where = ["shop_id = ?"];
  const params = [Number(shopId)];
  if (!includeInactive) where.push("is_active = 1");
  const [rows] = await db.query(
    `
    SELECT *
    FROM market_day_template_recipient
    WHERE ${where.join(" AND ")}
    ORDER BY is_active DESC, source ASC, last_order_at DESC, id DESC
    `,
    params,
  );
  return {
    synced_count,
    statuses: eligibleOrderStatuses(),
    recipients: (rows || []).map(mapRecipient),
  };
}

async function createMarketDayRecipient(shopId, payload) {
  await ensureMarketDayRecipientSchema();
  const phone = normalizeWaNumber(payload?.phone);
  if (!phone) {
    const err = new Error("Invalid phone");
    err.status = 400;
    throw err;
  }
  const name = String(payload?.name || "").trim() || null;
  await db.query(
    `
    INSERT INTO market_day_template_recipient
      (shop_id, customer_id, name, phone, is_active, orders_count, source)
    VALUES (?, NULL, ?, ?, 1, 0, 'manual')
    ON DUPLICATE KEY UPDATE
      name = VALUES(name),
      is_active = 1,
      source = 'manual',
      last_send_status = IF(phone <> VALUES(phone), 'not_sent', last_send_status),
      last_send_error = IF(last_send_status = 'not_sent', NULL, last_send_error),
      updated_at = CURRENT_TIMESTAMP
    `,
    [Number(shopId), name, phone],
  );
  const [[row]] = await db.query(
    `SELECT * FROM market_day_template_recipient WHERE shop_id = ? AND phone = ? LIMIT 1`,
    [Number(shopId), phone],
  );
  return mapRecipient(row);
}

async function updateMarketDayRecipient(shopId, recipientId, payload) {
  await ensureMarketDayRecipientSchema();
  const id = Number(recipientId);
  if (!Number.isInteger(id) || id <= 0) {
    const err = new Error("Invalid recipient id");
    err.status = 400;
    throw err;
  }
  const [[existing]] = await db.query(
    `SELECT * FROM market_day_template_recipient WHERE id = ? AND shop_id = ? LIMIT 1`,
    [id, Number(shopId)],
  );
  if (!existing) {
    const err = new Error("Recipient not found");
    err.status = 404;
    throw err;
  }

  const phone = payload?.phone === undefined ? existing.phone : normalizeWaNumber(payload.phone);
  if (!phone) {
    const err = new Error("Invalid phone");
    err.status = 400;
    throw err;
  }
  const name = payload?.name === undefined ? existing.name : (String(payload.name || "").trim() || null);
  try {
    await db.query(
      `
      UPDATE market_day_template_recipient
      SET
        name = ?,
        phone = ?,
        source = 'manual',
        is_active = 1,
        last_send_status = CASE WHEN phone <> ? THEN 'not_sent' ELSE last_send_status END,
        last_send_error = CASE WHEN phone <> ? THEN NULL ELSE last_send_error END,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ? AND shop_id = ?
      LIMIT 1
      `,
      [name, phone, phone, phone, id, Number(shopId)],
    );
  } catch (err) {
    if (err?.code === "ER_DUP_ENTRY") {
      const e = new Error("Phone already exists in recipients");
      e.status = 409;
      throw e;
    }
    throw err;
  }
  const [[row]] = await db.query(
    `SELECT * FROM market_day_template_recipient WHERE id = ? AND shop_id = ? LIMIT 1`,
    [id, Number(shopId)],
  );
  return mapRecipient(row);
}

async function deleteMarketDayRecipient(shopId, recipientId) {
  await ensureMarketDayRecipientSchema();
  const id = Number(recipientId);
  if (!Number.isInteger(id) || id <= 0) {
    const err = new Error("Invalid recipient id");
    err.status = 400;
    throw err;
  }
  const [[row]] = await db.query(
    `SELECT * FROM market_day_template_recipient WHERE id = ? AND shop_id = ? LIMIT 1`,
    [id, Number(shopId)],
  );
  if (!row) {
    const err = new Error("Recipient not found");
    err.status = 404;
    throw err;
  }
  await db.query(
    `UPDATE market_day_template_recipient SET is_active = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND shop_id = ? LIMIT 1`,
    [id, Number(shopId)],
  );
  return mapRecipient({ ...row, is_active: 0 });
}

async function getShopWhatsappPhoneNumberId(shopId) {
  const rows = await queryOrEmpty(
    `
    SELECT phone_number_id
    FROM shop_whatsapp_phone
    WHERE shop_id = ? AND is_active = 1
    ORDER BY id ASC
    LIMIT 1
    `,
    [Number(shopId)],
  );
  const fromDb = String(rows?.[0]?.phone_number_id || "").trim();
  return fromDb || String(process.env.WHATSAPP_PHONE_ID || "").trim() || null;
}

async function getMarketDayRecipientById(shopId, recipientId) {
  await ensureMarketDayRecipientSchema();
  const id = Number(recipientId);
  if (!Number.isInteger(id) || id <= 0) {
    const err = new Error("Invalid recipient id");
    err.status = 400;
    throw err;
  }
  const [[row]] = await db.query(
    `SELECT * FROM market_day_template_recipient WHERE id = ? AND shop_id = ? AND is_active = 1 LIMIT 1`,
    [id, Number(shopId)],
  );
  if (!row) {
    const err = new Error("Recipient not found");
    err.status = 404;
    throw err;
  }
  return mapRecipient(row);
}

async function sendTemplateToOneMarketDayRecipient(shopId, recipient, phoneNumberId = null) {
  const normalizedPhone = normalizeMarketDayRecipientPhone(recipient?.phone);
  if (!isValidMarketDayRecipientPhone(normalizedPhone)) {
    const error = "מספר טלפון לא תקין";
    await markRecipientSendResult(shopId, recipient.id, "failed", error);
    return { ok: false, recipient, error, invalid_phone: true };
  }

  const resolvedPhoneNumberId = phoneNumberId || await getShopWhatsappPhoneNumberId(shopId);
  try {
    const data = await sendWhatsAppTemplate(
      normalizedPhone,
      MARKET_DAY_TEMPLATE_NAME,
      MARKET_DAY_TEMPLATE_LANGUAGE,
      [],
      resolvedPhoneNumberId,
    );
    await markRecipientSendResult(shopId, recipient.id, "sent", null);
    return { ok: true, recipient: { ...recipient, phone: normalizedPhone }, data };
  } catch (err) {
    const error = publicSendError(err);
    await markRecipientSendResult(shopId, recipient.id, "failed", error);
    return {
      ok: false,
      recipient: { ...recipient, phone: normalizedPhone },
      error,
      raw: err?.response?.data || null,
    };
  }
}

async function sendMarketDayTemplateToRecipient(shopId, recipientId) {
  const recipient = await getMarketDayRecipientById(shopId, recipientId);
  const phoneNumberId = await getShopWhatsappPhoneNumberId(shopId);
  const result = await sendTemplateToOneMarketDayRecipient(shopId, recipient, phoneNumberId);
  return {
    ok: Boolean(result.ok),
    template_name: MARKET_DAY_TEMPLATE_NAME,
    template_language: MARKET_DAY_TEMPLATE_LANGUAGE,
    phone_number_id: phoneNumberId,
    eligible_count: 1,
    sent_count: result.ok ? 1 : 0,
    failed_count: result.ok ? 0 : 1,
    failures: result.ok ? [] : [{
      recipient_id: recipient.id,
      name: recipient.name,
      phone: recipient.phone,
      error: result.error,
      invalid_phone: Boolean(result.invalid_phone),
    }],
  };
}

async function sendMarketDayTemplateToRecipients(shopId, options = {}) {
  const recipientsData = await listMarketDayRecipients(shopId, { sync: true, includeInactive: false });
  const includeAlreadySent = Boolean(options.includeAlreadySent || options.include_already_sent);
  const recipients = recipientsData.recipients.filter((r) => {
    if (!r.is_active || !r.phone) return false;
    if (includeAlreadySent) return true;
    return (r.send_status || "not_sent") !== "sent";
  });
  const skippedAlreadySent = recipientsData.recipients.filter((r) => r.is_active && r.phone && (r.send_status || "not_sent") === "sent").length;
  const phoneNumberId = await getShopWhatsappPhoneNumberId(shopId);
  const concurrency = Math.max(1, Math.min(20, Number(process.env.MARKET_DAY_TEMPLATE_SEND_CONCURRENCY || 5) || 5));
  const results = [];
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < recipients.length) {
      const index = nextIndex;
      nextIndex += 1;
      const recipient = recipients[index];
      results[index] = await sendTemplateToOneMarketDayRecipient(shopId, recipient, phoneNumberId);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, recipients.length || 1) }, worker));
  const failures = results.filter((r) => r && !r.ok).map((r) => ({
    recipient_id: r.recipient.id,
    name: r.recipient.name,
    phone: r.recipient.phone,
    error: r.error,
    invalid_phone: Boolean(r.invalid_phone),
  }));

  return {
    ok: failures.length === 0,
    template_name: MARKET_DAY_TEMPLATE_NAME,
    template_language: MARKET_DAY_TEMPLATE_LANGUAGE,
    phone_number_id: phoneNumberId,
    eligible_statuses: recipientsData.statuses,
    send_scope_statuses: includeAlreadySent ? ["not_sent", "failed", "sent"] : ["not_sent", "failed"],
    eligible_count: recipients.length,
    skipped_already_sent_count: includeAlreadySent ? 0 : skippedAlreadySent,
    sent_count: results.filter((r) => r?.ok).length,
    failed_count: failures.length,
    failures,
  };
}

module.exports = {
  MARKET_DAY_DESCRIPTION,
  MARKET_DAY_TEMPLATE_NAME,
  MARKET_DAY_TEMPLATE_LANGUAGE,
  MARKET_DAY_TIMEZONE,
  DEFAULT_ELIGIBLE_ORDER_STATUSES,
  eligibleOrderStatuses,
  hasExplicitMarketDayPayload,
  isMarketDayPayload,
  isMarketDayDescription,
  shouldForceMarketDay,
  applyMarketDayOverrides,
  nearestTuesdayWindow,
  currentMarketDayCycleStart,
  isMarketDayRequest,
  buildMarketDayPromotionsReply,
  fetchMarketDayPromotions,
  fetchDashboardMarketDayPromotions,
  ensureMarketDayRecipientSchema,
  syncEligibleRecipients,
  listMarketDayRecipients,
  createMarketDayRecipient,
  updateMarketDayRecipient,
  deleteMarketDayRecipient,
  sendMarketDayTemplateToRecipient,
  sendMarketDayTemplateToRecipients,
};

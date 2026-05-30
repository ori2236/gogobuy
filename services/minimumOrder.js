const db = require("../config/db");
const { saveOpenQuestions } = require("../utilities/openQuestions");
const { calcLineTotalWithPromo } = require("../utilities/orders");
const { formatQuantity, normalizeMaxPerProduct } = require("../utilities/productQuantityLimit");

const OPEN_Q_TYPE_MIN_ORDER_TOPUP = "MIN_ORDER_TOPUP";
const SUGGESTION_SOURCE_MIN_ORDER = "MIN_ORDER_TOPUP";

let schemaReadyPromise = null;

function fmtMoney(value) {
  const n = Number(value);
  return (Number.isFinite(n) ? n : 0).toFixed(2);
}

function money(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : 0;
}

function displayName(row, isEnglish) {
  if (!row) return "";
  if (isEnglish) {
    return (row.display_name_en && String(row.display_name_en).trim()) || row.name || "";
  }
  return row.name || row.display_name_en || "";
}

async function hasShopColumn(columnName) {
  const [rows] = await db.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'shop'
      AND COLUMN_NAME = ?
    LIMIT 1
    `,
    [columnName],
  );
  return rows.length > 0;
}

async function addShopColumnIfMissing(columnName, definition, { copyFromMinOrderAmount = false } = {}) {
  if (await hasShopColumn(columnName)) return false;
  await db.query(`ALTER TABLE shop ADD COLUMN ${columnName} ${definition}`);
  if (copyFromMinOrderAmount && (await hasShopColumn("min_order_amount"))) {
    await db.query(
      `UPDATE shop
          SET ${columnName} = COALESCE(NULLIF(min_order_amount, 0), ${columnName})`,
    );
  }
  return true;
}

async function ensureMinimumOrderSchema() {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await addShopColumnIfMissing(
        "min_delivery_order_amount",
        "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
        { copyFromMinOrderAmount: true },
      );
      await addShopColumnIfMissing(
        "min_pickup_order_amount",
        "DECIMAL(10,2) NOT NULL DEFAULT 0.00",
        { copyFromMinOrderAmount: true },
      );
    })().catch((err) => {
      schemaReadyPromise = null;
      throw err;
    });
  }
  return schemaReadyPromise;
}

async function getMinimumConfig(shop_id) {
  await ensureMinimumOrderSchema();
  const [[row]] = await db.query(
    `SELECT min_delivery_order_amount, min_pickup_order_amount
       FROM shop
      WHERE id = ?
      LIMIT 1`,
    [Number(shop_id)],
  );
  return {
    delivery: money(row?.min_delivery_order_amount || 0),
    pickup: money(row?.min_pickup_order_amount || 0),
  };
}

async function getOrderItemsSubtotal(order_id) {
  const [[row]] = await db.query(
    `SELECT COALESCE(ROUND(SUM(price), 2), 0) AS subtotal
       FROM order_item
      WHERE order_id = ?`,
    [Number(order_id)],
  );
  return money(row?.subtotal || 0);
}

function promotionForRow(row) {
  if (!row?.promo_id) return null;
  return {
    id: row.promo_id,
    kind: row.kind,
    percent_off: row.percent_off,
    amount_off: row.amount_off,
    fixed_price: row.fixed_price,
    bundle_buy_qty: row.bundle_buy_qty,
    bundle_pay_price: row.bundle_pay_price,
  };
}

function promoText(row, isEnglish) {
  const promo = promotionForRow(row);
  if (!promo) return "";

  const kind = String(promo.kind || "").toUpperCase();
  if (kind === "PERCENT_OFF" && Number(promo.percent_off) > 0) {
    return isEnglish
      ? `deal: ${formatQuantity(promo.percent_off)}% off`
      : `במבצע: ${formatQuantity(promo.percent_off)}% הנחה`;
  }
  if (kind === "AMOUNT_OFF" && Number(promo.amount_off) > 0) {
    return isEnglish
      ? `deal: ₪${fmtMoney(promo.amount_off)} off`
      : `במבצע: ₪${fmtMoney(promo.amount_off)} הנחה`;
  }
  if (kind === "FIXED_PRICE" && Number(promo.fixed_price) > 0) {
    return isEnglish
      ? `deal: ₪${fmtMoney(promo.fixed_price)}`
      : `במבצע: ₪${fmtMoney(promo.fixed_price)}`;
  }
  if (kind === "BUNDLE" && Number(promo.bundle_buy_qty) > 0 && Number(promo.bundle_pay_price) > 0) {
    return isEnglish
      ? `deal: ${formatQuantity(promo.bundle_buy_qty)} for ₪${fmtMoney(promo.bundle_pay_price)}`
      : `מבצע: ${formatQuantity(promo.bundle_buy_qty)} ב-₪${fmtMoney(promo.bundle_pay_price)}`;
  }
  return isEnglish ? "deal" : "במבצע";
}

function priceForSuggestion(row) {
  const promo = promotionForRow(row);
  const { lineTotal } = calcLineTotalWithPromo({
    unitPrice: row.price,
    amount: 1,
    soldByWeight: false,
    promo,
  });
  return money(lineTotal ?? row.price);
}

async function fetchTopupSuggestions({ shop_id, order_id, missing, maxPerProduct, isEnglish, limit = 3 }) {
  const [existingRows] = await db.query(
    `SELECT product_id FROM order_item WHERE order_id = ?`,
    [Number(order_id)],
  );
  const existingIds = (existingRows || []).map((r) => Number(r.product_id)).filter(Boolean);

  let excludeSql = "";
  if (existingIds.length) {
    excludeSql = `AND p.id NOT IN (${existingIds.map(() => "?").join(",")})`;
  }

  const maxQty = normalizeMaxPerProduct(maxPerProduct, 10);
  const candidateLimit = Math.max(Number(limit) * 8, 24);

  const [rows] = await db.query(
    `SELECT
       p.id,
       p.name,
       p.display_name_en,
       p.price,
       p.stock_amount,
       p.emoji,
       p.category,
       p.sub_category,
       pr.id AS promo_id,
       pr.kind,
       pr.percent_off,
       pr.amount_off,
       pr.fixed_price,
       pr.bundle_buy_qty,
       pr.bundle_pay_price
     FROM product p
     LEFT JOIN promotion pr
       ON pr.product_id = p.id
      AND pr.shop_id = ?
      AND (pr.start_at IS NULL OR pr.start_at <= NOW())
      AND (pr.end_at IS NULL OR pr.end_at >= NOW())
     WHERE p.shop_id = ?
       AND p.price IS NOT NULL
       AND p.price > 0
       AND p.stock_amount >= 1
       ${excludeSql}
     ORDER BY
       CASE WHEN pr.id IS NULL THEN 1 ELSE 0 END ASC,
       CASE WHEN p.price >= ? THEN 0 ELSE 1 END ASC,
       ABS(p.price - ?) ASC,
       p.updated_at DESC,
       p.id DESC
     LIMIT ?`,
    [Number(shop_id), Number(shop_id), ...existingIds, Number(missing || 0), Number(missing || 0), candidateLimit],
  );

  const seen = new Set();
  const out = [];
  for (const row of rows || []) {
    const pid = Number(row.id);
    if (!pid || seen.has(pid)) continue;
    seen.add(pid);
    if (maxQty <= 0) continue;

    const stock = Number(row.stock_amount);
    if (Number.isFinite(stock) && stock < 1) continue;

    const name = displayName(row, isEnglish);
    const unitPrice = Number(row.price);
    const effectivePrice = priceForSuggestion(row);
    out.push({
      product_id: pid,
      product_name: row.name,
      display_name: name,
      display_name_en: row.display_name_en,
      amount_to_add: 1,
      sold_by_weight: false,
      source: SUGGESTION_SOURCE_MIN_ORDER,
      unit_price: unitPrice,
      effective_price: effectivePrice,
      emoji: row.emoji || "🛒",
      promo_text: promoText(row, isEnglish),
    });
    if (out.length >= limit) break;
  }

  return out;
}

function buildSuggestionLine(action, idx, isEnglish) {
  const emoji = action.emoji || "🛒";
  const name = action.display_name || action.product_name || "";
  const price = fmtMoney(action.effective_price || action.unit_price || 0);
  const regular = Number(action.unit_price || 0);
  const effective = Number(action.effective_price || regular);
  const promo = action.promo_text || "";

  if (isEnglish) {
    const priceText = promo
      ? `${promo} — ₪${price}${regular > effective + 0.009 ? ` instead of ₪${fmtMoney(regular)}` : ""}`
      : `₪${price}`;
    return `${idx + 1}. ${emoji} ${name} — ${priceText}`;
  }

  const priceText = promo
    ? `${promo} — ₪${price}${regular > effective + 0.009 ? ` במקום ₪${fmtMoney(regular)}` : ""}`
    : `₪${price}`;
  return `${idx + 1}. ${emoji} ${name} — ${priceText}`;
}

function methodLabel(method, isEnglish) {
  if (String(method) === "delivery") return isEnglish ? "delivery" : "משלוח";
  return isEnglish ? "pickup" : "איסוף עצמי";
}

function buildMinimumOrderMessage({ method, subtotal, minimum, suggestions, isEnglish }) {
  const missing = money(Number(minimum) - Number(subtotal));
  const suggestionLines = (suggestions || []).map((a, idx) => buildSuggestionLine(a, idx, isEnglish));

  if (isEnglish) {
    return [
      "🛒 Almost there!",
      "",
      `The products total is ₪${fmtMoney(subtotal)}.`,
      `The minimum for ${methodLabel(method, true)} is ₪${fmtMoney(minimum)} — delivery fees are not counted in this minimum.`,
      "",
      `Add ₪${fmtMoney(missing)} more in products to finish the order ✅`,
      suggestionLines.length
        ? [
            "",
            "You can add one of these:",
            ...suggestionLines,
            "",
            "Reply with the product number, 'all', or write any other product you'd like to add 🛍️",
          ].join("\n")
        : "\nYou can add another product to the order and then try to finish again ✅",
    ].filter(Boolean).join("\n");
  }

  return [
    "🛒 כמעט סיימנו!",
    "",
    `סכום המוצרים כרגע הוא ₪${fmtMoney(subtotal)}.`,
    `המינימום ל${methodLabel(method, false)} מהסניף הוא ₪${fmtMoney(minimum)} — דמי משלוח לא נספרים במינימום הזה.`,
    "",
    `חסרים עוד ₪${fmtMoney(missing)} במוצרים כדי שנוכל לסיים את ההזמנה ✅`,
    suggestionLines.length
      ? [
          "",
          "אפשר להשלים בקלות עם אחד מהמוצרים האלה:",
          ...suggestionLines,
          "",
          "אפשר להשיב עם מספר המוצר, לכתוב 'הכל', או פשוט לכתוב מוצר אחר שתרצה להוסיף 🛍️",
        ].join("\n")
      : "\nאפשר להוסיף עוד מוצר להזמנה, וכשתגיע למינימום נוכל לסיים אותה ✅",
  ].filter(Boolean).join("\n");
}

async function closeExistingMinimumOrderQuestions({ customer_id, shop_id, order_id }) {
  if (!customer_id || !shop_id || !order_id) return 0;
  const [res] = await db.query(
    `UPDATE chat_open_question
        SET status = 'close'
      WHERE customer_id = ?
        AND shop_id = ?
        AND order_id = ?
        AND status = 'open'
        AND (
          product_name IN ('minimum order top-up', 'השלמת מינימום הזמנה')
          OR JSON_EXTRACT(option_set, '$.type') = ?
        )`,
    [Number(customer_id), Number(shop_id), Number(order_id), OPEN_Q_TYPE_MIN_ORDER_TOPUP],
  );
  return res?.affectedRows || 0;
}

async function validateMinimumOrderBeforeCheckout({
  order_id,
  shop_id,
  customer_id,
  isEnglish,
  maxPerProduct,
}) {
  if (!order_id || !shop_id) return null;
  const [[order]] = await db.query(
    `SELECT id, fulfillment_method
       FROM orders
      WHERE id = ? AND shop_id = ?
      LIMIT 1`,
    [Number(order_id), Number(shop_id)],
  );

  const method = String(order?.fulfillment_method || "").trim();
  if (!order || !["delivery", "pickup"].includes(method)) return null;

  const config = await getMinimumConfig(shop_id);
  const minimum = money(method === "delivery" ? config.delivery : config.pickup);
  if (!(minimum > 0)) return null;

  const subtotal = await getOrderItemsSubtotal(order_id);
  if (subtotal + 0.0001 >= minimum) return null;

  const missing = money(minimum - subtotal);
  const suggestions = await fetchTopupSuggestions({
    shop_id,
    order_id,
    missing,
    maxPerProduct,
    isEnglish,
    limit: 3,
  });

  const message = buildMinimumOrderMessage({
    method,
    subtotal,
    minimum,
    suggestions,
    isEnglish,
  });

  if (suggestions.length && customer_id) {
    await closeExistingMinimumOrderQuestions({ customer_id, shop_id, order_id });
    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id,
      questions: [
        {
          name: isEnglish ? "minimum order top-up" : "השלמת מינימום הזמנה",
          question: message,
          options: {
            type: OPEN_Q_TYPE_MIN_ORDER_TOPUP,
            actions: suggestions,
            reply_config: {
              positive: ["כן", "תוסיף", "תוסיף לי", "yes", "add"],
              negative: ["לא", "לא תודה", "no", "skip"],
              all: ["הכל", "כולם", "all", "both"],
            },
          },
        },
      ],
    });
  }

  return {
    ok: false,
    message,
    subtotal,
    minimum,
    missing,
    suggestions,
  };
}

module.exports = {
  OPEN_Q_TYPE_MIN_ORDER_TOPUP,
  SUGGESTION_SOURCE_MIN_ORDER,
  ensureMinimumOrderSchema,
  getMinimumConfig,
  getOrderItemsSubtotal,
  fetchTopupSuggestions,
  closeExistingMinimumOrderQuestions,
  validateMinimumOrderBeforeCheckout,
};

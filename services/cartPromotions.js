const db = require("../config/db");
const { round2, calcLineTotalWithPromo } = require("../utilities/promotionPricing");
const {
  applyProductGroupPromotionsToItems,
  getOrderProductGroupPromotionApplications,
  formatProductGroupPromotionApplication,
} = require("./productGroupPromotions");

const RULE_TYPES = {
  DELIVERY_FEE_OVERRIDE: "DELIVERY_FEE_OVERRIDE",
  GIFT_PRODUCT: "GIFT_PRODUCT",
  THRESHOLD_PRODUCT_FIXED_PRICE: "THRESHOLD_PRODUCT_FIXED_PRICE",
};

const COLUMN_CACHE = new Map();
let schemaReadyPromise = null;

function money(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.round(n * 100) / 100;
}

function qty(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.round(n * 1000) / 1000;
}

function cleanText(value, limit = 255) {
  const s = String(value ?? "").trim().replace(/\s+/g, " ");
  return s ? s.slice(0, limit) : null;
}

async function hasColumn(conn, tableName, columnName) {
  const key = `${tableName}.${columnName}`;
  if (COLUMN_CACHE.has(key)) return COLUMN_CACHE.get(key);

  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?
    LIMIT 1
    `,
    [tableName, columnName],
  );
  const exists = Array.isArray(rows) && rows.length > 0;
  COLUMN_CACHE.set(key, exists);
  return exists;
}

async function addColumnIfMissing(conn, tableName, columnName, definition) {
  if (await hasColumn(conn, tableName, columnName)) return;
  await conn.query(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  COLUMN_CACHE.set(`${tableName}.${columnName}`, true);
}

async function ensureCartPromotionSchema(conn = db) {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await addColumnIfMissing(
        conn,
        "promotion",
        "max_discounted_qty",
        "DECIMAL(10,3) DEFAULT NULL AFTER bundle_pay_price",
      );

      await addColumnIfMissing(
        conn,
        "order_item",
        "is_gift",
        "TINYINT(1) NOT NULL DEFAULT 0 AFTER promo_id",
      );
      await addColumnIfMissing(
        conn,
        "order_item",
        "cart_promotion_rule_id",
        "BIGINT UNSIGNED DEFAULT NULL AFTER is_gift",
      );

      await addColumnIfMissing(
        conn,
        "orders",
        "delivery_fee_before_promo",
        "DECIMAL(10,2) DEFAULT NULL AFTER delivery_fee",
      );

      await conn.query(`
        CREATE TABLE IF NOT EXISTS cart_promotion_rule (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          shop_id INT UNSIGNED NOT NULL,
          rule_type ENUM('DELIVERY_FEE_OVERRIDE','GIFT_PRODUCT','THRESHOLD_PRODUCT_FIXED_PRICE') NOT NULL,
          title VARCHAR(255) NOT NULL,
          description VARCHAR(1000) DEFAULT NULL,
          threshold_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
          delivery_fee_override DECIMAL(10,2) DEFAULT NULL,
          reward_product_id INT UNSIGNED DEFAULT NULL,
          gift_text VARCHAR(255) DEFAULT NULL,
          reward_qty DECIMAL(10,3) DEFAULT NULL,
          reward_fixed_price DECIMAL(10,2) DEFAULT NULL,
          reward_max_qty DECIMAL(10,3) DEFAULT NULL,
          threshold_base_mode ENUM('ITEMS_SUBTOTAL','EXCLUDING_REWARD_PRODUCTS') NOT NULL DEFAULT 'ITEMS_SUBTOTAL',
          priority INT NOT NULL DEFAULT 100,
          is_active TINYINT(1) NOT NULL DEFAULT 1,
          notify_customer TINYINT(1) NOT NULL DEFAULT 1,
          start_at DATETIME DEFAULT NULL,
          end_at DATETIME DEFAULT NULL,
          source VARCHAR(80) DEFAULT NULL,
          external_reward_id VARCHAR(80) DEFAULT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uniq_cart_rule_source_reward (shop_id, source, external_reward_id),
          KEY idx_cart_rule_shop_active (shop_id, is_active, start_at, end_at),
          KEY idx_cart_rule_type (rule_type),
          KEY idx_cart_rule_reward_product (shop_id, reward_product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      `);

      await addColumnIfMissing(
        conn,
        "cart_promotion_rule",
        "gift_text",
        "VARCHAR(255) DEFAULT NULL AFTER reward_product_id",
      );

      await conn.query(`
        CREATE TABLE IF NOT EXISTS order_promotion_application (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
          order_id BIGINT UNSIGNED NOT NULL,
          shop_id INT UNSIGNED NOT NULL,
          cart_promotion_rule_id BIGINT UNSIGNED NOT NULL,
          rule_type VARCHAR(80) NOT NULL,
          title VARCHAR(255) NOT NULL,
          discount_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
          applied_value DECIMAL(10,2) DEFAULT NULL,
          metadata JSON DEFAULT NULL,
          applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          notified_at DATETIME DEFAULT NULL,
          PRIMARY KEY (id),
          UNIQUE KEY uniq_order_cart_rule (order_id, cart_promotion_rule_id),
          KEY idx_order_promo_order (order_id),
          KEY idx_order_promo_shop (shop_id, cart_promotion_rule_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      `);
    })().catch((err) => {
      schemaReadyPromise = null;
      throw err;
    });
  }

  return schemaReadyPromise;
}

async function fetchActiveCartPromotionRules(conn, shop_id) {
  await ensureCartPromotionSchema(conn);
  const [rows] = await conn.query(
    `
    SELECT *
    FROM cart_promotion_rule
    WHERE shop_id = ?
      AND is_active = 1
      AND (start_at IS NULL OR start_at <= NOW())
      AND (end_at IS NULL OR end_at >= NOW())
    ORDER BY priority ASC, threshold_amount DESC, id ASC
    `,
    [Number(shop_id)],
  );
  return rows || [];
}

function buildPromoObjFromItem(row) {
  if (!row?.promo_id) return null;
  return {
    id: row.promo_id,
    kind: row.promo_kind,
    percent_off: row.percent_off,
    amount_off: row.amount_off,
    fixed_price: row.fixed_price,
    bundle_buy_qty: row.bundle_buy_qty,
    bundle_pay_price: row.bundle_pay_price,
    max_discounted_qty: row.max_discounted_qty,
  };
}

function calculateThresholdBase(items, rule) {
  const mode = String(rule?.threshold_base_mode || "ITEMS_SUBTOTAL").toUpperCase();
  const rewardProductId = Number(rule?.reward_product_id);

  return (items || []).reduce((sum, item) => {
    if (Number(item?.is_gift)) return sum;
    if (
      mode === "EXCLUDING_REWARD_PRODUCTS" &&
      Number.isFinite(rewardProductId) &&
      rewardProductId > 0 &&
      Number(item.product_id) === rewardProductId
    ) {
      return sum;
    }
    return money(sum + money(item.base_line_total ?? item.price ?? 0));
  }, 0);
}

function ruleQualifies(items, rule) {
  const threshold = money(rule?.threshold_amount || 0);
  const base = calculateThresholdBase(items, rule);
  return { ok: base >= threshold, thresholdBase: base, threshold };
}

async function loadOrderItemsForPromotion(conn, order_id, shop_id) {
  const [rows] = await conn.query(
    `
    SELECT
      oi.id AS order_item_id,
      oi.product_id,
      oi.amount,
      oi.sold_by_weight,
      oi.price,
      oi.price_locked,
      oi.promo_id,
      COALESCE(oi.is_gift, 0) AS is_gift,
      oi.cart_promotion_rule_id,

      p.name,
      p.display_name_en,
      p.price AS unit_price,
      p.stock_amount,

      pr.id AS promo_id2,
      pr.kind AS promo_kind,
      pr.percent_off,
      pr.amount_off,
      pr.fixed_price,
      pr.bundle_buy_qty,
      pr.bundle_pay_price,
      pr.max_discounted_qty
    FROM order_item oi
    JOIN orders o ON o.id = oi.order_id
    JOIN product p ON p.id = oi.product_id AND p.shop_id = o.shop_id
    LEFT JOIN promotion pr ON pr.id = oi.promo_id AND pr.shop_id = o.shop_id
    WHERE oi.order_id = ? AND o.shop_id = ?
    ORDER BY oi.id ASC
    FOR UPDATE
    `,
    [Number(order_id), Number(shop_id)],
  );

  return (rows || []).map((r) => {
    const promo = buildPromoObjFromItem(r);
    const { lineTotal } = Number(r.is_gift)
      ? { lineTotal: 0 }
      : calcLineTotalWithPromo({
          unitPrice: r.unit_price,
          amount: r.amount,
          soldByWeight: r.sold_by_weight === 1 || r.sold_by_weight === true,
          promo,
        });

    return {
      ...r,
      promo,
      base_line_total: Number.isFinite(Number(lineTotal))
        ? money(lineTotal)
        : money(Number(r.unit_price || 0) * Number(r.amount || 0)),
    };
  });
}

async function restoreAndRemoveOldGiftItems(conn, { order_id, shop_id }) {
  const [giftRows] = await conn.query(
    `
    SELECT product_id, amount
    FROM order_item
    WHERE order_id = ? AND COALESCE(is_gift, 0) = 1
    FOR UPDATE
    `,
    [Number(order_id)],
  );

  for (const gift of giftRows || []) {
    await conn.query(
      `UPDATE product SET stock_amount = stock_amount + ? WHERE id = ? AND shop_id = ?`,
      [qty(gift.amount), Number(gift.product_id), Number(shop_id)],
    );
  }

  if ((giftRows || []).length) {
    await conn.query(
      `DELETE FROM order_item WHERE order_id = ? AND COALESCE(is_gift, 0) = 1`,
      [Number(order_id)],
    );
  }
}

async function resetNonGiftItemsToProductPromos(conn, items) {
  for (const item of items || []) {
    if (Number(item.is_gift)) continue;
    await conn.query(
      `
      UPDATE order_item
         SET price = ?,
             price_locked = 1,
             cart_promotion_rule_id = NULL,
             is_gift = 0
       WHERE id = ?
      `,
      [money(item.base_line_total), Number(item.order_item_id)],
    );
    item.current_line_total = money(item.base_line_total);
    item.applied_cart_rule_id = null;
  }
}

async function applyThresholdProductFixedPrice(conn, { items, rule, applications }) {
  const rewardProductId = Number(rule.reward_product_id);
  const fixedPrice = money(rule.reward_fixed_price);
  if (!Number.isFinite(rewardProductId) || rewardProductId <= 0) return;
  if (!Number.isFinite(fixedPrice) || fixedPrice < 0) return;

  const qualify = ruleQualifies(items, rule);
  if (!qualify.ok) return;

  const maxQty = Number(rule.reward_max_qty || rule.reward_qty || 0);

  for (const item of items || []) {
    if (Number(item.is_gift)) continue;
    if (Number(item.product_id) !== rewardProductId) continue;

    const amount = qty(item.amount);
    if (!(amount > 0)) continue;

    const discountedQty = maxQty > 0 ? Math.min(amount, maxQty) : amount;
    const regularQty = Math.max(0, amount - discountedQty);
    const regularUnit = money(item.unit_price);
    const newTotal = money(discountedQty * fixedPrice + regularQty * regularUnit);
    const discountAmount = money((item.current_line_total ?? item.base_line_total) - newTotal);

    await conn.query(
      `UPDATE order_item SET price = ?, price_locked = 1, cart_promotion_rule_id = ? WHERE id = ?`,
      [newTotal, Number(rule.id), Number(item.order_item_id)],
    );

    item.current_line_total = newTotal;
    item.applied_cart_rule_id = Number(rule.id);

    applications.push({
      rule,
      discount_amount: Math.max(0, discountAmount),
      applied_value: fixedPrice,
      metadata: {
        threshold_base: qualify.thresholdBase,
        threshold_amount: qualify.threshold,
        reward_product_id: rewardProductId,
        discounted_qty: discountedQty,
      },
    });
  }
}

async function applyGiftProduct(conn, { order_id, shop_id, items, rule, applications }) {
  const qualify = ruleQualifies(items, rule);
  if (!qualify.ok) return;

  const rewardProductId = Number(rule.reward_product_id);
  const giftQty = qty(rule.reward_qty || 1, 1);
  if (!Number.isFinite(rewardProductId) || rewardProductId <= 0 || !(giftQty > 0)) return;

  const alreadyPaidSameProduct = (items || []).some(
    (item) => !Number(item.is_gift) && Number(item.product_id) === rewardProductId,
  );
  if (alreadyPaidSameProduct) {
    applications.push({
      rule,
      discount_amount: 0,
      applied_value: 0,
      metadata: {
        threshold_base: qualify.thresholdBase,
        threshold_amount: qualify.threshold,
        reward_product_id: rewardProductId,
        skipped: "PRODUCT_ALREADY_IN_ORDER",
      },
    });
    return;
  }

  const [[product]] = await conn.query(
    `SELECT id, price, stock_amount FROM product WHERE id = ? AND shop_id = ? FOR UPDATE`,
    [rewardProductId, Number(shop_id)],
  );
  if (!product) return;

  const stock = Number(product.stock_amount);
  if (Number.isFinite(stock) && stock < giftQty) {
    applications.push({
      rule,
      discount_amount: 0,
      applied_value: 0,
      metadata: {
        threshold_base: qualify.thresholdBase,
        threshold_amount: qualify.threshold,
        reward_product_id: rewardProductId,
        skipped: "OUT_OF_STOCK",
        stock_amount: stock,
      },
    });
    return;
  }

  await conn.query(
    `UPDATE product SET stock_amount = stock_amount - ? WHERE id = ? AND shop_id = ?`,
    [giftQty, rewardProductId, Number(shop_id)],
  );

  await conn.query(
    `
    INSERT INTO order_item
      (order_id, product_id, amount, sold_by_weight, requested_units, price, price_locked, promo_id, is_gift, cart_promotion_rule_id, created_at)
    VALUES (?, ?, ?, 0, NULL, 0.00, 1, NULL, 1, ?, NOW(6))
    `,
    [Number(order_id), rewardProductId, giftQty, Number(rule.id)],
  );

  applications.push({
    rule,
    discount_amount: money(Number(product.price || 0) * giftQty),
    applied_value: 0,
    metadata: {
      threshold_base: qualify.thresholdBase,
      threshold_amount: qualify.threshold,
      reward_product_id: rewardProductId,
      gift_qty: giftQty,
    },
  });
}

function chooseBestDeliveryRule(items, rules) {
  const candidates = [];
  for (const rule of rules || []) {
    if (String(rule.rule_type) !== RULE_TYPES.DELIVERY_FEE_OVERRIDE) continue;
    const qualify = ruleQualifies(items, rule);
    if (!qualify.ok) continue;
    const fee = money(rule.delivery_fee_override);
    candidates.push({ rule, fee, qualify });
  }

  candidates.sort((a, b) => {
    if (a.fee !== b.fee) return a.fee - b.fee;
    return money(b.rule.threshold_amount) - money(a.rule.threshold_amount);
  });

  return candidates[0] || null;
}

async function updateApplications(conn, { order_id, shop_id, applications }) {
  await conn.query(
    `DELETE FROM order_promotion_application WHERE order_id = ? AND shop_id = ?`,
    [Number(order_id), Number(shop_id)],
  );

  for (const app of applications || []) {
    const rule = app.rule;
    await conn.query(
      `
      INSERT INTO order_promotion_application
        (order_id, shop_id, cart_promotion_rule_id, rule_type, title, discount_amount, applied_value, metadata, applied_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
      ON DUPLICATE KEY UPDATE
        rule_type = VALUES(rule_type),
        title = VALUES(title),
        discount_amount = VALUES(discount_amount),
        applied_value = VALUES(applied_value),
        metadata = VALUES(metadata),
        applied_at = NOW()
      `,
      [
        Number(order_id),
        Number(shop_id),
        Number(rule.id),
        String(rule.rule_type),
        cleanText(rule.title, 255) || String(rule.rule_type),
        money(app.discount_amount),
        app.applied_value === null || app.applied_value === undefined ? null : money(app.applied_value),
        JSON.stringify(app.metadata || {}),
      ],
    );
  }
}

async function applyCartPromotionsToOrder(conn = db, { order_id, shop_id = null } = {}) {
  await ensureCartPromotionSchema(conn);
  const orderId = Number(order_id);
  if (!Number.isFinite(orderId) || orderId <= 0) return null;

  const [[order]] = await conn.query(
    `SELECT id, shop_id, fulfillment_method, delivery_fee, delivery_fee_before_promo FROM orders WHERE id = ? LIMIT 1 FOR UPDATE`,
    [orderId],
  );
  if (!order) return null;

  const resolvedShopId = Number(shop_id || order.shop_id);
  const rules = await fetchActiveCartPromotionRules(conn, resolvedShopId);

  await restoreAndRemoveOldGiftItems(conn, { order_id: orderId, shop_id: resolvedShopId });

  const items = await loadOrderItemsForPromotion(conn, orderId, resolvedShopId);
  await resetNonGiftItemsToProductPromos(conn, items);

  const applications = [];

  for (const rule of rules || []) {
    if (String(rule.rule_type) === RULE_TYPES.THRESHOLD_PRODUCT_FIXED_PRICE) {
      await applyThresholdProductFixedPrice(conn, { items, rule, applications });
    }
  }

  await applyProductGroupPromotionsToItems(conn, {
    order_id: orderId,
    shop_id: resolvedShopId,
    items,
  });

  for (const rule of rules || []) {
    if (String(rule.rule_type) === RULE_TYPES.GIFT_PRODUCT) {
      await applyGiftProduct(conn, {
        order_id: orderId,
        shop_id: resolvedShopId,
        items,
        rule,
        applications,
      });
    }
  }

  const deliveryRule = chooseBestDeliveryRule(items, rules);
  let deliveryFee = 0;
  if (String(order.fulfillment_method || "") === "delivery") {
    const baseDeliveryFee =
      order.delivery_fee_before_promo !== null && order.delivery_fee_before_promo !== undefined
        ? money(order.delivery_fee_before_promo)
        : money(order.delivery_fee);
    deliveryFee = deliveryRule ? deliveryRule.fee : baseDeliveryFee;

    if (deliveryRule) {
      applications.push({
        rule: deliveryRule.rule,
        discount_amount: Math.max(0, money(baseDeliveryFee - deliveryFee)),
        applied_value: deliveryFee,
        metadata: {
          threshold_base: deliveryRule.qualify.thresholdBase,
          threshold_amount: deliveryRule.qualify.threshold,
          base_delivery_fee: baseDeliveryFee,
          delivery_fee: deliveryFee,
        },
      });
    }
  }

  await updateApplications(conn, {
    order_id: orderId,
    shop_id: resolvedShopId,
    applications,
  });

  const [[sumRow]] = await conn.query(
    `SELECT COALESCE(ROUND(SUM(price), 2), 0) AS itemsSubtotal FROM order_item WHERE order_id = ?`,
    [orderId],
  );
  const itemsSubtotal = money(sumRow?.itemsSubtotal || 0);
  const total = money(itemsSubtotal + deliveryFee);

  const sets = [`price = ?`, `updated_at = NOW(6)`];
  const params = [total];
  if (String(order.fulfillment_method || "") === "delivery") {
    sets.push(`delivery_fee = ?`);
    params.push(deliveryFee);
  }
  params.push(orderId);

  await conn.query(`UPDATE orders SET ${sets.join(", ")} WHERE id = ?`, params);

  return {
    order_id: orderId,
    shop_id: resolvedShopId,
    itemsSubtotal,
    deliveryFee,
    total,
    applications,
  };
}

async function getOrderCartPromotionApplications(order_id, shop_id = null) {
  await ensureCartPromotionSchema();
  const params = [Number(order_id)];
  let sql = `
    SELECT
      opa.*,
      cpr.threshold_amount,
      cpr.delivery_fee_override,
      cpr.reward_product_id,
      cpr.reward_qty,
      cpr.reward_fixed_price,
      cpr.reward_max_qty,
      cpr.threshold_base_mode,
      p.name AS reward_product_name,
      p.display_name_en AS reward_display_name_en
    FROM order_promotion_application opa
    LEFT JOIN cart_promotion_rule cpr ON cpr.id = opa.cart_promotion_rule_id
    LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = opa.shop_id
    WHERE opa.order_id = ?
  `;
  if (shop_id) {
    sql += ` AND opa.shop_id = ?`;
    params.push(Number(shop_id));
  }
  sql += ` ORDER BY opa.id ASC`;
  const [rows] = await db.query(sql, params);
  return rows || [];
}

function parseApplicationMetadata(row) {
  const raw = row?.metadata;
  if (!raw) return {};
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
}

function applicationThreshold(row) {
  const meta = parseApplicationMetadata(row);
  const fromRow = row?.threshold_amount;
  const fromMeta = meta?.threshold_amount;
  const value = fromRow !== null && fromRow !== undefined ? fromRow : fromMeta;
  const n = money(value);
  return n > 0 ? n : null;
}

function thresholdPrefix(row, isEnglish = false) {
  const threshold = applicationThreshold(row);
  if (!threshold) return "";
  return isEnglish ? `Above ₪${threshold.toFixed(2)} - ` : `בקנייה מעל ₪${threshold.toFixed(2)} - `;
}

function formatCartPromotionApplication(row, isEnglish = false) {
  const type = String(row?.rule_type || "");
  const title = String(row?.title || "").trim();
  const prefix = thresholdPrefix(row, isEnglish);
  const rewardName = isEnglish
    ? String(row?.reward_display_name_en || row?.reward_product_name || "").trim()
    : String(row?.reward_product_name || row?.reward_display_name_en || "").trim();

  if (type === RULE_TYPES.DELIVERY_FEE_OVERRIDE) {
    const fee = money(row?.applied_value);
    if (fee <= 0) return isEnglish ? `🚚 ${prefix}free delivery` : `🚚 ${prefix}משלוח חינם`;
    return isEnglish
      ? `🚚 ${prefix}delivery for ₪${fee.toFixed(2)}`
      : `🚚 ${prefix}משלוח ב-₪${fee.toFixed(2)}`;
  }

  if (type === RULE_TYPES.GIFT_PRODUCT) {
    return isEnglish
      ? `🎁 ${prefix}gift${rewardName ? `: ${rewardName}` : ""}`
      : `🎁 ${prefix}מתנה${rewardName ? `: ${rewardName}` : ""}`;
  }

  if (type === RULE_TYPES.THRESHOLD_PRODUCT_FIXED_PRICE) {
    const price = money(row?.applied_value);
    const maxQty = qty(row?.reward_max_qty || 0, 0);
    const maxText = maxQty > 0
      ? isEnglish
        ? `, up to ${maxQty} units`
        : `, עד ${maxQty} יח׳`
      : "";
    return isEnglish
      ? `🏷️ ${prefix}${rewardName || "selected product"} for ₪${price.toFixed(2)}${maxText}`
      : `🏷️ ${prefix}${rewardName || "מוצר נבחר"} ב-₪${price.toFixed(2)}${maxText}`;
  }

  return title ? `🏷️ ${title}` : isEnglish ? "🏷️ Basket promotion" : "🏷️ מבצע סל";
}

async function buildOrderCartPromotionLines(order_id, shop_id, isEnglish = false) {
  const rows = await getOrderCartPromotionApplications(order_id, shop_id);
  const groupRows = await getOrderProductGroupPromotionApplications(order_id, shop_id);
  return [
    ...groupRows.map((r) => formatProductGroupPromotionApplication(r, isEnglish)),
    ...rows.map((r) => formatCartPromotionApplication(r, isEnglish)),
  ].filter(Boolean);
}

async function fetchActiveCartPromotionOverview(shop_id, { limit = 50 } = {}) {
  await ensureCartPromotionSchema();
  const [rows] = await db.query(
    `
    SELECT cpr.*, p.name AS reward_product_name, p.display_name_en AS reward_display_name_en
    FROM cart_promotion_rule cpr
    LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = cpr.shop_id
    WHERE cpr.shop_id = ?
      AND cpr.is_active = 1
      AND (cpr.start_at IS NULL OR cpr.start_at <= NOW())
      AND (cpr.end_at IS NULL OR cpr.end_at >= NOW())
    ORDER BY cpr.priority ASC, cpr.threshold_amount ASC, cpr.id ASC
    LIMIT ${Math.min(Math.max(Number(limit) || 50, 1), 200)}
    `,
    [Number(shop_id)],
  );
  return rows || [];
}

function formatCartPromotionRule(row, isEnglish = false) {
  const type = String(row?.rule_type || "");
  const threshold = money(row?.threshold_amount || 0);
  const rewardName = isEnglish
    ? String(row?.reward_display_name_en || row?.reward_product_name || "").trim()
    : String(row?.reward_product_name || row?.reward_display_name_en || "").trim();

  if (type === RULE_TYPES.DELIVERY_FEE_OVERRIDE) {
    const fee = money(row.delivery_fee_override);
    if (isEnglish) {
      return fee <= 0
        ? `Above ₪${threshold.toFixed(2)} - free delivery`
        : `Above ₪${threshold.toFixed(2)} - delivery for ₪${fee.toFixed(2)}`;
    }
    return fee <= 0
      ? `בקנייה מעל ₪${threshold.toFixed(2)} - משלוח חינם`
      : `בקנייה מעל ₪${threshold.toFixed(2)} - משלוח ב-₪${fee.toFixed(2)}`;
  }

  if (type === RULE_TYPES.GIFT_PRODUCT) {
    return isEnglish
      ? `Above ₪${threshold.toFixed(2)} - gift${rewardName ? `: ${rewardName}` : ""}`
      : `בקנייה מעל ₪${threshold.toFixed(2)} - מתנה${rewardName ? `: ${rewardName}` : ""}`;
  }

  if (type === RULE_TYPES.THRESHOLD_PRODUCT_FIXED_PRICE) {
    const price = money(row.reward_fixed_price);
    return isEnglish
      ? `Above ₪${threshold.toFixed(2)} - ${rewardName || "selected product"} for ₪${price.toFixed(2)}`
      : `בקנייה מעל ₪${threshold.toFixed(2)} - ${rewardName || "מוצר נבחר"} ב-₪${price.toFixed(2)}`;
  }

  return String(row?.title || "").trim() || (isEnglish ? "Basket promotion" : "מבצע סל");
}

module.exports = {
  RULE_TYPES,
  ensureCartPromotionSchema,
  fetchActiveCartPromotionRules,
  applyCartPromotionsToOrder,
  getOrderCartPromotionApplications,
  formatCartPromotionApplication,
  buildOrderCartPromotionLines,
  fetchActiveCartPromotionOverview,
  formatCartPromotionRule,
};

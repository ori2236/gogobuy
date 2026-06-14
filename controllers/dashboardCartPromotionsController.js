const db = require("../config/db");
const { parseShopId, clampInt } = require("../utilities/dashboardUtils");
const {
  RULE_TYPES,
  ensureCartPromotionSchema,
} = require("../services/cartPromotions");

const ALLOWED_RULE_TYPES = new Set(Object.values(RULE_TYPES));
const ALLOWED_STATUS_FILTERS = new Set(["all", "active", "inactive"]);
const ALLOWED_THRESHOLD_BASE_MODES = new Set([
  "ITEMS_SUBTOTAL",
  "EXCLUDING_REWARD_PRODUCTS",
]);

function trimOrNull(value, limit = 1000) {
  const s = String(value ?? "").trim().replace(/\s+/g, " ");
  return s ? s.slice(0, limit) : null;
}

function moneyNumber(value, fieldName, { min = 0, required = true } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) return { error: `${fieldName} is required` };
    return { value: null };
  }

  const n = Number(value);
  if (!Number.isFinite(n)) return { error: `${fieldName} must be a number` };
  if (n < min) return { error: `${fieldName} is too small` };
  return { value: Math.round(n * 100) / 100 };
}

function qtyNumber(value, fieldName, { min = 0.001, required = true } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) return { error: `${fieldName} is required` };
    return { value: null };
  }

  const n = Number(value);
  if (!Number.isFinite(n)) return { error: `${fieldName} must be a number` };
  if (n < min) return { error: `${fieldName} is too small` };
  return { value: Math.round(n * 1000) / 1000 };
}

function intNumber(value, fieldName, { min = 0, required = true } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) return { error: `${fieldName} is required` };
    return { value: null };
  }

  const n = Number(value);
  if (!Number.isInteger(n)) return { error: `${fieldName} must be an integer` };
  if (n < min) return { error: `${fieldName} is too small` };
  return { value: n };
}

function boolValue(value, fallback = true) {
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "number") return value ? 1 : 0;
  if (typeof value === "string") {
    const s = value.trim().toLowerCase();
    if (["1", "true", "yes", "כן", "on"].includes(s)) return 1;
    if (["0", "false", "no", "לא", "off"].includes(s)) return 0;
  }
  return fallback ? 1 : 0;
}

function normalizeDateTime(value, fieldName, { required = false } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) return { error: `${fieldName} is required` };
    return { value: null, comparable: null };
  }

  const raw = String(value).trim().replace("T", " ");
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return { error: `${fieldName} must be YYYY-MM-DD HH:mm:ss` };

  const normalized = `${match[1]}-${match[2]}-${match[3]} ${match[4]}:${match[5]}:${match[6] || "00"}`;
  const comparable = new Date(`${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6] || "00"}`);

  if (Number.isNaN(comparable.getTime())) {
    return { error: `${fieldName} is not a valid datetime` };
  }

  return { value: normalized, comparable };
}

function activeRuleSql(alias = "cpr") {
  return `(${alias}.is_active = 1 AND (${alias}.start_at IS NULL OR ${alias}.start_at <= NOW()) AND (${alias}.end_at IS NULL OR ${alias}.end_at > NOW()))`;
}

function defaultTitleForRule(payload, product) {
  const threshold = Number(payload.threshold_amount || 0).toFixed(2);
  if (payload.rule_type === RULE_TYPES.DELIVERY_FEE_OVERRIDE) {
    const fee = Number(payload.delivery_fee_override || 0);
    return fee <= 0
      ? `בקנייה מעל ₪${threshold} - משלוח חינם`
      : `בקנייה מעל ₪${threshold} - משלוח ב-₪${fee.toFixed(2)}`;
  }
  if (payload.rule_type === RULE_TYPES.GIFT_PRODUCT) {
    return `בקנייה מעל ₪${threshold} - ${product?.name || payload.gift_text || "מתנה"} מתנה`;
  }
  if (payload.rule_type === RULE_TYPES.THRESHOLD_PRODUCT_FIXED_PRICE) {
    return `בקנייה מעל ₪${threshold} - ${product?.name || "מוצר"} ב-₪${Number(payload.reward_fixed_price || 0).toFixed(2)}`;
  }
  return "מבצע סל";
}

async function ensureProductExists(shopId, productId) {
  if (!productId) return null;
  const [rows] = await db.query(
    `
    SELECT id, name, display_name_en, price, category, sub_category
    FROM product
    WHERE id = ? AND shop_id = ?
    LIMIT 1
    `,
    [Number(productId), Number(shopId)],
  );
  return rows?.[0] || null;
}

function parseCartRulePayload(body) {
  const ruleType = String(body?.rule_type ?? body?.ruleType ?? "").trim().toUpperCase();
  if (!ALLOWED_RULE_TYPES.has(ruleType)) return { error: "Invalid rule_type" };

  const threshold = moneyNumber(
    body?.threshold_amount ?? body?.thresholdAmount,
    "threshold_amount",
    { min: 0, required: true },
  );
  if (threshold.error) return { error: threshold.error };

  const start = normalizeDateTime(body?.start_at ?? body?.startAt, "start_at", {
    required: false,
  });
  if (start.error) return { error: start.error };

  const end = normalizeDateTime(body?.end_at ?? body?.endAt, "end_at", {
    required: false,
  });
  if (end.error) return { error: end.error };

  if (start.comparable && end.comparable && end.comparable.getTime() <= start.comparable.getTime()) {
    return { error: "end_at must be after start_at" };
  }

  const priority = intNumber(body?.priority, "priority", { min: 0, required: false });
  if (priority.error) return { error: priority.error };

  const thresholdBaseMode = String(
    body?.threshold_base_mode ?? body?.thresholdBaseMode ?? "ITEMS_SUBTOTAL",
  ).trim().toUpperCase();
  if (!ALLOWED_THRESHOLD_BASE_MODES.has(thresholdBaseMode)) {
    return { error: "Invalid threshold_base_mode" };
  }

  const payload = {
    rule_type: ruleType,
    title: trimOrNull(body?.title, 255),
    description: trimOrNull(body?.description, 1000),
    threshold_amount: threshold.value,
    delivery_fee_override: null,
    reward_product_id: null,
    gift_text: null,
    reward_qty: null,
    reward_fixed_price: null,
    reward_max_qty: null,
    threshold_base_mode: thresholdBaseMode,
    priority: priority.value ?? 100,
    is_active: boolValue(body?.is_active ?? body?.isActive, true),
    notify_customer: boolValue(body?.notify_customer ?? body?.notifyCustomer, true),
    start_at: start.value,
    end_at: end.value,
    source: trimOrNull(body?.source, 80),
    external_reward_id: trimOrNull(body?.external_reward_id ?? body?.externalRewardId, 80),
  };

  if (ruleType === RULE_TYPES.DELIVERY_FEE_OVERRIDE) {
    const fee = moneyNumber(
      body?.delivery_fee_override ?? body?.deliveryFeeOverride,
      "delivery_fee_override",
      { min: 0, required: true },
    );
    if (fee.error) return { error: fee.error };
    payload.delivery_fee_override = fee.value;
  }

  if (ruleType === RULE_TYPES.GIFT_PRODUCT) {
    const rawProductId = body?.reward_product_id ?? body?.rewardProductId;
    const productId = rawProductId === null || rawProductId === undefined || rawProductId === ""
      ? null
      : Number(rawProductId);
    const giftText = trimOrNull(body?.gift_text ?? body?.giftText ?? body?.reward_text ?? body?.rewardText, 255);
    if (productId !== null && (!Number.isInteger(productId) || productId <= 0)) {
      return { error: "reward_product_id must be a positive integer" };
    }
    if (!productId && !giftText) {
      return { error: "reward_product_id or gift_text is required" };
    }
    const giftQty = qtyNumber(body?.reward_qty ?? body?.rewardQty ?? 1, "reward_qty", {
      min: 0.001,
      required: true,
    });
    if (giftQty.error) return { error: giftQty.error };
    payload.reward_product_id = productId;
    payload.gift_text = productId ? null : giftText;
    payload.reward_qty = giftQty.value;
  }

  if (ruleType === RULE_TYPES.THRESHOLD_PRODUCT_FIXED_PRICE) {
    const productId = Number(body?.reward_product_id ?? body?.rewardProductId);
    if (!Number.isInteger(productId) || productId <= 0) {
      return { error: "reward_product_id is required" };
    }
    const price = moneyNumber(
      body?.reward_fixed_price ?? body?.rewardFixedPrice,
      "reward_fixed_price",
      { min: 0, required: true },
    );
    if (price.error) return { error: price.error };
    const maxQty = qtyNumber(
      body?.reward_max_qty ?? body?.rewardMaxQty,
      "reward_max_qty",
      { min: 0.001, required: false },
    );
    if (maxQty.error) return { error: maxQty.error };
    payload.reward_product_id = productId;
    payload.reward_fixed_price = price.value;
    payload.reward_max_qty = maxQty.value;
  }

  return { payload };
}

function mapRuleRow(row) {
  const isCurrentActive = Boolean(row.is_currently_active);
  const isUpcoming = Boolean(row.is_upcoming);
  const isExpired = Boolean(row.is_expired);
  return {
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    rule_type: row.rule_type,
    title: row.title ?? "",
    description: row.description ?? null,
    threshold_amount: row.threshold_amount == null ? 0 : Number(row.threshold_amount),
    delivery_fee_override:
      row.delivery_fee_override == null ? null : Number(row.delivery_fee_override),
    reward_product_id:
      row.reward_product_id == null ? null : Number(row.reward_product_id),
    gift_text: row.gift_text ?? null,
    reward_product_name: row.reward_product_name ?? null,
    reward_display_name_en: row.reward_display_name_en ?? null,
    reward_product_price:
      row.reward_product_price == null ? null : Number(row.reward_product_price),
    reward_qty: row.reward_qty == null ? null : Number(row.reward_qty),
    reward_fixed_price:
      row.reward_fixed_price == null ? null : Number(row.reward_fixed_price),
    reward_max_qty: row.reward_max_qty == null ? null : Number(row.reward_max_qty),
    threshold_base_mode: row.threshold_base_mode ?? "ITEMS_SUBTOTAL",
    priority: row.priority == null ? 100 : Number(row.priority),
    is_active: Boolean(row.is_active),
    notify_customer: Boolean(row.notify_customer),
    start_at: row.start_at ?? null,
    end_at: row.end_at ?? null,
    source: row.source ?? null,
    external_reward_id: row.external_reward_id ?? null,
    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
    is_currently_active: isCurrentActive,
    is_upcoming: isUpcoming,
    is_expired: isExpired,
    status: isCurrentActive
      ? "active"
      : isUpcoming
        ? "upcoming"
        : isExpired
          ? "expired"
          : "inactive",
  };
}

async function getRuleById(shopId, id) {
  await ensureCartPromotionSchema();
  const [rows] = await db.query(
    `
    SELECT
      cpr.*,
      p.name AS reward_product_name,
      p.display_name_en AS reward_display_name_en,
      p.price AS reward_product_price,
      ${activeRuleSql("cpr")} AS is_currently_active,
      (cpr.is_active = 1 AND cpr.start_at IS NOT NULL AND cpr.start_at > NOW()) AS is_upcoming,
      (cpr.end_at IS NOT NULL AND cpr.end_at <= NOW()) AS is_expired
    FROM cart_promotion_rule cpr
    LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = cpr.shop_id
    WHERE cpr.id = ? AND cpr.shop_id = ?
    LIMIT 1
    `,
    [Number(id), Number(shopId)],
  );
  return rows?.[0] ? mapRuleRow(rows[0]) : null;
}

exports.listCartPromotionRules = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    await ensureCartPromotionSchema();

    const status = String(req.query.status || "all").trim().toLowerCase();
    const statusFilter = ALLOWED_STATUS_FILTERS.has(status) ? status : "all";
    const q = String(req.query.q ?? "").trim();
    const limit = clampInt(req.query.limit, 1, 500, 200);

    const baseWhere = ["cpr.shop_id = ?"];
    const baseParams = [shopId];

    if (q) {
      baseWhere.push(
        "(cpr.title LIKE ? OR cpr.description LIKE ? OR cpr.gift_text LIKE ? OR p.name LIKE ? OR p.display_name_en LIKE ? OR CAST(cpr.id AS CHAR) = ? OR cpr.external_reward_id = ?)",
      );
      const like = `%${q}%`;
      baseParams.push(like, like, like, like, like, q, q);
    }

    const activeCondition = activeRuleSql("cpr");
    const where = baseWhere.slice();
    const params = baseParams.slice();
    if (statusFilter === "active") where.push(activeCondition);
    if (statusFilter === "inactive") where.push(`NOT ${activeCondition}`);

    const [[countsRow]] = await db.query(
      `
      SELECT
        COUNT(*) AS total_count,
        SUM(CASE WHEN ${activeCondition} THEN 1 ELSE 0 END) AS active_count,
        SUM(CASE WHEN NOT ${activeCondition} THEN 1 ELSE 0 END) AS inactive_count
      FROM cart_promotion_rule cpr
      LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = cpr.shop_id
      WHERE ${baseWhere.join(" AND ")}
      `,
      baseParams,
    );

    const [rows] = await db.query(
      `
      SELECT
        cpr.*,
        p.name AS reward_product_name,
        p.display_name_en AS reward_display_name_en,
        p.price AS reward_product_price,
        ${activeCondition} AS is_currently_active,
        (cpr.is_active = 1 AND cpr.start_at IS NOT NULL AND cpr.start_at > NOW()) AS is_upcoming,
        (cpr.end_at IS NOT NULL AND cpr.end_at <= NOW()) AS is_expired
      FROM cart_promotion_rule cpr
      LEFT JOIN product p ON p.id = cpr.reward_product_id AND p.shop_id = cpr.shop_id
      WHERE ${where.join(" AND ")}
      ORDER BY
        CASE
          WHEN ${activeCondition} THEN 0
          WHEN cpr.is_active = 1 AND cpr.start_at IS NOT NULL AND cpr.start_at > NOW() THEN 1
          ELSE 2
        END ASC,
        cpr.priority ASC,
        cpr.threshold_amount ASC,
        cpr.id DESC
      LIMIT ?
      `,
      [...params, limit],
    );

    return res.json({
      ok: true,
      cart_promotion_rules: (rows || []).map(mapRuleRow),
      counts: {
        total: Number(countsRow?.total_count ?? 0),
        active: Number(countsRow?.active_count ?? 0),
        inactive: Number(countsRow?.inactive_count ?? 0),
      },
    });
  } catch (err) {
    console.error("[cartPromotions.listCartPromotionRules]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.createCartPromotionRule = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    await ensureCartPromotionSchema();

    const parsed = parseCartRulePayload(req.body || {});
    if (parsed.error) return res.status(400).json({ ok: false, message: parsed.error });

    const p = parsed.payload;
    let product = null;
    if (p.reward_product_id) {
      product = await ensureProductExists(shopId, p.reward_product_id);
      if (!product) return res.status(404).json({ ok: false, message: "Reward product not found" });
    }
    if (!p.title) p.title = defaultTitleForRule(p, product);

    const [ins] = await db.query(
      `
      INSERT INTO cart_promotion_rule
        (shop_id, rule_type, title, description, threshold_amount, delivery_fee_override,
         reward_product_id, gift_text, reward_qty, reward_fixed_price, reward_max_qty, threshold_base_mode,
         priority, is_active, notify_customer, start_at, end_at, source, external_reward_id,
         created_at, updated_at)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      `,
      [
        shopId,
        p.rule_type,
        p.title,
        p.description,
        p.threshold_amount,
        p.delivery_fee_override,
        p.reward_product_id,
        p.gift_text,
        p.reward_qty,
        p.reward_fixed_price,
        p.reward_max_qty,
        p.threshold_base_mode,
        p.priority,
        p.is_active,
        p.notify_customer,
        p.start_at,
        p.end_at,
        p.source,
        p.external_reward_id,
      ],
    );

    const rule = await getRuleById(shopId, ins.insertId);
    return res.status(201).json({ ok: true, cart_promotion_rule: rule });
  } catch (err) {
    console.error("[cartPromotions.createCartPromotionRule]", err);
    if (err?.code === "ER_DUP_ENTRY") {
      return res.status(409).json({ ok: false, message: "Cart promotion rule already exists" });
    }
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.updateCartPromotionRule = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid cart promotion rule id" });
    }

    await ensureCartPromotionSchema();

    const existing = await getRuleById(shopId, id);
    if (!existing) return res.status(404).json({ ok: false, message: "Cart promotion rule not found" });

    const parsed = parseCartRulePayload(req.body || {});
    if (parsed.error) return res.status(400).json({ ok: false, message: parsed.error });

    const p = parsed.payload;
    let product = null;
    if (p.reward_product_id) {
      product = await ensureProductExists(shopId, p.reward_product_id);
      if (!product) return res.status(404).json({ ok: false, message: "Reward product not found" });
    }
    if (!p.title) p.title = defaultTitleForRule(p, product);

    await db.query(
      `
      UPDATE cart_promotion_rule
      SET
        rule_type = ?,
        title = ?,
        description = ?,
        threshold_amount = ?,
        delivery_fee_override = ?,
        reward_product_id = ?,
        gift_text = ?,
        reward_qty = ?,
        reward_fixed_price = ?,
        reward_max_qty = ?,
        threshold_base_mode = ?,
        priority = ?,
        is_active = ?,
        notify_customer = ?,
        start_at = ?,
        end_at = ?,
        source = ?,
        external_reward_id = ?,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ? AND shop_id = ?
      LIMIT 1
      `,
      [
        p.rule_type,
        p.title,
        p.description,
        p.threshold_amount,
        p.delivery_fee_override,
        p.reward_product_id,
        p.gift_text,
        p.reward_qty,
        p.reward_fixed_price,
        p.reward_max_qty,
        p.threshold_base_mode,
        p.priority,
        p.is_active,
        p.notify_customer,
        p.start_at,
        p.end_at,
        p.source,
        p.external_reward_id,
        id,
        shopId,
      ],
    );

    const rule = await getRuleById(shopId, id);
    return res.json({ ok: true, cart_promotion_rule: rule });
  } catch (err) {
    console.error("[cartPromotions.updateCartPromotionRule]", err);
    if (err?.code === "ER_DUP_ENTRY") {
      return res.status(409).json({ ok: false, message: "Cart promotion rule already exists" });
    }
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.deleteCartPromotionRule = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid cart promotion rule id" });
    }

    await ensureCartPromotionSchema();

    const rule = await getRuleById(shopId, id);
    if (!rule) return res.status(404).json({ ok: false, message: "Cart promotion rule not found" });

    await db.query(`DELETE FROM cart_promotion_rule WHERE id = ? AND shop_id = ? LIMIT 1`, [
      id,
      shopId,
    ]);

    return res.json({ ok: true, cart_promotion_rule: rule });
  } catch (err) {
    console.error("[cartPromotions.deleteCartPromotionRule]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

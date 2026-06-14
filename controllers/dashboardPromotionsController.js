const db = require("../config/db");
const { parseShopId, clampInt } = require("../utilities/dashboardUtils");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");

const ALLOWED_KINDS = new Set([
  "PERCENT_OFF",
  "AMOUNT_OFF",
  "FIXED_PRICE",
  "BUNDLE",
]);

const ALLOWED_STATUS_FILTERS = new Set(["all", "active", "inactive"]);
const ALLOWED_SORT_BY = new Set(["default", "start_at", "end_at", "kind"]);
const ALLOWED_SORT_DIR = new Set(["asc", "desc"]);

function trimOrNull(value) {
  const s = String(value ?? "").trim();
  return s ? s : null;
}

function moneyNumber(value, fieldName, { min = 0, max = null, required = true } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) return { error: `${fieldName} is required` };
    return { value: null };
  }

  const n = Number(value);
  if (!Number.isFinite(n)) return { error: `${fieldName} must be a number` };
  if (n < min) return { error: `${fieldName} is too small` };
  if (max !== null && n > max) return { error: `${fieldName} is too large` };

  return { value: n };
}

function intNumber(value, fieldName, { min = 1, required = true } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) return { error: `${fieldName} is required` };
    return { value: null };
  }

  const n = Number(value);
  if (!Number.isInteger(n)) return { error: `${fieldName} must be an integer` };
  if (n < min) return { error: `${fieldName} is too small` };

  return { value: n };
}

function normalizeDateTime(value, fieldName, { required = true } = {}) {
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

function activeSql(alias = "pr") {
  return `(${alias}.start_at <= NOW() AND (${alias}.end_at IS NULL OR ${alias}.end_at > NOW()))`;
}

function buildPromotionOrderBy(sortBy, sortDir, activeCondition) {
  const dir = sortDir === "asc" ? "ASC" : "DESC";

  if (sortBy === "start_at") {
    return `pr.start_at ${dir}, pr.id DESC`;
  }

  if (sortBy === "end_at") {
    return `pr.end_at IS NULL ASC, pr.end_at ${dir}, pr.id DESC`;
  }

  if (sortBy === "kind") {
    return `pr.kind ${dir}, pr.start_at DESC, pr.id DESC`;
  }

  return `
        CASE
          WHEN ${activeCondition} THEN 0
          WHEN pr.start_at > NOW() THEN 1
          ELSE 2
        END ASC,
        pr.start_at DESC,
        pr.id DESC
      `;
}

function parsePromotionPayload(body) {
  const productId = Number(body?.product_id ?? body?.productId);
  if (!Number.isInteger(productId) || productId <= 0) {
    return { error: "Invalid product_id" };
  }

  const kind = String(body?.kind ?? "").trim().toUpperCase();
  if (!ALLOWED_KINDS.has(kind)) {
    return { error: "Invalid promotion kind" };
  }

  const start = normalizeDateTime(body?.start_at ?? body?.startAt, "start_at", {
    required: true,
  });
  if (start.error) return { error: start.error };

  const end = normalizeDateTime(body?.end_at ?? body?.endAt, "end_at", {
    required: false,
  });
  if (end.error) return { error: end.error };

  if (end.comparable && end.comparable.getTime() <= start.comparable.getTime()) {
    return { error: "end_at must be after start_at" };
  }

  const payload = {
    product_id: productId,
    kind,
    percent_off: null,
    amount_off: null,
    fixed_price: null,
    bundle_buy_qty: null,
    bundle_pay_price: null,
    max_discounted_qty: null,
    description: trimOrNull(body?.description),
    start_at: start.value,
    end_at: end.value,
  };

  if (kind === "PERCENT_OFF") {
    const parsed = moneyNumber(body?.percent_off ?? body?.percentOff, "percent_off", {
      min: 0.01,
      max: 100,
    });
    if (parsed.error) return { error: parsed.error };
    payload.percent_off = parsed.value;
  }

  if (kind === "AMOUNT_OFF") {
    const parsed = moneyNumber(body?.amount_off ?? body?.amountOff, "amount_off", {
      min: 0.01,
    });
    if (parsed.error) return { error: parsed.error };
    payload.amount_off = parsed.value;
  }

  if (kind === "FIXED_PRICE") {
    const parsed = moneyNumber(body?.fixed_price ?? body?.fixedPrice, "fixed_price", {
      min: 0,
    });
    if (parsed.error) return { error: parsed.error };
    payload.fixed_price = parsed.value;
  }

  if (kind === "BUNDLE") {
    const buyQty = intNumber(
      body?.bundle_buy_qty ?? body?.bundleBuyQty,
      "bundle_buy_qty",
      { min: 2 },
    );
    if (buyQty.error) return { error: buyQty.error };

    const payPrice = moneyNumber(
      body?.bundle_pay_price ?? body?.bundlePayPrice,
      "bundle_pay_price",
      { min: 0 },
    );
    if (payPrice.error) return { error: payPrice.error };

    payload.bundle_buy_qty = buyQty.value;
    payload.bundle_pay_price = payPrice.value;
  }

  const maxQtyRaw = body?.max_discounted_qty ?? body?.maxDiscountedQty;
  if (maxQtyRaw !== null && maxQtyRaw !== undefined && maxQtyRaw !== "") {
    const parsedMaxQty = moneyNumber(maxQtyRaw, "max_discounted_qty", {
      min: 0.001,
      required: false,
    });
    if (parsedMaxQty.error) return { error: parsedMaxQty.error };
    payload.max_discounted_qty = parsedMaxQty.value;
  }

  return { payload };
}

async function ensureProductExists(shopId, productId) {
  const [rows] = await db.query(
    `
    SELECT id, name, display_name_en, price, category, sub_category
    FROM product
    WHERE id = ? AND shop_id = ?
    LIMIT 1
    `,
    [productId, shopId],
  );
  return rows?.[0] || null;
}

function mapPromotionRow(row) {
  const isActive = Boolean(row.is_active);
  const isUpcoming = Boolean(row.is_upcoming);
  const isExpired = Boolean(row.is_expired);

  return {
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    product_id: Number(row.product_id),
    product_name: row.product_name ?? null,
    product_display_name_en: row.product_display_name_en ?? null,
    product_price: row.product_price == null ? null : Number(row.product_price),
    product_category: row.product_category ?? null,
    product_sub_category: row.product_sub_category ?? null,
    kind: row.kind,
    percent_off: row.percent_off == null ? null : Number(row.percent_off),
    amount_off: row.amount_off == null ? null : Number(row.amount_off),
    fixed_price: row.fixed_price == null ? null : Number(row.fixed_price),
    bundle_buy_qty:
      row.bundle_buy_qty == null ? null : Number(row.bundle_buy_qty),
    bundle_pay_price:
      row.bundle_pay_price == null ? null : Number(row.bundle_pay_price),
    max_discounted_qty:
      row.max_discounted_qty == null ? null : Number(row.max_discounted_qty),
    description: row.description ?? null,
    start_at: row.start_at ?? null,
    end_at: row.end_at ?? null,
    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
    is_active: isActive,
    is_upcoming: isUpcoming,
    is_expired: isExpired,
    status: isActive ? "active" : isUpcoming ? "upcoming" : isExpired ? "expired" : "inactive",
  };
}

async function getPromotionById(shopId, id) {
  const [rows] = await db.query(
    `
    SELECT
      pr.id,
      pr.shop_id,
      pr.product_id,
      p.name AS product_name,
      p.display_name_en AS product_display_name_en,
      p.price AS product_price,
      p.category AS product_category,
      p.sub_category AS product_sub_category,
      pr.kind,
      pr.percent_off,
      pr.amount_off,
      pr.fixed_price,
      pr.bundle_buy_qty,
      pr.bundle_pay_price,
      pr.max_discounted_qty,
      pr.description,
      pr.start_at,
      pr.end_at,
      pr.created_at,
      pr.updated_at,
      ${activeSql("pr")} AS is_active,
      (pr.start_at > NOW()) AS is_upcoming,
      (pr.end_at IS NOT NULL AND pr.end_at <= NOW()) AS is_expired
    FROM promotion pr
    LEFT JOIN product p ON p.id = pr.product_id AND p.shop_id = pr.shop_id
    WHERE pr.id = ? AND pr.shop_id = ?
    LIMIT 1
    `,
    [id, shopId],
  );

  return rows?.[0] ? mapPromotionRow(rows[0]) : null;
}

exports.listPromotions = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    await ensureCartPromotionSchema();

    const status = String(req.query.status || "all").trim().toLowerCase();
    const statusFilter = ALLOWED_STATUS_FILTERS.has(status) ? status : "all";
    const q = String(req.query.q ?? "").trim();
    const category = trimOrNull(req.query.category);
    const subCategory = trimOrNull(req.query.sub_category ?? req.query.subCategory);
    const requestedSortBy = String(req.query.sort_by || req.query.sortBy || "default")
      .trim()
      .toLowerCase();
    const requestedSortDir = String(req.query.sort_dir || req.query.sortDir || "desc")
      .trim()
      .toLowerCase();
    const sortBy = ALLOWED_SORT_BY.has(requestedSortBy) ? requestedSortBy : "default";
    const sortDir = ALLOWED_SORT_DIR.has(requestedSortDir) ? requestedSortDir : "desc";
    const limit = clampInt(req.query.limit, 1, 500, 200);

    const baseWhere = ["pr.shop_id = ?"];
    const baseParams = [shopId];

    if (q) {
      baseWhere.push(
        "(p.name LIKE ? OR p.display_name_en LIKE ? OR pr.description LIKE ? OR CAST(pr.id AS CHAR) = ?)",
      );
      const like = `%${q}%`;
      baseParams.push(like, like, like, q);
    }

    if (category) {
      baseWhere.push("p.category = ?");
      baseParams.push(category);
    }

    if (subCategory) {
      baseWhere.push("p.sub_category = ?");
      baseParams.push(subCategory);
    }

    const activeCondition = activeSql("pr");
    const where = baseWhere.slice();
    const params = baseParams.slice();

    if (statusFilter === "active") where.push(activeCondition);
    if (statusFilter === "inactive") where.push(`NOT ${activeCondition}`);

    const orderBy = buildPromotionOrderBy(sortBy, sortDir, activeCondition);

    const [[countsRow]] = await db.query(
      `
      SELECT
        COUNT(*) AS total_count,
        SUM(CASE WHEN ${activeCondition} THEN 1 ELSE 0 END) AS active_count,
        SUM(CASE WHEN NOT ${activeCondition} THEN 1 ELSE 0 END) AS inactive_count
      FROM promotion pr
      LEFT JOIN product p ON p.id = pr.product_id AND p.shop_id = pr.shop_id
      WHERE ${baseWhere.join(" AND ")}
      `,
      baseParams,
    );

    const [rows] = await db.query(
      `
      SELECT
        pr.id,
        pr.shop_id,
        pr.product_id,
        p.name AS product_name,
        p.display_name_en AS product_display_name_en,
        p.price AS product_price,
        p.category AS product_category,
        p.sub_category AS product_sub_category,
        pr.kind,
        pr.percent_off,
        pr.amount_off,
        pr.fixed_price,
        pr.bundle_buy_qty,
        pr.bundle_pay_price,
        pr.description,
        pr.start_at,
        pr.end_at,
        pr.created_at,
        pr.updated_at,
        ${activeCondition} AS is_active,
        (pr.start_at > NOW()) AS is_upcoming,
        (pr.end_at IS NOT NULL AND pr.end_at <= NOW()) AS is_expired
      FROM promotion pr
      LEFT JOIN product p ON p.id = pr.product_id AND p.shop_id = pr.shop_id
      WHERE ${where.join(" AND ")}
      ORDER BY ${orderBy}
      LIMIT ?
      `,
      [...params, limit],
    );

    return res.json({
      ok: true,
      promotions: (rows || []).map(mapPromotionRow),
      counts: {
        total: Number(countsRow?.total_count ?? 0),
        active: Number(countsRow?.active_count ?? 0),
        inactive: Number(countsRow?.inactive_count ?? 0),
      },
    });
  } catch (err) {
    console.error("[promotions.listPromotions]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.createPromotion = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    await ensureCartPromotionSchema();

    const parsed = parsePromotionPayload(req.body || {});
    if (parsed.error) {
      return res.status(400).json({ ok: false, message: parsed.error });
    }

    const product = await ensureProductExists(shopId, parsed.payload.product_id);
    if (!product) {
      return res.status(404).json({ ok: false, message: "Product not found" });
    }

    const p = parsed.payload;
    const [ins] = await db.query(
      `
      INSERT INTO promotion
        (shop_id, product_id, kind, percent_off, amount_off, fixed_price,
         bundle_buy_qty, bundle_pay_price, max_discounted_qty, description, start_at, end_at,
         created_at, updated_at)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      `,
      [
        shopId,
        p.product_id,
        p.kind,
        p.percent_off,
        p.amount_off,
        p.fixed_price,
        p.bundle_buy_qty,
        p.bundle_pay_price,
        p.max_discounted_qty,
        p.description,
        p.start_at,
        p.end_at,
      ],
    );

    const promotion = await getPromotionById(shopId, ins.insertId);
    return res.status(201).json({ ok: true, promotion });
  } catch (err) {
    console.error("[promotions.createPromotion]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.updatePromotion = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);

    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid promotion id" });
    }

    await ensureCartPromotionSchema();

    const existing = await getPromotionById(shopId, id);
    if (!existing) {
      return res.status(404).json({ ok: false, message: "Promotion not found" });
    }

    const parsed = parsePromotionPayload(req.body || {});
    if (parsed.error) {
      return res.status(400).json({ ok: false, message: parsed.error });
    }

    const product = await ensureProductExists(shopId, parsed.payload.product_id);
    if (!product) {
      return res.status(404).json({ ok: false, message: "Product not found" });
    }

    const p = parsed.payload;
    await db.query(
      `
      UPDATE promotion
      SET
        product_id = ?,
        kind = ?,
        percent_off = ?,
        amount_off = ?,
        fixed_price = ?,
        bundle_buy_qty = ?,
        bundle_pay_price = ?,
        max_discounted_qty = ?,
        description = ?,
        start_at = ?,
        end_at = ?,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ? AND shop_id = ?
      LIMIT 1
      `,
      [
        p.product_id,
        p.kind,
        p.percent_off,
        p.amount_off,
        p.fixed_price,
        p.bundle_buy_qty,
        p.bundle_pay_price,
        p.max_discounted_qty,
        p.description,
        p.start_at,
        p.end_at,
        id,
        shopId,
      ],
    );

    const promotion = await getPromotionById(shopId, id);
    return res.json({ ok: true, promotion });
  } catch (err) {
    console.error("[promotions.updatePromotion]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.deletePromotion = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);

    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid promotion id" });
    }

    await ensureCartPromotionSchema();

    const promotion = await getPromotionById(shopId, id);
    if (!promotion) {
      return res.status(404).json({ ok: false, message: "Promotion not found" });
    }

    await db.query(`DELETE FROM promotion WHERE id = ? AND shop_id = ? LIMIT 1`, [
      id,
      shopId,
    ]);

    return res.json({ ok: true, promotion });
  } catch (err) {
    console.error("[promotions.deletePromotion]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

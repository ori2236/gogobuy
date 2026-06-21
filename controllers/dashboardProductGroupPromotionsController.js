const db = require("../config/db");
const { parseShopId, clampInt } = require("../utilities/dashboardUtils");
const { applyMarketDayOverrides, isMarketDayDescription } = require("../services/marketDayPromotions");

const ALLOWED_STATUS_FILTERS = new Set(["all", "active", "inactive"]);
const ALLOWED_SORT_BY = new Set(["default", "priority", "start_at", "end_at", "created_at"]);
const ALLOWED_SORT_DIR = new Set(["asc", "desc"]);

function trimOrNull(value, limit = 1000) {
  const s = String(value ?? "").trim().replace(/\s+/g, " ");
  return s ? s.slice(0, limit) : null;
}

function boolValue(value, fallback = true) {
  if (value === true || value === 1 || value === "1" || value === "true") return 1;
  if (value === false || value === 0 || value === "0" || value === "false") return 0;
  return fallback ? 1 : 0;
}

function numberValue(value, fieldName, { min = 0, required = true, integer = false } = {}) {
  if (value === null || value === undefined || value === "") {
    if (required) return { error: `${fieldName} is required` };
    return { value: null };
  }

  const n = Number(value);
  if (!Number.isFinite(n)) return { error: `${fieldName} must be a number` };
  if (integer && !Number.isInteger(n)) return { error: `${fieldName} must be an integer` };
  if (n < min) return { error: `${fieldName} is too small` };
  return { value: n };
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

  if (Number.isNaN(comparable.getTime())) return { error: `${fieldName} is not a valid datetime` };
  return { value: normalized, comparable };
}

function activeSql(alias = "pgp") {
  return `(${alias}.is_active = 1 AND (${alias}.start_at IS NULL OR ${alias}.start_at <= NOW()) AND (${alias}.end_at IS NULL OR ${alias}.end_at > NOW()))`;
}

function buildOrderBy(sortBy, sortDir, activeCondition) {
  const dir = sortDir === "asc" ? "ASC" : "DESC";

  if (sortBy === "priority") return `pgp.priority ${dir}, pgp.id DESC`;
  if (sortBy === "start_at") return `pgp.start_at ${dir}, pgp.id DESC`;
  if (sortBy === "end_at") return `pgp.end_at IS NULL ASC, pgp.end_at ${dir}, pgp.id DESC`;
  if (sortBy === "created_at") return `pgp.created_at ${dir}, pgp.id DESC`;

  return `
    CASE
      WHEN ${activeCondition} THEN 0
      WHEN pgp.start_at > NOW() THEN 1
      ELSE 2
    END ASC,
    pgp.priority ASC,
    pgp.id DESC
  `;
}

function normalizeProductIds(raw) {
  const source = Array.isArray(raw) ? raw : String(raw ?? "").split(",");
  return [...new Set(source.map((x) => Number(x)).filter((x) => Number.isInteger(x) && x > 0))];
}

function parsePayload(body) {
  const title = trimOrNull(body?.title, 255);
  if (!title) return { error: "title is required" };

  const productIds = normalizeProductIds(body?.product_ids ?? body?.productIds);
  if (productIds.length < 2) return { error: "At least 2 products are required" };

  const buyQty = numberValue(body?.bundle_buy_qty ?? body?.bundleBuyQty, "bundle_buy_qty", {
    min: 2,
    integer: true,
  });
  if (buyQty.error) return { error: buyQty.error };

  const payPrice = numberValue(body?.bundle_pay_price ?? body?.bundlePayPrice, "bundle_pay_price", {
    min: 0.01,
  });
  if (payPrice.error) return { error: payPrice.error };

  const start = normalizeDateTime(body?.start_at ?? body?.startAt, "start_at", { required: true });
  if (start.error) return { error: start.error };

  const end = normalizeDateTime(body?.end_at ?? body?.endAt, "end_at", { required: false });
  if (end.error) return { error: end.error };
  if (end.comparable && end.comparable.getTime() <= start.comparable.getTime()) {
    return { error: "end_at must be after start_at" };
  }

  let maxDiscountedQty = null;
  const maxRaw = body?.max_discounted_qty ?? body?.maxDiscountedQty;
  if (maxRaw !== null && maxRaw !== undefined && maxRaw !== "") {
    const maxQty = numberValue(maxRaw, "max_discounted_qty", { min: 0.001, required: false });
    if (maxQty.error) return { error: maxQty.error };
    maxDiscountedQty = maxQty.value;
  }

  const priority = numberValue(body?.priority, "priority", {
    min: 1,
    required: false,
    integer: true,
  });
  if (priority.error) return { error: priority.error };

  return {
    payload: {
      title,
      description: trimOrNull(body?.description, 1000),
      kind: "BUNDLE",
      bundle_buy_qty: buyQty.value,
      bundle_pay_price: payPrice.value,
      max_discounted_qty: maxDiscountedQty,
      priority: priority.value ?? 100,
      is_active: boolValue(body?.is_active ?? body?.isActive, true),
      start_at: start.value,
      end_at: end.value,
      product_ids: productIds,
    },
  };
}

async function ensureProductsExist(shopId, productIds) {
  if (!productIds.length) return [];
  const placeholders = productIds.map(() => "?").join(",");
  const [rows] = await db.query(
    `
    SELECT id, name, display_name_en, price, category, sub_category
    FROM product
    WHERE shop_id = ? AND id IN (${placeholders})
    `,
    [Number(shopId), ...productIds],
  );
  return rows || [];
}

function parseProductsJson(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (Buffer.isBuffer(value)) {
    try {
      const parsed = JSON.parse(value.toString("utf8"));
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  if (typeof value === "object") {
    return Array.isArray(value) ? value : [];
  }
  try {
    const parsed = JSON.parse(String(value));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function mapRow(row) {
  const products = parseProductsJson(row.products_json).filter((p) => p && Number(p.id) > 0);

  const isActive = Boolean(row.is_currently_active);
  const isUpcoming = Boolean(row.is_upcoming);
  const isExpired = Boolean(row.is_expired);

  return {
    id: Number(row.id),
    shop_id: Number(row.shop_id),
    title: row.title,
    description: row.description ?? null,
    kind: row.kind || "BUNDLE",
    bundle_buy_qty: Number(row.bundle_buy_qty),
    bundle_pay_price: Number(row.bundle_pay_price),
    max_discounted_qty:
      row.max_discounted_qty == null ? null : Number(row.max_discounted_qty),
    priority: Number(row.priority ?? 100),
    is_active: Boolean(row.is_active),
    start_at: row.start_at ?? null,
    end_at: row.end_at ?? null,
    created_at: row.created_at ?? null,
    updated_at: row.updated_at ?? null,
    products,
    product_ids: products.map((p) => Number(p.id)).filter(Boolean),
    is_currently_active: isActive,
    is_upcoming: isUpcoming,
    is_expired: isExpired,
    status: isActive ? "active" : isUpcoming ? "upcoming" : isExpired ? "expired" : "inactive",
    is_market_day: isMarketDayDescription(row.description),
  };
}

async function getGroupPromotionById(shopId, id) {
  const [rows] = await db.query(
    `
    SELECT
      pgp.*,
      ${activeSql("pgp")} AS is_currently_active,
      (pgp.start_at IS NOT NULL AND pgp.start_at > NOW()) AS is_upcoming,
      (pgp.end_at IS NOT NULL AND pgp.end_at <= NOW()) AS is_expired,
      COALESCE(
        JSON_ARRAYAGG(
          CASE
            WHEN p.id IS NULL THEN NULL
            ELSE JSON_OBJECT(
              'id', p.id,
              'name', p.name,
              'display_name_en', p.display_name_en,
              'price', p.price,
              'category', p.category,
              'sub_category', p.sub_category
            )
          END
        ),
        JSON_ARRAY()
      ) AS products_json
    FROM product_group_promotion pgp
    LEFT JOIN product_group_promotion_item pgpi
      ON pgpi.group_promotion_id = pgp.id AND pgpi.shop_id = pgp.shop_id
    LEFT JOIN product p
      ON p.id = pgpi.product_id AND p.shop_id = pgp.shop_id
    WHERE pgp.shop_id = ? AND pgp.id = ?
    GROUP BY pgp.id
    LIMIT 1
    `,
    [Number(shopId), Number(id)],
  );

  return rows?.[0] ? mapRow(rows[0]) : null;
}

async function replaceGroupProducts(conn, { shopId, groupId, productIds }) {
  await conn.query(
    `DELETE FROM product_group_promotion_item WHERE shop_id = ? AND group_promotion_id = ?`,
    [Number(shopId), Number(groupId)],
  );

  if (!productIds.length) return;

  const valuesSql = productIds.map(() => `(?, ?, ?)`).join(", ");
  const params = [];
  for (const productId of productIds) {
    params.push(Number(groupId), Number(shopId), Number(productId));
  }

  await conn.query(
    `
    INSERT INTO product_group_promotion_item
      (group_promotion_id, shop_id, product_id)
    VALUES ${valuesSql}
    `,
    params,
  );
}

exports.listProductGroupPromotions = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }


    const status = String(req.query.status || "all").trim().toLowerCase();
    const statusFilter = ALLOWED_STATUS_FILTERS.has(status) ? status : "all";
    const q = String(req.query.q ?? "").trim();
    const requestedSortBy = String(req.query.sort_by || req.query.sortBy || "default").trim().toLowerCase();
    const requestedSortDir = String(req.query.sort_dir || req.query.sortDir || "desc").trim().toLowerCase();
    const sortBy = ALLOWED_SORT_BY.has(requestedSortBy) ? requestedSortBy : "default";
    const sortDir = ALLOWED_SORT_DIR.has(requestedSortDir) ? requestedSortDir : "desc";
    const limit = clampInt(req.query.limit, 1, 500, 200);
    const activeCondition = activeSql("pgp");

    const where = ["pgp.shop_id = ?"];
    const params = [shopId];

    if (statusFilter === "active") where.push(activeCondition);
    if (statusFilter === "inactive") where.push(`NOT ${activeCondition}`);

    if (q) {
      where.push(`(
        pgp.title LIKE ?
        OR pgp.description LIKE ?
        OR CAST(pgp.id AS CHAR) = ?
        OR EXISTS (
          SELECT 1
          FROM product_group_promotion_item qpi
          JOIN product qp ON qp.id = qpi.product_id AND qp.shop_id = qpi.shop_id
          WHERE qpi.group_promotion_id = pgp.id
            AND qpi.shop_id = pgp.shop_id
            AND (qp.name LIKE ? OR qp.display_name_en LIKE ?)
        )
      )`);
      const like = `%${q}%`;
      params.push(like, like, q, like, like);
    }

    const [[countsRow]] = await db.query(
      `
      SELECT
        COUNT(*) AS total_count,
        SUM(CASE WHEN ${activeCondition} THEN 1 ELSE 0 END) AS active_count,
        SUM(CASE WHEN NOT ${activeCondition} THEN 1 ELSE 0 END) AS inactive_count
      FROM product_group_promotion pgp
      WHERE pgp.shop_id = ?
      `,
      [shopId],
    );

    const orderBy = buildOrderBy(sortBy, sortDir, activeCondition);
    const [rows] = await db.query(
      `
      SELECT
        pgp.*,
        ${activeCondition} AS is_currently_active,
        (pgp.start_at IS NOT NULL AND pgp.start_at > NOW()) AS is_upcoming,
        (pgp.end_at IS NOT NULL AND pgp.end_at <= NOW()) AS is_expired,
        COALESCE(
          JSON_ARRAYAGG(
            CASE
              WHEN p.id IS NULL THEN NULL
              ELSE JSON_OBJECT(
                'id', p.id,
                'name', p.name,
                'display_name_en', p.display_name_en,
                'price', p.price,
                'category', p.category,
                'sub_category', p.sub_category
              )
            END
          ),
          JSON_ARRAY()
        ) AS products_json
      FROM product_group_promotion pgp
      LEFT JOIN product_group_promotion_item pgpi
        ON pgpi.group_promotion_id = pgp.id AND pgpi.shop_id = pgp.shop_id
      LEFT JOIN product p
        ON p.id = pgpi.product_id AND p.shop_id = pgp.shop_id
      WHERE ${where.join(" AND ")}
      GROUP BY pgp.id
      ORDER BY ${orderBy}
      LIMIT ?
      `,
      [...params, limit],
    );

    return res.json({
      ok: true,
      product_group_promotions: (rows || []).map(mapRow),
      counts: {
        total: Number(countsRow?.total_count ?? 0),
        active: Number(countsRow?.active_count ?? 0),
        inactive: Number(countsRow?.inactive_count ?? 0),
      },
    });
  } catch (err) {
    console.error("[productGroupPromotions.list]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.createProductGroupPromotion = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const parsed = parsePayload(req.body || {});
    if (parsed.error) {
      return res.status(400).json({ ok: false, message: parsed.error });
    }

    const products = await ensureProductsExist(shopId, parsed.payload.product_ids);
    if (products.length !== parsed.payload.product_ids.length) {
      return res.status(404).json({ ok: false, message: "One or more products were not found" });
    }

    await conn.beginTransaction();
    const p = applyMarketDayOverrides(parsed.payload, req.body || {});
    const [ins] = await conn.query(
      `
      INSERT INTO product_group_promotion
        (shop_id, title, description, kind, bundle_buy_qty, bundle_pay_price,
         max_discounted_qty, priority, is_active, start_at, end_at, created_at, updated_at)
      VALUES (?, ?, ?, 'BUNDLE', ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      `,
      [
        shopId,
        p.title,
        p.description,
        p.bundle_buy_qty,
        p.bundle_pay_price,
        p.max_discounted_qty,
        p.priority,
        p.is_active,
        p.start_at,
        p.end_at,
      ],
    );
    await replaceGroupProducts(conn, { shopId, groupId: ins.insertId, productIds: p.product_ids });
    await conn.commit();

    const promotion = await getGroupPromotionById(shopId, ins.insertId);
    return res.status(201).json({ ok: true, product_group_promotion: promotion });
  } catch (err) {
    try { await conn.rollback(); } catch {}
    console.error("[productGroupPromotions.create]", err);
    return res.status(err.status || 500).json({ ok: false, message: err.status ? err.message : "Server error" });
  } finally {
    if (conn && conn.release) conn.release();
  }
};

exports.updateProductGroupPromotion = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid promotion id" });
    }

    const existing = await getGroupPromotionById(shopId, id);
    if (!existing) {
      return res.status(404).json({ ok: false, message: "Product group promotion not found" });
    }

    const parsed = parsePayload(req.body || {});
    if (parsed.error) {
      return res.status(400).json({ ok: false, message: parsed.error });
    }

    const products = await ensureProductsExist(shopId, parsed.payload.product_ids);
    if (products.length !== parsed.payload.product_ids.length) {
      return res.status(404).json({ ok: false, message: "One or more products were not found" });
    }

    await conn.beginTransaction();
    const p = applyMarketDayOverrides(parsed.payload, req.body || {}, existing);
    await conn.query(
      `
      UPDATE product_group_promotion
      SET
        title = ?,
        description = ?,
        bundle_buy_qty = ?,
        bundle_pay_price = ?,
        max_discounted_qty = ?,
        priority = ?,
        is_active = ?,
        start_at = ?,
        end_at = ?,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ? AND shop_id = ?
      LIMIT 1
      `,
      [
        p.title,
        p.description,
        p.bundle_buy_qty,
        p.bundle_pay_price,
        p.max_discounted_qty,
        p.priority,
        p.is_active,
        p.start_at,
        p.end_at,
        id,
        shopId,
      ],
    );
    await replaceGroupProducts(conn, { shopId, groupId: id, productIds: p.product_ids });
    await conn.commit();

    const promotion = await getGroupPromotionById(shopId, id);
    return res.json({ ok: true, product_group_promotion: promotion });
  } catch (err) {
    try { await conn.rollback(); } catch {}
    console.error("[productGroupPromotions.update]", err);
    return res.status(err.status || 500).json({ ok: false, message: err.status ? err.message : "Server error" });
  } finally {
    if (conn && conn.release) conn.release();
  }
};

exports.deleteProductGroupPromotion = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid promotion id" });
    }

    const promotion = await getGroupPromotionById(shopId, id);
    if (!promotion) {
      return res.status(404).json({ ok: false, message: "Product group promotion not found" });
    }

    await conn.beginTransaction();
    await conn.query(
      `DELETE FROM product_group_promotion_item WHERE shop_id = ? AND group_promotion_id = ?`,
      [shopId, id],
    );
    await conn.query(
      `DELETE FROM product_group_promotion WHERE shop_id = ? AND id = ? LIMIT 1`,
      [shopId, id],
    );
    await conn.commit();

    return res.json({ ok: true, product_group_promotion: promotion });
  } catch (err) {
    try { await conn.rollback(); } catch {}
    console.error("[productGroupPromotions.delete]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  } finally {
    if (conn && conn.release) conn.release();
  }
};

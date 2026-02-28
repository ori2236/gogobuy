const db = require("../config/db");
const { sendWhatsAppText } = require("../config/whatsapp");
const { fetchAlternatives } = require("../services/products");
const {
  isNonEmptyString,
  clampInt,
  parseShopId,
  normalizeWaNumber,
} = require("../utilities/dashboardUtils");
const { rebuildTokenWeightsForShop } = require("../services/buildTokenWeights");
const { fetchCategoriesMap } = require("../repositories/categories");

const DELETE_FROM_ORDER_STATUSES = [
  "pending",
  "checkout_pending",
  "confirmed",
  "preparing",
  "cancel_pending",
];

function validateCategory(category, categoryMap) {
  return Object.prototype.hasOwnProperty.call(categoryMap, category);
}

function validateSubcategory(category, sub, categoryMap) {
  if (!validateCategory(category, categoryMap)) return false;
  return (categoryMap[category] || []).includes(sub);
}

function fmtIls(n) {
  const x = Number(n);
  return Number.isFinite(x) ? `₪${x.toFixed(2)}` : null;
}

function buildProductDeletedMsg({ orderId, productName, altNames, newTotal }) {
  let msg =
    `עדכון חשוב לגבי ההזמנה שלך (#${orderId}):\n` +
    `המוצר "${productName}" כבר לא נמכר ולכן הוסר מההזמנה.`;

  const money = fmtIls(newTotal);
  if (money) msg += `\nהסכום עודכן, ועכשיו הוא ${money}.`;
  else msg += `\nהסכום עודכן בהתאם.`;

  if (Array.isArray(altNames) && altNames.length) {
    msg += `\n\nחלופות שאולי יתאימו:\n• ${altNames.join("\n• ")}`;
    msg += `\n\nאם תרצה חלופה אחרת כתוב מה אתה מעדיף.`;
  } else {
    msg += `\n\nאם תרצה חלופה כתוב מה אתה מחפש ונציע משהו מתאים.`;
  }

  return msg;
}

exports.getStockCategories = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const categoryMap = await fetchCategoriesMap();
    const categories = Object.entries(categoryMap).map(([category, subs]) => ({
      category,
      sub_categories: subs,
    }));

    return res.json({ ok: true, categories });
  } catch (err) {
    console.error("[stock.getStockCategories]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.listStockProducts = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const q = String(req.query.q ?? "").trim();
    const category = isNonEmptyString(req.query.category)
      ? String(req.query.category).trim()
      : null;
    const sub_category = isNonEmptyString(req.query.sub_category)
      ? String(req.query.sub_category).trim()
      : null;

    if (!category && q.length < 2) {
      return res.status(400).json({
        ok: false,
        message:
          "When category is not provided, q must be at least 2 characters",
      });
    }

    const categoryMap = await fetchCategoriesMap();

    if (category && !validateCategory(category, categoryMap)) {
      return res.status(400).json({ ok: false, message: "Invalid category" });
    }
    if (sub_category && !category) {
      return res
        .status(400)
        .json({ ok: false, message: "sub_category requires category" });
    }
    if (
      category &&
      sub_category &&
      !validateSubcategory(category, sub_category, categoryMap)
    ) {
      return res
        .status(400)
        .json({ ok: false, message: "Invalid sub_category for category" });
    }

    const limit = clampInt(req.query.limit, 1, 200, 40);
    const cursorRaw = req.query.cursor;
    const cursor = cursorRaw ? Number(cursorRaw) : null;
    if (cursorRaw && (!Number.isFinite(cursor) || cursor <= 0)) {
      return res.status(400).json({ ok: false, message: "Invalid cursor" });
    }

    const where = [];
    const params = [];

    where.push("p.shop_id = ?");
    params.push(shopId);

    if (category) {
      where.push("p.category = ?");
      params.push(category);
    }
    if (sub_category) {
      where.push("p.sub_category = ?");
      params.push(sub_category);
    }

    if (q) {
      where.push("(p.name LIKE ? OR p.display_name_en LIKE ?)");
      const like = `%${q}%`;
      params.push(like, like);
    }

    const whereCount = where.slice();
    const paramsCount = params.slice();

    const [[countRow]] = await db.query(
      `
      SELECT COUNT(*) AS total_count
      FROM product p
      WHERE ${whereCount.join(" AND ")}
      `,
      paramsCount,
    );

    const total_count = Number(countRow?.total_count ?? 0);

    if (cursor) {
      where.push("p.id < ?");
      params.push(cursor);
    }

    const [rows] = await db.query(
      `
      SELECT
        p.id,
        p.shop_id,
        p.name,
        p.display_name_en,
        p.price,
        p.stock_amount,
        p.category,
        p.sub_category,
        p.created_at,
        p.updated_at
      FROM product p
      WHERE ${where.join(" AND ")}
      ORDER BY p.id DESC
      LIMIT ?
      `,
      [...params, limit],
    );

    const products = (rows || []).map((r) => ({
      id: Number(r.id),
      shop_id: Number(r.shop_id),
      name: r.name ?? "",
      display_name_en: r.display_name_en ?? "",
      price: r.price == null ? null : Number(r.price),
      stock_amount: r.stock_amount == null ? null : Number(r.stock_amount),
      category: r.category ?? null,
      sub_category: r.sub_category ?? null,
      created_at: r.created_at ?? null,
      updated_at: r.updated_at ?? null,
    }));

    const next_cursor =
      products.length === limit ? products[products.length - 1].id : null;

    return res.json({ ok: true, products, next_cursor, total_count });
  } catch (err) {
    console.error("[stock.listStockProducts]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.createStockProduct = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const name = String(req.body?.name ?? "").trim();
    const display_name_en = String(req.body?.display_name_en ?? "").trim();

    const price = Number(req.body?.price);
    const stock_amount = Number(req.body?.stock_amount);

    const category = String(req.body?.category ?? "").trim();
    const sub_category = String(req.body?.sub_category ?? "").trim();

    if (!name)
      return res.status(400).json({ ok: false, message: "name is required" });

    if (!Number.isFinite(price) || price < 0)
      return res.status(400).json({ ok: false, message: "Invalid price" });

    if (!Number.isFinite(stock_amount) || stock_amount < 0)
      return res
        .status(400)
        .json({ ok: false, message: "Invalid stock_amount" });

    const categoryMap = await fetchCategoriesMap();

    if (!category || !validateCategory(category, categoryMap))
      return res.status(400).json({ ok: false, message: "Invalid category" });

    if (
      !sub_category ||
      !validateSubcategory(category, sub_category, categoryMap)
    )
      return res
        .status(400)
        .json({ ok: false, message: "Invalid sub_category for category" });

    const [ins] = await db.query(
      `
      INSERT INTO product
        (shop_id, name, display_name_en, price, stock_amount, category, sub_category, created_at, updated_at)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      `,
      [
        shopId,
        name,
        display_name_en,
        price,
        stock_amount,
        category,
        sub_category,
      ],
    );

    const id = ins.insertId;

    const [rows] = await db.query(
      `
      SELECT id, shop_id, name, display_name_en, price, stock_amount, category, sub_category, created_at, updated_at
      FROM product
      WHERE id = ? AND shop_id = ?
      LIMIT 1
      `,
      [id, shopId],
    );
    rebuildTokenWeightsForShop(shopId).catch((e) =>
      console.error("[tokens.rebuild] failed", e?.message || e),
    );
    return res.status(201).json({ ok: true, product: rows[0] || { id } });
  } catch (err) {
    console.error("[stock.createStockProduct]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.updateStockProduct = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);

    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isFinite(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid product id" });
    }

    await conn.beginTransaction();

    const [existingRows] = await conn.query(
      `
      SELECT id, shop_id, name, category, sub_category
      FROM product
      WHERE id = ? AND shop_id = ?
      FOR UPDATE
      `,
      [id, shopId],
    );

    if (!existingRows.length) {
      await conn.rollback();
      return res.status(404).json({ ok: false, message: "Product not found" });
    }

    const existing = existingRows[0];

    const hasName = Object.prototype.hasOwnProperty.call(
      req.body || {},
      "name",
    );
    const nextName = hasName
      ? String(req.body.name ?? "").trim()
      : existing.name;
    const shouldRebuild = hasName && nextName && nextName !== existing.name;

    const hasCat = Object.prototype.hasOwnProperty.call(
      req.body || {},
      "category",
    );
    const hasSub = Object.prototype.hasOwnProperty.call(
      req.body || {},
      "sub_category",
    );

    const nextCategory = hasCat
      ? String(req.body.category ?? "").trim()
      : existing.category;

    const nextSub = hasSub
      ? String(req.body.sub_category ?? "").trim()
      : existing.sub_category;

    const categoryMap = await fetchCategoriesMap();

    if (!nextCategory || !validateCategory(nextCategory, categoryMap)) {
      await conn.rollback();
      return res.status(400).json({ ok: false, message: "Invalid category" });
    }

    if (!nextSub || !validateSubcategory(nextCategory, nextSub, categoryMap)) {
      await conn.rollback();
      return res
        .status(400)
        .json({ ok: false, message: "Invalid sub_category for category" });
    }

    const sets = [];
    const params = [];

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "name")) {
      const name = String(req.body.name ?? "").trim();
      if (!name) {
        await conn.rollback();
        return res
          .status(400)
          .json({ ok: false, message: "name cannot be empty" });
      }
      sets.push("name = ?");
      params.push(name);
    }

    if (
      Object.prototype.hasOwnProperty.call(req.body || {}, "display_name_en")
    ) {
      const en = String(req.body.display_name_en ?? "").trim();
      sets.push("display_name_en = ?");
      params.push(en);
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "price")) {
      const price = Number(req.body.price);
      if (!Number.isFinite(price) || price < 0) {
        await conn.rollback();
        return res.status(400).json({ ok: false, message: "Invalid price" });
      }
      sets.push("price = ?");
      params.push(price);
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "stock_amount")) {
      const stock_amount = Number(req.body.stock_amount);
      if (!Number.isFinite(stock_amount) || stock_amount < 0) {
        await conn.rollback();
        return res
          .status(400)
          .json({ ok: false, message: "Invalid stock_amount" });
      }
      sets.push("stock_amount = ?");
      params.push(stock_amount);
    }

    if (hasCat) {
      sets.push("category = ?");
      params.push(nextCategory);
    }
    if (hasSub) {
      sets.push("sub_category = ?");
      params.push(nextSub);
    }

    if (!sets.length) {
      await conn.rollback();
      return res
        .status(400)
        .json({ ok: false, message: "No fields to update" });
    }

    sets.push("updated_at = CURRENT_TIMESTAMP");
    params.push(id, shopId);

    await conn.query(
      `UPDATE product SET ${sets.join(", ")} WHERE id = ? AND shop_id = ?`,
      params,
    );

    await conn.commit();

    const [rows] = await db.query(
      `
      SELECT id, shop_id, name, display_name_en, price, stock_amount, category, sub_category, created_at, updated_at
      FROM product
      WHERE id = ? AND shop_id = ?
      LIMIT 1
      `,
      [id, shopId],
    );

    if (shouldRebuild) {
      rebuildTokenWeightsForShop(shopId).catch((e) =>
        console.error("[tokens.rebuild] failed", e?.message || e),
      );
    }

    return res.json({ ok: true, product: rows[0] || { id } });
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    console.error("[stock.updateStockProduct]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  } finally {
    conn.release();
  }
};

exports.deleteStockProduct = async (req, res) => {
  const conn = await db.getConnection();
  try {
    const shopId = parseShopId(req);
    const id = Number(req.params.id);

    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isFinite(id) || id <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid product id" });
    }

    await conn.beginTransaction();

    const [prodRows] = await conn.query(
      `
      SELECT
        id, shop_id, name, display_name_en, price, stock_amount,
        category, sub_category, description, image, created_at, updated_at
      FROM product
      WHERE id = ? AND shop_id = ?
      FOR UPDATE
      `,
      [id, shopId],
    );

    if (!prodRows.length) {
      await conn.rollback();
      return res.status(404).json({ ok: false, message: "Product not found" });
    }

    const p = prodRows[0];

    const statusPlaceholders = DELETE_FROM_ORDER_STATUSES.map(() => "?").join(
      ",",
    );
    const [affectedOrders] = await conn.query(
      `
      SELECT DISTINCT
        o.id AS order_id,
        c.phone AS customer_phone
      FROM order_item oi
      JOIN orders o ON o.id = oi.order_id
      JOIN customer c ON c.id = o.customer_id
      WHERE o.shop_id = ?
        AND oi.product_id = ?
        AND o.status IN (${statusPlaceholders})
      FOR UPDATE
      `,
      [shopId, id, ...DELETE_FROM_ORDER_STATUSES],
    );

    const orderIds = (affectedOrders || [])
      .map((x) => Number(x.order_id))
      .filter((n) => Number.isFinite(n) && n > 0);

    let deletedCount = 0;
    if (orderIds.length) {
      const CHUNK = 500;
      for (let i = 0; i < orderIds.length; i += CHUNK) {
        const chunk = orderIds.slice(i, i + CHUNK);
        const inPlaceholders = chunk.map(() => "?").join(",");
        const [delRes] = await conn.query(
          `
          DELETE FROM order_item
          WHERE product_id = ?
            AND order_id IN (${inPlaceholders})
          `,
          [id, ...chunk],
        );
        deletedCount += Number(delRes.affectedRows || 0);
      }
    }

    await conn.query(
      `
      INSERT INTO deleted_product
        (id, shop_id, name, display_name_en, price, stock_amount, category, sub_category, description, image, created_at, updated_at, deleted_at)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
      ON DUPLICATE KEY UPDATE
        shop_id = VALUES(shop_id),
        name = VALUES(name),
        display_name_en = VALUES(display_name_en),
        price = VALUES(price),
        stock_amount = VALUES(stock_amount),
        category = VALUES(category),
        sub_category = VALUES(sub_category),
        description = VALUES(description),
        image = VALUES(image),
        created_at = VALUES(created_at),
        updated_at = VALUES(updated_at),
        deleted_at = VALUES(deleted_at)
      `,
      [
        p.id,
        p.shop_id,
        p.name,
        p.display_name_en,
        p.price,
        p.stock_amount,
        p.category,
        p.sub_category,
        p.description,
        p.image,
        p.created_at,
        p.updated_at,
      ],
    );

    await conn.query(
      `DELETE FROM product WHERE id = ? AND shop_id = ? LIMIT 1`,
      [id, shopId],
    );

    await conn.commit();

    res.json({
      ok: true,
      product: { id: Number(p.id), name: p.name },
      deleted_order_items: deletedCount,
      affected_orders: orderIds,
    });

    if (orderIds.length) {
      (async () => {
        try {
          const altRows = await fetchAlternatives(
            shopId,
            p.category,
            p.sub_category,
            [id],
            3,
            p.name,
            [],
          );
          const altNames = (altRows || [])
            .map((r) => String(r.name || "").trim())
            .filter(Boolean)
            .slice(0, 3);

          const priceByOrder = new Map();
          const CHUNK = 500;
          for (let i = 0; i < orderIds.length; i += CHUNK) {
            const chunk = orderIds.slice(i, i + CHUNK);
            const ph = chunk.map(() => "?").join(",");
            const [rows] = await db.query(
              `SELECT id, price FROM orders WHERE id IN (${ph})`,
              chunk,
            );
            for (const r of rows || []) {
              priceByOrder.set(
                Number(r.id),
                r.price == null ? null : Number(r.price),
              );
            }
          }

          const targets = (affectedOrders || [])
            .map((x) => ({
              order_id: Number(x.order_id),
              phone: normalizeWaNumber(x.customer_phone),
            }))
            .filter((t) => t.order_id > 0 && t.phone);

          for (const t of targets) {
            const msg = buildProductDeletedMsg({
              orderId: t.order_id,
              productName: p.name,
              altNames,
              newTotal: priceByOrder.get(t.order_id),
            });

            try {
              await sendWhatsAppText(t.phone, msg);
              console.log("[stock.deleteStockProduct] WhatsApp sent", {
                orderId: t.order_id,
                to: t.phone,
              });
            } catch (err) {
              console.error(
                "[stock.deleteStockProduct] WhatsApp send failed:",
                err?.response?.data || err.message,
              );
            }
          }
        } catch (e) {
          console.error(
            "[stock.deleteStockProduct] WhatsApp flow error:",
            e?.message || e,
          );
        }
      })();
    }

    rebuildTokenWeightsForShop(shopId).catch((e) =>
      console.error("[tokens.rebuild] failed", e?.message || e),
    );
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    console.error("[stock.deleteStockProduct]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  } finally {
    conn.release();
  }
};
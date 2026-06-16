const db = require("../config/db");


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

async function fetchActiveProductGroupPromotions(conn, shop_id) {
  const [groups] = await conn.query(
    `
    SELECT *
    FROM product_group_promotion
    WHERE shop_id = ?
      AND is_active = 1
      AND (start_at IS NULL OR start_at <= NOW())
      AND (end_at IS NULL OR end_at >= NOW())
    ORDER BY priority ASC, id ASC
    `,
    [Number(shop_id)],
  );

  const ids = (groups || []).map((g) => Number(g.id)).filter(Boolean);
  if (!ids.length) return [];

  const placeholders = ids.map(() => "?").join(",");
  const [items] = await conn.query(
    `
    SELECT
      gpi.group_promotion_id,
      gpi.product_id,
      p.name,
      p.display_name_en
    FROM product_group_promotion_item gpi
    LEFT JOIN product p ON p.id = gpi.product_id AND p.shop_id = gpi.shop_id
    WHERE gpi.shop_id = ?
      AND gpi.group_promotion_id IN (${placeholders})
    ORDER BY gpi.group_promotion_id ASC, p.name ASC, gpi.product_id ASC
    `,
    [Number(shop_id), ...ids],
  );

  const byGroup = new Map();
  for (const item of items || []) {
    const gid = Number(item.group_promotion_id);
    if (!byGroup.has(gid)) byGroup.set(gid, []);
    byGroup.get(gid).push({
      product_id: Number(item.product_id),
      name: item.name ?? null,
      display_name_en: item.display_name_en ?? null,
    });
  }

  return (groups || [])
    .map((group) => ({
      ...group,
      products: byGroup.get(Number(group.id)) || [],
    }))
    .filter((group) => (group.products || []).length > 0);
}

function buildUnitSlots(items) {
  const slots = [];

  for (const item of items || []) {
    if (Number(item.is_gift)) continue;
    if (item.sold_by_weight === 1 || item.sold_by_weight === true) continue;

    const amount = qty(item.amount);
    const wholeUnits = Math.floor(amount);
    if (!(wholeUnits > 0)) continue;

    const lineTotal = money(item.current_line_total ?? item.base_line_total ?? item.price ?? 0);
    const unitPrice = amount > 0 ? money(lineTotal / amount) : money(item.unit_price || 0);
    if (!(unitPrice >= 0)) continue;

    for (let i = 0; i < wholeUnits; i += 1) {
      slots.push({
        item,
        itemId: Number(item.order_item_id),
        productId: Number(item.product_id),
        name: item.name || `#${item.product_id}`,
        unitPrice,
        used: false,
      });
    }
  }

  return slots;
}

function distributeDiscount(bundleSlots, discount, discountByItemId) {
  const total = bundleSlots.reduce((sum, slot) => sum + money(slot.unitPrice), 0);
  if (!(total > 0) || !(discount > 0)) return;

  for (let i = 0; i < bundleSlots.length; i += 1) {
    const slot = bundleSlots[i];
    const ratio = money(slot.unitPrice) / total;
    const part = i === bundleSlots.length - 1
      ? discount - Array.from(discountByItemId.__tempParts || []).reduce((sum, x) => sum + x, 0)
      : discount * ratio;

    const roundedPart = Math.max(0, part);
    discountByItemId.set(
      slot.itemId,
      (discountByItemId.get(slot.itemId) || 0) + roundedPart,
    );

    if (!discountByItemId.__tempParts) discountByItemId.__tempParts = [];
    discountByItemId.__tempParts.push(roundedPart);
  }
  discountByItemId.__tempParts = [];
}

async function replaceProductGroupApplications(conn, { order_id, shop_id, applications }) {
  await conn.query(
    `DELETE FROM order_product_group_promotion_application WHERE order_id = ? AND shop_id = ?`,
    [Number(order_id), Number(shop_id)],
  );

  for (const app of applications || []) {
    await conn.query(
      `
      INSERT INTO order_product_group_promotion_application
        (order_id, shop_id, group_promotion_id, title, bundle_buy_qty, bundle_pay_price,
         discount_amount, applied_count, discounted_qty, metadata, applied_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
      ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        bundle_buy_qty = VALUES(bundle_buy_qty),
        bundle_pay_price = VALUES(bundle_pay_price),
        discount_amount = VALUES(discount_amount),
        applied_count = VALUES(applied_count),
        discounted_qty = VALUES(discounted_qty),
        metadata = VALUES(metadata),
        applied_at = NOW()
      `,
      [
        Number(order_id),
        Number(shop_id),
        Number(app.group.id),
        cleanText(app.group.title, 255) || "מבצע קבוצת מוצרים",
        Number(app.group.bundle_buy_qty),
        money(app.group.bundle_pay_price),
        money(app.discount_amount),
        Number(app.applied_count),
        qty(app.discounted_qty),
        JSON.stringify(app.metadata || {}),
      ],
    );
  }
}

async function applyProductGroupPromotionsToItems(conn, { order_id, shop_id, items }) {
  const orderId = Number(order_id);
  const shopId = Number(shop_id);
  if (!Number.isFinite(orderId) || orderId <= 0 || !Number.isFinite(shopId) || shopId <= 0) {
    return [];
  }

  const groups = await fetchActiveProductGroupPromotions(conn, shopId);
  if (!groups.length) {
    await replaceProductGroupApplications(conn, { order_id: orderId, shop_id: shopId, applications: [] });
    return [];
  }

  const slots = buildUnitSlots(items);
  const discountByItemId = new Map();
  const applications = [];

  for (const group of groups) {
    const bundleQty = Math.max(2, Math.floor(Number(group.bundle_buy_qty || 0)));
    const bundlePayPrice = money(group.bundle_pay_price);
    const productIds = new Set((group.products || []).map((p) => Number(p.product_id)).filter(Boolean));
    if (!productIds.size || !(bundleQty >= 2)) continue;

    let candidates = slots
      .filter((slot) => !slot.used && productIds.has(slot.productId))
      .sort((a, b) => {
        if (b.unitPrice !== a.unitPrice) return b.unitPrice - a.unitPrice;
        return a.itemId - b.itemId;
      });

    const maxDiscountedQty = qty(group.max_discounted_qty, 0);
    if (maxDiscountedQty > 0) {
      candidates = candidates.slice(0, Math.floor(maxDiscountedQty));
    }

    const possibleBundles = Math.floor(candidates.length / bundleQty);
    if (possibleBundles <= 0) continue;

    let groupDiscount = 0;
    let appliedCount = 0;
    let discountedQty = 0;
    const appliedBundles = [];

    for (let i = 0; i < possibleBundles; i += 1) {
      const bundleSlots = candidates.slice(i * bundleQty, (i + 1) * bundleQty);
      const regularValue = money(bundleSlots.reduce((sum, slot) => sum + money(slot.unitPrice), 0));
      const discount = money(regularValue - bundlePayPrice);

      if (!(discount > 0)) continue;

      for (const slot of bundleSlots) slot.used = true;
      distributeDiscount(bundleSlots, discount, discountByItemId);

      groupDiscount = money(groupDiscount + discount);
      appliedCount += 1;
      discountedQty += bundleSlots.length;

      if (appliedBundles.length < 20) {
        appliedBundles.push({
          product_ids: bundleSlots.map((slot) => slot.productId),
          product_names: bundleSlots.map((slot) => slot.name),
          regular_value: regularValue,
          pay_price: bundlePayPrice,
          discount,
        });
      }
    }

    if (appliedCount > 0 && groupDiscount > 0) {
      applications.push({
        group,
        discount_amount: groupDiscount,
        applied_count: appliedCount,
        discounted_qty: qty(discountedQty),
        metadata: {
          bundle_buy_qty: bundleQty,
          bundle_pay_price: bundlePayPrice,
          max_discounted_qty: maxDiscountedQty || null,
          product_ids: Array.from(productIds),
          bundles: appliedBundles,
        },
      });
    }
  }

  for (const item of items || []) {
    if (Number(item.is_gift)) continue;
    const itemId = Number(item.order_item_id);
    const discount = money(discountByItemId.get(itemId) || 0);
    if (!(discount > 0)) continue;

    const current = money(item.current_line_total ?? item.base_line_total ?? item.price ?? 0);
    const next = money(Math.max(0, current - discount));

    await conn.query(
      `UPDATE order_item SET price = ?, price_locked = 1 WHERE id = ?`,
      [next, itemId],
    );

    item.current_line_total = next;
    item.product_group_promotion_discount = money((item.product_group_promotion_discount || 0) + discount);
  }

  await replaceProductGroupApplications(conn, {
    order_id: orderId,
    shop_id: shopId,
    applications,
  });

  return applications;
}

function parseMetadata(row) {
  const raw = row?.metadata;
  if (!raw) return {};
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return {};
  }
}

function fmtMoney(value) {
  return money(value).toFixed(2);
}

function fmtQty(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(3).replace(/\.?0+$/, "");
}

function formatProductGroupPromotionApplication(row, isEnglish = false) {
  const title = cleanText(row?.title, 255) || (isEnglish ? "Product group promotion" : "מבצע קבוצת מוצרים");
  const buy = fmtQty(row?.bundle_buy_qty || parseMetadata(row).bundle_buy_qty || 0);
  const price = fmtMoney(row?.bundle_pay_price || parseMetadata(row).bundle_pay_price || 0);
  const applied = Number(row?.applied_count || 0);
  const appliedText = applied > 1
    ? isEnglish
      ? ` × ${applied}`
      : ` × ${applied}`
    : "";

  return isEnglish
    ? `🏷️ ${title}: ${buy} for ₪${price}${appliedText}`
    : `🏷️ ${title}: ${buy} ב-₪${price}${appliedText}`;
}

async function getOrderProductGroupPromotionApplications(order_id, shop_id = null) {
  const params = [Number(order_id)];
  let sql = `
    SELECT *
    FROM order_product_group_promotion_application
    WHERE order_id = ?
  `;
  if (shop_id) {
    sql += ` AND shop_id = ?`;
    params.push(Number(shop_id));
  }
  sql += ` ORDER BY id ASC`;
  const [rows] = await db.query(sql, params);
  return rows || [];
}

module.exports = {
  fetchActiveProductGroupPromotions,
  applyProductGroupPromotionsToItems,
  getOrderProductGroupPromotionApplications,
  formatProductGroupPromotionApplication,
};

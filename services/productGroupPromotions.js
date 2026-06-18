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

function maxApplications(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function canUseGroupAgain(group, groupUseCounts) {
  const max = maxApplications(group?.max_discounted_qty);
  if (!max) return true;
  const used = groupUseCounts.get(Number(group?.id)) || 0;
  return used < max;
}

function cleanText(value, limit = 255) {
  const s = String(value ?? "").trim().replace(/\s+/g, " ");
  return s ? s.slice(0, limit) : null;
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

const COLUMN_CACHE = new Map();

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

async function ensureProductGroupPromotionColumns(conn = db) {
  await addColumnIfMissing(
    conn,
    "product_group_promotion",
    "emoji",
    "VARCHAR(16) DEFAULT NULL AFTER description",
  );
}

function normalizeEmoji(value) {
  const s = String(value ?? "").trim();
  if (!s) return null;
  return Array.from(s)[0] || null;
}

function majorityProductEmoji(products) {
  const counts = new Map();
  for (const product of products || []) {
    const emoji = normalizeEmoji(product?.emoji || product?.product_emoji || product?.subcategory_emoji);
    if (!emoji) continue;
    counts.set(emoji, (counts.get(emoji) || 0) + 1);
  }

  let best = null;
  let bestCount = 0;
  for (const [emoji, count] of counts.entries()) {
    if (count > bestCount) {
      best = emoji;
      bestCount = count;
    }
  }
  return best;
}

function resolveProductGroupPromotionEmoji(group) {
  return normalizeEmoji(group?.emoji) || majorityProductEmoji(group?.products || []) || "🏷️";
}

async function fetchActiveProductGroupPromotions(conn, shop_id) {
  await ensureProductGroupPromotionColumns(conn);

  const [groups] = await conn.query(
    `
    SELECT *
    FROM product_group_promotion
    WHERE shop_id = ?
      AND is_active = 1
      AND (start_at IS NULL OR start_at <= NOW())
      AND (end_at IS NULL OR end_at >= NOW())
    ORDER BY id ASC
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
      p.display_name_en,
      p.emoji
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
      emoji: item.emoji ?? null,
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
  let slotCounter = 1;

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
        slotId: slotCounter++,
        item,
        itemId: Number(item.order_item_id),
        productId: Number(item.product_id),
        name: item.name || `#${item.product_id}`,
        unitPrice,
      });
    }
  }

  return slots;
}

function pushLimitedCombinationCandidates({
  group,
  sortedSlots,
  bundleQty,
  bundlePayPrice,
  candidates,
}) {
  const maxSlotsForCombinations = bundleQty <= 2 ? 28 : bundleQty === 3 ? 18 : 14;
  const maxCandidatesPerGroup = bundleQty <= 2 ? 450 : 300;
  const pool = sortedSlots.slice(0, Math.max(bundleQty, maxSlotsForCombinations));
  const current = [];

  function walk(startIndex) {
    if (candidates.length >= maxCandidatesPerGroup) return;

    if (current.length === bundleQty) {
      const regularValue = money(current.reduce((sum, slot) => sum + money(slot.unitPrice), 0));
      const discount = money(regularValue - bundlePayPrice);
      if (discount > 0) {
        candidates.push({
          group,
          slots: current.slice(),
          slotIds: current.map((slot) => slot.slotId),
          discount,
          regularValue,
          bundlePayPrice,
        });
      }
      return;
    }

    const remainingNeeded = bundleQty - current.length;
    for (let i = startIndex; i <= pool.length - remainingNeeded; i += 1) {
      current.push(pool[i]);
      walk(i + 1);
      current.pop();
      if (candidates.length >= maxCandidatesPerGroup) return;
    }
  }

  walk(0);
}

function buildBundleCandidates(groups, slots) {
  const allCandidates = [];

  for (const group of groups || []) {
    const bundleQty = Math.max(2, Math.floor(Number(group.bundle_buy_qty || 0)));
    const bundlePayPrice = money(group.bundle_pay_price);
    const productIds = new Set((group.products || []).map((p) => Number(p.product_id)).filter(Boolean));
    if (!productIds.size || !(bundleQty >= 2)) continue;

    let eligible = slots
      .filter((slot) => productIds.has(slot.productId))
      .sort((a, b) => {
        if (b.unitPrice !== a.unitPrice) return b.unitPrice - a.unitPrice;
        if (a.itemId !== b.itemId) return a.itemId - b.itemId;
        return a.slotId - b.slotId;
      });

    if (eligible.length < bundleQty) continue;

    const groupCandidates = [];
    pushLimitedCombinationCandidates({
      group,
      sortedSlots: eligible,
      bundleQty,
      bundlePayPrice,
      candidates: groupCandidates,
    });

    groupCandidates.sort((a, b) => {
      if (b.discount !== a.discount) return b.discount - a.discount;
      if (b.regularValue !== a.regularValue) return b.regularValue - a.regularValue;
      return Number(a.group.id) - Number(b.group.id);
    });

    allCandidates.push(...groupCandidates.slice(0, 250));
  }

  allCandidates.sort((a, b) => {
    if (b.discount !== a.discount) return b.discount - a.discount;
    if (b.regularValue !== a.regularValue) return b.regularValue - a.regularValue;
    return Number(a.group.id) - Number(b.group.id);
  });

  return allCandidates.slice(0, 700);
}

function chooseBestNonConflictingBundles(candidates) {
  const rows = Array.isArray(candidates) ? candidates : [];
  if (!rows.length) return [];

  const suffix = new Array(rows.length + 1).fill(0);
  for (let i = rows.length - 1; i >= 0; i -= 1) {
    suffix[i] = suffix[i + 1] + money(rows[i].discount);
  }

  let bestDiscount = 0;
  let bestSelection = [];
  let visited = 0;
  const maxVisited = 80000;
  const startedAt = Date.now();
  const maxMs = 160;

  function dfs(index, usedSlotIds, groupUseCounts, selected, discount) {
    visited += 1;
    if (visited > maxVisited || Date.now() - startedAt > maxMs) return;

    if (money(discount + suffix[index]) <= bestDiscount + 0.0001) return;

    if (index >= rows.length) {
      const rounded = money(discount);
      if (rounded > bestDiscount + 0.0001) {
        bestDiscount = rounded;
        bestSelection = selected.slice();
      }
      return;
    }

    const candidate = rows[index];
    let canTake = true;
    for (const slotId of candidate.slotIds) {
      if (usedSlotIds.has(slotId)) {
        canTake = false;
        break;
      }
    }

    if (canTake && canUseGroupAgain(candidate.group, groupUseCounts)) {
      const gid = Number(candidate.group.id);
      for (const slotId of candidate.slotIds) usedSlotIds.add(slotId);
      groupUseCounts.set(gid, (groupUseCounts.get(gid) || 0) + 1);
      selected.push(candidate);
      dfs(index + 1, usedSlotIds, groupUseCounts, selected, money(discount + candidate.discount));
      selected.pop();
      const nextCount = (groupUseCounts.get(gid) || 1) - 1;
      if (nextCount > 0) groupUseCounts.set(gid, nextCount);
      else groupUseCounts.delete(gid);
      for (const slotId of candidate.slotIds) usedSlotIds.delete(slotId);
    }

    dfs(index + 1, usedSlotIds, groupUseCounts, selected, discount);
  }

  dfs(0, new Set(), new Map(), [], 0);

  if (bestSelection.length) return bestSelection;

  // Safe fallback: if the exact search stopped too early, still pick greedily by savings.
  const used = new Set();
  const groupUseCounts = new Map();
  const greedy = [];
  for (const candidate of rows) {
    if (candidate.slotIds.some((slotId) => used.has(slotId))) continue;
    if (!canUseGroupAgain(candidate.group, groupUseCounts)) continue;
    greedy.push(candidate);
    for (const slotId of candidate.slotIds) used.add(slotId);
    const gid = Number(candidate.group.id);
    groupUseCounts.set(gid, (groupUseCounts.get(gid) || 0) + 1);
  }
  return greedy;
}

function addDiscountPart(bundleSlots, discount, discountByItemId) {
  const total = money(bundleSlots.reduce((sum, slot) => sum + money(slot.unitPrice), 0));
  if (!(total > 0) || !(discount > 0)) return;

  let allocated = 0;
  for (let i = 0; i < bundleSlots.length; i += 1) {
    const slot = bundleSlots[i];
    const part = i === bundleSlots.length - 1
      ? money(discount - allocated)
      : money(discount * (money(slot.unitPrice) / total));
    allocated = money(allocated + part);
    discountByItemId.set(slot.itemId, money((discountByItemId.get(slot.itemId) || 0) + part));
  }
}

function applicationsFromSelection(selection, discountByItemId) {
  const byGroup = new Map();

  for (const candidate of selection || []) {
    addDiscountPart(candidate.slots, candidate.discount, discountByItemId);

    const gid = Number(candidate.group.id);
    if (!byGroup.has(gid)) {
      byGroup.set(gid, {
        group: candidate.group,
        discount_amount: 0,
        applied_count: 0,
        discounted_qty: 0,
        bundles: [],
      });
    }

    const app = byGroup.get(gid);
    app.discount_amount = money(app.discount_amount + candidate.discount);
    app.applied_count += 1;
    app.discounted_qty += candidate.slots.length;
    if (app.bundles.length < 30) {
      app.bundles.push({
        item_ids: candidate.slots.map((slot) => slot.itemId),
        order_item_ids: candidate.slots.map((slot) => slot.itemId),
        product_ids: candidate.slots.map((slot) => slot.productId),
        product_names: candidate.slots.map((slot) => slot.name),
        regular_value: candidate.regularValue,
        pay_price: candidate.bundlePayPrice,
        discount: candidate.discount,
      });
    }
  }

  return Array.from(byGroup.values()).map((app) => ({
    ...app,
    discounted_qty: qty(app.discounted_qty),
    metadata: {
      bundle_buy_qty: Number(app.group.bundle_buy_qty),
      bundle_pay_price: money(app.group.bundle_pay_price),
      max_discounted_qty: maxApplications(app.group.max_discounted_qty),
      max_applications: maxApplications(app.group.max_discounted_qty),
      product_ids: (app.group.products || []).map((p) => Number(p.product_id)).filter(Boolean),
      bundles: app.bundles,
    },
  }));
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
  if (!slots.length) {
    await replaceProductGroupApplications(conn, { order_id: orderId, shop_id: shopId, applications: [] });
    return [];
  }

  const candidates = buildBundleCandidates(groups, slots);
  const selectedBundles = chooseBestNonConflictingBundles(candidates);
  const discountByItemId = new Map();
  const applications = applicationsFromSelection(selectedBundles, discountByItemId)
    .filter((app) => app.applied_count > 0 && money(app.discount_amount) > 0);

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

function fmtMoney(value) {
  return money(value).toFixed(2);
}

function fmtQty(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(3).replace(/\.?0+$/, "");
}

function formatProductGroupPromotionApplication(row, isEnglish = false) {
  const meta = parseMetadata(row);
  const title = cleanText(row?.title, 255) || (isEnglish ? "Product group promotion" : "מבצע קבוצת מוצרים");
  const buy = fmtQty(row?.bundle_buy_qty || meta.bundle_buy_qty || 0);
  const price = fmtMoney(row?.bundle_pay_price || meta.bundle_pay_price || 0);
  const applied = Number(row?.applied_count || 0);
  const appliedText = applied > 1 ? ` × ${applied}` : "";

  const emoji = resolveProductGroupPromotionEmoji(row);
  return isEnglish
    ? `${emoji} ${title}: ${buy} for ₪${price}${appliedText}`
    : `${emoji} ${title}: ${buy} ב-₪${price}${appliedText}`;
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

async function fetchProductsForGroupPromotions(shopId, groupIds) {
  const ids = [...new Set((groupIds || []).map((id) => Number(id)).filter(Boolean))];
  if (!ids.length) return new Map();

  const placeholders = ids.map(() => "?").join(",");
  const [rows] = await db.query(
    `
    SELECT
      gpi.group_promotion_id,
      p.id,
      p.name,
      p.display_name_en,
      p.price,
      p.stock_amount,
      p.category,
      p.sub_category,
      p.emoji
    FROM product_group_promotion_item gpi
    JOIN product p ON p.id = gpi.product_id AND p.shop_id = gpi.shop_id
    WHERE gpi.shop_id = ?
      AND gpi.group_promotion_id IN (${placeholders})
    ORDER BY gpi.group_promotion_id ASC, p.name ASC, p.id ASC
    `,
    [Number(shopId), ...ids],
  );

  const byGroup = new Map();
  for (const row of rows || []) {
    const gid = Number(row.group_promotion_id);
    if (!byGroup.has(gid)) byGroup.set(gid, []);
    byGroup.get(gid).push({
      id: Number(row.id),
      product_id: Number(row.id),
      name: row.name,
      display_name_en: row.display_name_en,
      price: row.price,
      stock_amount: row.stock_amount,
      category: row.category,
      sub_category: row.sub_category,
      emoji: row.emoji,
    });
  }
  return byGroup;
}

function decorateGroupWithProducts(group, products) {
  const list = Array.isArray(products) ? products : [];
  return {
    ...group,
    type: "PRODUCT_GROUP",
    products: list,
    emoji: resolveProductGroupPromotionEmoji({ ...group, products: list }),
  };
}

async function fetchActiveProductGroupPromotionsForProduct(shop_id, product_id, { limit = 5 } = {}) {
  await ensureProductGroupPromotionColumns();

  const shopId = Number(shop_id);
  const productId = Number(product_id);
  if (!Number.isFinite(shopId) || shopId <= 0 || !Number.isFinite(productId) || productId <= 0) {
    return [];
  }

  const safeLimit = Math.min(Math.max(Number(limit) || 5, 1), 20);
  const [groups] = await db.query(
    `
    SELECT DISTINCT pgp.*
    FROM product_group_promotion_item gpi
    JOIN product_group_promotion pgp
      ON pgp.id = gpi.group_promotion_id
     AND pgp.shop_id = gpi.shop_id
    WHERE gpi.shop_id = ?
      AND gpi.product_id = ?
      AND pgp.is_active = 1
      AND (pgp.start_at IS NULL OR pgp.start_at <= NOW())
      AND (pgp.end_at IS NULL OR pgp.end_at >= NOW())
    ORDER BY
      CASE WHEN pgp.end_at IS NULL THEN 1 ELSE 0 END ASC,
      pgp.end_at ASC,
      pgp.id DESC
    LIMIT ${safeLimit}
    `,
    [shopId, productId],
  );

  const ids = (groups || []).map((g) => Number(g.id)).filter(Boolean);
  if (!ids.length) return [];
  const productsByGroup = await fetchProductsForGroupPromotions(shopId, ids);
  return (groups || []).map((group) => decorateGroupWithProducts(group, productsByGroup.get(Number(group.id)) || []));
}

function groupDealSortValue(group) {
  const buyQty = Math.max(1, Number(group?.bundle_buy_qty || 1));
  const pay = Number(group?.bundle_pay_price);
  if (!Number.isFinite(pay)) return Number.MAX_SAFE_INTEGER;
  return pay / buyQty;
}

async function getActiveProductGroupPromotionHintsForProducts(shop_id, product_ids) {
  await ensureProductGroupPromotionColumns();

  const shopId = Number(shop_id);
  const ids = [...new Set((product_ids || []).map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))];
  const out = new Map();
  if (!Number.isFinite(shopId) || shopId <= 0 || !ids.length) return out;

  const placeholders = ids.map(() => "?").join(",");
  const [rows] = await db.query(
    `
    SELECT
      gpi.product_id AS matched_product_id,
      pgp.*
    FROM product_group_promotion_item gpi
    JOIN product_group_promotion pgp
      ON pgp.id = gpi.group_promotion_id
     AND pgp.shop_id = gpi.shop_id
    WHERE gpi.shop_id = ?
      AND gpi.product_id IN (${placeholders})
      AND pgp.is_active = 1
      AND (pgp.start_at IS NULL OR pgp.start_at <= NOW())
      AND (pgp.end_at IS NULL OR pgp.end_at >= NOW())
    ORDER BY gpi.product_id ASC, pgp.id DESC
    `,
    [shopId, ...ids],
  );

  const groupIds = [...new Set((rows || []).map((r) => Number(r.id)).filter(Boolean))];
  const productsByGroup = await fetchProductsForGroupPromotions(shopId, groupIds);

  for (const row of rows || []) {
    const pid = Number(row.matched_product_id);
    const group = decorateGroupWithProducts(row, productsByGroup.get(Number(row.id)) || []);
    const current = out.get(pid);
    if (!current || groupDealSortValue(group) < groupDealSortValue(current)) {
      out.set(pid, group);
    }
  }

  return out;
}

async function attachProductGroupPromotionHintsToItems({ shop_id, items }) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return list;

  const productIds = list.map((item) => Number(item?.product_id ?? item?.productId)).filter((id) => Number.isFinite(id) && id > 0);
  if (!productIds.length) return list;

  const hints = await getActiveProductGroupPromotionHintsForProducts(shop_id, productIds);
  if (!hints.size) return list;

  return list.map((item) => {
    const productId = Number(item?.product_id ?? item?.productId);
    const hint = hints.get(productId);
    return hint ? { ...item, group_promo: hint, group_promo_hint: hint } : item;
  });
}

module.exports = {
  ensureProductGroupPromotionColumns,
  normalizeEmoji,
  resolveProductGroupPromotionEmoji,
  fetchActiveProductGroupPromotions,
  fetchActiveProductGroupPromotionsForProduct,
  getActiveProductGroupPromotionHintsForProducts,
  attachProductGroupPromotionHintsToItems,
  applyProductGroupPromotionsToItems,
  getOrderProductGroupPromotionApplications,
  formatProductGroupPromotionApplication,
  parseMetadata,
};

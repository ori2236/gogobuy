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

async function fetchActiveProductGroupPromotions(conn, shop_id) {
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

    const maxDiscountedQty = qty(group.max_discounted_qty, 0);
    if (maxDiscountedQty > 0) {
      eligible = eligible.slice(0, Math.floor(maxDiscountedQty));
    }

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

  function dfs(index, usedSlotIds, selected, discount) {
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

    if (canTake) {
      for (const slotId of candidate.slotIds) usedSlotIds.add(slotId);
      selected.push(candidate);
      dfs(index + 1, usedSlotIds, selected, money(discount + candidate.discount));
      selected.pop();
      for (const slotId of candidate.slotIds) usedSlotIds.delete(slotId);
    }

    dfs(index + 1, usedSlotIds, selected, discount);
  }

  dfs(0, new Set(), [], 0);

  if (bestSelection.length) return bestSelection;

  // Safe fallback: if the exact search stopped too early, still pick greedily by savings.
  const used = new Set();
  const greedy = [];
  for (const candidate of rows) {
    if (candidate.slotIds.some((slotId) => used.has(slotId))) continue;
    greedy.push(candidate);
    for (const slotId of candidate.slotIds) used.add(slotId);
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
      max_discounted_qty: qty(app.group.max_discounted_qty, 0) || null,
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
  parseMetadata,
};

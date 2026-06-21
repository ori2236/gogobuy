require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");
const { ensureProductGroupPromotionColumns } = require("../services/productGroupPromotions");

const DEFAULT_SOURCE = "leshem_excel_2026_06_14";
const DEFAULT_DATA_FILE = path.join(__dirname, "..", "data", "leshem_promotions_2026_06_14.json");
const DEFAULT_MAPPING_FILE = path.join(__dirname, "..", "data", "leshem_manual_promo_mapping.json");
const REPORTS_DIR = path.join(__dirname, "..", "reports");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

const SHOP_ID = Number(argValue("--shopId", process.env.PROMO_IMPORT_SHOP_ID || 2));
const DATA_FILE = path.resolve(argValue("--data", process.env.PROMO_IMPORT_DATA_FILE || DEFAULT_DATA_FILE));
const MAPPING_FILE = path.resolve(argValue("--mapping", DEFAULT_MAPPING_FILE));
const CONFIRM = Boolean(argValue("--confirm", false));
const DRY_RUN = !CONFIRM || Boolean(argValue("--dryRun", false));
const REPLACE = Boolean(argValue("--replace", false));
const INCLUDE_EXPIRED = Boolean(argValue("--includeExpired", false));
const SOURCE = String(argValue("--source", process.env.PROMO_IMPORT_SOURCE || DEFAULT_SOURCE)).trim() || DEFAULT_SOURCE;

function pad2(value) {
  return String(value).padStart(2, "0");
}

function stamp() {
  const d = new Date();
  return [
    d.getFullYear(),
    pad2(d.getMonth() + 1),
    pad2(d.getDate()),
    "_",
    pad2(d.getHours()),
    pad2(d.getMinutes()),
    pad2(d.getSeconds()),
  ].join("");
}

function parseDealText(value) {
  const m = String(value || "").match(/(\d+(?:\.\d+)?)\s*ב\s*(\d+(?:\.\d+)?)/);
  if (!m) return null;
  const qty = Number(m[1]);
  const price = Number(m[2]);
  if (!Number.isFinite(qty) || !Number.isFinite(price) || qty <= 0 || price < 0) return null;
  return { qty, price };
}

function parseThresholdProductFixedPrice(title) {
  const s = String(title || "");
  const thresholdMatch = s.match(/מעל\s*(\d+(?:\.\d+)?)/);
  const fixedPriceMatch = s.match(/ב\s*-?\s*(\d+(?:\.\d+)?)/g);
  if (!thresholdMatch || !fixedPriceMatch || fixedPriceMatch.length < 1) return null;

  const last = fixedPriceMatch[fixedPriceMatch.length - 1];
  const price = Number((last.match(/(\d+(?:\.\d+)?)/) || [])[1]);
  const threshold = Number(thresholdMatch[1]);
  if (!Number.isFinite(threshold) || !Number.isFinite(price)) return null;
  return { threshold, price };
}

function startDateTime(dateText) {
  return dateText ? `${dateText} 00:00:00` : null;
}

function endDateTime(dateText) {
  return dateText ? `${dateText} 23:59:59` : null;
}

function isExpired(dateText) {
  if (!dateText) return false;
  const end = new Date(`${dateText}T23:59:59`);
  return Number.isFinite(end.getTime()) && end.getTime() < Date.now();
}

function cleanDescription(parts, limit = 1000) {
  return parts
    .filter((part) => part !== null && part !== undefined && String(part).trim())
    .join(" | ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, limit);
}

function parseMaxQty(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return { error: "invalid_max_qty" };
  return n;
}

function buildProductPromotionPayload({ promo, product, mapping }) {
  const deal = parseDealText(promo.deal_text);
  if (!deal) return { skip_reason: "deal_text_not_supported" };
  if (deal.qty < 1) return { skip_reason: "fractional_bundle_qty_requires_code_or_weight_support" };
  if (!Number.isInteger(deal.qty)) return { skip_reason: "non_integer_bundle_qty_requires_code_support" };

  const maxQty = parseMaxQty(promo.max_qty);
  if (maxQty && typeof maxQty === "object" && maxQty.error) return { skip_reason: maxQty.error };

  const description = cleanDescription(
    [
      promo.title,
      `מקור אקסל ${SOURCE}`,
      `תגמול ${promo.reward_id}`,
      "התאמה ידנית",
      mapping.note ? `הערה: ${mapping.note}` : null,
    ],
    255,
  );

  if (deal.qty === 1) {
    return {
      product_id: product.id,
      product_name: product.name,
      kind: "FIXED_PRICE",
      percent_off: null,
      amount_off: null,
      fixed_price: deal.price,
      bundle_buy_qty: null,
      bundle_pay_price: null,
      max_discounted_qty: maxQty,
      description,
      start_at: startDateTime(promo.start_date),
      end_at: endDateTime(promo.end_date),
    };
  }

  return {
    product_id: product.id,
    product_name: product.name,
    kind: "BUNDLE",
    percent_off: null,
    amount_off: null,
    fixed_price: null,
    bundle_buy_qty: deal.qty,
    bundle_pay_price: deal.price,
    max_discounted_qty: maxQty,
    description,
    start_at: startDateTime(promo.start_date),
    end_at: endDateTime(promo.end_date),
  };
}

function buildProductGroupPromotionPayload({ promo, products, mapping }) {
  const deal = parseDealText(promo.deal_text);
  if (!deal) return { skip_reason: "deal_text_not_supported_for_group" };
  if (!Number.isInteger(deal.qty) || deal.qty < 2) return { skip_reason: "group_promotion_requires_integer_qty_of_at_least_2" };
  if (!Array.isArray(products) || products.length < 2) return { skip_reason: "group_promotion_requires_at_least_2_products" };

  const maxQty = parseMaxQty(promo.max_qty);
  if (maxQty && typeof maxQty === "object" && maxQty.error) return { skip_reason: maxQty.error };

  return {
    title: String(promo.title || "").trim().slice(0, 255),
    description: cleanDescription([
      promo.title,
      `מקור אקסל ${SOURCE}`,
      `תגמול ${promo.reward_id}`,
      "התאמה ידנית כמבצע קבוצה",
      mapping.note ? `הערה: ${mapping.note}` : null,
    ]),
    emoji: null,
    kind: "BUNDLE",
    bundle_buy_qty: deal.qty,
    bundle_pay_price: deal.price,
    max_discounted_qty: maxQty,
    priority: 100,
    is_active: 1,
    start_at: startDateTime(promo.start_date),
    end_at: endDateTime(promo.end_date),
    product_ids: products.map((p) => Number(p.id)).filter(Boolean),
    product_names: products.map((p) => p.name),
  };
}

function buildCartRewardRulePayload({ promo, product, mapping }) {
  const parsed = parseThresholdProductFixedPrice(promo.title);
  if (!parsed) return { skip_reason: "cart_reward_rule_not_parseable" };

  return {
    rule_type: "THRESHOLD_PRODUCT_FIXED_PRICE",
    title: `${promo.title} - ${product.name}`.slice(0, 255),
    description: cleanDescription([
      promo.title,
      `מקור אקסל ${SOURCE}`,
      `תגמול ${promo.reward_id}`,
      "התאמה ידנית למוצר הטבה",
      mapping.note ? `הערה: ${mapping.note}` : null,
    ]),
    threshold_amount: parsed.threshold,
    delivery_fee_override: null,
    reward_product_id: product.id,
    reward_qty: null,
    reward_fixed_price: parsed.price,
    reward_max_qty: null,
    threshold_base_mode: "EXCLUDING_REWARD_PRODUCTS",
    priority: 30,
    source: SOURCE,
    external_reward_id: `${promo.reward_id}_${product.id}`,
    start_at: startDateTime(promo.start_date),
    end_at: endDateTime(promo.end_date),
  };
}

async function loadProducts(conn, shopId) {
  const [rows] = await conn.query(
    `
    SELECT id, name, display_name_en, price, stock_amount, category, sub_category
    FROM product
    WHERE shop_id = ?
    ORDER BY id ASC
    `,
    [shopId],
  );

  const map = new Map();
  for (const row of rows || []) {
    map.set(Number(row.id), {
      id: Number(row.id),
      name: row.name,
      display_name_en: row.display_name_en,
      price: row.price == null ? null : Number(row.price),
      stock_amount: row.stock_amount == null ? null : Number(row.stock_amount),
      category: row.category,
      sub_category: row.sub_category,
    });
  }
  return map;
}

async function productPromotionExists(conn, shopId, productId, rewardId) {
  const [rows] = await conn.query(
    `
    SELECT id
    FROM promotion
    WHERE shop_id = ?
      AND product_id = ?
      AND description LIKE ?
    LIMIT 1
    `,
    [shopId, productId, `%תגמול ${rewardId}%`],
  );
  return rows?.[0] || null;
}

async function productGroupPromotionExists(conn, shopId, rewardId) {
  const [rows] = await conn.query(
    `
    SELECT id
    FROM product_group_promotion
    WHERE shop_id = ?
      AND description LIKE ?
    LIMIT 1
    `,
    [shopId, `%תגמול ${rewardId}%`],
  );
  return rows?.[0] || null;
}

async function cartRuleExists(conn, shopId, externalRewardId) {
  const [rows] = await conn.query(
    `
    SELECT id
    FROM cart_promotion_rule
    WHERE shop_id = ?
      AND source = ?
      AND external_reward_id = ?
    LIMIT 1
    `,
    [shopId, SOURCE, String(externalRewardId)],
  );
  return rows?.[0] || null;
}

async function insertProductPromotion(conn, shopId, payload) {
  await conn.query(
    `
    INSERT INTO promotion
      (shop_id, product_id, kind, percent_off, amount_off, fixed_price,
       bundle_buy_qty, bundle_pay_price, max_discounted_qty, description,
       start_at, end_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
    `,
    [
      shopId,
      payload.product_id,
      payload.kind,
      payload.percent_off,
      payload.amount_off,
      payload.fixed_price,
      payload.bundle_buy_qty,
      payload.bundle_pay_price,
      payload.max_discounted_qty,
      payload.description,
      payload.start_at,
      payload.end_at,
    ],
  );
}

async function insertProductGroupPromotion(conn, shopId, payload) {
  const [result] = await conn.query(
    `
    INSERT INTO product_group_promotion
      (shop_id, title, description, emoji, kind, bundle_buy_qty, bundle_pay_price,
       max_discounted_qty, priority, is_active, start_at, end_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, 'BUNDLE', ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
    `,
    [
      shopId,
      payload.title,
      payload.description,
      payload.emoji,
      payload.bundle_buy_qty,
      payload.bundle_pay_price,
      payload.max_discounted_qty,
      payload.priority,
      payload.is_active,
      payload.start_at,
      payload.end_at,
    ],
  );
  const groupId = Number(result.insertId);
  if (!groupId) throw new Error("Failed to insert product group promotion");

  const uniqueProductIds = Array.from(new Set(payload.product_ids.map(Number).filter(Boolean)));
  const valuesSql = uniqueProductIds.map(() => `(?, ?, ?)`).join(", ");
  const params = [];
  for (const productId of uniqueProductIds) params.push(groupId, shopId, productId);
  await conn.query(
    `INSERT INTO product_group_promotion_item (group_promotion_id, shop_id, product_id) VALUES ${valuesSql}`,
    params,
  );
  return groupId;
}

async function insertCartRule(conn, shopId, rule) {
  await conn.query(
    `
    INSERT INTO cart_promotion_rule
      (shop_id, rule_type, title, description, threshold_amount, delivery_fee_override,
       reward_product_id, reward_qty, reward_fixed_price, reward_max_qty,
       threshold_base_mode, priority, is_active, notify_customer,
       source, external_reward_id, start_at, end_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?, NOW(), NOW())
    `,
    [
      shopId,
      rule.rule_type,
      rule.title,
      rule.description,
      rule.threshold_amount,
      rule.delivery_fee_override,
      rule.reward_product_id,
      rule.reward_qty,
      rule.reward_fixed_price,
      rule.reward_max_qty,
      rule.threshold_base_mode,
      rule.priority,
      rule.source,
      rule.external_reward_id,
      rule.start_at,
      rule.end_at,
    ],
  );
}

async function backupTable(conn, tableName, backupName, shopId) {
  await conn.query(`CREATE TABLE \`${backupName}\` AS SELECT * FROM \`${tableName}\` WHERE shop_id = ?`, [shopId]);
}

function writeReport(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const file = path.join(REPORTS_DIR, `leshem_manual_promotions_apply_report_${stamp()}.json`);
  fs.writeFileSync(file, JSON.stringify(report, null, 2), "utf8");
  return file;
}

function mappingProductIds(mapping) {
  const ids = mapping.product_ids || mapping.productIds || mapping.selected_product_ids || mapping.selectedProductIds || [];
  return Array.from(new Set(ids.map((id) => Number(id)).filter((id) => Number.isInteger(id) && id > 0)));
}

function cleanDateText(value) {
  const s = String(value ?? "").trim();
  return s || null;
}

function promoWithMappingDates(promo, mapping) {
  return {
    ...promo,
    start_date: cleanDateText(mapping.start_date ?? mapping.startDate) || promo.start_date || null,
    end_date: cleanDateText(mapping.end_date ?? mapping.endDate) || promo.end_date || null,
  };
}

function isGroupMapping(mapping) {
  const action = String(mapping.action || "").toLowerCase();
  const mode = String(mapping.mapping_mode || mapping.mappingMode || "").toLowerCase();
  return Boolean(mapping.is_group_promotion || mapping.isGroupPromotion || action === "promotion_group" || action === "group" || mode === "group");
}

async function analyzeMappings({ conn, shopId, promotionsByRewardId, productsById, mappingRows }) {
  const inserts = [];
  const skipped = [];

  for (const mapping of mappingRows) {
    const rewardId = Number(mapping.reward_id ?? mapping.rewardId);
    const productIds = mappingProductIds(mapping);
    const promo = promotionsByRewardId.get(rewardId);

    if (!promo) {
      skipped.push({ reward_id: rewardId, reason: "reward_id_not_found_in_source", product_ids: productIds });
      continue;
    }

    const effectivePromo = promoWithMappingDates(promo, mapping);

    if (effectivePromo.active !== "כן") {
      skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: "inactive_in_excel", product_ids: productIds });
      continue;
    }
    if (!INCLUDE_EXPIRED && isExpired(effectivePromo.end_date)) {
      skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: "expired_in_excel", product_ids: productIds });
      continue;
    }
    if (!productIds.length) {
      skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: "no_products_selected" });
      continue;
    }

    if (isGroupMapping(mapping)) {
      if (effectivePromo.type !== "כמות בסכום") {
        skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: "group_mapping_supported_only_for_quantity_bundle", product_ids: productIds });
        continue;
      }

      const selectedProducts = [];
      for (const productId of productIds) {
        const product = productsById.get(Number(productId));
        if (!product) {
          skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: "product_id_not_found_in_shop", product_id: productId, mapping_mode: "group" });
          continue;
        }
        selectedProducts.push(product);
      }

      const payload = buildProductGroupPromotionPayload({ promo: effectivePromo, products: selectedProducts, mapping });
      if (payload.skip_reason) {
        skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: payload.skip_reason, product_ids: productIds });
        continue;
      }

      const duplicate = await productGroupPromotionExists(conn, shopId, rewardId);
      if (duplicate && !REPLACE) {
        skipped.push({
          reward_id: rewardId,
          title: effectivePromo.title,
          reason: "already_exists",
          existing_id: duplicate.id,
          product_ids: payload.product_ids,
          mapping_mode: "group",
        });
        continue;
      }

      inserts.push({
        kind: "product_group_promotion",
        reward_id: rewardId,
        title: effectivePromo.title,
        product_ids: payload.product_ids,
        product_names: payload.product_names,
        existing_id: duplicate?.id || null,
        payload,
      });
      continue;
    }

    const action = String(mapping.action || (effectivePromo.type === "קנה בסכום הוסף קבל" ? "cart_reward_product" : "product_promotion"));

    for (const productId of productIds) {
      const product = productsById.get(Number(productId));
      if (!product) {
        skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: "product_id_not_found_in_shop", product_id: productId });
        continue;
      }

      if (action === "cart_reward_product" || effectivePromo.type === "קנה בסכום הוסף קבל") {
        const payload = buildCartRewardRulePayload({ promo: effectivePromo, product, mapping });
        if (payload.skip_reason) {
          skipped.push({
            reward_id: rewardId,
            title: effectivePromo.title,
            reason: payload.skip_reason,
            product_id: product.id,
            product_name: product.name,
          });
          continue;
        }

        const duplicate = await cartRuleExists(conn, shopId, payload.external_reward_id);
        if (duplicate && !REPLACE) {
          skipped.push({
            reward_id: rewardId,
            title: effectivePromo.title,
            reason: "already_exists",
            existing_id: duplicate.id,
            product_id: product.id,
            product_name: product.name,
          });
          continue;
        }

        inserts.push({
          kind: "cart_rule",
          reward_id: rewardId,
          title: effectivePromo.title,
          product_id: product.id,
          product_name: product.name,
          existing_id: duplicate?.id || null,
          payload,
        });
        continue;
      }

      if (effectivePromo.type !== "כמות בסכום") {
        skipped.push({ reward_id: rewardId, title: effectivePromo.title, reason: "unsupported_manual_promotion_type", product_id: product.id });
        continue;
      }

      const payload = buildProductPromotionPayload({ promo: effectivePromo, product, mapping });
      if (payload.skip_reason) {
        skipped.push({
          reward_id: rewardId,
          title: effectivePromo.title,
          reason: payload.skip_reason,
          product_id: product.id,
          product_name: product.name,
        });
        continue;
      }

      const duplicate = await productPromotionExists(conn, shopId, product.id, rewardId);
      if (duplicate && !REPLACE) {
        skipped.push({
          reward_id: rewardId,
          title: effectivePromo.title,
          reason: "already_exists",
          existing_id: duplicate.id,
          product_id: product.id,
          product_name: product.name,
        });
        continue;
      }

      inserts.push({
        kind: "product_promotion",
        reward_id: rewardId,
        title: effectivePromo.title,
        product_id: product.id,
        product_name: product.name,
        existing_id: duplicate?.id || null,
        payload,
      });
    }
  }

  return { inserts, skipped };
}

function printableSummary(report) {
  return {
    mode: report.mode,
    shop_id: report.shop_id,
    data_file: report.data_file,
    mapping_file: report.mapping_file,
    manual_mappings: report.manual_mappings,
    planned_inserts: report.planned_inserts,
    inserted_product_promotions: report.inserted_product_promotions,
    inserted_product_group_promotions: report.inserted_product_group_promotions,
    inserted_cart_rules: report.inserted_cart_rules,
    skipped: report.skipped.length,
    replace: report.replace,
    backup_tables: report.backup_tables,
    report_file: report.report_file,
  };
}

async function main() {
  if (!Number.isInteger(SHOP_ID) || SHOP_ID <= 0) {
    throw new Error(`Invalid --shopId: ${SHOP_ID}`);
  }
  if (!fs.existsSync(DATA_FILE)) {
    throw new Error(`Data file was not found: ${DATA_FILE}`);
  }
  if (!fs.existsSync(MAPPING_FILE)) {
    throw new Error(`Mapping file was not found: ${MAPPING_FILE}\nCreate it with: npm run manual:leshem-promos:form`);
  }

  const sourcePromotions = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  const manualMapping = JSON.parse(fs.readFileSync(MAPPING_FILE, "utf8"));
  const mappingRows = Array.isArray(manualMapping.mappings) ? manualMapping.mappings : [];
  const promotionsByRewardId = new Map(sourcePromotions.map((promo) => [Number(promo.reward_id), promo]));

  await ensureCartPromotionSchema();
  await ensureProductGroupPromotionColumns();
  const conn = await db.getConnection();

  try {
    const productsById = await loadProducts(conn, SHOP_ID);
    const analysis = await analyzeMappings({
      conn,
      shopId: SHOP_ID,
      promotionsByRewardId,
      productsById,
      mappingRows,
    });

    const report = {
      mode: DRY_RUN ? "dryRun" : "confirm",
      shop_id: SHOP_ID,
      source: SOURCE,
      data_file: DATA_FILE,
      mapping_file: MAPPING_FILE,
      generated_at: new Date().toISOString(),
      replace: REPLACE,
      include_expired: INCLUDE_EXPIRED,
      manual_mappings: mappingRows.length,
      planned_inserts: analysis.inserts.length,
      inserted_product_promotions: 0,
      inserted_product_group_promotions: 0,
      inserted_cart_rules: 0,
      inserts: analysis.inserts.map((item) => ({
        kind: item.kind,
        reward_id: item.reward_id,
        title: item.title,
        product_id: item.product_id ?? null,
        product_name: item.product_name ?? null,
        product_ids: item.product_ids ?? null,
        product_names: item.product_names ?? null,
        promotion_kind: item.payload.kind || item.payload.rule_type,
        fixed_price: item.payload.fixed_price ?? item.payload.reward_fixed_price ?? null,
        bundle_buy_qty: item.payload.bundle_buy_qty ?? null,
        bundle_pay_price: item.payload.bundle_pay_price ?? null,
        threshold_amount: item.payload.threshold_amount ?? null,
        start_at: item.payload.start_at,
        end_at: item.payload.end_at,
        existing_id: item.existing_id,
      })),
      skipped: analysis.skipped,
      backup_tables: [],
    };

    if (!DRY_RUN && analysis.inserts.length) {
      const suffix = stamp();
      const promoBackup = `bak_promo_manual_s${SHOP_ID}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");
      const cartBackup = `bak_cart_rule_manual_s${SHOP_ID}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");
      const groupBackup = `bak_group_promo_manual_s${SHOP_ID}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");
      const groupItemBackup = `bak_group_item_manual_s${SHOP_ID}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");

      await backupTable(conn, "promotion", promoBackup, SHOP_ID);
      await backupTable(conn, "cart_promotion_rule", cartBackup, SHOP_ID);
      await backupTable(conn, "product_group_promotion", groupBackup, SHOP_ID);
      await backupTable(conn, "product_group_promotion_item", groupItemBackup, SHOP_ID);
      report.backup_tables.push(promoBackup, cartBackup, groupBackup, groupItemBackup);

      await conn.beginTransaction();
      try {
        for (const item of analysis.inserts) {
          if (REPLACE && item.existing_id) {
            if (item.kind === "product_promotion") {
              await conn.query(`DELETE FROM promotion WHERE id = ? AND shop_id = ?`, [item.existing_id, SHOP_ID]);
            } else if (item.kind === "product_group_promotion") {
              await conn.query(`DELETE FROM product_group_promotion_item WHERE group_promotion_id = ? AND shop_id = ?`, [item.existing_id, SHOP_ID]);
              await conn.query(`DELETE FROM product_group_promotion WHERE id = ? AND shop_id = ?`, [item.existing_id, SHOP_ID]);
            } else {
              await conn.query(`DELETE FROM cart_promotion_rule WHERE id = ? AND shop_id = ?`, [item.existing_id, SHOP_ID]);
            }
          }

          if (item.kind === "product_promotion") {
            await insertProductPromotion(conn, SHOP_ID, item.payload);
            report.inserted_product_promotions += 1;
          } else if (item.kind === "product_group_promotion") {
            await insertProductGroupPromotion(conn, SHOP_ID, item.payload);
            report.inserted_product_group_promotions += 1;
          } else {
            await insertCartRule(conn, SHOP_ID, item.payload);
            report.inserted_cart_rules += 1;
          }
        }
        await conn.commit();
      } catch (err) {
        await conn.rollback();
        throw err;
      }
    }

    const reportFile = writeReport(report);
    report.report_file = reportFile;

    console.log(JSON.stringify(printableSummary(report), null, 2));
    if (DRY_RUN) {
      console.log("\nDry run only. To insert the manually selected promotions, run with --confirm.");
    }
  } finally {
    conn.release();
  }
}

main()
  .catch((err) => {
    console.error("[apply-leshem-manual-promo-mappings]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });

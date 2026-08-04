require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");
const {
  MARKET_DAY_DESCRIPTION,
  cleanProductPromotionTitle,
  cleanTitleForComparison,
  contentHash,
  endDateTime,
  identityHash,
  intervalContains,
  intervalsOverlap,
  isMarketDayEntity,
  marketDayWindow,
  normalizeDateTime,
  normalizeText,
  parseDealText,
  productLookup,
  roundMoney,
  roundQty,
  sameNullableNumber,
  sha256,
  sourceSlotKey,
  stableProductKey,
  startDateTime,
  stripMarketDayPrefix,
} = require("../services/promotionImportIdentity");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function boolArg(name, fallback = false) {
  const value = argValue(name, fallback);
  if (value === true || value === false) return value;
  return ["1", "true", "yes", "y"].includes(String(value).trim().toLowerCase());
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function stamp() {
  const date = new Date();
  return `${date.getFullYear()}${pad2(date.getMonth() + 1)}${pad2(date.getDate())}_${pad2(date.getHours())}${pad2(date.getMinutes())}${pad2(date.getSeconds())}`;
}

const SHOP_ID = Number(argValue("--shopId", process.env.PROMO_IMPORT_SHOP_ID || 2));
const DATA_FILE = path.resolve(argValue("--data", path.join(__dirname, "..", "data", "shop2_promotions_32065.json")));
const SOURCE = String(argValue("--source", "leshem_erp")).trim() || "leshem_erp";
const SEED_MAPPING_FILE = argValue("--seedMapping", null) ? path.resolve(argValue("--seedMapping")) : null;
const APPROVED_MAPPING_FILE = argValue("--mapping", null) ? path.resolve(argValue("--mapping")) : null;
const OUT_FILE = path.resolve(argValue("--out", path.join(__dirname, "..", "reports", `promotion_import_plan_shop${SHOP_ID}_${stamp()}.json`)));
const REVIEW_OUT_FILE = path.resolve(argValue("--reviewOut", OUT_FILE.replace(/\.json$/i, "_review.json")));
const INCLUDE_EXPIRED = boolArg("--includeExpired", false);

const STOP_WORDS = new Set([
  "ב", "של", "עם", "על", "מעל", "ללא", "לא", "עלות", "בקנייה", "בקניה", "חינם", "שח", "שחח", "ש״ח", "רק",
  "יח", "יחידות", "גרם", "קג", "לקג", "קילו", "מל", "ליטר", "לי", "מגוון", "מוצרי", "מארז", "גדול", "קטן",
]);

const GENERIC_GROUP_TOKENS = new Set([
  "חטיפים", "יינות", "ירק", "ירקות", "פירות", "צלחות", "כוס", "כוסות", "שקיות", "תבניות", "עוגיות", "תבליני", "מוצרי", "מגוון",
]);

function tokenize(value) {
  return normalizeText(value)
    .split(" ")
    .map((token) => token.trim())
    .filter(Boolean)
    .filter((token) => !STOP_WORDS.has(token))
    .filter((token) => !/^\d+(?:\.\d+)?$/.test(token));
}

function stripPromotionPriceText(title) {
  return cleanTitleForComparison(String(title || "")
    .replace(/\d+(?:\.\d+)?\s*(?:שח|ש״ח|לקג|לקילו|ליחידה|יחידות|יחידה)/gi, " ")
    .replace(/\d+(?:\.\d+)?/g, " "));
}

function productTokenSet(product) {
  return new Set(tokenize([product.name, product.display_name_en].filter(Boolean).join(" ")));
}

function scoreProductCandidate(product, phrase, tokens) {
  if (!tokens.length) return 0;
  const tokenSet = product._tokenSet;
  const hits = tokens.filter((token) => tokenSet.has(token));
  if (!hits.length) return 0;

  const recall = hits.length / tokens.length;
  const precision = hits.length / Math.max(1, tokenSet.size);
  let score = recall * 62 + precision * 16;
  if (phrase && product._normName.includes(phrase)) score += 28;
  if (phrase && phrase.includes(product._normName) && product._normName.length >= 4) score += 10;
  if (tokens.length >= 2 && tokens.every((token) => tokenSet.has(token))) score += 18;
  if (tokens[0] && tokenSet.has(tokens[0])) score += 4;
  return Math.round(score * 100) / 100;
}

function findSuggestions(promo, products, preferredIds = []) {
  const phrase = stripPromotionPriceText(promo.title);
  const tokens = [...new Set(tokenize(phrase))];
  const preferred = new Set((preferredIds || []).map(Number));
  const scored = products
    .map((product) => ({ product, score: scoreProductCandidate(product, phrase, tokens), preferred: preferred.has(Number(product.id)) }))
    .filter((row) => row.preferred || row.score >= 35)
    .sort((a, b) => {
      if (a.preferred !== b.preferred) return a.preferred ? -1 : 1;
      if (b.score !== a.score) return b.score - a.score;
      return Number(a.product.id) - Number(b.product.id);
    })
    .slice(0, 35)
    .map((row) => ({
      id: Number(row.product.id),
      name: row.product.name,
      price: row.product.price == null ? null : Number(row.product.price),
      stock_amount: row.product.stock_amount == null ? null : Number(row.product.stock_amount),
      category: row.product.category || null,
      sub_category: row.product.sub_category || null,
      barcode: row.product.barcode || null,
      chain_product_key: row.product.chain_product_key || null,
      match_score: row.score,
      from_seed_mapping: row.preferred,
    }));

  const generic = tokens.length <= 1 || tokens.every((token) => GENERIC_GROUP_TOKENS.has(token));
  return { phrase, tokens, generic, candidates: scored };
}

function isExpired(endDate) {
  if (!endDate) return false;
  const date = new Date(`${endDate}T23:59:59`);
  return Number.isFinite(date.getTime()) && date.getTime() < Date.now();
}

function loadJsonFile(filePath, required = false) {
  if (!filePath) return null;
  if (!fs.existsSync(filePath)) {
    if (required) throw new Error(`File not found: ${filePath}`);
    return null;
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function loadMappings(filePath, trust) {
  const parsed = loadJsonFile(filePath, false);
  if (!parsed) return new Map();
  const rows = Array.isArray(parsed.mappings) ? parsed.mappings : [];
  return new Map(rows.map((mapping) => {
    const mappingTrust = trust === "approved_mapping" && mapping.approved === false
      ? "draft_mapping"
      : trust;
    return [
      Number(mapping.reward_id ?? mapping.rewardId),
      { ...mapping, _trust: mappingTrust, _file: filePath },
    ];
  }));
}

function combineMappings(seedMappings, approvedMappings) {
  const result = new Map(seedMappings);
  for (const [rewardId, mapping] of approvedMappings) result.set(rewardId, mapping);
  return result;
}

function mappingProductReferences(mapping) {
  const lookups = Array.isArray(mapping?.product_lookups) ? mapping.product_lookups : [];
  if (lookups.length) return lookups.map((lookup) => ({ ...lookup }));
  const ids = mapping?.product_ids || mapping?.productIds || mapping?.selected_product_ids || [];
  return [...new Set(ids.map(Number).filter((id) => Number.isInteger(id) && id > 0))]
    .map((id) => ({ expected_product_id: id }));
}

function isGroupMapping(mapping) {
  const action = String(mapping?.action || "").toLowerCase();
  const mode = String(mapping?.mapping_mode || mapping?.mappingMode || "").toLowerCase();
  return Boolean(mapping?.is_group_promotion || mapping?.isGroupPromotion || action === "promotion_group" || action === "group" || mode === "group");
}

function parseThresholdFixedPrice(title) {
  const match = String(title || "").match(/מעל\s*(\d+(?:\.\d+)?).*?(?:רק\s*)?ב\s*-?\s*(\d+(?:\.\d+)?)/);
  if (!match) return null;
  return { threshold: roundMoney(match[1]), price: roundMoney(match[2]) };
}

function parseThresholdGift(title) {
  const match = String(title || "").match(/מעל\s*(\d+(?:\.\d+)?).*?(?:מתנה|חינם)/);
  if (!match) return null;
  return { threshold: roundMoney(match[1]) };
}

function parseDeliveryRule(promo) {
  if (promo.type !== "קנה בסכום הוסף קבל") return null;
  const title = String(promo.title || "");
  let match = title.match(/מעל\s*(\d+(?:\.\d+)?).*משלוח\s*(?:ב|עלות)?\s*(\d+(?:\.\d+)?)/);
  if (match) {
    return {
      rule_type: "DELIVERY_FEE_OVERRIDE",
      threshold_amount: roundMoney(match[1]),
      delivery_fee_override: roundMoney(match[2]),
      priority: Number(match[2]) <= 0 ? 10 : 20,
    };
  }
  match = title.match(/מעל\s*(\d+(?:\.\d+)?).*משלוח\s*חינם/);
  if (match) {
    return {
      rule_type: "DELIVERY_FEE_OVERRIDE",
      threshold_amount: roundMoney(match[1]),
      delivery_fee_override: 0,
      priority: 10,
    };
  }
  return null;
}

async function tableExists(conn, tableName) {
  const [rows] = await conn.query(
    `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
    [tableName],
  );
  return Boolean(rows?.length);
}

async function loadProducts(conn, shopId) {
  const [rows] = await conn.query(
    `
    SELECT id, name, display_name_en, price, stock_amount, category, sub_category, barcode, chain_product_key
    FROM product
    WHERE shop_id = ?
    ORDER BY id ASC
    `,
    [shopId],
  );

  const products = (rows || []).map((row) => {
    const product = {
      id: Number(row.id),
      name: row.name,
      display_name_en: row.display_name_en || null,
      price: row.price == null ? null : Number(row.price),
      stock_amount: row.stock_amount == null ? null : Number(row.stock_amount),
      category: row.category || null,
      sub_category: row.sub_category || null,
      barcode: row.barcode || null,
      chain_product_key: row.chain_product_key || null,
    };
    product.product_key = stableProductKey(product);
    product._normName = normalizeText([product.name, product.display_name_en].filter(Boolean).join(" "));
    product._tokenSet = productTokenSet(product);
    return product;
  });

  const byId = new Map(products.map((product) => [product.id, product]));
  const indexes = { chain: new Map(), barcode: new Map(), name: new Map() };
  function add(index, key, product) {
    if (!key) return;
    const values = index.get(key) || [];
    values.push(product);
    index.set(key, values);
  }
  for (const product of products) {
    add(indexes.chain, String(product.chain_product_key || "").trim(), product);
    add(indexes.barcode, String(product.barcode || "").trim(), product);
    add(indexes.name, normalizeText(product.name), product);
  }
  return { products, byId, indexes };
}

function resolveProductReference(reference, productStore) {
  const attempts = [];
  const candidates = [];
  function pushCandidates(method, rows) {
    attempts.push({ method, count: rows.length });
    for (const product of rows) {
      if (!candidates.some((candidate) => candidate.product.id === product.id)) candidates.push({ method, product });
    }
  }

  const chainKey = String(reference?.chain_product_key || "").trim();
  if (chainKey) pushCandidates("chain_product_key", productStore.indexes.chain.get(chainKey) || []);
  const barcode = String(reference?.barcode || "").trim();
  if (barcode) pushCandidates("barcode", productStore.indexes.barcode.get(barcode) || []);
  const name = normalizeText(reference?.name);
  if (name) pushCandidates("name", productStore.indexes.name.get(name) || []);
  const expectedId = Number(reference?.expected_product_id || reference?.id || 0);
  if (Number.isInteger(expectedId) && expectedId > 0 && productStore.byId.has(expectedId)) {
    pushCandidates("legacy_product_id", [productStore.byId.get(expectedId)]);
  }

  const uniqueIds = [...new Set(candidates.map((candidate) => candidate.product.id))];
  if (uniqueIds.length === 1) {
    const product = productStore.byId.get(uniqueIds[0]);
    return { ok: true, product, method: candidates.find((candidate) => candidate.product.id === uniqueIds[0])?.method || null, attempts };
  }
  if (!uniqueIds.length) return { ok: false, reason: "product_not_found", attempts, reference };
  return {
    ok: false,
    reason: "ambiguous_product_lookup",
    attempts,
    reference,
    candidates: uniqueIds.map((id) => ({ id, name: productStore.byId.get(id)?.name || null })),
  };
}

function resolveMappingProducts(mapping, productStore) {
  const references = mappingProductReferences(mapping);
  if (!references.length) return { ok: false, reason: "mapping_has_no_products", products: [], errors: [] };
  const products = [];
  const errors = [];
  const resolution = [];
  for (const reference of references) {
    const result = resolveProductReference(reference, productStore);
    if (!result.ok) errors.push(result);
    else {
      products.push(result.product);
      resolution.push({ product_id: result.product.id, product_name: result.product.name, method: result.method, reference });
    }
  }
  const uniqueProducts = [...new Map(products.map((product) => [product.id, product])).values()];
  return { ok: errors.length === 0 && uniqueProducts.length > 0, products: uniqueProducts, errors, resolution };
}

function commonEntityFields(promo) {
  const titleInfo = stripMarketDayPrefix(promo.title);
  const isMarketDay = Boolean(promo.is_market_day || titleInfo.is_market_day);
  const window = isMarketDay ? marketDayWindow() : null;
  return {
    title: titleInfo.title.slice(0, 255),
    description: isMarketDay ? MARKET_DAY_DESCRIPTION : null,
    is_market_day: isMarketDay,
    max_discounted_qty: roundQty(promo.max_qty),
    start_at: isMarketDay ? window.start_at : startDateTime(promo.start_date),
    end_at: isMarketDay ? window.end_at : endDateTime(promo.end_date),
  };
}

function buildProductEntity(promo, product) {
  const deal = parseDealText(promo.deal_text);
  if (!deal) return { error: "deal_text_not_supported" };
  const fractionalFixedPrice = deal.qty > 0 && deal.qty < 1;
  if (!fractionalFixedPrice && deal.qty !== 1 && !Number.isInteger(deal.qty)) return { error: "non_integer_quantity_not_supported" };
  const common = commonEntityFields(promo);
  const entity = {
    entity_type: "promotion",
    product_id: product.id,
    product_key: product.product_key,
    product_lookup: productLookup(product),
    kind: deal.qty === 1 || fractionalFixedPrice ? "FIXED_PRICE" : "BUNDLE",
    percent_off: null,
    amount_off: null,
    fixed_price: deal.qty === 1 || fractionalFixedPrice ? deal.price : null,
    bundle_buy_qty: deal.qty === 1 || fractionalFixedPrice ? null : Number(deal.qty),
    bundle_pay_price: deal.qty === 1 || fractionalFixedPrice ? null : deal.price,
    ...common,
  };
  entity.source_slot_key = sourceSlotKey(entity);
  entity.identity_hash = identityHash(entity);
  entity.content_hash = contentHash(entity);
  return entity;
}

function buildGroupEntity(promo, products) {
  const deal = parseDealText(promo.deal_text);
  if (!deal) return { error: "deal_text_not_supported" };
  if (!Number.isInteger(deal.qty) || deal.qty < 2) return { error: "group_requires_integer_quantity_of_at_least_2" };
  if (products.length < 2) return { error: "group_requires_at_least_2_products" };
  const sortedProducts = [...products].sort((a, b) => String(a.product_key).localeCompare(String(b.product_key)));
  const common = commonEntityFields(promo);
  const entity = {
    entity_type: "product_group_promotion",
    product_ids: sortedProducts.map((product) => product.id),
    product_keys: sortedProducts.map((product) => product.product_key),
    product_lookups: sortedProducts.map(productLookup),
    kind: "BUNDLE",
    bundle_buy_qty: Number(deal.qty),
    bundle_pay_price: deal.price,
    priority: 100,
    is_active: 1,
    ...common,
    title: cleanProductPromotionTitle(common.title, deal.qty, deal.price).slice(0, 255),
  };
  entity.source_slot_key = sourceSlotKey(entity);
  entity.identity_hash = identityHash(entity);
  entity.content_hash = contentHash(entity);
  return entity;
}

function buildCartEntity(promo, products, source) {
  const delivery = parseDeliveryRule(promo);
  if (delivery) {
    const entity = {
      entity_type: "cart_promotion_rule",
      ...delivery,
      reward_product_id: null,
      reward_product_key: null,
      reward_product_lookup: null,
      gift_text: null,
      reward_qty: null,
      reward_fixed_price: null,
      reward_max_qty: null,
      threshold_base_mode: "ITEMS_SUBTOTAL",
      is_active: 1,
      notify_customer: 1,
      source,
      external_reward_id: String(promo.reward_id),
      ...commonEntityFields(promo),
    };
    entity.source_slot_key = sourceSlotKey(entity);
    entity.identity_hash = identityHash(entity);
    entity.content_hash = contentHash(entity);
    return entity;
  }

  if (!products.length) return { error: "cart_reward_requires_product" };
  if (products.length > 1) return { error: "cart_reward_supports_one_product" };
  const product = products[0];
  const fixed = parseThresholdFixedPrice(promo.title);
  const gift = parseThresholdGift(promo.title);
  if (!fixed && !gift) return { error: "cart_reward_rule_not_parseable" };
  const entity = {
    entity_type: "cart_promotion_rule",
    rule_type: gift ? "GIFT_PRODUCT" : "THRESHOLD_PRODUCT_FIXED_PRICE",
    threshold_amount: gift ? gift.threshold : fixed.threshold,
    delivery_fee_override: null,
    reward_product_id: product.id,
    reward_product_key: product.product_key,
    reward_product_lookup: productLookup(product),
    gift_text: gift ? product.name : null,
    reward_qty: gift ? 1 : null,
    reward_fixed_price: gift ? null : fixed.price,
    reward_max_qty: gift ? 1 : null,
    threshold_base_mode: "EXCLUDING_REWARD_PRODUCTS",
    priority: gift ? 25 : 30,
    is_active: 1,
    notify_customer: 1,
    source,
    external_reward_id: String(promo.reward_id),
    ...commonEntityFields(promo),
  };
  entity.source_slot_key = sourceSlotKey(entity);
  entity.identity_hash = identityHash(entity);
  entity.content_hash = contentHash(entity);
  return entity;
}

function buildEntitiesForPromo(promo, mapping, products, source) {
  if (promo.type === "קנה בסכום הוסף קבל") {
    const entity = buildCartEntity(promo, products, source);
    return entity.error ? { errors: [entity.error], entities: [] } : { errors: [], entities: [entity] };
  }
  if (promo.type !== "כמות בסכום") return { errors: ["unsupported_promotion_type"], entities: [] };

  if (isGroupMapping(mapping)) {
    const group = buildGroupEntity(promo, products);
    if (!group.error) return { errors: [], entities: [group] };
    const deal = parseDealText(promo.deal_text);
    if (deal && (deal.qty === 1 || (deal.qty > 0 && deal.qty < 1))) {
      const entities = products.map((product) => buildProductEntity(promo, product));
      const errors = entities.filter((entity) => entity.error).map((entity) => entity.error);
      return { errors, entities: entities.filter((entity) => !entity.error) };
    }
    return { errors: [group.error], entities: [] };
  }

  const entities = products.map((product) => buildProductEntity(promo, product));
  return {
    errors: entities.filter((entity) => entity.error).map((entity) => entity.error),
    entities: entities.filter((entity) => !entity.error),
  };
}

async function loadExistingEntities(conn, shopId, productStore) {
  const existing = [];
  const [promotions] = await conn.query(`SELECT * FROM promotion WHERE shop_id = ? ORDER BY id ASC`, [shopId]);
  for (const row of promotions || []) {
    const product = productStore.byId.get(Number(row.product_id));
    if (!product) continue;
    const entity = {
      entity_type: "promotion",
      target_id: Number(row.id),
      product_id: product.id,
      product_key: product.product_key,
      product_lookup: productLookup(product),
      kind: row.kind,
      percent_off: row.percent_off == null ? null : Number(row.percent_off),
      amount_off: row.amount_off == null ? null : Number(row.amount_off),
      fixed_price: row.fixed_price == null ? null : Number(row.fixed_price),
      bundle_buy_qty: row.bundle_buy_qty == null ? null : Number(row.bundle_buy_qty),
      bundle_pay_price: row.bundle_pay_price == null ? null : Number(row.bundle_pay_price),
      max_discounted_qty: row.max_discounted_qty == null ? null : Number(row.max_discounted_qty),
      description: row.description || null,
      is_market_day: String(row.description || "").trim() === MARKET_DAY_DESCRIPTION,
      start_at: normalizeDateTime(row.start_at),
      end_at: normalizeDateTime(row.end_at),
      raw: row,
    };
    entity.source_slot_key = sourceSlotKey(entity);
    entity.identity_hash = identityHash(entity);
    entity.content_hash = contentHash(entity);
    existing.push(entity);
  }

  if (await tableExists(conn, "product_group_promotion")) {
    const [groups] = await conn.query(`SELECT * FROM product_group_promotion WHERE shop_id = ? ORDER BY id ASC`, [shopId]);
    const [items] = await conn.query(`SELECT group_promotion_id, product_id FROM product_group_promotion_item WHERE shop_id = ? ORDER BY group_promotion_id, product_id`, [shopId]);
    const byGroup = new Map();
    for (const item of items || []) {
      const values = byGroup.get(Number(item.group_promotion_id)) || [];
      values.push(Number(item.product_id));
      byGroup.set(Number(item.group_promotion_id), values);
    }
    for (const row of groups || []) {
      const products = (byGroup.get(Number(row.id)) || []).map((id) => productStore.byId.get(id)).filter(Boolean);
      if (!products.length) continue;
      const sortedProducts = [...products].sort((a, b) => String(a.product_key).localeCompare(String(b.product_key)));
      const entity = {
        entity_type: "product_group_promotion",
        target_id: Number(row.id),
        title: row.title,
        product_ids: sortedProducts.map((product) => product.id),
        product_keys: sortedProducts.map((product) => product.product_key),
        product_lookups: sortedProducts.map(productLookup),
        kind: "BUNDLE",
        bundle_buy_qty: Number(row.bundle_buy_qty),
        bundle_pay_price: Number(row.bundle_pay_price),
        max_discounted_qty: row.max_discounted_qty == null ? null : Number(row.max_discounted_qty),
        description: row.description || null,
        is_market_day: String(row.description || "").trim() === MARKET_DAY_DESCRIPTION,
        priority: Number(row.priority || 100),
        is_active: Number(row.is_active || 0),
        start_at: normalizeDateTime(row.start_at),
        end_at: normalizeDateTime(row.end_at),
        raw: row,
      };
      entity.source_slot_key = sourceSlotKey(entity);
      entity.identity_hash = identityHash(entity);
      entity.content_hash = contentHash(entity);
      existing.push(entity);
    }
  }

  if (await tableExists(conn, "cart_promotion_rule")) {
    const [rules] = await conn.query(`SELECT * FROM cart_promotion_rule WHERE shop_id = ? ORDER BY id ASC`, [shopId]);
    for (const row of rules || []) {
      const rewardProduct = row.reward_product_id ? productStore.byId.get(Number(row.reward_product_id)) : null;
      const entity = {
        entity_type: "cart_promotion_rule",
        target_id: Number(row.id),
        title: row.title,
        description: row.description || null,
        is_market_day: String(row.description || "").trim() === MARKET_DAY_DESCRIPTION,
        rule_type: row.rule_type,
        threshold_amount: Number(row.threshold_amount || 0),
        delivery_fee_override: row.delivery_fee_override == null ? null : Number(row.delivery_fee_override),
        reward_product_id: rewardProduct?.id || null,
        reward_product_key: rewardProduct?.product_key || null,
        reward_product_lookup: rewardProduct ? productLookup(rewardProduct) : null,
        gift_text: row.gift_text || null,
        reward_qty: row.reward_qty == null ? null : Number(row.reward_qty),
        reward_fixed_price: row.reward_fixed_price == null ? null : Number(row.reward_fixed_price),
        reward_max_qty: row.reward_max_qty == null ? null : Number(row.reward_max_qty),
        threshold_base_mode: row.threshold_base_mode,
        priority: Number(row.priority || 100),
        is_active: Number(row.is_active || 0),
        notify_customer: Number(row.notify_customer ?? 1),
        source: row.source || null,
        external_reward_id: row.external_reward_id || null,
        start_at: normalizeDateTime(row.start_at),
        end_at: normalizeDateTime(row.end_at),
        raw: row,
      };
      entity.source_slot_key = sourceSlotKey(entity);
      entity.identity_hash = identityHash(entity);
      entity.content_hash = contentHash(entity);
      existing.push(entity);
    }
  }

  const byIdentity = new Map();
  for (const entity of existing) {
    const values = byIdentity.get(entity.identity_hash) || [];
    values.push(entity);
    byIdentity.set(entity.identity_hash, values);
  }
  return { list: existing, byIdentity };
}

async function loadSourceLinks(conn, shopId, source) {
  if (!(await tableExists(conn, "promotion_source_link"))) return [];
  const [rows] = await conn.query(
    `SELECT * FROM promotion_source_link WHERE shop_id = ? AND source = ? ORDER BY id ASC`,
    [shopId, source],
  );
  return rows || [];
}

function targetTypeForEntity(entityType) {
  if (entityType === "promotion") return "promotion";
  if (entityType === "product_group_promotion") return "product_group_promotion";
  if (entityType === "cart_promotion_rule") return "cart_promotion_rule";
  return null;
}

function sameMaxValue(sourceEntity, existingEntity) {
  if (sourceEntity.entity_type === "cart_promotion_rule") {
    return sameNullableNumber(sourceEntity.reward_max_qty, existingEntity.reward_max_qty);
  }
  return sameNullableNumber(sourceEntity.max_discounted_qty, existingEntity.max_discounted_qty);
}

function overlappingConflicts(entity, existingStore) {
  return existingStore.list.filter((existing) => {
    if (existing.entity_type !== entity.entity_type) return false;
    if (!intervalsOverlap(entity.start_at, entity.end_at, existing.start_at, existing.end_at)) return false;
    if (existing.identity_hash === entity.identity_hash) return false;
    if (entity.entity_type === "promotion") return existing.product_key === entity.product_key;
    if (entity.entity_type === "product_group_promotion") {
      return JSON.stringify(existing.product_keys || []) === JSON.stringify(entity.product_keys || []);
    }
    if (entity.entity_type === "cart_promotion_rule") {
      if (existing.rule_type !== entity.rule_type) return false;
      if (!sameNullableNumber(existing.threshold_amount, entity.threshold_amount, 2)) return false;
      if (entity.rule_type === "DELIVERY_FEE_OVERRIDE") return true;
      return existing.reward_product_key === entity.reward_product_key;
    }
    return false;
  });
}

function planEntity(entity, existingStore, linkedTargetId = null) {
  const sameIdentity = existingStore.byIdentity.get(entity.identity_hash) || [];
  const identityCandidates = isMarketDayEntity(entity)
    ? sameIdentity
    : sameIdentity.filter((existing) => intervalsOverlap(
      entity.start_at,
      entity.end_at,
      existing.start_at,
      existing.end_at,
    ));

  if (linkedTargetId) {
    const linked = existingStore.list.find((existing) => existing.target_id === Number(linkedTargetId) && existing.entity_type === entity.entity_type);
    if (!linked) return { status: "BLOCKED", action: "NONE", reason: "source_link_target_missing" };
    if (linked.identity_hash !== entity.identity_hash) {
      return { status: "BLOCKED", action: "NONE", reason: "source_link_terms_changed", existing: summarizeExisting(linked) };
    }
  }

  if (identityCandidates.length > 1) {
    return {
      status: "BLOCKED",
      action: "NONE",
      reason: "multiple_existing_promotions_with_same_identity",
      existing_candidates: identityCandidates.map(summarizeExisting),
    };
  }

  if (identityCandidates.length === 1) {
    const existing = identityCandidates[0];
    if (linkedTargetId && Number(linkedTargetId) !== existing.target_id) {
      return {
        status: "BLOCKED",
        action: "NONE",
        reason: "source_link_points_to_different_existing_target",
        existing: summarizeExisting(existing),
      };
    }
    if (!sameMaxValue(entity, existing)) {
      return {
        status: "BLOCKED",
        action: "NONE",
        reason: "same_identity_but_max_usage_differs",
        existing: summarizeExisting(existing),
      };
    }

    if (isMarketDayEntity(entity)) {
      const sameWindow = normalizeDateTime(existing.start_at) === normalizeDateTime(entity.start_at)
        && normalizeDateTime(existing.end_at) === normalizeDateTime(entity.end_at);
      if (sameWindow) {
        return {
          status: "READY",
          action: "LINK_EXISTING",
          reason: existing.content_hash === entity.content_hash ? "exact_existing" : "market_day_window_already_current",
          target_id: existing.target_id,
          existing: summarizeExisting(existing),
        };
      }
      return {
        status: "READY",
        action: "REFRESH_MARKET_DAY_DATES",
        reason: "same_market_day_promotion_refreshes_tuesday_window",
        target_id: existing.target_id,
        existing: summarizeExisting(existing),
        update: { start_at: entity.start_at, end_at: entity.end_at, is_active: 1 },
      };
    }

    if (intervalContains(existing.start_at, existing.end_at, entity.start_at, entity.end_at)) {
      return {
        status: "READY",
        action: "LINK_EXISTING",
        reason: existing.content_hash === entity.content_hash ? "exact_existing" : "existing_period_contains_source_period",
        target_id: existing.target_id,
        existing: summarizeExisting(existing),
      };
    }

    const mergedStart = earlierDate(existing.start_at, entity.start_at);
    const mergedEnd = laterDate(existing.end_at, entity.end_at);
    return {
      status: "READY",
      action: "EXTEND_EXISTING_DATES",
      reason: "same_identity_source_extends_existing_period",
      target_id: existing.target_id,
      existing: summarizeExisting(existing),
      update: { start_at: mergedStart, end_at: mergedEnd, is_active: 1 },
    };
  }

  const conflicts = overlappingConflicts(entity, existingStore);
  if (conflicts.length) {
    return {
      status: "BLOCKED",
      action: "NONE",
      reason: "overlapping_different_promotion_for_same_target",
      existing_candidates: conflicts.map(summarizeExisting),
    };
  }

  return { status: "READY", action: "INSERT", reason: "new_promotion" };
}

function earlierDate(a, b) {
  if (!a) return a;
  if (!b) return b;
  return String(a) <= String(b) ? a : b;
}

function laterDate(a, b) {
  if (!a || !b) return null;
  return String(a) >= String(b) ? a : b;
}

function summarizeExisting(existing) {
  return {
    target_id: existing.target_id,
    entity_type: existing.entity_type,
    title: existing.title || existing.raw?.description || null,
    description: existing.description || null,
    is_market_day: isMarketDayEntity(existing),
    product_id: existing.product_id || null,
    product_ids: existing.product_ids || null,
    identity_hash: existing.identity_hash,
    content_hash: existing.content_hash,
    start_at: existing.start_at,
    end_at: existing.end_at,
    is_active: existing.is_active ?? null,
  };
}

function mappingFromSourceLinks(promo, links, existingStore, productStore) {
  const rewardLinks = links.filter((link) => String(link.external_reward_id) === String(promo.reward_id));
  if (!rewardLinks.length) return null;
  const products = [];
  let group = false;
  let action = promo.type === "קנה בסכום הוסף קבל" ? "cart_reward_product" : "product_promotion";
  for (const link of rewardLinks) {
    const existing = existingStore.list.find((entity) => entity.entity_type === link.target_type && entity.target_id === Number(link.target_id));
    if (!existing) continue;
    if (existing.entity_type === "promotion") {
      const product = productStore.byId.get(existing.product_id);
      if (product) products.push(product);
    } else if (existing.entity_type === "product_group_promotion") {
      group = true;
      action = "promotion_group";
      for (const productId of existing.product_ids || []) {
        const product = productStore.byId.get(productId);
        if (product) products.push(product);
      }
    } else if (existing.entity_type === "cart_promotion_rule" && existing.reward_product_id) {
      const product = productStore.byId.get(existing.reward_product_id);
      if (product) products.push(product);
    }
  }
  const uniqueProducts = [...new Map(products.map((product) => [product.id, product])).values()];
  return {
    reward_id: Number(promo.reward_id),
    action,
    mapping_mode: group ? "group" : uniqueProducts.length > 1 ? "separate_product_promotions" : "single_product",
    is_group_promotion: group,
    product_lookups: uniqueProducts.map(productLookup),
    _trust: "source_link",
    _source_links: rewardLinks,
  };
}

function sourceEntitiesShareTarget(left, right) {
  if (!left || !right || left.entity_type !== right.entity_type) return false;

  if (left.entity_type === "promotion") {
    return Boolean(left.product_key) && left.product_key === right.product_key;
  }

  if (left.entity_type === "product_group_promotion") {
    return JSON.stringify(left.product_keys || []) === JSON.stringify(right.product_keys || []);
  }

  if (left.entity_type === "cart_promotion_rule") {
    if (left.rule_type !== right.rule_type) return false;
    if (!sameNullableNumber(left.threshold_amount, right.threshold_amount, 2)) return false;
    if (left.rule_type === "DELIVERY_FEE_OVERRIDE") return true;
    return left.reward_product_key === right.reward_product_key;
  }

  return false;
}

function sourceCollisionGroups(rows) {
  const actions = [];
  for (const row of rows) {
    for (const action of row.actions || []) {
      if (action.entity && action.entity.identity_hash) actions.push({ row, action });
    }
  }
  const collisions = [];
  for (let i = 0; i < actions.length; i += 1) {
    for (let j = i + 1; j < actions.length; j += 1) {
      const a = actions[i];
      const b = actions[j];
      if (a.row.reward_id === b.row.reward_id) continue;
      if (!intervalsOverlap(
        a.action.entity.start_at,
        a.action.entity.end_at,
        b.action.entity.start_at,
        b.action.entity.end_at,
      )) continue;

      const sameIdentity = a.action.entity.identity_hash === b.action.entity.identity_hash;
      const sameTarget = sourceEntitiesShareTarget(a.action.entity, b.action.entity);
      if (!sameIdentity && !sameTarget) continue;

      collisions.push([a, b]);
    }
  }
  return collisions;
}

function reasonForReview(row) {
  if (row.mapping_trust === "seed_mapping") return "seed_mapping_requires_approval";
  if (row.mapping_trust === "draft_mapping") return "draft_mapping_requires_approval";
  if (row.reason) return row.reason;
  return "manual_mapping_required";
}

function reviewItem(row, promo, suggestions, preferredIds = []) {
  return {
    reward_id: promo.reward_id,
    title: promo.title,
    original_title: promo.original_title || null,
    is_market_day: Boolean(promo.is_market_day),
    type: promo.type,
    deal_text: promo.deal_text,
    start_date: promo.start_date,
    end_date: promo.end_date,
    max_qty: promo.max_qty ?? null,
    reason: reasonForReview(row),
    search_phrase: suggestions.phrase,
    tokens: suggestions.tokens,
    candidates_count: suggestions.candidates.length,
    candidates: suggestions.candidates,
    preferred_product_ids: preferredIds,
    mapping_trust: row.mapping_trust || null,
    plan_status: row.status,
    plan_reason: row.reason || null,
  };
}

function summarizeEntity(entity) {
  return {
    entity_type: entity.entity_type,
    source_slot_key: entity.source_slot_key,
    identity_hash: entity.identity_hash,
    content_hash: entity.content_hash,
    product_id: entity.product_id || null,
    product_ids: entity.product_ids || null,
    product_key: entity.product_key || null,
    product_keys: entity.product_keys || null,
    product_lookup: entity.product_lookup || null,
    product_lookups: entity.product_lookups || null,
    reward_product_key: entity.reward_product_key || null,
    reward_product_lookup: entity.reward_product_lookup || null,
    kind: entity.kind || null,
    percent_off: entity.percent_off ?? null,
    amount_off: entity.amount_off ?? null,
    rule_type: entity.rule_type || null,
    fixed_price: entity.fixed_price ?? null,
    bundle_buy_qty: entity.bundle_buy_qty ?? null,
    bundle_pay_price: entity.bundle_pay_price ?? null,
    threshold_amount: entity.threshold_amount ?? null,
    delivery_fee_override: entity.delivery_fee_override ?? null,
    reward_qty: entity.reward_qty ?? null,
    reward_fixed_price: entity.reward_fixed_price ?? null,
    reward_max_qty: entity.reward_max_qty ?? null,
    max_discounted_qty: entity.max_discounted_qty ?? null,
    title: entity.title || null,
    description: entity.description || null,
    is_market_day: Boolean(entity.is_market_day),
    priority: entity.priority ?? null,
    is_active: entity.is_active ?? null,
    notify_customer: entity.notify_customer ?? null,
    threshold_base_mode: entity.threshold_base_mode || null,
    source: entity.source || null,
    external_reward_id: entity.external_reward_id || null,
    start_at: entity.start_at,
    end_at: entity.end_at,
  };
}

async function main() {
  if (!Number.isInteger(SHOP_ID) || SHOP_ID <= 0) throw new Error(`Invalid --shopId: ${SHOP_ID}`);
  if (!fs.existsSync(DATA_FILE)) throw new Error(`Data file not found: ${DATA_FILE}`);

  const sourceRows = loadJsonFile(DATA_FILE, true);
  if (!Array.isArray(sourceRows)) throw new Error("Promotion data file must contain an array");
  const seedMappings = loadMappings(SEED_MAPPING_FILE, "seed_mapping");
  const approvedMappings = loadMappings(APPROVED_MAPPING_FILE, "approved_mapping");
  const combinedMappings = combineMappings(seedMappings, approvedMappings);
  const dataBuffer = fs.readFileSync(DATA_FILE);

  const conn = await db.getConnection();
  try {
    const productStore = await loadProducts(conn, SHOP_ID);
    const existingStore = await loadExistingEntities(conn, SHOP_ID, productStore);
    const sourceLinks = await loadSourceLinks(conn, SHOP_ID, SOURCE);
    const rows = [];
    const review = [];

    for (const promo of sourceRows) {
      const row = {
        reward_id: Number(promo.reward_id),
        title: promo.title,
        type: promo.type,
        status: null,
        reason: null,
        mapping_trust: null,
        mapping_file: null,
        actions: [],
        source: promo,
      };

      if (String(promo.active || "").trim() !== "כן") {
        row.status = "SKIPPED";
        row.reason = "inactive_in_source";
        rows.push(row);
        continue;
      }
      if (!INCLUDE_EXPIRED && isExpired(promo.end_date)) {
        row.status = "SKIPPED";
        row.reason = "expired_in_source";
        rows.push(row);
        continue;
      }

      let mapping = mappingFromSourceLinks(promo, sourceLinks, existingStore, productStore);
      if (!mapping) mapping = combinedMappings.get(Number(promo.reward_id)) || null;

      const delivery = parseDeliveryRule(promo);
      let products = [];
      let resolution = null;
      if (delivery && !mapping) {
        mapping = { reward_id: promo.reward_id, action: "cart_delivery", product_lookups: [], _trust: "parsed_delivery_rule" };
      }

      if (mapping) {
        row.mapping_trust = mapping._trust || "unknown";
        row.mapping_file = mapping._file || null;
        resolution = resolveMappingProducts(mapping, productStore);
        if (mappingProductReferences(mapping).length && !resolution.ok) {
          row.status = "REVIEW_REQUIRED";
          row.reason = resolution.reason || "product_mapping_resolution_failed";
          row.mapping_errors = resolution.errors;
          const preferredIds = mappingProductReferences(mapping).map((ref) => Number(ref.expected_product_id || ref.id || 0)).filter(Boolean);
          const suggestions = findSuggestions(promo, productStore.products, preferredIds);
          review.push(reviewItem(row, promo, suggestions, preferredIds));
          rows.push(row);
          continue;
        }
        products = resolution?.products || [];
      }

      if (!mapping) {
        row.status = "REVIEW_REQUIRED";
        row.reason = promo.type === "קנה בסכום הוסף קבל" ? "cart_reward_product_mapping_required" : "manual_mapping_required";
        const suggestions = findSuggestions(promo, productStore.products);
        review.push(reviewItem(row, promo, suggestions));
        rows.push(row);
        continue;
      }

      const built = buildEntitiesForPromo(promo, mapping, products, SOURCE);
      if (built.errors.length || !built.entities.length) {
        row.status = "REVIEW_REQUIRED";
        row.reason = built.errors[0] || "could_not_build_promotion";
        const preferredIds = products.map((product) => product.id);
        const suggestions = findSuggestions(promo, productStore.products, preferredIds);
        review.push(reviewItem(row, promo, suggestions, preferredIds));
        rows.push(row);
        continue;
      }

      for (const entity of built.entities) {
        const linked = (mapping._source_links || []).find((link) => link.source_slot_key === entity.source_slot_key)
          || (mapping._source_links || []).find((link) => link.target_type === entity.entity_type);
        const planned = planEntity(entity, existingStore, linked?.target_id || null);
        row.actions.push({
          ...planned,
          entity: summarizeEntity(entity),
          target_type: targetTypeForEntity(entity.entity_type),
          source_slot_key: entity.source_slot_key,
          identity_hash: entity.identity_hash,
          content_hash: entity.content_hash,
        });
      }

      const blockedAction = row.actions.find((action) => action.status === "BLOCKED");
      const writeAction = row.actions.find((action) => ["INSERT", "EXTEND_EXISTING_DATES", "REFRESH_MARKET_DAY_DATES"].includes(action.action));
      const untrusted = ["seed_mapping", "draft_mapping"].includes(row.mapping_trust);
      if (blockedAction) {
        row.status = "BLOCKED";
        row.reason = blockedAction.reason;
      } else if (untrusted && writeAction) {
        row.status = "REVIEW_REQUIRED";
        row.reason = row.mapping_trust === "draft_mapping"
          ? "draft_mapping_requires_approval"
          : "seed_mapping_requires_approval";
      } else if (untrusted && row.actions.every((action) => action.action === "LINK_EXISTING")) {
        row.status = "EXISTING_UNCHANGED";
        row.reason = "existing_match_from_unapproved_seed_mapping";
        for (const action of row.actions) action.record_source_link = false;
      } else {
        row.status = "READY";
        row.reason = row.actions.every((action) => action.action === "LINK_EXISTING") ? "already_exists" : "safe_to_apply";
        for (const action of row.actions) action.record_source_link = true;
      }

      if (["REVIEW_REQUIRED", "BLOCKED"].includes(row.status)) {
        const preferredIds = products.map((product) => product.id);
        const suggestions = findSuggestions(promo, productStore.products, preferredIds);
        review.push(reviewItem(row, promo, suggestions, preferredIds));
      }
      rows.push(row);
    }

    const collisions = sourceCollisionGroups(rows);
    for (const [left, right] of collisions) {
      for (const item of [left, right]) {
        item.row.status = "BLOCKED";
        item.row.reason = "multiple_source_promotions_resolve_to_same_target_and_overlap";
        for (const action of item.row.actions) {
          if (action.entity?.identity_hash === item.action.entity.identity_hash) {
            action.status = "BLOCKED";
            action.action = "NONE";
            action.reason = "source_mapping_collision";
          }
        }
        if (!review.some((entry) => Number(entry.reward_id) === Number(item.row.reward_id))) {
          const promo = item.row.source;
          const preferredIds = (item.row.actions || []).flatMap((action) => [action.entity?.product_id, ...(action.entity?.product_ids || [])]).filter(Boolean);
          review.push(reviewItem(item.row, promo, findSuggestions(promo, productStore.products, preferredIds), preferredIds));
        }
      }
    }

    const summary = {
      total_source_rows: rows.length,
      ready_rows: rows.filter((row) => row.status === "READY").length,
      existing_unchanged_rows: rows.filter((row) => row.status === "EXISTING_UNCHANGED").length,
      review_required_rows: rows.filter((row) => row.status === "REVIEW_REQUIRED").length,
      blocked_rows: rows.filter((row) => row.status === "BLOCKED").length,
      skipped_rows: rows.filter((row) => row.status === "SKIPPED").length,
      ready_actions: rows.flatMap((row) => row.status === "READY" ? row.actions : []).filter((action) => action.action !== "NONE").length,
      insert_actions: rows.flatMap((row) => row.status === "READY" ? row.actions : []).filter((action) => action.action === "INSERT").length,
      link_existing_actions: rows.flatMap((row) => row.status === "READY" ? row.actions : []).filter((action) => action.action === "LINK_EXISTING").length,
      extend_existing_actions: rows.flatMap((row) => row.status === "READY" ? row.actions : []).filter((action) => action.action === "EXTEND_EXISTING_DATES").length,
      refresh_market_day_actions: rows.flatMap((row) => row.status === "READY" ? row.actions : []).filter((action) => action.action === "REFRESH_MARKET_DAY_DATES").length,
    };

    const plan = {
      schema_version: 1,
      metadata: {
        generated_at: new Date().toISOString(),
        shop_id: SHOP_ID,
        source: SOURCE,
        data_file: DATA_FILE,
        original_filename: path.basename(DATA_FILE),
        file_sha256: sha256(dataBuffer),
        seed_mapping_file: SEED_MAPPING_FILE,
        approved_mapping_file: APPROVED_MAPPING_FILE,
        include_expired: INCLUDE_EXPIRED,
        products_in_shop: productStore.products.length,
        existing_entities: existingStore.list.length,
        existing_source_links: sourceLinks.length,
      },
      summary,
      rows,
    };
    plan.plan_sha256 = sha256(plan);

    const reviewReport = {
      mode: "safe_import_review",
      shop_id: SHOP_ID,
      source: SOURCE,
      data_file: DATA_FILE,
      generated_at: plan.metadata.generated_at,
      plan_file: OUT_FILE,
      plan_sha256: plan.plan_sha256,
      excel_promotions_total: rows.length,
      safe_product_promotions: [],
      safe_cart_rules: [],
      skipped: review,
      summary,
    };

    fs.mkdirSync(path.dirname(OUT_FILE), { recursive: true });
    fs.writeFileSync(OUT_FILE, JSON.stringify(plan, null, 2), "utf8");
    fs.mkdirSync(path.dirname(REVIEW_OUT_FILE), { recursive: true });
    fs.writeFileSync(REVIEW_OUT_FILE, JSON.stringify(reviewReport, null, 2), "utf8");

    console.log(JSON.stringify({
      ...summary,
      shop_id: SHOP_ID,
      source: SOURCE,
      data_file: DATA_FILE,
      plan_file: OUT_FILE,
      review_file: REVIEW_OUT_FILE,
      plan_sha256: plan.plan_sha256,
    }, null, 2));
    if (summary.review_required_rows || summary.blocked_rows) {
      console.log("\nNo DB changes were made. Review the review_file before applying the plan.");
    }
  } finally {
    conn.release();
  }
}

module.exports = {
  buildCartEntity,
  buildEntitiesForPromo,
  buildGroupEntity,
  buildProductEntity,
  loadExistingEntities,
  loadProducts,
  loadSourceLinks,
  overlappingConflicts,
  planEntity,
  resolveMappingProducts,
  resolveProductReference,
  sourceCollisionGroups,
  sourceEntitiesShareTarget,
  summarizeExisting,
  tableExists,
};

if (require.main === module) {
  main()
    .catch((error) => {
      console.error("[plan-leshem-promotions-safe]", error);
      process.exitCode = 1;
    })
    .finally(async () => {
      await db.end();
    });
}

require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");
const { ensureProductGroupPromotionColumns } = require("../services/productGroupPromotions");

const DEFAULT_SOURCE = "leshem_excel_2026_06_14";
const DEFAULT_DATA_FILE = path.join(__dirname, "..", "data", "leshem_promotions_2026_06_14.json");
const REPORTS_DIR = path.join(__dirname, "..", "reports");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

const SHOP_ID = Number(argValue("--shopId", process.env.PROMO_IMPORT_SHOP_ID || 2));
const CONFIRM = Boolean(argValue("--confirm", false));
const DRY_RUN = !CONFIRM || Boolean(argValue("--dryRun", false));
const INCLUDE_EXPIRED = Boolean(argValue("--includeExpired", false));
const DATA_FILE = path.resolve(argValue("--data", process.env.PROMO_IMPORT_DATA_FILE || DEFAULT_DATA_FILE));
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

function normalizeText(value) {
  let s = String(value || "").toLowerCase();
  s = s.normalize("NFKD").replace(/[\u0591-\u05C7]/g, "");
  const finalLetters = { ך: "כ", ם: "מ", ן: "נ", ף: "פ", ץ: "צ" };
  s = s.replace(/[ךםןףץ]/g, (ch) => finalLetters[ch] || ch);
  for (const ch of ["״", "”", "“", "׳", "’", "‘", "'", '"', "`", "´"]) s = s.split(ch).join("");
  for (const ch of ["־", "–", "—", "-", "&", "+", "₪", "%", ".", ",", ":", ";", "(", ")", "[", "]", "{", "}", "/", "\\", "|"]) {
    s = s.split(ch).join(" ");
  }
  return s.replace(/\s+/g, " ").trim();
}

const STOP_WORDS = new Set([
  "ב",
  "של",
  "עם",
  "על",
  "מעל",
  "ללא",
  "לא",
  "עלות",
  "בקנייה",
  "בקניה",
  "חינם",
  "שח",
  "שחח",
  "ש״ח",
  "רק",
  "יח",
  "יחידות",
  "גרם",
  "קג",
  "לקג",
  "קילו",
  "מל",
  "ליטר",
  "לי",
  "מגוון",
  "מוצרי",
  "מארז",
  "גדול",
  "קטן",
]);

const GENERIC_GROUP_TOKENS = new Set([
  "חטיפים",
  "יינות",
  "ירק",
  "ירקות",
  "פירות",
  "צלחות",
  "כוס",
  "כוסות",
  "שקיות",
  "תבניות",
  "עוגיות",
  "תבליני",
  "מוצרי",
  "מגוון",
]);

function stripPromotionPriceText(title) {
  let s = String(title || "").toLowerCase().replace(/₪/g, " ").replace(/&/g, " ");
  for (const ch of ["״", "”", "“", "׳", "’", "‘", "'", '"']) s = s.split(ch).join("");

  s = s.replace(/בקני(?:י|)ה\s+מעל\s*\d+(?:\.\d+)?/g, " ");
  s = s.replace(/\d+(?:\.\d+)?\s*ב\s*-?\s*\d+(?:\.\d+)?/g, " ");
  s = s.replace(/רק\s*ב\s*-?\s*\d+(?:\.\d+)?/g, " ");
  s = s.replace(/\bב\s*-?\s*\d+(?:\.\d+)?/g, " ");
  s = s.replace(/\d+(?:\.\d+)?\s*(?:שח|ש״ח)/g, " ");
  s = s.replace(/(?:ללא עלות|עלות משלוח|משלוח חינם|משלוח)/g, " ");
  return normalizeText(s);
}

function tokenize(value) {
  return normalizeText(value)
    .split(" ")
    .map((t) => t.trim())
    .filter(Boolean)
    .filter((t) => !STOP_WORDS.has(t))
    .filter((t) => !/^\d+(?:\.\d+)?$/.test(t));
}

function uniq(items) {
  return [...new Set(items)];
}

function parseDealText(value) {
  const m = String(value || "").match(/(\d+(?:\.\d+)?)\s*ב\s*(\d+(?:\.\d+)?)/);
  if (!m) return null;
  const qty = Number(m[1]);
  const price = Number(m[2]);
  if (!Number.isFinite(qty) || !Number.isFinite(price) || qty <= 0 || price < 0) return null;
  return { qty, price };
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

function productTokenSet(product) {
  return new Set(tokenize([product.name, product.display_name_en].filter(Boolean).join(" ")));
}

function productContainsAllTokens(product, tokens) {
  const set = product._tokenSet;
  return tokens.every((token) => set.has(token));
}

function scoreProductCandidate(product, phrase, tokens) {
  if (!tokens.length) return 0;

  const tokenSet = product._tokenSet || new Set();
  const hits = tokens.filter((token) => tokenSet.has(token));
  if (!hits.length) return 0;

  const recall = hits.length / tokens.length;
  const precision = hits.length / Math.max(1, tokenSet.size);
  let score = recall * 62 + precision * 16;

  if (phrase && product._normName.includes(phrase)) score += 28;
  if (phrase && phrase.includes(product._normName) && product._normName.length >= 4) score += 10;
  if (tokens.length >= 2 && productContainsAllTokens(product, tokens)) score += 18;
  if (tokens[0] && tokenSet.has(tokens[0])) score += 4;
  if (String(product.name || "").trim() === String(phrase || "").trim()) score += 20;

  return Math.round(score * 100) / 100;
}

function isGenericSingleToken(tokens) {
  return tokens.length <= 1 || tokens.every((token) => GENERIC_GROUP_TOKENS.has(token));
}

function findCandidates(promo, products) {
  const phrase = stripPromotionPriceText(promo.title);
  const tokens = uniq(tokenize(phrase));

  if (!tokens.length) {
    return { phrase, tokens, candidates: [], reason: "too_few_specific_tokens" };
  }

  const scored = products
    .map((product) => ({ product, score: scoreProductCandidate(product, phrase, tokens) }))
    .filter((row) => row.score >= 42)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const aStock = Number(a.product.stock_amount || 0);
      const bStock = Number(b.product.stock_amount || 0);
      if (bStock !== aStock) return bStock - aStock;
      return Number(a.product.id) - Number(b.product.id);
    });

  const allTokenMatches = products.filter((product) => productContainsAllTokens(product, tokens));
  const best = scored[0];
  const second = scored[1];
  const generic = isGenericSingleToken(tokens);
  const safeByAllTokens = !generic && allTokenMatches.length === 1 && scoreProductCandidate(allTokenMatches[0], phrase, tokens) >= 68;
  const safeByScore = !generic && best && best.score >= 82 && (!second || best.score - second.score >= 9);

  if (safeByAllTokens) {
    const product = allTokenMatches[0];
    return {
      phrase,
      tokens,
      candidates: [{ ...product, match_score: scoreProductCandidate(product, phrase, tokens) }],
      reason: "unique_fuzzy_token_match",
    };
  }

  if (safeByScore) {
    return {
      phrase,
      tokens,
      candidates: [{ ...best.product, match_score: best.score }],
      reason: "unique_fuzzy_score_match",
    };
  }

  return {
    phrase,
    tokens,
    candidates: scored.slice(0, 35).map((row) => ({ ...row.product, match_score: row.score })),
    reason: scored.length ? (generic ? "generic_name_requires_manual_mapping" : "multiple_fuzzy_matches") : "no_match",
  };
}

function buildProductPromotionPayload(promo, product, matchInfo) {
  const deal = parseDealText(promo.deal_text);
  if (!deal) return { skip_reason: "deal_text_not_supported" };

  if (deal.qty < 1) return { skip_reason: "fractional_bundle_qty_requires_manual_mapping" };
  if (!Number.isInteger(deal.qty)) return { skip_reason: "non_integer_bundle_qty_requires_manual_mapping" };

  const maxQty = promo.max_qty === null || promo.max_qty === undefined || promo.max_qty === ""
    ? null
    : Number(promo.max_qty);
  if (maxQty !== null && (!Number.isFinite(maxQty) || maxQty <= 0)) {
    return { skip_reason: "invalid_max_qty" };
  }

  const description = [
    promo.title,
    `מקור אקסל ${SOURCE}`,
    `תגמול ${promo.reward_id}`,
    `התאמה: ${matchInfo.reason}`,
  ].join(" | ").slice(0, 255);

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

function cartRuleFromPromo(promo) {
  const title = String(promo.title || "");
  if (promo.type !== "קנה בסכום הוסף קבל") return null;
  if (promo.active !== "כן") return null;
  if (!INCLUDE_EXPIRED && isExpired(promo.end_date)) return null;

  let match = title.match(/מעל\s*(\d+(?:\.\d+)?).*משלוח\s*(?:ב|עלות)?\s*(\d+(?:\.\d+)?)/);
  if (match) {
    const threshold = Number(match[1]);
    const fee = Number(match[2]);
    return {
      rule_type: "DELIVERY_FEE_OVERRIDE",
      title,
      description: `מקור אקסל ${SOURCE} | תגמול ${promo.reward_id}`,
      threshold_amount: threshold,
      delivery_fee_override: fee,
      reward_product_id: null,
      reward_qty: null,
      reward_fixed_price: null,
      reward_max_qty: null,
      threshold_base_mode: "ITEMS_SUBTOTAL",
      priority: fee <= 0 ? 10 : 20,
      source: SOURCE,
      external_reward_id: String(promo.reward_id),
      start_at: startDateTime(promo.start_date),
      end_at: endDateTime(promo.end_date),
    };
  }

  match = title.match(/מעל\s*(\d+(?:\.\d+)?).*משלוח\s*חינם/);
  if (match) {
    const threshold = Number(match[1]);
    return {
      rule_type: "DELIVERY_FEE_OVERRIDE",
      title,
      description: `מקור אקסל ${SOURCE} | תגמול ${promo.reward_id}`,
      threshold_amount: threshold,
      delivery_fee_override: 0,
      reward_product_id: null,
      reward_qty: null,
      reward_fixed_price: null,
      reward_max_qty: null,
      threshold_base_mode: "ITEMS_SUBTOTAL",
      priority: 10,
      source: SOURCE,
      external_reward_id: String(promo.reward_id),
      start_at: startDateTime(promo.start_date),
      end_at: endDateTime(promo.end_date),
    };
  }

  return null;
}

function overlaps(a, b) {
  const aStart = new Date(String(a.start_at || "1970-01-01 00:00:00").replace(" ", "T"));
  const bStart = new Date(String(b.start_at || "1970-01-01 00:00:00").replace(" ", "T"));
  const aEnd = a.end_at ? new Date(String(a.end_at).replace(" ", "T")) : new Date("2999-12-31T23:59:59");
  const bEnd = b.end_at ? new Date(String(b.end_at).replace(" ", "T")) : new Date("2999-12-31T23:59:59");
  return aStart <= bEnd && bStart <= aEnd;
}

function dedupeConflictingProductPromotions(safePromos, skipped) {
  const byProduct = new Map();
  for (const item of safePromos) {
    const arr = byProduct.get(item.payload.product_id) || [];
    arr.push(item);
    byProduct.set(item.payload.product_id, arr);
  }

  const conflictIds = new Set();
  for (const arr of byProduct.values()) {
    for (let i = 0; i < arr.length; i += 1) {
      for (let j = i + 1; j < arr.length; j += 1) {
        if (overlaps(arr[i].payload, arr[j].payload)) {
          conflictIds.add(arr[i].promo.reward_id);
          conflictIds.add(arr[j].promo.reward_id);
        }
      }
    }
  }

  if (!conflictIds.size) return safePromos;

  const filtered = [];
  for (const item of safePromos) {
    if (conflictIds.has(item.promo.reward_id)) {
      skipped.push({
        reward_id: item.promo.reward_id,
        title: item.promo.title,
        reason: "conflicting_promotion_for_same_product_and_overlapping_dates",
        matched_product_id: item.payload.product_id,
        matched_product_name: item.payload.product_name,
      });
    } else {
      filtered.push(item);
    }
  }
  return filtered;
}

async function loadProducts(conn, shopId) {
  const [rows] = await conn.query(
    `
    SELECT id, name, display_name_en, price, stock_amount
    FROM product
    WHERE shop_id = ?
    ORDER BY id ASC
    `,
    [shopId],
  );

  return (rows || []).map((row) => {
    const product = {
      id: Number(row.id),
      name: row.name,
      display_name_en: row.display_name_en,
      price: row.price == null ? null : Number(row.price),
      stock_amount: row.stock_amount == null ? null : Number(row.stock_amount),
    };
    product._normName = normalizeText([row.name, row.display_name_en].filter(Boolean).join(" "));
    product._tokenSet = productTokenSet(product);
    return product;
  });
}

function analyzePromotions(rows, products) {
  const safeProductPromotions = [];
  const safeCartRules = [];
  const skipped = [];

  for (const promo of rows) {
    if (promo.active !== "כן") {
      skipped.push({ reward_id: promo.reward_id, title: promo.title, type: promo.type, deal_text: promo.deal_text, start_date: promo.start_date, end_date: promo.end_date, reason: "inactive_in_excel" });
      continue;
    }

    if (!INCLUDE_EXPIRED && isExpired(promo.end_date)) {
      skipped.push({ reward_id: promo.reward_id, title: promo.title, type: promo.type, deal_text: promo.deal_text, start_date: promo.start_date, end_date: promo.end_date, reason: "expired_in_excel" });
      continue;
    }

    const cartRule = cartRuleFromPromo(promo);
    if (cartRule) {
      safeCartRules.push({ promo, rule: cartRule });
      continue;
    }

    if (promo.type === "קנה בסכום הוסף קבל") {
      skipped.push({
        reward_id: promo.reward_id,
        title: promo.title,
        type: promo.type,
        deal_text: promo.deal_text,
        start_date: promo.start_date,
        end_date: promo.end_date,
        reason: "cart_rule_requires_manual_reward_product_mapping",
      });
      continue;
    }

    if (promo.type !== "כמות בסכום") {
      skipped.push({ reward_id: promo.reward_id, title: promo.title, type: promo.type, deal_text: promo.deal_text, start_date: promo.start_date, end_date: promo.end_date, reason: "unsupported_promotion_type" });
      continue;
    }

    const matchInfo = findCandidates(promo, products);
    if (matchInfo.candidates.length !== 1 || !String(matchInfo.reason).startsWith("unique_")) {
      skipped.push({
        reward_id: promo.reward_id,
        title: promo.title,
        type: promo.type,
        deal_text: promo.deal_text,
        start_date: promo.start_date,
        end_date: promo.end_date,
        max_qty: promo.max_qty ?? null,
        reason: matchInfo.reason,
        search_phrase: matchInfo.phrase,
        tokens: matchInfo.tokens,
        candidates_count: matchInfo.candidates.length,
        candidates: matchInfo.candidates.slice(0, 35).map((p) => ({
          id: p.id,
          name: p.name,
          price: p.price,
          stock_amount: p.stock_amount,
          match_score: p.match_score,
        })),
      });
      continue;
    }

    const product = matchInfo.candidates[0];
    const payload = buildProductPromotionPayload(promo, product, matchInfo);
    if (payload.skip_reason) {
      skipped.push({
        reward_id: promo.reward_id,
        title: promo.title,
        type: promo.type,
        deal_text: promo.deal_text,
        start_date: promo.start_date,
        end_date: promo.end_date,
        max_qty: promo.max_qty ?? null,
        reason: payload.skip_reason,
        matched_product_id: product.id,
        matched_product_name: product.name,
      });
      continue;
    }

    safeProductPromotions.push({ promo, payload, matchInfo });
  }

  return {
    safeProductPromotions: dedupeConflictingProductPromotions(safeProductPromotions, skipped),
    safeCartRules,
    skipped,
  };
}

async function tableExists(conn, tableName) {
  const [rows] = await conn.query(
    `
    SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = ?
    LIMIT 1
    `,
    [tableName],
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function backupTable(conn, tableName, backupName, shopId) {
  if (!(await tableExists(conn, tableName))) return false;
  await conn.query(`CREATE TABLE \`${backupName}\` AS SELECT * FROM \`${tableName}\` WHERE shop_id = ?`, [shopId]);
  return true;
}

async function countShopRows(conn, tableName, shopId) {
  if (!(await tableExists(conn, tableName))) return 0;
  const [[row]] = await conn.query(`SELECT COUNT(*) AS count FROM \`${tableName}\` WHERE shop_id = ?`, [shopId]);
  return Number(row?.count || 0);
}

async function clearShopPromotions(conn, shopId) {
  const deleted = {};

  deleted.order_product_group_promotion_application = await countShopRows(conn, "order_product_group_promotion_application", shopId);
  if (await tableExists(conn, "order_product_group_promotion_application")) {
    await conn.query(`DELETE FROM order_product_group_promotion_application WHERE shop_id = ?`, [shopId]);
  }

  deleted.order_promotion_application = await countShopRows(conn, "order_promotion_application", shopId);
  if (await tableExists(conn, "order_promotion_application")) {
    await conn.query(`DELETE FROM order_promotion_application WHERE shop_id = ?`, [shopId]);
  }

  await conn.query(
    `
    UPDATE order_item oi
    JOIN orders o ON o.id = oi.order_id
    SET oi.promo_id = NULL,
        oi.cart_promotion_rule_id = NULL,
        oi.is_gift = 0
    WHERE o.shop_id = ?
      AND (oi.promo_id IS NOT NULL OR oi.cart_promotion_rule_id IS NOT NULL OR oi.is_gift = 1)
    `,
    [shopId],
  );

  deleted.product_group_promotion_item = await countShopRows(conn, "product_group_promotion_item", shopId);
  if (await tableExists(conn, "product_group_promotion_item")) {
    await conn.query(`DELETE FROM product_group_promotion_item WHERE shop_id = ?`, [shopId]);
  }

  deleted.product_group_promotion = await countShopRows(conn, "product_group_promotion", shopId);
  if (await tableExists(conn, "product_group_promotion")) {
    await conn.query(`DELETE FROM product_group_promotion WHERE shop_id = ?`, [shopId]);
  }

  deleted.cart_promotion_rule = await countShopRows(conn, "cart_promotion_rule", shopId);
  if (await tableExists(conn, "cart_promotion_rule")) {
    await conn.query(`DELETE FROM cart_promotion_rule WHERE shop_id = ?`, [shopId]);
  }

  deleted.promotion = await countShopRows(conn, "promotion", shopId);
  await conn.query(`DELETE FROM promotion WHERE shop_id = ?`, [shopId]);

  return deleted;
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

function writeReport(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const file = path.join(REPORTS_DIR, `leshem_promotions_import_report_${stamp()}.json`);
  fs.writeFileSync(file, JSON.stringify(report, null, 2), "utf8");
  return file;
}

function printableSummary(report) {
  return {
    mode: report.mode,
    shop_id: report.shop_id,
    source: report.source,
    data_file: report.data_file,
    products_in_shop: report.products_in_shop,
    excel_promotions_total: report.excel_promotions_total,
    safe_product_promotions: report.safe_product_promotions.length,
    safe_cart_rules: report.safe_cart_rules.length,
    skipped: report.skipped.length,
    deleted_existing: report.deleted_existing,
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

  const rows = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  await ensureCartPromotionSchema();
  await ensureProductGroupPromotionColumns();

  const conn = await db.getConnection();
  try {
    const products = await loadProducts(conn, SHOP_ID);
    const analysis = analyzePromotions(rows, products);

    const report = {
      mode: DRY_RUN ? "dryRun" : "confirm",
      shop_id: SHOP_ID,
      source: SOURCE,
      data_file: DATA_FILE,
      generated_at: new Date().toISOString(),
      products_in_shop: products.length,
      excel_promotions_total: rows.length,
      safe_product_promotions: analysis.safeProductPromotions.map((item) => ({
        reward_id: item.promo.reward_id,
        title: item.promo.title,
        type: item.promo.type,
        deal_text: item.promo.deal_text,
        matched_product_id: item.payload.product_id,
        matched_product_name: item.payload.product_name,
        kind: item.payload.kind,
        fixed_price: item.payload.fixed_price,
        bundle_buy_qty: item.payload.bundle_buy_qty,
        bundle_pay_price: item.payload.bundle_pay_price,
        max_discounted_qty: item.payload.max_discounted_qty,
        start_at: item.payload.start_at,
        end_at: item.payload.end_at,
        search_phrase: item.matchInfo.phrase,
        tokens: item.matchInfo.tokens,
        match_reason: item.matchInfo.reason,
        match_score: item.matchInfo.candidates?.[0]?.match_score ?? null,
      })),
      safe_cart_rules: analysis.safeCartRules.map((item) => ({
        reward_id: item.promo.reward_id,
        title: item.promo.title,
        type: item.promo.type,
        rule_type: item.rule.rule_type,
        threshold_amount: item.rule.threshold_amount,
        delivery_fee_override: item.rule.delivery_fee_override,
        start_at: item.rule.start_at,
        end_at: item.rule.end_at,
      })),
      skipped: analysis.skipped,
      deleted_existing: {},
      backup_tables: [],
    };

    if (!DRY_RUN) {
      const suffix = stamp();
      const backupSpecs = [
        ["promotion", `bak_promo_s${SHOP_ID}_${suffix}`],
        ["cart_promotion_rule", `bak_cart_rule_s${SHOP_ID}_${suffix}`],
        ["product_group_promotion", `bak_group_promo_s${SHOP_ID}_${suffix}`],
        ["product_group_promotion_item", `bak_group_item_s${SHOP_ID}_${suffix}`],
        ["order_promotion_application", `bak_order_cart_app_s${SHOP_ID}_${suffix}`],
        ["order_product_group_promotion_application", `bak_order_group_app_s${SHOP_ID}_${suffix}`],
      ];

      for (const [table, backup] of backupSpecs) {
        const safeBackup = backup.replace(/[^a-zA-Z0-9_]/g, "_");
        if (await backupTable(conn, table, safeBackup, SHOP_ID)) report.backup_tables.push(safeBackup);
      }

      await conn.beginTransaction();
      try {
        report.deleted_existing = await clearShopPromotions(conn, SHOP_ID);

        for (const item of analysis.safeProductPromotions) {
          await insertProductPromotion(conn, SHOP_ID, item.payload);
        }
        for (const item of analysis.safeCartRules) {
          await insertCartRule(conn, SHOP_ID, item.rule);
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
      console.log("\nDry run only. To delete existing shop promotions and insert the safe automatic matches, run with --confirm.");
    }
  } finally {
    conn.release();
  }
}

main()
  .catch((err) => {
    console.error("[import-leshem-promotions]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });

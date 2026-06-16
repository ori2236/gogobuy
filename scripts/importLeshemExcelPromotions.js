require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");

const SOURCE = "leshem_excel_2026_06_14";
const DATA_FILE = path.join(__dirname, "..", "data", "leshem_promotions_2026_06_14.json");
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
  // Hebrew quote/apostrophe marks usually appear inside acronyms/brand names.
  // Removing them turns סכו"ם -> סכום and ג'ריס -> גריס.
  for (const ch of ["״", "”", "“", "׳", "’", "‘", "'", '"']) {
    s = s.split(ch).join("");
  }
  const replacements = {
    "־": " ",
    "–": " ",
    "—": " ",
    "-": " ",
    "&": " ",
    "+": " ",
    "₪": " ",
    "%": " ",
    ".": " ",
    ",": " ",
    ":": " ",
    ";": " ",
    "(": " ",
    ")": " ",
    "[": " ",
    "]": " ",
    "/": " ",
    "\\": " ",
  };
  for (const [from, to] of Object.entries(replacements)) {
    s = s.split(from).join(to);
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
  "רק",
  "יח",
  "גרם",
  "קג",
  "לקג",
  "קילו",
  "מל",
  "ליטר",
  "לי",
  "מגוון",
  "מוצרי",
]);

function stripPromotionPriceText(title) {
  let s = String(title || "").toLowerCase().replace(/₪/g, " ").replace(/&/g, " ");
  for (const ch of ["״", "”", "“", "׳", "’", "‘", "'", '"']) {
    s = s.split(ch).join("");
  }

  s = s.replace(/\b\d+(?:\.\d+)?\s*ב\s*-?\s*\d+(?:\.\d+)?\b/g, " ");
  s = s.replace(/\bרק\s*ב\s*-?\s*\d+(?:\.\d+)?\b/g, " ");
  s = s.replace(/\bב\s*-?\s*\d+(?:\.\d+)?\b/g, " ");
  s = s.replace(/\b\d+(?:\.\d+)?\s*₪\b/g, " ");
  return normalizeText(s);
}

function tokenize(value) {
  return normalizeText(value)
    .split(" ")
    .map((t) => t.trim())
    .filter(Boolean)
    .filter((t) => !STOP_WORDS.has(t))
    .filter((t) => !/^\d+$/.test(t));
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
  return new Set(tokenize(product.name));
}

function productContainsAllTokens(product, tokens) {
  const set = product._tokenSet;
  return tokens.every((token) => set.has(token));
}

function findCandidates(promo, products) {
  const phrase = stripPromotionPriceText(promo.title);
  const tokens = tokenize(phrase);

  if (tokens.length < 2) {
    return { phrase, tokens, candidates: [], reason: "too_few_specific_tokens" };
  }

  const phraseMatches = products.filter((product) => {
    return product._normName.includes(phrase);
  });

  if (phraseMatches.length === 1) {
    return { phrase, tokens, candidates: phraseMatches, reason: "unique_phrase_match" };
  }

  const tokenMatches = products.filter((product) => productContainsAllTokens(product, tokens));
  if (tokenMatches.length === 1) {
    return { phrase, tokens, candidates: tokenMatches, reason: "unique_token_match" };
  }

  return {
    phrase,
    tokens,
    candidates: tokenMatches.length ? tokenMatches : phraseMatches,
    reason: tokenMatches.length ? "multiple_token_matches" : "no_match",
  };
}

function buildProductPromotionPayload(promo, product, matchInfo) {
  const deal = parseDealText(promo.deal_text);
  if (!deal) return { skip_reason: "deal_text_not_supported" };

  // The current promotion engine supports bundles per single product line.
  // 0.01 ב-price is usually a weight/kg shorthand in the source file, not a safe bundle qty.
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

  return (rows || []).map((row) => ({
    id: Number(row.id),
    name: row.name,
    display_name_en: row.display_name_en,
    price: row.price == null ? null : Number(row.price),
    stock_amount: row.stock_amount == null ? null : Number(row.stock_amount),
    _normName: normalizeText(row.name),
    _tokenSet: productTokenSet(row),
  }));
}

function analyzePromotions(rows, products) {
  const safeProductPromotions = [];
  const safeCartRules = [];
  const skipped = [];

  for (const promo of rows) {
    if (promo.active !== "כן") {
      skipped.push({ reward_id: promo.reward_id, title: promo.title, reason: "inactive_in_excel" });
      continue;
    }

    if (!INCLUDE_EXPIRED && isExpired(promo.end_date)) {
      skipped.push({ reward_id: promo.reward_id, title: promo.title, reason: "expired_in_excel" });
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
        reason: "cart_rule_requires_manual_reward_product_mapping",
      });
      continue;
    }

    if (promo.type !== "כמות בסכום") {
      skipped.push({ reward_id: promo.reward_id, title: promo.title, reason: "unsupported_promotion_type" });
      continue;
    }

    const matchInfo = findCandidates(promo, products);
    if (matchInfo.candidates.length !== 1) {
      skipped.push({
        reward_id: promo.reward_id,
        title: promo.title,
        reason: matchInfo.reason,
        search_phrase: matchInfo.phrase,
        tokens: matchInfo.tokens,
        candidates_count: matchInfo.candidates.length,
        candidates: matchInfo.candidates.slice(0, 20).map((p) => ({ id: p.id, name: p.name })),
      });
      continue;
    }

    const product = matchInfo.candidates[0];
    const payload = buildProductPromotionPayload(promo, product, matchInfo);
    if (payload.skip_reason) {
      skipped.push({
        reward_id: promo.reward_id,
        title: promo.title,
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

async function backupTable(conn, tableName, backupName, shopId) {
  await conn.query(`CREATE TABLE \`${backupName}\` AS SELECT * FROM \`${tableName}\` WHERE shop_id = ?`, [shopId]);
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
    products_in_shop: report.products_in_shop,
    excel_promotions_total: report.excel_promotions_total,
    safe_product_promotions: report.safe_product_promotions.length,
    safe_cart_rules: report.safe_cart_rules.length,
    skipped: report.skipped.length,
    deleted_existing_product_promotions: report.deleted_existing_product_promotions,
    deleted_existing_cart_rules: report.deleted_existing_cart_rules,
    backup_tables: report.backup_tables,
    report_file: report.report_file,
  };
}

async function main() {
  if (!Number.isInteger(SHOP_ID) || SHOP_ID <= 0) {
    throw new Error(`Invalid --shopId: ${SHOP_ID}`);
  }

  const rows = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  await ensureCartPromotionSchema();

  const conn = await db.getConnection();
  try {
    const products = await loadProducts(conn, SHOP_ID);
    const analysis = analyzePromotions(rows, products);

    const report = {
      mode: DRY_RUN ? "dryRun" : "confirm",
      shop_id: SHOP_ID,
      source: SOURCE,
      generated_at: new Date().toISOString(),
      products_in_shop: products.length,
      excel_promotions_total: rows.length,
      safe_product_promotions: analysis.safeProductPromotions.map((item) => ({
        reward_id: item.promo.reward_id,
        title: item.promo.title,
        matched_product_id: item.payload.product_id,
        matched_product_name: item.payload.product_name,
        kind: item.payload.kind,
        fixed_price: item.payload.fixed_price,
        bundle_buy_qty: item.payload.bundle_buy_qty,
        bundle_pay_price: item.payload.bundle_pay_price,
        max_discounted_qty: item.payload.max_discounted_qty,
        start_at: item.payload.start_at,
        end_at: item.payload.end_at,
        match_reason: item.matchInfo.reason,
      })),
      safe_cart_rules: analysis.safeCartRules.map((item) => ({
        reward_id: item.promo.reward_id,
        title: item.promo.title,
        rule_type: item.rule.rule_type,
        threshold_amount: item.rule.threshold_amount,
        delivery_fee_override: item.rule.delivery_fee_override,
        start_at: item.rule.start_at,
        end_at: item.rule.end_at,
      })),
      skipped: analysis.skipped,
      deleted_existing_product_promotions: 0,
      deleted_existing_cart_rules: 0,
      backup_tables: [],
    };

    if (!DRY_RUN) {
      const [[promoCount]] = await conn.query(
        `SELECT COUNT(*) AS count FROM promotion WHERE shop_id = ?`,
        [SHOP_ID],
      );
      const [[cartCount]] = await conn.query(
        `SELECT COUNT(*) AS count FROM cart_promotion_rule WHERE shop_id = ?`,
        [SHOP_ID],
      );
      report.deleted_existing_product_promotions = Number(promoCount.count || 0);
      report.deleted_existing_cart_rules = Number(cartCount.count || 0);

      const suffix = stamp();
      const promoBackup = `bak_promo_s${SHOP_ID}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");
      const cartBackup = `bak_cart_rule_s${SHOP_ID}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");

      // MySQL commits implicitly around CREATE TABLE, so backups are created before
      // the actual transactional delete+insert step.
      await backupTable(conn, "promotion", promoBackup, SHOP_ID);
      await backupTable(conn, "cart_promotion_rule", cartBackup, SHOP_ID);
      report.backup_tables.push(promoBackup, cartBackup);

      await conn.beginTransaction();
      try {
        await conn.query(`DELETE FROM promotion WHERE shop_id = ?`, [SHOP_ID]);
        await conn.query(`DELETE FROM cart_promotion_rule WHERE shop_id = ?`, [SHOP_ID]);

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
      console.log("\nDry run only. To actually delete existing shop promotions and insert the safe ones, run with --confirm.");
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

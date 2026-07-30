require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");

const DEFAULT_SOURCE = "leshem_excel_2026_07_12";
const DEFAULT_DATA_FILE = path.join(__dirname, "..", "data", "shop2_promotions_2026_07_12.json");
const DEFAULT_OUT_FILE = path.join(__dirname, "..", "data", "leshem_manual_promo_mapping_31433_existing.json");
const REPORTS_DIR = path.join(__dirname, "..", "reports");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

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

const SHOP_ID = Number(argValue("--shopId", process.env.PROMO_IMPORT_SHOP_ID || 2));
const DATA_FILE = path.resolve(argValue("--data", process.env.PROMO_IMPORT_DATA_FILE || DEFAULT_DATA_FILE));
const OUT_FILE = path.resolve(argValue("--out", DEFAULT_OUT_FILE));
const SOURCE = String(argValue("--source", process.env.PROMO_IMPORT_SOURCE || DEFAULT_SOURCE)).trim() || DEFAULT_SOURCE;
const MIN_PRODUCT_SCORE = Number(argValue("--minProductScore", 10));

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
    .map((token) => token.trim())
    .filter(Boolean)
    .filter((token) => !STOP_WORDS.has(token))
    .filter((token) => !/^\d+(?:\.\d+)?$/.test(token));
}

function uniq(items) {
  return [...new Set(items)];
}

function parseDealText(value) {
  const match = String(value || "").match(/(\d+(?:\.\d+)?)\s*ב\s*(\d+(?:\.\d+)?)/);
  if (!match) return null;
  const qty = Number(match[1]);
  const price = Number(match[2]);
  if (!Number.isFinite(qty) || !Number.isFinite(price) || qty <= 0 || price < 0) return null;
  return { qty, price };
}

function round2(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
}

function promotionSignatureFromExcel(promo) {
  if (promo.type !== "כמות בסכום") return null;
  const deal = parseDealText(promo.deal_text);
  if (!deal) return null;
  const fractionalFixedUnitPrice = deal.qty > 0 && deal.qty < 1;
  if (deal.qty === 1 || fractionalFixedUnitPrice) {
    return `FIXED_PRICE:${round2(deal.price)}`;
  }
  if (Number.isInteger(deal.qty) && deal.qty >= 2) {
    return `BUNDLE:${deal.qty}:${round2(deal.price)}`;
  }
  return null;
}

function promotionSignatureFromDb(row) {
  if (row.kind === "FIXED_PRICE") return `FIXED_PRICE:${round2(row.fixed_price)}`;
  if (row.kind === "BUNDLE") return `BUNDLE:${Number(row.bundle_buy_qty)}:${round2(row.bundle_pay_price)}`;
  return null;
}

function baseCartTitle(title) {
  return String(title || "").split(" - ")[0].trim();
}

function scoreProductNameForPromo(productName, promoTitle) {
  const phrase = stripPromotionPriceText(promoTitle);
  const promoTokens = uniq(tokenize(phrase));
  const productTokens = new Set(tokenize(productName));
  const hits = promoTokens.filter((token) => productTokens.has(token));
  if (!hits.length) return { score: 0, hits };

  const productNorm = normalizeText(productName);
  let score = hits.length * 10;
  if (phrase && productNorm.includes(phrase)) score += 25;
  if (phrase && phrase.includes(productNorm) && productNorm.length >= 4) score += 20;
  if (hits.length >= 2) score += 10;
  return { score, hits };
}

function makeMapping({ promo, action, mappingMode, isGroupPromotion, productIds, note }) {
  return {
    reward_id: Number(promo.reward_id),
    title: promo.title,
    type: promo.type,
    start_date: promo.start_date || null,
    end_date: promo.end_date || null,
    action,
    mapping_mode: mappingMode,
    is_group_promotion: Boolean(isGroupPromotion),
    product_ids: Array.from(new Set((productIds || []).map(Number).filter((id) => Number.isInteger(id) && id > 0))),
    note: note || "",
  };
}

async function loadProducts(conn, shopId) {
  const [rows] = await conn.query(
    `
    SELECT id, name
    FROM product
    WHERE shop_id = ?
    ORDER BY id ASC
    `,
    [shopId],
  );
  const byId = new Map();
  for (const row of rows || []) byId.set(Number(row.id), { id: Number(row.id), name: row.name });
  return byId;
}

async function loadProductPromotions(conn, shopId, productsById) {
  const [rows] = await conn.query(
    `
    SELECT id, product_id, kind, fixed_price, bundle_buy_qty, bundle_pay_price, max_discounted_qty, start_at, end_at
    FROM promotion
    WHERE shop_id = ?
    ORDER BY id ASC
    `,
    [shopId],
  );
  return (rows || []).map((row) => ({
    ...row,
    product_id: Number(row.product_id),
    product_name: productsById.get(Number(row.product_id))?.name || "",
    signature: promotionSignatureFromDb(row),
  }));
}

async function loadGroupPromotions(conn, shopId) {
  const [rows] = await conn.query(
    `
    SELECT
      g.id,
      g.title,
      g.bundle_buy_qty,
      g.bundle_pay_price,
      g.max_discounted_qty,
      GROUP_CONCAT(i.product_id ORDER BY i.product_id ASC) AS product_ids
    FROM product_group_promotion g
    JOIN product_group_promotion_item i
      ON i.group_promotion_id = g.id
     AND i.shop_id = g.shop_id
    WHERE g.shop_id = ?
    GROUP BY g.id, g.title, g.bundle_buy_qty, g.bundle_pay_price, g.max_discounted_qty
    ORDER BY g.id ASC
    `,
    [shopId],
  );
  return rows || [];
}

async function loadCartRules(conn, shopId) {
  const [rows] = await conn.query(
    `
    SELECT id, rule_type, title, reward_product_id, external_reward_id
    FROM cart_promotion_rule
    WHERE shop_id = ?
    ORDER BY id ASC
    `,
    [shopId],
  );
  return rows || [];
}

function writeReport(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const file = path.join(REPORTS_DIR, `existing_promo_mapping_report_${stamp()}.json`);
  fs.writeFileSync(file, JSON.stringify(report, null, 2), "utf8");
  return file;
}

async function main() {
  if (!Number.isInteger(SHOP_ID) || SHOP_ID <= 0) throw new Error(`Invalid --shopId: ${SHOP_ID}`);
  if (!fs.existsSync(DATA_FILE)) throw new Error(`Data file was not found: ${DATA_FILE}`);

  const sourcePromotions = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  const activePromotions = sourcePromotions.filter((promo) => String(promo.active || "").trim() === "כן");
  const mappings = [];
  const mappedRewardIds = new Set();
  const report = {
    shop_id: SHOP_ID,
    source: SOURCE,
    data_file: DATA_FILE,
    out_file: OUT_FILE,
    excel_promotions_total: sourcePromotions.length,
    active_promotions: activePromotions.length,
    generated_at: new Date().toISOString(),
    matched_groups_by_title: [],
    matched_cart_reward_rules: [],
    matched_product_promotions_by_signature: [],
    skipped_product_candidates: [],
  };

  const conn = await db.getConnection();
  try {
    const productsById = await loadProducts(conn, SHOP_ID);
    const productPromotions = await loadProductPromotions(conn, SHOP_ID, productsById);
    const groupPromotions = await loadGroupPromotions(conn, SHOP_ID);
    const cartRules = await loadCartRules(conn, SHOP_ID);

    const groupsByTitle = new Map(groupPromotions.map((group) => [normalizeText(group.title), group]));
    const cartByExternalBaseId = new Map();
    const cartByTitle = new Map();
    for (const rule of cartRules) {
      const external = String(rule.external_reward_id || "").trim();
      const base = external.split("_")[0];
      if (base) cartByExternalBaseId.set(base, rule);
      cartByTitle.set(normalizeText(baseCartTitle(rule.title)), rule);
    }

    const productPromotionsBySignature = new Map();
    for (const row of productPromotions) {
      if (!row.signature) continue;
      const arr = productPromotionsBySignature.get(row.signature) || [];
      arr.push(row);
      productPromotionsBySignature.set(row.signature, arr);
    }

    for (const promo of activePromotions) {
      if (promo.type !== "כמות בסכום") continue;
      const group = groupsByTitle.get(normalizeText(promo.title));
      if (!group) continue;
      const productIds = String(group.product_ids || "")
        .split(",")
        .map((id) => Number(id))
        .filter((id) => Number.isInteger(id) && id > 0);
      if (!productIds.length) continue;
      mappings.push(makeMapping({
        promo,
        action: "promotion_group",
        mappingMode: "group",
        isGroupPromotion: true,
        productIds,
        note: `שוחזר ממבצע קבוצה קיים בסניף ${SHOP_ID} לפי כותרת זהה`,
      }));
      mappedRewardIds.add(Number(promo.reward_id));
      report.matched_groups_by_title.push({ reward_id: Number(promo.reward_id), title: promo.title, group_promotion_id: Number(group.id), product_ids: productIds });
    }

    for (const promo of activePromotions) {
      if (mappedRewardIds.has(Number(promo.reward_id))) continue;
      if (promo.type !== "קנה בסכום הוסף קבל") continue;
      const byId = cartByExternalBaseId.get(String(promo.reward_id));
      const byTitle = cartByTitle.get(normalizeText(promo.title));
      const rule = byId || byTitle;
      const rewardProductId = Number(rule?.reward_product_id || 0);
      if (!rule || !rewardProductId) continue;
      mappings.push(makeMapping({
        promo,
        action: "cart_reward_product",
        mappingMode: "single_product",
        isGroupPromotion: false,
        productIds: [rewardProductId],
        note: `שוחזר ממבצע סל קיים בסניף ${SHOP_ID}`,
      }));
      mappedRewardIds.add(Number(promo.reward_id));
      report.matched_cart_reward_rules.push({ reward_id: Number(promo.reward_id), title: promo.title, cart_rule_id: Number(rule.id), product_id: rewardProductId });
    }

    for (const promo of activePromotions) {
      if (mappedRewardIds.has(Number(promo.reward_id))) continue;
      if (promo.type !== "כמות בסכום") continue;
      const signature = promotionSignatureFromExcel(promo);
      if (!signature) continue;
      const candidates = productPromotionsBySignature.get(signature) || [];
      const scored = candidates
        .map((row) => ({ row, ...scoreProductNameForPromo(row.product_name, promo.title) }))
        .filter((item) => item.score >= MIN_PRODUCT_SCORE)
        .sort((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          return a.row.product_id - b.row.product_id;
        });

      if (!scored.length) {
        report.skipped_product_candidates.push({ reward_id: Number(promo.reward_id), title: promo.title, signature, reason: "no_existing_product_promotion_name_match", db_candidates_with_same_signature: candidates.length });
        continue;
      }

      const topScore = scored[0].score;
      const selected = scored.filter((item) => item.score === topScore || item.score >= Math.max(MIN_PRODUCT_SCORE, topScore - 5));
      const productIds = selected.map((item) => item.row.product_id);
      const mappingMode = productIds.length > 1 ? "separate_product_promotions" : "single_product";
      mappings.push(makeMapping({
        promo,
        action: "product_promotion",
        mappingMode,
        isGroupPromotion: false,
        productIds,
        note: `שוחזר ממבצע מוצר קיים בסניף ${SHOP_ID} לפי מחיר/כמות ושם מוצר`,
      }));
      mappedRewardIds.add(Number(promo.reward_id));
      report.matched_product_promotions_by_signature.push({
        reward_id: Number(promo.reward_id),
        title: promo.title,
        signature,
        product_ids: productIds,
        product_names: selected.map((item) => item.row.product_name),
        scores: selected.map((item) => item.score),
      });
    }
  } finally {
    conn.release();
  }

  const output = {
    source: SOURCE,
    shop_id: SHOP_ID,
    based_on_data: DATA_FILE,
    generated_at: new Date().toISOString(),
    mappings,
  };

  fs.mkdirSync(path.dirname(OUT_FILE), { recursive: true });
  fs.writeFileSync(OUT_FILE, JSON.stringify(output, null, 2), "utf8");
  report.manual_mappings_written = mappings.length;
  report.unmapped_active_reward_ids = activePromotions
    .map((promo) => Number(promo.reward_id))
    .filter((rewardId) => !mappedRewardIds.has(rewardId));
  const reportFile = writeReport(report);

  console.log(JSON.stringify({
    shop_id: SHOP_ID,
    source: SOURCE,
    data_file: DATA_FILE,
    out_file: OUT_FILE,
    excel_promotions_total: sourcePromotions.length,
    active_promotions: activePromotions.length,
    matched_groups_by_title: report.matched_groups_by_title.length,
    matched_cart_reward_rules: report.matched_cart_reward_rules.length,
    matched_product_promotions_by_signature: report.matched_product_promotions_by_signature.length,
    manual_mappings_written: mappings.length,
    still_unmapped_active_promotions: report.unmapped_active_reward_ids.length,
    report_file: reportFile,
  }, null, 2));
}

main()
  .catch((err) => {
    console.error("[generate-existing-promo-mappings]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await db.end?.();
    } catch (_) {}
  });

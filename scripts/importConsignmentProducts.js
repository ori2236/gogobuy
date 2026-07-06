/*
  Import supplier Excel files as consignment products for a specific shop.

  This script is intentionally safe:
  - Dry run by default. Use --confirm to write to DB.
  - Reads supplier XLSX files and deduplicates by product name.
  - If the same Excel name appears with different prices, keeps the LOWEST price.
  - Existing products are updated instead of duplicated when the match is safe.
  - New products are inserted with is_consignment=1 and stock_amount=0.
  - If several Excel rows resolve to the same existing product, updates are merged and the lowest price wins.
  - Existing product categories/default flags/translations are preserved.
  - Unclear matches are put in the review report and are not changed automatically.

  Usage from project root:
    node scripts/importConsignmentProducts.js --shopId=2 --dryRun
    node scripts/importConsignmentProducts.js --shopId=2 --confirm

  Optional:
    node scripts/importConsignmentProducts.js --shopId=2 --dryRun --excel=data/consignment/vegetables.xlsx,data/consignment/shargis.xlsx
    node scripts/importConsignmentProducts.js --shopId=2 --confirm --autoMatchThreshold=0.96
    node scripts/importConsignmentProducts.js --shopId=2 --dryRun --noFuzzy
*/

require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const xlsx = require("xlsx");
const db = require("../config/db");
const { rebuildTokenWeightsForShop } = require("../services/buildTokenWeights");
const { ensureProductDefaultSchemaNow } = require("../utilities/productDefaultSchema");

const DEFAULT_FILES = [
  "data/consignment/vegetables.xlsx",
  "data/consignment/shargis.xlsx",
  "data/consignment/yonatan.xlsx",
];

const DEFAULT_FUZZY_THRESHOLD = 0.96;
const DEFAULT_FUZZY_GAP = 0.04;
const STOCK_AMOUNT_FOR_CONSIGNMENT = 0;

function getArg(name, defaultValue = null) {
  const prefix = `--${name}=`;
  const arg = process.argv.find((x) => x.startsWith(prefix));
  if (!arg) return defaultValue;
  return arg.slice(prefix.length);
}

function getAllArgs(name) {
  const prefix = `--${name}=`;
  return process.argv.filter((x) => x.startsWith(prefix)).map((x) => x.slice(prefix.length));
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function toPositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const text = String(value)
    .replace(/^\uFEFF/, "")
    .replace(/,/g, "")
    .replace(/₪/g, "")
    .trim();
  if (!text) return null;
  const n = Number(text);
  return Number.isFinite(n) ? n : null;
}

function roundPrice(value) {
  const n = toNumber(value);
  if (n === null) return null;
  return Math.round(n * 100) / 100;
}

function cleanText(value) {
  return String(value ?? "")
    .replace(/^\uFEFF/, "")
    .replace(/[\u200E\u200F\u202A-\u202E]/g, "")
    .replace(/[“”]/g, '"')
    .replace(/[‘’`´]/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanName(value) {
  return cleanText(value).slice(0, 200);
}

function cleanBarcode(value) {
  const text = cleanText(value);
  if (!text) return null;
  const n = toNumber(text);
  if (n !== null && /^\d+(\.0+)?$/.test(text)) return String(Math.trunc(n));
  return text.slice(0, 64);
}

function normalizeForExact(value) {
  return cleanText(value)
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[\u0591-\u05C7]/g, "")
    .replace(/[״"“”]/g, "")
    .replace(/[׳'‘’`´]/g, "")
    .replace(/[%٪]/g, "%")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeForLoose(value) {
  return normalizeForExact(value)
    .replace(/\([^)]*\)/g, " ")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/[\\/|,:;._*#~+=\-–—]/g, " ")
    .replace(/\b(יחידות|יחידה|יח|גרם|גר|ג|קג|ק"ג|ק״ג|מל|מ"ל|מ״ל|ליטר|לי)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenizeForMatch(value) {
  const loose = normalizeForLoose(value);
  if (!loose) return [];
  return loose.split(/\s+/).filter((x) => x.length > 1 || /\d/.test(x));
}

function levenshtein(a, b) {
  const s = String(a || "");
  const t = String(b || "");
  if (s === t) return 0;
  if (!s) return t.length;
  if (!t) return s.length;

  let prev = Array.from({ length: t.length + 1 }, (_, i) => i);
  let curr = new Array(t.length + 1);

  for (let i = 1; i <= s.length; i += 1) {
    curr[0] = i;
    for (let j = 1; j <= t.length; j += 1) {
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;
      curr[j] = Math.min(
        prev[j] + 1,
        curr[j - 1] + 1,
        prev[j - 1] + cost,
      );
    }
    [prev, curr] = [curr, prev];
  }

  return prev[t.length];
}

function stringSimilarity(a, b) {
  const s = normalizeForLoose(a);
  const t = normalizeForLoose(b);
  if (!s || !t) return 0;
  if (s === t) return 1;
  const dist = levenshtein(s, t);
  const maxLen = Math.max(s.length, t.length, 1);
  return Math.max(0, 1 - dist / maxLen);
}

function jaccard(aTokens, bTokens) {
  const a = new Set(aTokens);
  const b = new Set(bTokens);
  if (!a.size || !b.size) return 0;
  let intersection = 0;
  for (const x of a) if (b.has(x)) intersection += 1;
  const union = new Set([...a, ...b]).size;
  return union ? intersection / union : 0;
}

function matchScore(excelName, productName) {
  const exactA = normalizeForExact(excelName);
  const exactB = normalizeForExact(productName);
  if (exactA && exactA === exactB) return 1;

  const looseA = normalizeForLoose(excelName);
  const looseB = normalizeForLoose(productName);
  if (looseA && looseA === looseB) return 0.985;

  const sim = stringSimilarity(excelName, productName);
  const jac = jaccard(tokenizeForMatch(excelName), tokenizeForMatch(productName));
  return Number((sim * 0.68 + jac * 0.32).toFixed(4));
}

function inferSourceType(filePath) {
  const base = path.basename(String(filePath || "")).toLowerCase();
  if (base.includes("veget") || base.includes("ירק")) return "produce";
  if (base.includes("sharg") || base.includes("שרג")) return "bakery";
  if (base.includes("yonatan") || base.includes("יונתן")) return "bakery";
  return "general";
}

function defaultCategoryForSource(sourceType) {
  if (sourceType === "produce") return { category: "Produce", sub_category: "Vegetables", emoji: "🍅" };
  if (sourceType === "bakery") return { category: "Bakery", sub_category: "Bread", emoji: "🍞" };
  return { category: "Pantry", sub_category: "General Pantry", emoji: "🛒" };
}

function isFuzzyCategoryCompatible(item, product) {
  const sourceType = String(item?.source_type || "general");
  const category = String(product?.category || "").trim();

  // Exact-name matches are allowed to keep existing categorization, but fuzzy matches
  // should not overwrite a product from a different department. Example: fresh
  // horseradish from the produce file must not overwrite Strauss prepared horseradish.
  if (!category) return true;
  if (sourceType === "produce") return category === "Produce";
  if (sourceType === "bakery") return category === "Bakery";
  return true;
}

function classifyNewProductByName(name, sourceType) {
  const n = normalizeForLoose(name);

  // A few supplier files contain non-produce lines. Classify those by name before
  // falling back to the source file default.
  if (n.includes("טונה")) return { category: "Pantry", sub_category: "Canned Fish", emoji: "🐟" };
  if (n.includes("אורז")) return { category: "Pantry", sub_category: "Rice & Grains", emoji: "🌾" };
  if (n.includes("פסטה") || n.includes("נודלס") || n.includes("אטריות")) return { category: "Pantry", sub_category: "Pasta", emoji: "🍝" };
  if (n.includes("קמח")) return { category: "Pantry", sub_category: "Flour & Baking", emoji: "🌾" };

  if (sourceType === "produce") {
    const herbs = ["כוסברה", "פטרוזיל", "שמיר", "נענע", "בזיליקום", "רוזמרין", "טימין", "עירית", "רוקט"];
    const fruits = [
      "אבטיח", "מלון", "ענבים", "תפוח", "בננה", "תפוז", "קלמנט", "אפרסק", "נקטרינה", "שזיף", "מנגו",
      "אננס", "אגס", "לימון", "ליים", "אשכולית", "רימון", "תות", "אוכמ", "קיווי", "משמש", "דובדבן", "אבוקדו", "שסק", "פטיה", "פיטאיה",
    ];
    const prepped = ["חתוך", "חתוכה", "מקולף", "מקולפת", "מוכן לאכילה", "לקט", "סלט", "עלים", "נבטים"];

    if (herbs.some((w) => n.includes(w))) return { category: "Produce", sub_category: "Fresh Herbs", emoji: "🌿" };
    if (fruits.some((w) => n.includes(w))) return { category: "Produce", sub_category: "Fruits", emoji: "🍎" };
    if (prepped.some((w) => n.includes(w))) return { category: "Produce", sub_category: "Prepped Produce", emoji: "🥗" };
    return { category: "Produce", sub_category: "Vegetables", emoji: "🍅" };
  }

  if (sourceType === "bakery") {
    if (["פיתה", "פיתות", "לאפה", "טורטיה", "סלוף"].some((w) => n.includes(w))) {
      return { category: "Bakery", sub_category: "Pita & Flatbread", emoji: "🫓" };
    }
    if (["לחמניה", "לחמנייה", "לחמניות", "באן", "המבורגר"].some((w) => n.includes(w))) {
      return { category: "Bakery", sub_category: "Rolls & Buns", emoji: "🥯" };
    }
    if (["רוגעל", "בורקס", "עוג", "קראנץ", "קרואסון", "מאפה", "קוגל", "שמרים", "דניש"].some((w) => n.includes(w))) {
      return { category: "Bakery", sub_category: "Cakes & Pastries", emoji: "🥐" };
    }
    return { category: "Bakery", sub_category: "Bread", emoji: "🍞" };
  }

  return defaultCategoryForSource(sourceType);
}

function sheetRowsFromWorkbook(filePath) {
  const workbook = xlsx.readFile(filePath, { cellDates: false, raw: false });
  const rows = [];

  for (const sheetName of workbook.SheetNames) {
    const sheet = workbook.Sheets[sheetName];
    const matrix = xlsx.utils.sheet_to_json(sheet, { header: 1, raw: false, defval: "" });
    if (!matrix.length) continue;

    const headers = matrix[0].map((h) => cleanText(h));
    for (let i = 1; i < matrix.length; i += 1) {
      const row = matrix[i];
      if (!row.some((cell) => cleanText(cell))) continue;
      const obj = { __sheetName: sheetName, __rowNumber: i + 1 };
      headers.forEach((header, idx) => {
        if (header) obj[header] = row[idx] ?? "";
      });
      rows.push(obj);
    }
  }

  return rows;
}

function findHeaderValue(row, options) {
  for (const key of options) {
    if (Object.prototype.hasOwnProperty.call(row, key)) return row[key];
  }
  return undefined;
}

function cleanImportedRows(files) {
  const byName = new Map();
  const skipped = [];
  const duplicatePriceNames = [];
  const sourceStats = [];

  for (const file of files) {
    const filePath = path.resolve(process.cwd(), file);
    const sourceType = inferSourceType(file);
    if (!fs.existsSync(filePath)) {
      skipped.push({ source_file: file, reason: "file_not_found" });
      sourceStats.push({ source_file: file, source_type: sourceType, rows: 0, imported_candidates: 0, skipped: 1 });
      continue;
    }

    const rawRows = sheetRowsFromWorkbook(filePath);
    let importedCandidates = 0;
    let skippedInFile = 0;

    for (const raw of rawRows) {
      const name = cleanName(findHeaderValue(raw, ["שם פריט", "שם", "name", "Name", "פריט"]));
      const price = roundPrice(findHeaderValue(raw, ["מחיר מחירון 8", "מחיר מחירון 2", "מחיר מחירון", "מחיר", "price", "Price"]));
      const barcode = cleanBarcode(findHeaderValue(raw, ["ברקוד", "barcode", "Barcode", "קוד"]));
      const sourceItemCode = cleanText(findHeaderValue(raw, ["פריט", "קוד פריט", "item", "Item"]));

      if (!name) {
        skipped.push({ source_file: file, source_row: raw.__rowNumber, reason: "missing_name" });
        skippedInFile += 1;
        continue;
      }
      if (price === null || price < 0) {
        skipped.push({ source_file: file, source_row: raw.__rowNumber, name, reason: "invalid_price" });
        skippedInFile += 1;
        continue;
      }

      importedCandidates += 1;
      const key = normalizeForExact(name);
      const item = {
        name,
        price,
        barcode,
        source_item_code: sourceItemCode || null,
        source_type: sourceType,
        source_file: file,
        source_rows: [raw.__rowNumber],
        source_files: [file],
        prices_seen: [price],
        barcodes_seen: barcode ? [barcode] : [],
      };

      const existing = byName.get(key);
      if (!existing) {
        byName.set(key, item);
        continue;
      }

      existing.source_rows.push(raw.__rowNumber);
      existing.source_files.push(file);
      existing.prices_seen.push(price);
      if (barcode) existing.barcodes_seen.push(barcode);
      if (!existing.barcode && barcode) existing.barcode = barcode;
      if (price < existing.price) {
        existing.price = price;
        existing.source_file = file;
        existing.source_item_code = sourceItemCode || existing.source_item_code;
      }
    }

    sourceStats.push({
      source_file: file,
      source_type: sourceType,
      rows: rawRows.length,
      imported_candidates: importedCandidates,
      skipped: skippedInFile,
    });
  }

  for (const item of byName.values()) {
    const uniquePrices = Array.from(new Set(item.prices_seen.map((p) => Number(p).toFixed(2)))).sort((a, b) => Number(a) - Number(b));
    item.prices_seen = uniquePrices;
    item.source_files = Array.from(new Set(item.source_files));
    item.barcodes_seen = Array.from(new Set(item.barcodes_seen));

    if (uniquePrices.length > 1) {
      duplicatePriceNames.push({
        name: item.name,
        chosen_price: item.price,
        prices_seen: uniquePrices,
        source_files: item.source_files,
      });
    }
  }

  const cleaned = Array.from(byName.values()).sort((a, b) => a.name.localeCompare(b.name, "he"));
  return { cleaned, skipped, duplicatePriceNames, sourceStats };
}

function quoteCsv(value) {
  const s = String(value ?? "");
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function writeCsv(filePath, rows) {
  if (!rows.length) {
    fs.writeFileSync(filePath, "", "utf8");
    return;
  }
  const headers = Object.keys(rows[0]);
  const lines = [headers.map(quoteCsv).join(",")];
  for (const row of rows) lines.push(headers.map((h) => quoteCsv(row[h])).join(","));
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

async function getProductColumns(conn = db) {
  const [rows] = await conn.query(
    `
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'product'
    `,
  );
  return new Set(rows.map((r) => r.COLUMN_NAME));
}

async function loadExistingProducts(shopId, conn = db) {
  const [rows] = await conn.query(
    `
    SELECT id, shop_id, name, display_name_en, price, stock_amount,
           COALESCE(is_default,0) AS is_default,
           COALESCE(is_consignment,0) AS is_consignment,
           category, sub_category, emoji, barcode, updated_at
    FROM product
    WHERE shop_id = ?
    `,
    [shopId],
  );
  return rows.map((row) => ({
    ...row,
    id: Number(row.id),
    price: row.price === null || row.price === undefined ? null : Number(row.price),
    stock_amount: row.stock_amount === null || row.stock_amount === undefined ? null : Number(row.stock_amount),
    exact_key: normalizeForExact(row.name),
    loose_key: normalizeForLoose(row.name),
    tokens: tokenizeForMatch(row.name),
  }));
}

function buildExistingIndexes(existingProducts) {
  const byExact = new Map();
  const byBarcode = new Map();

  for (const p of existingProducts) {
    if (p.exact_key) {
      if (!byExact.has(p.exact_key)) byExact.set(p.exact_key, []);
      byExact.get(p.exact_key).push(p);
    }
    const barcode = cleanBarcode(p.barcode);
    if (barcode) {
      if (!byBarcode.has(barcode)) byBarcode.set(barcode, []);
      byBarcode.get(barcode).push(p);
    }
  }

  return { byExact, byBarcode };
}

function newestFirst(a, b) {
  const at = a.updated_at ? new Date(a.updated_at).getTime() : 0;
  const bt = b.updated_at ? new Date(b.updated_at).getTime() : 0;
  if (bt !== at) return bt - at;
  return b.id - a.id;
}

function findBestFuzzyMatch(item, existingProducts, threshold, minGap) {
  const scored = [];
  for (const p of existingProducts) {
    if (!isFuzzyCategoryCompatible(item, p)) continue;
    const score = matchScore(item.name, p.name);
    if (score >= Math.max(0.88, threshold - 0.08)) {
      scored.push({ product: p, score });
    }
  }
  scored.sort((a, b) => b.score - a.score || newestFirst(a.product, b.product));

  if (!scored.length) return { decision: "none", candidates: [] };

  const top = scored[0];
  const second = scored[1] || null;
  const candidates = scored.slice(0, 5).map((x) => ({
    product_id: x.product.id,
    product_name: x.product.name,
    price: x.product.price,
    score: x.score,
  }));

  if (top.score < threshold) {
    return { decision: "review", reason: "below_auto_match_threshold", top, second, candidates };
  }
  if (second && top.score - second.score < minGap && second.score >= threshold - 0.015) {
    return { decision: "review", reason: "ambiguous_fuzzy_match", top, second, candidates };
  }

  return { decision: "match", top, second, candidates };
}

function decideActions({ cleaned, existingProducts, autoMatchThreshold, fuzzyEnabled }) {
  const { byExact, byBarcode } = buildExistingIndexes(existingProducts);

  const actions = [];
  const review = [];

  for (const item of cleaned) {
    const exactMatches = byExact.get(normalizeForExact(item.name)) || [];
    if (exactMatches.length) {
      const sorted = exactMatches.slice().sort(newestFirst);
      actions.push({
        action: "update_exact",
        item,
        products: sorted,
        match_score: 1,
        note: sorted.length > 1 ? `exact name matched ${sorted.length} existing products; all will be updated` : "exact name match",
      });
      continue;
    }

    const barcodeMatches = item.barcode ? (byBarcode.get(item.barcode) || []) : [];
    if (barcodeMatches.length === 1) {
      actions.push({
        action: "update_barcode",
        item,
        products: barcodeMatches,
        match_score: 1,
        note: "single barcode match",
      });
      continue;
    }
    if (barcodeMatches.length > 1) {
      review.push({
        action: "review",
        reason: "barcode_matches_multiple_products",
        item,
        candidates: barcodeMatches.slice().sort(newestFirst).map((p) => ({ product_id: p.id, product_name: p.name, price: p.price, score: 1 })),
      });
      continue;
    }

    if (fuzzyEnabled) {
      const fuzzy = findBestFuzzyMatch(item, existingProducts, autoMatchThreshold, DEFAULT_FUZZY_GAP);
      if (fuzzy.decision === "match") {
        actions.push({
          action: "update_fuzzy",
          item,
          products: [fuzzy.top.product],
          match_score: fuzzy.top.score,
          candidates: fuzzy.candidates,
          note: "high-confidence fuzzy match",
        });
        continue;
      }
      if (fuzzy.decision === "review" && fuzzy.candidates.length) {
        review.push({
          action: "review",
          reason: fuzzy.reason,
          item,
          candidates: fuzzy.candidates,
        });
        continue;
      }
    }

    const classification = classifyNewProductByName(item.name, item.source_type);
    actions.push({
      action: "insert_new",
      item,
      products: [],
      match_score: null,
      classification,
      note: "new consignment product",
    });
  }

  return { actions, review };
}

function updateActionRank(action) {
  if (action === "update_exact") return 3;
  if (action === "update_barcode") return 2;
  if (action === "update_fuzzy") return 1;
  return 0;
}

function mergeItemInto(target, source) {
  target.source_rows = Array.from(new Set([...(target.source_rows || []), ...(source.source_rows || [])]));
  target.source_files = Array.from(new Set([...(target.source_files || []), ...(source.source_files || [])]));
  target.prices_seen = Array.from(new Set([...(target.prices_seen || []), ...(source.prices_seen || [])]))
    .sort((a, b) => Number(a) - Number(b));
  target.barcodes_seen = Array.from(new Set([...(target.barcodes_seen || []), ...(source.barcodes_seen || [])].filter(Boolean)));
  if (!target.barcode && source.barcode) target.barcode = source.barcode;
}

function normalizeActions(actions) {
  const updateByProductId = new Map();
  const inserts = [];

  for (const action of actions) {
    if (!String(action.action || "").startsWith("update_")) {
      inserts.push(action);
      continue;
    }

    for (const product of action.products || []) {
      const key = String(product.id);
      const existing = updateByProductId.get(key);
      if (!existing) {
        updateByProductId.set(key, {
          action: action.action,
          item: { ...action.item, source_rows: [...(action.item.source_rows || [])], source_files: [...(action.item.source_files || [])], prices_seen: [...(action.item.prices_seen || [])], barcodes_seen: [...(action.item.barcodes_seen || [])] },
          products: [product],
          match_score: action.match_score,
          candidates: action.candidates || [],
          note: action.note || "",
          merged_names: new Set([action.item.name]),
          merged_action_count: 1,
        });
        continue;
      }

      existing.merged_action_count += 1;
      existing.merged_names.add(action.item.name);
      mergeItemInto(existing.item, action.item);
      if (Number(action.item.price) < Number(existing.item.price)) {
        existing.item.price = action.item.price;
        existing.item.name = action.item.name;
        existing.item.source_file = action.item.source_file;
        existing.item.source_item_code = action.item.source_item_code;
        existing.item.source_type = action.item.source_type;
      }
      if (updateActionRank(action.action) > updateActionRank(existing.action)) existing.action = action.action;
      if (Number(action.match_score || 0) > Number(existing.match_score || 0)) existing.match_score = action.match_score;
      if (!existing.candidates?.length && action.candidates?.length) existing.candidates = action.candidates;
    }
  }

  const mergedUpdates = Array.from(updateByProductId.values()).map((entry) => {
    const names = Array.from(entry.merged_names);
    if (entry.merged_action_count > 1) {
      entry.note = `merged ${entry.merged_action_count} matches to the same existing product; lowest price chosen` +
        (names.length > 1 ? ` (${names.join(" | ")})` : "");
    }
    delete entry.merged_names;
    delete entry.merged_action_count;
    return entry;
  });

  return [...mergedUpdates, ...inserts].sort((a, b) => String(a.item?.name || "").localeCompare(String(b.item?.name || ""), "he"));
}

function summarizeActions(actions, review, skipped, duplicatePriceNames, excelUniqueProducts = null) {
  const summary = {
    excel_unique_products: excelUniqueProducts ?? actions.length + review.length,
    update_exact_products: actions.filter((a) => a.action === "update_exact").reduce((sum, a) => sum + a.products.length, 0),
    update_exact_excel_rows: actions.filter((a) => a.action === "update_exact").length,
    update_barcode_excel_rows: actions.filter((a) => a.action === "update_barcode").length,
    update_fuzzy_excel_rows: actions.filter((a) => a.action === "update_fuzzy").length,
    insert_new_rows: actions.filter((a) => a.action === "insert_new").length,
    review_rows: review.length,
    skipped_rows: skipped.length,
    names_with_multiple_prices: duplicatePriceNames.length,
  };
  summary.total_db_updates = actions
    .filter((a) => a.action.startsWith("update_"))
    .reduce((sum, a) => sum + a.products.length, 0);
  return summary;
}

function actionRowsForCsv(actions, review) {
  const rows = [];

  for (const a of actions) {
    if (a.products.length) {
      for (const p of a.products) {
        rows.push({
          action: a.action,
          excel_name: a.item.name,
          excel_price: a.item.price,
          excel_barcode: a.item.barcode || "",
          existing_product_id: p.id,
          existing_product_name: p.name,
          existing_price: p.price ?? "",
          match_score: a.match_score ?? "",
          category: p.category || a.classification?.category || "",
          sub_category: p.sub_category || a.classification?.sub_category || "",
          note: a.note || "",
        });
      }
    } else {
      rows.push({
        action: a.action,
        excel_name: a.item.name,
        excel_price: a.item.price,
        excel_barcode: a.item.barcode || "",
        existing_product_id: "",
        existing_product_name: "",
        existing_price: "",
        match_score: "",
        category: a.classification?.category || "",
        sub_category: a.classification?.sub_category || "",
        note: a.note || "",
      });
    }
  }

  for (const r of review) {
    const top = r.candidates?.[0] || {};
    rows.push({
      action: "review",
      excel_name: r.item.name,
      excel_price: r.item.price,
      excel_barcode: r.item.barcode || "",
      existing_product_id: top.product_id || "",
      existing_product_name: top.product_name || "",
      existing_price: top.price ?? "",
      match_score: top.score ?? "",
      category: "",
      sub_category: "",
      note: r.reason,
    });
  }

  return rows;
}

async function updateExistingProduct(conn, product, item, productColumns) {
  const sets = ["price = ?", "stock_amount = ?", "is_consignment = 1", "updated_at = CURRENT_TIMESTAMP"];
  const params = [item.price, STOCK_AMOUNT_FOR_CONSIGNMENT];

  if (productColumns.has("barcode") && item.barcode && !cleanBarcode(product.barcode)) {
    sets.push("barcode = ?");
    params.push(item.barcode);
  }

  params.push(product.id);
  await conn.query(
    `UPDATE product SET ${sets.join(", ")} WHERE id = ? LIMIT 1`,
    params,
  );
}

async function insertProduct(conn, shopId, item, classification, productColumns) {
  const valuesByColumn = {
    shop_id: shopId,
    name: item.name,
    display_name_en: item.name,
    price: item.price,
    stock_amount: STOCK_AMOUNT_FOR_CONSIGNMENT,
    is_default: 0,
    is_consignment: 1,
    category: classification.category,
    sub_category: classification.sub_category,
    emoji: classification.emoji,
    barcode: item.barcode,
    created_at: new Date(),
    updated_at: new Date(),
  };

  const columns = ["shop_id", "name", "display_name_en", "price", "stock_amount", "is_default", "is_consignment", "category", "sub_category"];
  for (const optional of ["emoji", "barcode", "created_at", "updated_at"]) {
    if (productColumns.has(optional)) columns.push(optional);
  }

  const finalColumns = columns.filter((col) => productColumns.has(col));
  const placeholders = finalColumns.map(() => "?").join(", ");
  const params = finalColumns.map((col) => valuesByColumn[col]);

  const [result] = await conn.query(
    `INSERT INTO product (${finalColumns.map((c) => `\`${c}\``).join(", ")}) VALUES (${placeholders})`,
    params,
  );

  return Number(result.insertId || 0);
}

async function applyActions({ shopId, actions }) {
  const conn = await db.getConnection();
  const result = { updated_product_ids: [], inserted_product_ids: [] };

  try {
    await conn.beginTransaction();
    await ensureProductDefaultSchemaNow(conn);
    const productColumns = await getProductColumns(conn);

    for (const action of actions) {
      if (action.action.startsWith("update_")) {
        for (const product of action.products) {
          await updateExistingProduct(conn, product, action.item, productColumns);
          result.updated_product_ids.push(product.id);
        }
      } else if (action.action === "insert_new") {
        const id = await insertProduct(conn, shopId, action.item, action.classification, productColumns);
        result.inserted_product_ids.push(id);
      }
    }

    await conn.commit();
    return result;
  } catch (err) {
    try { await conn.rollback(); } catch {}
    throw err;
  } finally {
    conn.release();
  }
}

function ensureReportDir() {
  const dir = path.resolve(process.cwd(), "reports");
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function timestamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return [
    d.getFullYear(), pad(d.getMonth() + 1), pad(d.getDate()), "_",
    pad(d.getHours()), pad(d.getMinutes()), pad(d.getSeconds()),
  ].join("");
}

function resolveExcelFiles() {
  const repeated = getAllArgs("excel");
  if (repeated.length) {
    return repeated.flatMap((x) => x.split(",")).map((x) => x.trim()).filter(Boolean);
  }
  const single = getArg("excels", null);
  if (single) return single.split(",").map((x) => x.trim()).filter(Boolean);
  return DEFAULT_FILES;
}

async function main() {
  const shopId = toPositiveInt(getArg("shopId", "2"), 2);
  const confirm = hasFlag("confirm");
  const dryRun = hasFlag("dryRun") || !confirm;
  const fuzzyEnabled = !hasFlag("noFuzzy");
  const autoMatchThreshold = Number(getArg("autoMatchThreshold", DEFAULT_FUZZY_THRESHOLD));
  const files = resolveExcelFiles();

  if (confirm && hasFlag("dryRun")) {
    throw new Error("Use either --dryRun or --confirm, not both.");
  }

  console.log(`[consignment-import] shopId=${shopId}`);
  console.log(`[consignment-import] mode=${dryRun ? "dryRun" : "confirm"}`);
  console.log(`[consignment-import] fuzzy=${fuzzyEnabled ? "on" : "off"}, autoMatchThreshold=${autoMatchThreshold}`);
  console.log(`[consignment-import] files=${files.join(", ")}`);

  await ensureProductDefaultSchemaNow(db);

  const [[shop]] = await db.query(`SELECT id, name FROM shop WHERE id = ? LIMIT 1`, [shopId]);
  if (!shop) throw new Error(`shop_id=${shopId} was not found in table shop.`);

  const { cleaned, skipped, duplicatePriceNames, sourceStats } = cleanImportedRows(files);
  const existingProducts = await loadExistingProducts(shopId);
  const decided = decideActions({
    cleaned,
    existingProducts,
    autoMatchThreshold,
    fuzzyEnabled,
  });
  const actions = normalizeActions(decided.actions);
  const review = decided.review;

  const summary = summarizeActions(actions, review, skipped, duplicatePriceNames, cleaned.length);
  const reportDir = ensureReportDir();
  const ts = timestamp();
  const reportPath = path.join(reportDir, `consignment_import_report_${ts}.json`);
  const csvPath = path.join(reportDir, `consignment_import_actions_${ts}.csv`);
  const reviewPath = path.join(reportDir, `consignment_import_review_${ts}.csv`);

  let applyResult = null;
  if (!dryRun) {
    applyResult = await applyActions({ shopId, actions });
    console.log(`[consignment-import] DB updated: ${applyResult.updated_product_ids.length} updated, ${applyResult.inserted_product_ids.length} inserted`);

    if (!hasFlag("skipTokenRebuild")) {
      console.log(`[consignment-import] rebuilding token weights for shop_id=${shopId}`);
      await rebuildTokenWeightsForShop(shopId);
      console.log(`[consignment-import] token weights rebuilt for shop_id=${shopId}`);
    }
  }

  const report = {
    generated_at: new Date().toISOString(),
    mode: dryRun ? "dryRun" : "confirm",
    shop: { id: Number(shop.id), name: shop.name },
    options: { fuzzyEnabled, autoMatchThreshold, files },
    source_stats: sourceStats,
    summary,
    duplicate_price_names: duplicatePriceNames,
    skipped,
    actions: actions.map((a) => ({
      action: a.action,
      item: a.item,
      match_score: a.match_score,
      note: a.note,
      classification: a.classification || null,
      products: a.products.map((p) => ({
        id: p.id,
        name: p.name,
        price: p.price,
        stock_amount: p.stock_amount,
        is_consignment: Number(p.is_consignment || 0),
        is_default: Number(p.is_default || 0),
        category: p.category,
        sub_category: p.sub_category,
      })),
      candidates: a.candidates || [],
    })),
    review: review.map((r) => ({
      reason: r.reason,
      item: r.item,
      candidates: r.candidates || [],
    })),
    apply_result: applyResult,
  };

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  writeCsv(csvPath, actionRowsForCsv(actions, review));
  writeCsv(reviewPath, actionRowsForCsv([], review));

  console.log("\n[consignment-import] summary:");
  console.table(summary);
  console.log(`[consignment-import] report: ${path.relative(process.cwd(), reportPath)}`);
  console.log(`[consignment-import] actions csv: ${path.relative(process.cwd(), csvPath)}`);
  console.log(`[consignment-import] review csv: ${path.relative(process.cwd(), reviewPath)}`);

  if (dryRun) {
    console.log("\n[consignment-import] dry run only. To apply, run again with --confirm");
  } else if (review.length) {
    console.log(`\n[consignment-import] done, but ${review.length} rows were left for review and were not imported/updated.`);
  } else {
    console.log("\n[consignment-import] done.");
  }
}

main()
  .catch((err) => {
    console.error("[consignment-import] failed:", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try { await db.end(); } catch {}
  });

/*
  Marks products listed in an Excel file as default products.

  Dry run is the default. Nothing is changed unless --confirm is supplied.
  Existing default products that are not in the Excel file are not changed.

  Matching order:
  1. Barcode
  2. Exact normalized product name
  3. Unique normalized name after removing brackets and punctuation

  Usage:
    node scripts/setDefaultProductsFromExcel.js --shopId=2 --excel="data/consignment/vegetables.xlsx" --dryRun
    node scripts/setDefaultProductsFromExcel.js --shopId=2 --excel="data/consignment/vegetables.xlsx" --confirm
*/

require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const xlsx = require("xlsx");
const db = require("../config/db");
const { ensureProductDefaultSchemaNow } = require("../utilities/productDefaultSchema");

function getArg(name, fallback = null) {
  const prefix = `--${name}=`;
  const arg = process.argv.find((value) => value.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : fallback;
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
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

function cleanBarcode(value) {
  const text = cleanText(value);
  if (!text) return null;
  if (/^\d+(\.0+)?$/.test(text)) return String(Math.trunc(Number(text)));
  return text.slice(0, 64);
}

function normalizeName(value) {
  return cleanText(value)
    .toLowerCase()
    .normalize("NFKC")
    .replace(/[\u0591-\u05C7]/g, "")
    .replace(/[״"“”׳'‘’`´]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeLooseName(value) {
  return normalizeName(value)
    .replace(/\([^)]*\)/g, " ")
    .replace(/\[[^\]]*\]/g, " ")
    .replace(/[\\/|,:;._*#~+=\-–—]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function findValue(row, possibleHeaders) {
  for (const header of possibleHeaders) {
    if (Object.prototype.hasOwnProperty.call(row, header)) return row[header];
  }
  return undefined;
}

function readExcelProducts(excelPath) {
  const workbook = xlsx.readFile(excelPath, { raw: false, cellDates: false });
  const products = [];
  const skipped = [];

  for (const sheetName of workbook.SheetNames) {
    const sheet = workbook.Sheets[sheetName];
    const rows = xlsx.utils.sheet_to_json(sheet, { defval: "", raw: false });

    rows.forEach((row, index) => {
      const itemCode = cleanBarcode(
        findValue(row, ["פריט", "קוד פריט", "item", "Item"]),
      );
      const barcode = cleanBarcode(
        findValue(row, ["ברקוד", "barcode", "Barcode", "קוד ברקוד"]),
      ) || itemCode;
      const name = cleanText(
        findValue(row, ["שם פריט", "שם מוצר", "שם", "name", "Name"]),
      ).slice(0, 200);

      if (!name && !barcode) {
        skipped.push({ sheet: sheetName, row: index + 2, reason: "missing_name_and_barcode" });
        return;
      }

      products.push({
        name,
        barcode,
        sheet: sheetName,
        row: index + 2,
      });
    });
  }

  const unique = new Map();
  for (const product of products) {
    const key = product.barcode
      ? `barcode:${product.barcode}`
      : `name:${normalizeName(product.name)}`;
    if (!unique.has(key)) unique.set(key, product);
  }

  return {
    products: Array.from(unique.values()),
    rawRows: products.length,
    skipped,
  };
}

function addToIndex(index, key, product) {
  if (!key) return;
  if (!index.has(key)) index.set(key, []);
  index.get(key).push(product);
}

function buildProductIndexes(products) {
  const byBarcode = new Map();
  const byName = new Map();
  const byLooseName = new Map();

  for (const product of products) {
    addToIndex(byBarcode, cleanBarcode(product.barcode), product);
    addToIndex(byName, normalizeName(product.name), product);
    addToIndex(byLooseName, normalizeLooseName(product.name), product);
  }

  return { byBarcode, byName, byLooseName };
}

function findMatches(excelProducts, dbProducts) {
  const indexes = buildProductIndexes(dbProducts);
  const matched = [];
  const review = [];
  const unmatched = [];

  for (const excelProduct of excelProducts) {
    const barcodeMatches = excelProduct.barcode
      ? indexes.byBarcode.get(excelProduct.barcode) || []
      : [];

    if (barcodeMatches.length) {
      matched.push({ excelProduct, method: "barcode", products: barcodeMatches });
      continue;
    }

    const exactMatches = indexes.byName.get(normalizeName(excelProduct.name)) || [];
    if (exactMatches.length) {
      matched.push({ excelProduct, method: "exact_name", products: exactMatches });
      continue;
    }

    const looseMatches = indexes.byLooseName.get(normalizeLooseName(excelProduct.name)) || [];
    if (looseMatches.length === 1) {
      matched.push({ excelProduct, method: "unique_loose_name", products: looseMatches });
      continue;
    }

    if (looseMatches.length > 1) {
      review.push({
        excelProduct,
        reason: "name_matches_multiple_products",
        candidates: looseMatches.map((product) => ({
          id: product.id,
          name: product.name,
          barcode: product.barcode,
        })),
      });
      continue;
    }

    unmatched.push({ excelProduct, reason: "product_not_found" });
  }

  return { matched, review, unmatched };
}

function getUniqueMatchedProducts(matches) {
  const productsById = new Map();

  for (const match of matches) {
    for (const product of match.products) {
      if (!productsById.has(product.id)) {
        productsById.set(product.id, {
          product,
          methods: new Set(),
          excelNames: new Set(),
          excelBarcodes: new Set(),
        });
      }

      const entry = productsById.get(product.id);
      entry.methods.add(match.method);
      if (match.excelProduct.name) entry.excelNames.add(match.excelProduct.name);
      if (match.excelProduct.barcode) entry.excelBarcodes.add(match.excelProduct.barcode);
    }
  }

  return Array.from(productsById.values()).map((entry) => ({
    product: entry.product,
    methods: Array.from(entry.methods),
    excelNames: Array.from(entry.excelNames),
    excelBarcodes: Array.from(entry.excelBarcodes),
  }));
}

function timestamp() {
  const date = new Date();
  const pad = (value) => String(value).padStart(2, "0");
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}_${pad(date.getHours())}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

function writeReport(report) {
  const reportsDir = path.resolve(process.cwd(), "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const reportPath = path.join(reportsDir, `default_products_from_excel_${timestamp()}.json`);
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
  return reportPath;
}

async function main() {
  const shopId = Number(getArg("shopId", "2"));
  const excelArg = getArg("excel");
  const confirm = hasFlag("confirm");

  if (!Number.isInteger(shopId) || shopId <= 0) {
    throw new Error("Invalid --shopId");
  }
  if (!excelArg) {
    throw new Error('Missing --excel. Example: --excel="data/consignment/vegetables.xlsx"');
  }
  if (confirm && hasFlag("dryRun")) {
    throw new Error("Use either --dryRun or --confirm, not both.");
  }

  const excelPath = path.resolve(process.cwd(), excelArg);
  if (!fs.existsSync(excelPath)) {
    throw new Error(`Excel file was not found: ${excelPath}`);
  }

  console.log(`[default-products] shopId=${shopId}`);
  console.log(`[default-products] mode=${confirm ? "confirm" : "dryRun"}`);
  console.log(`[default-products] excel=${excelArg}`);

  await ensureProductDefaultSchemaNow(db);

  const [[shop]] = await db.query(
    `SELECT id, name FROM shop WHERE id = ? LIMIT 1`,
    [shopId],
  );
  if (!shop) throw new Error(`shop_id=${shopId} was not found.`);

  const excelData = readExcelProducts(excelPath);
  const [dbProducts] = await db.query(
    `
    SELECT id, name, barcode, category, sub_category,
           COALESCE(is_default, 0) AS is_default
    FROM product
    WHERE shop_id = ?
    `,
    [shopId],
  );

  const matches = findMatches(excelData.products, dbProducts);
  const matchedProducts = getUniqueMatchedProducts(matches.matched);
  const idsToUpdate = matchedProducts
    .filter((entry) => Number(entry.product.is_default || 0) !== 1)
    .map((entry) => Number(entry.product.id));

  let updatedRows = 0;
  if (confirm && idsToUpdate.length) {
    const conn = await db.getConnection();
    try {
      await conn.beginTransaction();
      await ensureProductDefaultSchemaNow(conn);

      const placeholders = idsToUpdate.map(() => "?").join(", ");
      const [result] = await conn.query(
        `
        UPDATE product
        SET is_default = 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE shop_id = ?
          AND id IN (${placeholders})
          AND COALESCE(is_default, 0) <> 1
        `,
        [shopId, ...idsToUpdate],
      );

      updatedRows = Number(result.affectedRows || 0);
      await conn.commit();
    } catch (error) {
      try { await conn.rollback(); } catch {}
      throw error;
    } finally {
      conn.release();
    }
  }

  const summary = {
    excel_rows: excelData.rawRows,
    excel_unique_products: excelData.products.length,
    matched_excel_products: matches.matched.length,
    matched_db_products: matchedProducts.length,
    already_default: matchedProducts.filter(
      (entry) => Number(entry.product.is_default || 0) === 1,
    ).length,
    products_to_update: idsToUpdate.length,
    review: matches.review.length,
    unmatched: matches.unmatched.length,
    skipped_excel_rows: excelData.skipped.length,
    db_rows_updated: updatedRows,
  };

  const report = {
    generated_at: new Date().toISOString(),
    mode: confirm ? "confirm" : "dryRun",
    shop: { id: Number(shop.id), name: shop.name },
    excel: excelArg,
    summary,
    matched_products: matchedProducts.map((entry) => ({
      product_id: Number(entry.product.id),
      product_name: entry.product.name,
      product_barcode: entry.product.barcode || null,
      category: entry.product.category || null,
      sub_category: entry.product.sub_category || null,
      was_default: Number(entry.product.is_default || 0) === 1,
      match_methods: entry.methods,
      excel_names: entry.excelNames,
      excel_barcodes: entry.excelBarcodes,
    })),
    review: matches.review,
    unmatched: matches.unmatched,
    skipped_excel_rows: excelData.skipped,
  };

  const reportPath = writeReport(report);

  console.log("\n[default-products] summary:");
  console.table(summary);
  console.log(`[default-products] report: ${path.relative(process.cwd(), reportPath)}`);

  if (!confirm) {
    console.log("\n[default-products] dry run only. Run again with --confirm to update the database.");
  } else {
    console.log(`\n[default-products] done. ${updatedRows} products were set as default.`);
  }
}

if (require.main === module) {
  main()
    .catch((error) => {
      console.error("[default-products] failed:", error);
      process.exitCode = 1;
    })
    .finally(async () => {
      try { await db.end(); } catch {}
    });
}

module.exports = {
  cleanBarcode,
  normalizeName,
  normalizeLooseName,
  findMatches,
  getUniqueMatchedProducts,
};

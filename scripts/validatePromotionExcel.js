require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const EXPECTED_HEADERS = [
  "שם מבצע",
  "תיאור מבצע",
  "סוג מבצע",
  "מוצר",
  "מקסימום מימושים להזמנה",
  "מבצע יום השוק",
  "תאריך התחלה",
  "תאריך סוף",
  "אחוז הנחה",
  "מחיר קבוע",
  "כמות במבצע",
  "מחיר כולל במבצע",
  "סכום הנחה",
  "סכום סל מינימלי",
  "דמי משלוח במבצע",
  "כמות מתנה",
  "מחיר מיוחד למוצר",
  "סטטוס",
  "שגיאות",
];

const TYPE_CONFIG = {
  "אחוז הנחה": { internal_type: "PERCENT_OFF", product_required: true },
  "מחיר קבוע": { internal_type: "FIXED_PRICE", product_required: true },
  "כמות בסכום": { internal_type: "BUNDLE", product_required: true },
  "הנחה בשקלים": { internal_type: "AMOUNT_OFF", product_required: true },
  "מבצע משלוח לפי סכום סל": { internal_type: "DELIVERY_FEE_OVERRIDE", product_required: false },
  "מתנה לפי סכום סל": { internal_type: "GIFT_PRODUCT", product_required: true },
  "מחיר מיוחד למוצר לפי סכום סל": { internal_type: "THRESHOLD_PRODUCT_FIXED_PRICE", product_required: true },
};

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function cleanText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function isBlank(value) {
  return value === null || value === undefined || cleanText(value) === "";
}

function parseNumber(value, { min = null, max = null, integer = false } = {}) {
  if (isBlank(value)) return { value: null };
  const normalized = typeof value === "string"
    ? value.replace(/₪|%/g, "").replace(/,/g, "").trim()
    : value;
  const number = Number(normalized);
  if (!Number.isFinite(number)) return { error: "חייב להיות מספר" };
  if (integer && !Number.isInteger(number)) return { error: "חייב להיות מספר שלם" };
  if (min !== null && number < min) return { error: `חייב להיות לפחות ${min}` };
  if (max !== null && number > max) return { error: `חייב להיות לכל היותר ${max}` };
  return { value: number };
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function formatDate(date) {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`;
}

function parseDate(value) {
  if (isBlank(value)) return { value: null };
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return { value: formatDate(value), comparable: new Date(value.getFullYear(), value.getMonth(), value.getDate()) };
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const parsed = XLSX.SSF.parse_date_code(value);
    if (parsed) {
      const date = new Date(parsed.y, parsed.m - 1, parsed.d);
      return { value: formatDate(date), comparable: date };
    }
  }
  const raw = cleanText(value);
  let match = raw.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!match) match = raw.match(/^(\d{1,2})[/.](\d{1,2})[/.](\d{4})$/);
  let year;
  let month;
  let day;
  if (match && raw.match(/^\d{4}-/)) {
    year = Number(match[1]);
    month = Number(match[2]);
    day = Number(match[3]);
  } else if (match) {
    day = Number(match[1]);
    month = Number(match[2]);
    year = Number(match[3]);
  } else {
    return { error: "תאריך לא תקין. יש להשתמש ב-DD/MM/YYYY" };
  }
  const date = new Date(year, month - 1, day);
  if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) {
    return { error: "תאריך לא תקין" };
  }
  return { value: formatDate(date), comparable: date };
}

function loadProducts(file, expectedShopId) {
  const payload = JSON.parse(fs.readFileSync(file, "utf8"));
  if (Number(payload.shop_id) !== Number(expectedShopId)) {
    throw new Error(`Products JSON belongs to shop ${payload.shop_id}, not shop ${expectedShopId}`);
  }
  const bySelector = new Map();
  const byId = new Map();
  const byName = new Map();
  for (const item of payload.products || []) {
    const product = {
      product_id: Number(item.product_id),
      name: cleanText(item.name),
      selector: cleanText(item.selector || `${item.name} [ID:${item.product_id}]`),
    };
    bySelector.set(product.selector, product);
    byId.set(product.product_id, product);
    if (!byName.has(product.name)) byName.set(product.name, []);
    byName.get(product.name).push(product);
  }
  return { payload, bySelector, byId, byName };
}

function resolveProduct(rawValue, catalog) {
  const value = cleanText(rawValue);
  if (!value) return null;
  if (catalog.bySelector.has(value)) return catalog.bySelector.get(value);
  const idMatch = value.match(/\[ID:(\d+)\]\s*$/i);
  if (idMatch && catalog.byId.has(Number(idMatch[1]))) return catalog.byId.get(Number(idMatch[1]));
  const exactName = catalog.byName.get(value) || [];
  if (exactName.length === 1) return exactName[0];
  return null;
}

function findHeaderRow(matrix) {
  for (let index = 0; index < Math.min(matrix.length, 20); index += 1) {
    if (cleanText(matrix[index]?.[0]) === EXPECTED_HEADERS[0]) return index;
  }
  return -1;
}

function addNumberField({ row, headerIndex, key, label, errors, output, options, required = false }) {
  const raw = row[headerIndex.get(label)];
  if (required && isBlank(raw)) {
    errors.push(`${label}: שדה חובה`);
    return;
  }
  const parsed = parseNumber(raw, options);
  if (parsed.error) errors.push(`${label}: ${parsed.error}`);
  else output[key] = parsed.value;
}

function ensureBlank(row, headerIndex, labels, errors) {
  for (const label of labels) {
    if (!isBlank(row[headerIndex.get(label)])) errors.push(`${label}: לא רלוונטי לסוג המבצע ויש להשאיר ריק`);
  }
}

function validateRow(row, excelRow, headerIndex, catalog) {
  const errors = [];
  const title = cleanText(row[headerIndex.get("שם מבצע")]);
  const description = cleanText(row[headerIndex.get("תיאור מבצע")]);
  const type = cleanText(row[headerIndex.get("סוג מבצע")]);
  const marketDay = cleanText(row[headerIndex.get("מבצע יום השוק")]);

  if (!title) errors.push("שם מבצע: שדה חובה");
  if (title.length > 255) errors.push("שם מבצע: עד 255 תווים");
  if (description.length > 1000) errors.push("תיאור מבצע: עד 1000 תווים");
  if (!TYPE_CONFIG[type]) errors.push("סוג מבצע: יש לבחור ערך מהרשימה");
  if (!["כן", "לא"].includes(marketDay)) errors.push("מבצע יום השוק: יש לבחור כן או לא");

  const start = parseDate(row[headerIndex.get("תאריך התחלה")]);
  const end = parseDate(row[headerIndex.get("תאריך סוף")]);
  if (!start.value) errors.push(`תאריך התחלה: ${start.error || "שדה חובה"}`);
  if (!end.value) errors.push(`תאריך סוף: ${end.error || "שדה חובה"}`);
  if (start.comparable && end.comparable && end.comparable < start.comparable) {
    errors.push("תאריך סוף: חייב להיות שווה או מאוחר מתאריך ההתחלה");
  }

  const maxRaw = row[headerIndex.get("מקסימום מימושים להזמנה")];
  const maxParsed = parseNumber(maxRaw, { min: 1, integer: true });
  if (maxParsed.error) errors.push(`מקסימום מימושים להזמנה: ${maxParsed.error}`);

  const config = TYPE_CONFIG[type] || {};
  const productRaw = row[headerIndex.get("מוצר")];
  const product = resolveProduct(productRaw, catalog);
  if (config.product_required && isBlank(productRaw)) errors.push("מוצר: שדה חובה בסוג המבצע שנבחר");
  if (!isBlank(productRaw) && !product) errors.push("מוצר: המוצר אינו קיים ברשימת המוצרים של הסניף");
  if (config.product_required === false && !isBlank(productRaw)) errors.push("מוצר: במבצע משלוח יש להשאיר את המוצר ריק");

  const output = {
    excel_row: excelRow,
    title,
    description: description || null,
    type,
    internal_type: config.internal_type || null,
    product_id: product?.product_id ?? null,
    product_name: product?.name ?? null,
    max_redemptions_per_order: maxParsed.value,
    is_market_day: marketDay === "כן",
    start_date: start.value,
    end_date: end.value,
    percent_off: null,
    fixed_price: null,
    bundle_qty: null,
    bundle_price: null,
    amount_off: null,
    threshold_amount: null,
    delivery_fee: null,
    gift_qty: null,
    reward_fixed_price: null,
  };

  const allSpecific = [
    "אחוז הנחה",
    "מחיר קבוע",
    "כמות במבצע",
    "מחיר כולל במבצע",
    "סכום הנחה",
    "סכום סל מינימלי",
    "דמי משלוח במבצע",
    "כמות מתנה",
    "מחיר מיוחד למוצר",
  ];

  if (type === "אחוז הנחה") {
    addNumberField({ row, headerIndex, key: "percent_off", label: "אחוז הנחה", errors, output, options: { min: 0.01, max: 100 }, required: true });
    ensureBlank(row, headerIndex, allSpecific.filter((x) => x !== "אחוז הנחה"), errors);
  } else if (type === "מחיר קבוע") {
    addNumberField({ row, headerIndex, key: "fixed_price", label: "מחיר קבוע", errors, output, options: { min: 0 }, required: true });
    ensureBlank(row, headerIndex, allSpecific.filter((x) => x !== "מחיר קבוע"), errors);
  } else if (type === "כמות בסכום") {
    addNumberField({ row, headerIndex, key: "bundle_qty", label: "כמות במבצע", errors, output, options: { min: 2, integer: true }, required: true });
    addNumberField({ row, headerIndex, key: "bundle_price", label: "מחיר כולל במבצע", errors, output, options: { min: 0 }, required: true });
    ensureBlank(row, headerIndex, allSpecific.filter((x) => !["כמות במבצע", "מחיר כולל במבצע"].includes(x)), errors);
  } else if (type === "הנחה בשקלים") {
    addNumberField({ row, headerIndex, key: "amount_off", label: "סכום הנחה", errors, output, options: { min: 0.01 }, required: true });
    ensureBlank(row, headerIndex, allSpecific.filter((x) => x !== "סכום הנחה"), errors);
  } else if (type === "מבצע משלוח לפי סכום סל") {
    addNumberField({ row, headerIndex, key: "threshold_amount", label: "סכום סל מינימלי", errors, output, options: { min: 0 }, required: true });
    addNumberField({ row, headerIndex, key: "delivery_fee", label: "דמי משלוח במבצע", errors, output, options: { min: 0 }, required: true });
    ensureBlank(row, headerIndex, allSpecific.filter((x) => !["סכום סל מינימלי", "דמי משלוח במבצע"].includes(x)), errors);
    if (maxParsed.value !== null) errors.push("מקסימום מימושים להזמנה: לא רלוונטי למבצע משלוח ויש להשאיר ריק");
  } else if (type === "מתנה לפי סכום סל") {
    addNumberField({ row, headerIndex, key: "threshold_amount", label: "סכום סל מינימלי", errors, output, options: { min: 0 }, required: true });
    addNumberField({ row, headerIndex, key: "gift_qty", label: "כמות מתנה", errors, output, options: { min: 1, integer: true }, required: true });
    ensureBlank(row, headerIndex, allSpecific.filter((x) => !["סכום סל מינימלי", "כמות מתנה"].includes(x)), errors);
    if (maxParsed.value !== null) errors.push("מקסימום מימושים להזמנה: לא רלוונטי למתנה לפי סכום סל ויש להשאיר ריק");
  } else if (type === "מחיר מיוחד למוצר לפי סכום סל") {
    addNumberField({ row, headerIndex, key: "threshold_amount", label: "סכום סל מינימלי", errors, output, options: { min: 0 }, required: true });
    addNumberField({ row, headerIndex, key: "reward_fixed_price", label: "מחיר מיוחד למוצר", errors, output, options: { min: 0 }, required: true });
    ensureBlank(row, headerIndex, allSpecific.filter((x) => !["סכום סל מינימלי", "מחיר מיוחד למוצר"].includes(x)), errors);
  }

  return { errors, output };
}

function main() {
  const shopId = Number(argValue("--shopId", process.env.PROMOTION_EXCEL_SHOP_ID || 2));
  if (!Number.isInteger(shopId) || shopId <= 0) throw new Error("shopId must be a positive integer");

  const rootDir = path.resolve(__dirname, "..");
  const inputFile = path.resolve(argValue("--file", path.join(rootDir, "data", "promotions.xlsx")));
  const productsFile = path.resolve(
    argValue("--products", path.join(rootDir, "data", `promotion_products_shop_${shopId}.json`)),
  );
  const outputFile = path.resolve(
    argValue("--out", path.join(rootDir, "data", `structured_promotions_shop_${shopId}.json`)),
  );
  const reportFile = path.resolve(
    argValue("--report", path.join(rootDir, "reports", `promotion_excel_validation_shop_${shopId}.json`)),
  );

  if (!fs.existsSync(inputFile)) throw new Error(`Excel file not found: ${inputFile}`);
  if (!fs.existsSync(productsFile)) throw new Error(`Products JSON not found: ${productsFile}`);

  const catalog = loadProducts(productsFile, shopId);
  const workbook = XLSX.readFile(inputFile, { cellDates: true, raw: true });
  const sheetName = workbook.SheetNames.includes("מבצעים") ? "מבצעים" : workbook.SheetNames[0];
  if (!sheetName) throw new Error("The workbook has no worksheets");
  const matrix = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { header: 1, defval: null, raw: true });
  const headerRowIndex = findHeaderRow(matrix);
  if (headerRowIndex < 0) throw new Error("Could not find the promotions header row");

  const headers = EXPECTED_HEADERS.map((_, index) => cleanText(matrix[headerRowIndex]?.[index]));
  const headerErrors = EXPECTED_HEADERS
    .map((expected, index) => headers[index] === expected ? null : `עמודה ${index + 1}: נדרש '${expected}', נמצא '${headers[index] || "ריק"}'`)
    .filter(Boolean);
  if (headerErrors.length) {
    throw new Error(`Excel columns were changed:\n${headerErrors.join("\n")}`);
  }
  const headerIndex = new Map(EXPECTED_HEADERS.map((header, index) => [header, index]));

  const validPromotions = [];
  const invalidRows = [];
  for (let index = headerRowIndex + 1; index < matrix.length; index += 1) {
    const row = matrix[index] || [];
    const hasContent = row.slice(0, 17).some((value) => !isBlank(value));
    if (!hasContent) continue;
    const result = validateRow(row, index + 1, headerIndex, catalog);
    if (result.errors.length) invalidRows.push({ excel_row: index + 1, errors: result.errors });
    else validPromotions.push(result.output);
  }

  const report = {
    mode: "validate_promotion_excel",
    generated_at: new Date().toISOString(),
    shop_id: shopId,
    shop_name: catalog.payload.shop_name,
    input_file: inputFile,
    products_file: productsFile,
    worksheet: sheetName,
    valid_count: validPromotions.length,
    invalid_count: invalidRows.length,
    invalid_rows: invalidRows,
  };
  fs.mkdirSync(path.dirname(reportFile), { recursive: true });
  fs.writeFileSync(reportFile, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  if (invalidRows.length) {
    console.log(JSON.stringify({ ...report, report_file: reportFile }, null, 2));
    process.exitCode = 1;
    return;
  }

  const payload = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    shop_id: shopId,
    shop_name: catalog.payload.shop_name,
    source_file: path.basename(inputFile),
    promotion_count: validPromotions.length,
    promotions: validPromotions,
  };
  fs.mkdirSync(path.dirname(outputFile), { recursive: true });
  fs.writeFileSync(outputFile, `${JSON.stringify(payload, null, 2)}\n`, "utf8");

  console.log(JSON.stringify({
    ...report,
    output_file: outputFile,
    report_file: reportFile,
  }, null, 2));
}

try {
  main();
} catch (error) {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
}

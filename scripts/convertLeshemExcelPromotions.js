require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

const DEFAULT_EXCEL = path.join(__dirname, "..", "data", "leshem_promotions.xlsx");
const DEFAULT_OUT = path.join(__dirname, "..", "data", "leshem_promotions_2026_06_14.json");

const EXCEL_FILE = path.resolve(argValue("--excel", process.env.PROMO_EXCEL_FILE || DEFAULT_EXCEL));
const OUT_FILE = path.resolve(argValue("--out", process.env.PROMO_IMPORT_DATA_FILE || DEFAULT_OUT));

function excelSerialToIsoDate(value) {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date && Number.isFinite(value.getTime())) return value.toISOString().slice(0, 10);

  if (typeof value === "number" && Number.isFinite(value)) {
    const parsed = XLSX.SSF.parse_date_code(value);
    if (parsed && parsed.y && parsed.m && parsed.d) {
      return `${parsed.y}-${String(parsed.m).padStart(2, "0")}-${String(parsed.d).padStart(2, "0")}`;
    }
  }

  const raw = String(value).trim();
  if (!raw) return null;

  const iso = raw.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (iso) return `${iso[1]}-${iso[2].padStart(2, "0")}-${iso[3].padStart(2, "0")}`;

  const il = raw.match(/^(\d{1,2})[/\.\-](\d{1,2})[/\.\-](\d{2,4})$/);
  if (il) {
    const year = il[3].length === 2 ? `20${il[3]}` : il[3];
    return `${year}-${il[2].padStart(2, "0")}-${il[1].padStart(2, "0")}`;
  }

  return raw;
}

function textOrNull(value) {
  const s = String(value ?? "").trim();
  return s ? s : null;
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const raw = String(value).trim().replace(/,/g, "");
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function findHeaderRow(rows) {
  return rows.findIndex((row) => {
    const cells = (row || []).map((cell) => String(cell ?? "").trim());
    return cells.includes("תגמול") && cells.includes("שם") && cells.includes("סוג");
  });
}

function normalizePromotions(rows) {
  const headerIndex = findHeaderRow(rows);
  if (headerIndex < 0) throw new Error("Could not find the Hebrew header row with תגמול / שם / סוג");

  const headers = rows[headerIndex].map((cell) => String(cell ?? "").trim());
  const index = new Map(headers.map((header, i) => [header, i]));
  const required = ["תגמול", "שם", "סוג", "מתאריך", "עד תאריך", "פעיל", "מהות"];
  for (const header of required) {
    if (!index.has(header)) throw new Error(`Missing required column: ${header}`);
  }

  const result = [];
  for (let i = headerIndex + 1; i < rows.length; i += 1) {
    const row = rows[i] || [];
    const rewardId = numberOrNull(row[index.get("תגמול")]);
    const title = textOrNull(row[index.get("שם")]);
    if (!rewardId || !title) continue;

    result.push({
      excel_row: i + 1,
      reward_id: rewardId,
      title,
      type: textOrNull(row[index.get("סוג")]),
      start_date: excelSerialToIsoDate(row[index.get("מתאריך")]),
      end_date: excelSerialToIsoDate(row[index.get("עד תאריך")]),
      active: textOrNull(row[index.get("פעיל")]) || "לא",
      deal_text: textOrNull(row[index.get("מהות")]),
      max_qty: index.has("מקסימום") ? numberOrNull(row[index.get("מקסימום")]) : null,
    });
  }
  return result;
}

function decodeHtmlEntities(value) {
  return String(value || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&amp;/gi, "&")
    .replace(/&#(\d+);/g, (_, code) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCodePoint(parseInt(code, 16)));
}

function htmlCellText(html) {
  return decodeHtmlEntities(
    String(html || "")
      .replace(/<br\s*\/?>/gi, " ")
      .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
      .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim(),
  );
}

function readTextFileWithBom(filePath) {
  const buffer = fs.readFileSync(filePath);
  if (buffer.length >= 2 && buffer[0] === 0xff && buffer[1] === 0xfe) {
    return buffer.toString("utf16le");
  }
  if (buffer.length >= 3 && buffer[0] === 0xef && buffer[1] === 0xbb && buffer[2] === 0xbf) {
    return buffer.toString("utf8");
  }
  return buffer.toString("utf8");
}

function looksLikeHtmlTableFile(filePath) {
  const buffer = fs.readFileSync(filePath);
  let sample;
  if (buffer.length >= 2 && buffer[0] === 0xff && buffer[1] === 0xfe) {
    sample = buffer.subarray(0, 4000).toString("utf16le");
  } else {
    sample = buffer.subarray(0, 4000).toString("utf8");
  }
  return /<\s*(html|table|tr|td)\b/i.test(sample);
}

function readHtmlTableRows(filePath) {
  const html = readTextFileWithBom(filePath);
  const rows = [];
  const trRegex = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  let trMatch;

  while ((trMatch = trRegex.exec(html))) {
    const rowHtml = trMatch[1];
    const row = [];
    const cellRegex = /<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi;
    let cellMatch;

    while ((cellMatch = cellRegex.exec(rowHtml))) {
      row.push(htmlCellText(cellMatch[1]));
    }

    if (row.length) rows.push(row);
  }

  return rows;
}

function readWorkbookRows(filePath) {
  if (looksLikeHtmlTableFile(filePath)) {
    return { sheetName: "HTML table", rows: readHtmlTableRows(filePath), reader: "html" };
  }

  const workbook = XLSX.readFile(filePath, { cellDates: false, raw: true });
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) throw new Error("The workbook has no sheets");

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: null, raw: true });
  return { sheetName, rows, reader: "xlsx" };
}

function main() {
  if (!fs.existsSync(EXCEL_FILE)) {
    throw new Error(`Excel file was not found: ${EXCEL_FILE}`);
  }

  const { sheetName, rows, reader } = readWorkbookRows(EXCEL_FILE);
  const promotions = normalizePromotions(rows);

  fs.mkdirSync(path.dirname(OUT_FILE), { recursive: true });
  fs.writeFileSync(OUT_FILE, JSON.stringify(promotions, null, 2), "utf8");

  console.log(JSON.stringify({
    excel_file: EXCEL_FILE,
    reader,
    sheet: sheetName,
    rows: rows.length,
    promotions: promotions.length,
    out_file: OUT_FILE,
  }, null, 2));
}

try {
  main();
} catch (err) {
  console.error("[convert-leshem-excel-promotions]", err);
  process.exitCode = 1;
}

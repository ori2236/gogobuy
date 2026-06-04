function envFlagEnabled(name, fallback = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw === "") return fallback;
  const value = String(raw).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on", "enabled", "enable"].includes(value)) return true;
  if (["0", "false", "no", "n", "off", "disabled", "disable"].includes(value)) return false;
  return fallback;
}

function areProductAlternativesEnabled() {
  return (
    envFlagEnabled("SUGGEST_PRODUCT_ALTERNATIVES", false) ||
    envFlagEnabled("ENABLE_PRODUCT_ALTERNATIVES", false)
  );
}

function stripWhatsappControlChars(value) {
  return String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/[*_~`]+/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function boldProductName(value) {
  const clean = stripWhatsappControlChars(value);
  return clean ? `*${clean}*` : "";
}

function formatQty(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return String(n)
    .replace(/\.0+$/, "")
    .replace(/(\.\d*?)0+$/, "$1");
}

function requestedNameFromNotFound(item = {}, isEnglish = false) {
  const he =
    item.requested_name ||
    item.name ||
    item.requested_original_user_text ||
    item.searchTerm ||
    "";
  const en =
    item.requested_output_name ||
    item.outputName ||
    item.requested_original_user_text ||
    he;
  return String(isEnglish ? en || he : he || en).trim();
}

function requestedNameFromInsufficient(item = {}, isEnglish = false) {
  const he = item.requested_name || item.matched_name || item.name || "";
  const en =
    item.requested_output_name ||
    item.matched_display_name_en ||
    item.display_name_en ||
    he;
  return String(isEnglish ? en || he : he || en).trim();
}

function buildNotSoldLine(name, isEnglish) {
  const product = boldProductName(name) || (isEnglish ? "this item" : "המוצר הזה");
  return isEnglish
    ? `• ${product} is not sold in our store right now, so I did not add it to the order.`
    : `• ${product} לא נמכר כרגע אצלנו, לכן לא הוספתי אותו להזמנה.`;
}

function buildOutOfStockLine({ name, requestedAmount, available }, isEnglish) {
  const product = boldProductName(name) || (isEnglish ? "this item" : "המוצר הזה");
  const req = formatQty(requestedAmount);
  const stock = formatQty(available);
  const hasStock = Number.isFinite(Number(available)) && Number(available) > 0;

  if (isEnglish) {
    if (hasStock && req) {
      return `• ${product} is not available in the requested quantity${stock ? ` (requested ${req}, available ${stock})` : ""}, so I did not add it to the order.`;
    }
    return `• ${product} is currently out of stock, so I did not add it to the order.`;
  }

  if (hasStock && req) {
    return `• ${product} לא זמין בכמות שביקשת${stock ? ` (התבקשה כמות ${req}, במלאי ${stock})` : ""}, לכן לא הוספתי אותו להזמנה.`;
  }
  return `• ${product} חסר כרגע במלאי, לכן לא הוספתי אותו להזמנה.`;
}

function buildUnavailableProductsBlock({ notFound = [], insufficient = [], isEnglish = false } = {}) {
  const lines = [];

  for (const item of Array.isArray(notFound) ? notFound : []) {
    const name = requestedNameFromNotFound(item, isEnglish);
    lines.push(buildNotSoldLine(name, isEnglish));
  }

  for (const item of Array.isArray(insufficient) ? insufficient : []) {
    const name = requestedNameFromInsufficient(item, isEnglish);
    lines.push(
      buildOutOfStockLine(
        {
          name,
          requestedAmount: item.requested_amount ?? item.requested,
          available: item.in_stock ?? item.available,
        },
        isEnglish,
      ),
    );
  }

  if (!lines.length) return "";

  const header = isEnglish
    ? "⚠️ A quick update about products I couldn’t add:"
    : "⚠️ עדכון קטן לגבי מוצרים שלא נוספו:";

  return [header, ...lines].join("\n");
}

function buildModifyNotSoldLine(name, isEnglish) {
  const product = boldProductName(name) || (isEnglish ? "this item" : "המוצר הזה");
  return isEnglish
    ? `• ${product} is not sold in our store right now, so I did not add it to the order.`
    : `• ${product} לא נמכר כרגע אצלנו, לכן לא הוספתי אותו להזמנה.`;
}

function buildModifyOutOfStockLine({ name, requestedAmount, available, keepCurrent = false }, isEnglish) {
  const product = boldProductName(name) || (isEnglish ? "this item" : "המוצר הזה");
  const req = formatQty(requestedAmount);
  const stock = formatQty(available);
  const hasStock = Number.isFinite(Number(available)) && Number(available) > 0;
  const ending = keepCurrent
    ? isEnglish
      ? "so I kept the current quantity."
      : "לכן השארתי את הכמות הקיימת."
    : isEnglish
      ? "so I did not add it to the order."
      : "לכן לא הוספתי אותו להזמנה.";

  if (isEnglish) {
    if (hasStock && req) {
      return `• ${product} is not available in the requested quantity${stock ? ` (requested ${req}, available ${stock})` : ""}, ${ending}`;
    }
    return `• ${product} is currently out of stock, ${ending}`;
  }

  if (hasStock && req) {
    return `• ${product} לא זמין בכמות שביקשת${stock ? ` (התבקשה כמות ${req}, במלאי ${stock})` : ""}, ${ending}`;
  }
  return `• ${product} חסר כרגע במלאי, ${ending}`;
}

function buildModifyUnavailableBlock(lines, isEnglish = false) {
  const clean = (Array.isArray(lines) ? lines : []).map((x) => String(x || "").trim()).filter(Boolean);
  if (!clean.length) return "";
  return [
    isEnglish
      ? "⚠️ A quick update about changes I couldn’t apply:"
      : "⚠️ עדכון קטן לגבי שינויים שלא בוצעו:",
    ...clean,
  ].join("\n");
}

module.exports = {
  envFlagEnabled,
  areProductAlternativesEnabled,
  boldProductName,
  stripWhatsappControlChars,
  buildUnavailableProductsBlock,
  buildModifyNotSoldLine,
  buildModifyOutOfStockLine,
  buildModifyUnavailableBlock,
};

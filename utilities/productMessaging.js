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

function uniqueByName(items, pickName) {
  const seen = new Set();
  const result = [];

  for (const item of Array.isArray(items) ? items : []) {
    const name = String(pickName(item) || "").trim();
    const cleanKey = stripWhatsappControlChars(name).toLowerCase();
    if (!cleanKey || seen.has(cleanKey)) continue;
    seen.add(cleanKey);
    result.push({ item, name });
  }

  return result;
}

function joinReadableList(names, isEnglish) {
  const clean = names.map((name) => boldProductName(name)).filter(Boolean);
  if (!clean.length) return "";
  return clean.join(", ");
}

function formatStockDetails({ requestedAmount, available }, isEnglish) {
  const req = formatQty(requestedAmount);
  const stock = formatQty(available);
  const hasStock = Number.isFinite(Number(available)) && Number(available) > 0;

  if (!hasStock) return "";

  if (isEnglish) {
    if (req && stock) return ` - requested ${req}, available ${stock}`;
    if (stock) return ` - available ${stock}`;
    return "";
  }

  if (req && stock) return ` - התבקשה כמות ${req}, במלאי ${stock}`;
  if (stock) return ` - במלאי ${stock}`;
  return "";
}

function buildGroupedSection({ title, names, isEnglish }) {
  const list = joinReadableList(names, isEnglish);
  if (!list) return "";
  return `${title} ${list}`;
}

function compactIssueLine(line) {
  return String(line || "")
    .replace(/^\s*•\s*/, "")
    .replace(/\s+/g, " ")
    .trim();
}

function buildUnavailableProductsBlock({ notFound = [], insufficient = [], isEnglish = false } = {}) {
  const sections = [];

  const notSold = uniqueByName(notFound, (item) => requestedNameFromNotFound(item, isEnglish));
  if (notSold.length) {
    sections.push(
      buildGroupedSection({
        isEnglish,
        title: isEnglish
          ? "I couldn’t find these products in our store right now, so I didn’t add them to the order:"
          : "את המוצרים האלה לא מצאתי אצלנו כרגע, לכן לא הוספתי אותם להזמנה:",
        names: notSold.map((x) => x.name),
      }),
    );
  }

  const insufficientList = uniqueByName(insufficient, (item) => requestedNameFromInsufficient(item, isEnglish));
  const outOfStockNames = [];
  const partialStockLines = [];

  for (const { item, name } of insufficientList) {
    const available = item.in_stock ?? item.available;
    const hasStock = Number.isFinite(Number(available)) && Number(available) > 0;
    if (hasStock) {
      const product = boldProductName(name);
      if (product) {
        partialStockLines.push(
          `${product}${formatStockDetails(
            {
              requestedAmount: item.requested_amount ?? item.requested,
              available,
            },
            isEnglish,
          )}`,
        );
      }
    } else {
      outOfStockNames.push(name);
    }
  }

  if (outOfStockNames.length) {
    sections.push(
      buildGroupedSection({
        isEnglish,
        title: isEnglish
          ? "These products are currently out of stock, so I didn’t add them to the order:"
          : "המוצרים האלה חסרים כרגע במלאי, לכן לא הוספתי אותם להזמנה:",
        names: outOfStockNames,
      }),
    );
  }

  if (partialStockLines.length) {
    sections.push(
      `${
        isEnglish
          ? "These products are not available in the requested quantity, so I didn’t add the missing quantity:"
          : "המוצרים האלה לא זמינים בכמות שביקשת, לכן לא הוספתי את הכמות החסרה:"
      } ${partialStockLines.map(compactIssueLine).join(", ")}`,
    );
  }

  const cleanSections = sections.filter(Boolean);
  if (!cleanSections.length) return "";

  const header = isEnglish
    ? "⚠️ A quick update about products I couldn’t add:"
    : "⚠️ עדכון קטן לגבי מוצרים שלא נוספו:";

  const footer = isEnglish
    ? "If you want, send another product name or brand and I’ll check it."
    : "אם תרצה, אפשר לשלוח שם מוצר או מותג אחר ואבדוק לך בשמחה.";

  return [header, ...cleanSections, footer].join("\n\n");
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
  const clean = (Array.isArray(lines) ? lines : [])
    .map((x) => String(x || "").trim())
    .filter(Boolean);

  if (!clean.length) return "";

  const notSoldNames = [];
  const outOfStockNames = [];
  const partialStockLines = [];
  const otherLines = [];

  for (const line of clean) {
    const normalized = stripWhatsappControlChars(line);
    const nameMatch = String(line).match(/•\s*\*([^*]+)\*/);
    const name = nameMatch ? nameMatch[1].trim() : "";

    if (
      name &&
      (normalized.includes("לא נמכר כרגע") ||
        normalized.includes("is not sold in our store"))
    ) {
      notSoldNames.push(name);
      continue;
    }

    if (
      name &&
      (normalized.includes("חסר כרגע במלאי") ||
        normalized.includes("currently out of stock"))
    ) {
      outOfStockNames.push(name);
      continue;
    }

    if (
      name &&
      (normalized.includes("לא זמין בכמות") ||
        normalized.includes("not available in the requested quantity"))
    ) {
      partialStockLines.push(line);
      continue;
    }

    otherLines.push(line);
  }

  const sections = [];

  if (notSoldNames.length) {
    sections.push(
      buildGroupedSection({
        isEnglish,
        title: isEnglish
          ? "I couldn’t find these products in our store right now, so I didn’t apply this change:"
          : "את המוצרים האלה לא מצאתי אצלנו כרגע, לכן לא ביצעתי את השינוי עבורם:",
        names: notSoldNames,
      }),
    );
  }

  if (outOfStockNames.length) {
    sections.push(
      buildGroupedSection({
        isEnglish,
        title: isEnglish
          ? "These products are currently out of stock, so I didn’t apply this change:"
          : "המוצרים האלה חסרים כרגע במלאי, לכן לא ביצעתי את השינוי עבורם:",
        names: outOfStockNames,
      }),
    );
  }

  if (partialStockLines.length) {
    sections.push(
      `${
        isEnglish
          ? "These products are not available in the requested quantity, so I kept the order as close as possible:"
          : "המוצרים האלה לא זמינים בכמות שביקשת, לכן השארתי את ההזמנה הכי קרובה למה שאפשר:"
      } ${partialStockLines.map(compactIssueLine).join(", ")}`,
    );
  }

  if (otherLines.length) {
    sections.push(otherLines.map(compactIssueLine).join(", "));
  }

  return [
    isEnglish
      ? "⚠️ A quick update about changes I couldn’t apply:"
      : "⚠️ עדכון קטן לגבי שינויים שלא בוצעו:",
    ...sections.filter(Boolean),
  ].join("\n\n");
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

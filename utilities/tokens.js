function normalizeToken(value) {
  return String(value || "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[\u0591-\u05C7]/g, "")
    .replace(/['’"`\u05F3\u05F4]/g, "")
    .trim();
}

const FINAL_HEBREW_LETTERS = Object.freeze({
  ך: "כ",
  ם: "מ",
  ן: "נ",
  ף: "פ",
  ץ: "צ",
});

const HEBREW_NEGATIONS = new Set([
  "ללא",
  "בלי",
  "נטול",
  "נטולת",
  "נטולי",
  "נטולות",
]);

const ENGLISH_PREFIX_NEGATIONS = new Set(["without", "no"]);
const ENGLISH_POSTFIX_NEGATIONS = new Set(["free"]);
const STRONG_HEBREW_NEGATIONS = new Set(["ללא", "נטול", "נטולת", "נטולי", "נטולות"]);
const STRONG_ENGLISH_NEGATIONS = new Set(["without", "no", "free"]);
const NEGATION_CANONICAL = "__neg__";

const UNIT_CANONICAL = new Map([
  ["יח", "unit"],
  ["יחידה", "unit"],
  ["יחידות", "unit"],
  ["unit", "unit"],
  ["units", "unit"],
  ["pc", "unit"],
  ["pcs", "unit"],
  ["piece", "unit"],
  ["pieces", "unit"],
  ["מנה", "portion"],
  ["מנות", "portion"],
  ["portion", "portion"],
  ["portions", "portion"],
  ["חבילה", "pack"],
  ["חבילות", "pack"],
  ["מארז", "pack"],
  ["מארזים", "pack"],
  ["pack", "pack"],
  ["packs", "pack"],
  ["גר", "g"],
  ["גרם", "g"],
  ["גרמים", "g"],
  ["g", "g"],
  ["gram", "g"],
  ["grams", "g"],
  ["קג", "kg"],
  ["קילו", "kg"],
  ["קילוגרם", "kg"],
  ["קילוגרמים", "kg"],
  ["kg", "kg"],
  ["מל", "ml"],
  ["מיליליטר", "ml"],
  ["מיליליטרים", "ml"],
  ["ml", "ml"],
  ["ליטר", "l"],
  ["ליטרים", "l"],
  ["liter", "l"],
  ["liters", "l"],
  ["litre", "l"],
  ["litres", "l"],
  ["l", "l"],
  ["אחוז", "%"],
  ["אחוזים", "%"],
  ["percent", "%"],
  ["percentage", "%"],
  ["pct", "%"],
]);

const NEGATION_BOUNDARIES = new Set([
  "עם",
  "אבל",
  "וגם",
  "או",
  "and",
  "but",
  "or",
  "with",
]);

const NEGATION_MODIFIERS = new Set([
  "של",
  "תוספת",
  "תוספות",
  "added",
  "additional",
  "from",
  "of",
]);

const POSITIVE_MARKERS = new Set(["עם", "with"]);
const UNIT_TOKENS = new Set(["unit", "portion", "pack", "g", "kg", "ml", "l", "%"]);

function normalizeTokenForSimilarity(value) {
  const rawNormalized = normalizeToken(value);

  if (HEBREW_NEGATIONS.has(rawNormalized) || ENGLISH_PREFIX_NEGATIONS.has(rawNormalized)) {
    return NEGATION_CANONICAL;
  }

  const canonicalUnit = UNIT_CANONICAL.get(rawNormalized);
  if (canonicalUnit) return canonicalUnit;

  return rawNormalized
    .replace(/[ךםןףץ]/g, (character) => FINAL_HEBREW_LETTERS[character] || character)
    .replace(/(.)\1{2,}/g, "$1$1");
}

function rawTokens(value) {
  if (!value) return [];

  const normalized = String(value)
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[\u0591-\u05C7]/g, "")
    .replace(/['’"`\u05F3\u05F4]/g, "")
    .replace(/%/g, " אחוז ")
    .replace(/([0-9])([a-z\u0590-\u05FF])/gi, "$1 $2")
    .replace(/([a-z\u0590-\u05FF])([0-9])/gi, "$1 $2");

  return (normalized.match(/\d+(?:[.,]\d+)?|[a-z]+|[\u0590-\u05FF]+/gi) || [])
    .map((token) => normalizeToken(token).replace(",", "."))
    .filter(Boolean);
}

function canonicalizeSequence(tokens) {
  const result = tokens.map(normalizeTokenForSimilarity);

  for (let index = 0; index < tokens.length; index += 1) {
    const raw = normalizeToken(tokens[index]);
    if (!ENGLISH_POSTFIX_NEGATIONS.has(raw)) continue;

    const next = normalizeToken(tokens[index + 1] || "");
    if (next === "range") continue;

    const previous = normalizeToken(tokens[index - 1] || "");
    if (index > 0 || previous === "from" || next === "from") {
      result[index] = NEGATION_CANONICAL;
    }
  }

  return result;
}

function tokenizeName(value) {
  return rawTokens(value);
}

function tokenizeForMatching(value) {
  return canonicalizeSequence(rawTokens(value));
}

function isNumericToken(token) {
  return /^\d+(?:\.\d+)?$/.test(String(token || ""));
}

function isUnitToken(token) {
  return UNIT_TOKENS.has(String(token || ""));
}

function isNegationToken(token) {
  return String(token || "") === NEGATION_CANONICAL;
}

function tokenImportance(token) {
  const normalized = normalizeTokenForSimilarity(token);
  if (isNumericToken(normalized)) return 1.5;
  if (isNegationToken(normalized)) return 1.3;
  if (isUnitToken(normalized)) return 0.85;
  if (normalized.length <= 2) return 0.75;
  return 1;
}

function isHebrewToken(value) {
  return /^[\u0590-\u05FF]+$/.test(String(value || ""));
}

function stripHebrewPluralSuffix(value) {
  const token = String(value || "");
  if (!isHebrewToken(token) || token.length < 5) return token;
  if (token.endsWith("ימ") || token.endsWith("ות")) return token.slice(0, -2);
  return token;
}

function stripEnglishPluralSuffix(value) {
  const token = String(value || "");
  if (!/^[a-z]+$/.test(token) || token.length < 5) return token;
  if (token.endsWith("ies") && token.length >= 6) return `${token.slice(0, -3)}y`;
  if (token.endsWith("es") && token.length >= 6) return token.slice(0, -2);
  if (token.endsWith("s") && !token.endsWith("ss")) return token.slice(0, -1);
  return token;
}

function stripSingleHebrewPrefix(value) {
  const token = String(value || "");
  if (!isHebrewToken(token) || token.length < 6) return token;
  if (["ו", "ה"].includes(token[0])) return token.slice(1);
  return token;
}

function damerauLevenshtein(leftValue, rightValue) {
  const left = String(leftValue || "");
  const right = String(rightValue || "");
  if (left === right) return 0;
  if (!left) return right.length;
  if (!right) return left.length;

  const matrix = Array.from({ length: left.length + 1 }, () =>
    new Array(right.length + 1).fill(0),
  );

  for (let index = 0; index <= left.length; index += 1) matrix[index][0] = index;
  for (let index = 0; index <= right.length; index += 1) matrix[0][index] = index;

  for (let leftIndex = 1; leftIndex <= left.length; leftIndex += 1) {
    for (let rightIndex = 1; rightIndex <= right.length; rightIndex += 1) {
      const substitutionCost = left[leftIndex - 1] === right[rightIndex - 1] ? 0 : 1;
      matrix[leftIndex][rightIndex] = Math.min(
        matrix[leftIndex - 1][rightIndex] + 1,
        matrix[leftIndex][rightIndex - 1] + 1,
        matrix[leftIndex - 1][rightIndex - 1] + substitutionCost,
      );

      if (
        leftIndex > 1 &&
        rightIndex > 1 &&
        left[leftIndex - 1] === right[rightIndex - 2] &&
        left[leftIndex - 2] === right[rightIndex - 1]
      ) {
        matrix[leftIndex][rightIndex] = Math.min(
          matrix[leftIndex][rightIndex],
          matrix[leftIndex - 2][rightIndex - 2] + 1,
        );
      }
    }
  }

  return matrix[left.length][right.length];
}

function isSingleAdjacentSwap(left, right) {
  if (left.length !== right.length || left.length < 5) return false;
  const differences = [];

  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) differences.push(index);
    if (differences.length > 2) return false;
  }

  if (differences.length !== 2 || differences[1] !== differences[0] + 1) return false;
  const [first, second] = differences;
  return left[first] === right[second] && left[second] === right[first];
}

function tokenSimilarity(leftValue, rightValue) {
  const left = normalizeTokenForSimilarity(leftValue);
  const right = normalizeTokenForSimilarity(rightValue);
  if (!left || !right) return 0;
  if (left === right) return 1;

  if (isNumericToken(left) || isNumericToken(right)) return 0;
  if (isUnitToken(left) || isUnitToken(right)) return 0;
  if (isNegationToken(left) || isNegationToken(right)) return 0;

  const leftHebrewStem = stripHebrewPluralSuffix(left);
  const rightHebrewStem = stripHebrewPluralSuffix(right);
  if (leftHebrewStem === rightHebrewStem && leftHebrewStem.length >= 4) return 0.96;

  const leftEnglishStem = stripEnglishPluralSuffix(left);
  const rightEnglishStem = stripEnglishPluralSuffix(right);
  if (leftEnglishStem === rightEnglishStem && leftEnglishStem.length >= 4) return 0.96;

  const leftWithoutPrefix = stripSingleHebrewPrefix(leftHebrewStem);
  const rightWithoutPrefix = stripSingleHebrewPrefix(rightHebrewStem);
  if (
    leftWithoutPrefix === rightHebrewStem ||
    rightWithoutPrefix === leftHebrewStem ||
    leftWithoutPrefix === rightWithoutPrefix
  ) {
    const shared = leftWithoutPrefix === rightHebrewStem
      ? rightHebrewStem
      : rightWithoutPrefix === leftHebrewStem
        ? leftHebrewStem
        : leftWithoutPrefix;
    if (shared.length >= 4) return 0.94;
  }

  if (isSingleAdjacentSwap(left, right)) return 0.9;

  const maximumLength = Math.max(left.length, right.length);
  const minimumLength = Math.min(left.length, right.length);
  if (minimumLength < 6 || Math.abs(left.length - right.length) !== 1) return 0;
  if (left[0] !== right[0]) return 0;
  if (damerauLevenshtein(left, right) !== 1) return 0;

  return Math.max(0.86, 1 - 1 / maximumLength);
}

function findPhraseOccurrences(fieldTokens, requestedTokens) {
  const occurrences = [];
  if (!fieldTokens.length || !requestedTokens.length) return occurrences;

  for (let start = 0; start <= fieldTokens.length - requestedTokens.length; start += 1) {
    let matches = true;
    for (let offset = 0; offset < requestedTokens.length; offset += 1) {
      if (tokenSimilarity(fieldTokens[start + offset], requestedTokens[offset]) < 0.9) {
        matches = false;
        break;
      }
    }
    if (matches) occurrences.push({ start, end: start + requestedTokens.length - 1 });
  }

  return occurrences;
}

function isNegatedOccurrence(rawFieldTokens, canonicalFieldTokens, occurrence) {
  const { start, end } = occurrence;

  const nextRaw = normalizeToken(rawFieldTokens[end + 1] || "");
  if (ENGLISH_POSTFIX_NEGATIONS.has(nextRaw)) return true;

  if (
    start >= 2 &&
    normalizeToken(rawFieldTokens[start - 1]) === "from" &&
    normalizeToken(rawFieldTokens[start - 2]) === "free"
  ) {
    return true;
  }

  let ordinaryModifiers = 0;
  for (let index = start - 1; index >= 0 && start - index <= 4; index -= 1) {
    const raw = normalizeToken(rawFieldTokens[index]);
    const canonical = canonicalFieldTokens[index];

    if (NEGATION_BOUNDARIES.has(raw)) return false;
    if (canonical === NEGATION_CANONICAL) return true;
    if (!NEGATION_MODIFIERS.has(raw)) {
      ordinaryModifiers += 1;
      if (ordinaryModifiers > 1) return false;
    }
  }

  return false;
}

function extractPolarityTargets(value) {
  const raw = rawTokens(value);
  const canonical = canonicalizeSequence(raw);
  const negated = [];
  const strongNegated = [];
  const positive = [];

  for (let index = 0; index < canonical.length; index += 1) {
    if (canonical[index] === NEGATION_CANONICAL) {
      const rawMarker = normalizeToken(raw[index]);
      const isStrongMarker =
        STRONG_HEBREW_NEGATIONS.has(rawMarker) ||
        STRONG_ENGLISH_NEGATIONS.has(rawMarker);

      if (rawMarker === "free" && index > 0) {
        const previous = canonical[index - 1];
        if (previous && !isNumericToken(previous) && !isUnitToken(previous)) {
          negated.push(previous);
          strongNegated.push(previous);
        }
      }

      for (let next = index + 1; next < Math.min(canonical.length, index + 5); next += 1) {
        const rawNext = normalizeToken(raw[next]);
        const token = canonical[next];
        if (!token || isNegationToken(token) || isUnitToken(token) || isNumericToken(token)) {
          continue;
        }
        if (NEGATION_MODIFIERS.has(rawNext)) continue;
        if (NEGATION_BOUNDARIES.has(rawNext)) break;
        negated.push(token);
        if (isStrongMarker) strongNegated.push(token);
        break;
      }
    }

    const rawCurrent = normalizeToken(raw[index]);
    if (POSITIVE_MARKERS.has(rawCurrent)) {
      for (let next = index + 1; next < Math.min(canonical.length, index + 4); next += 1) {
        const token = canonical[next];
        const rawNext = normalizeToken(raw[next]);
        if (!token || isNegationToken(token) || isUnitToken(token) || isNumericToken(token)) {
          continue;
        }
        if (NEGATION_MODIFIERS.has(rawNext)) continue;
        if (NEGATION_BOUNDARIES.has(rawNext)) break;
        positive.push(token);
        break;
      }
    }
  }

  return {
    negated: Array.from(new Set(negated)),
    strongNegated: Array.from(new Set(strongNegated)),
    positive: Array.from(new Set(positive)),
  };
}

function candidateHasTargetWithPolarity(value, target, shouldBeNegated) {
  const raw = rawTokens(value);
  const canonical = canonicalizeSequence(raw);
  const occurrences = findPhraseOccurrences(canonical, [normalizeTokenForSimilarity(target)]);
  if (!occurrences.length) return false;

  return occurrences.some((occurrence) =>
    isNegatedOccurrence(raw, canonical, occurrence) === shouldBeNegated,
  );
}

function containsPositiveExcludedToken(value, excludedValue) {
  const rawField = rawTokens(value);
  const canonicalField = canonicalizeSequence(rawField);
  const canonicalExcluded = canonicalizeSequence(rawTokens(excludedValue)).filter(
    (token) => !isNegationToken(token),
  );

  if (!canonicalField.length || !canonicalExcluded.length) return false;

  const occurrences = findPhraseOccurrences(canonicalField, canonicalExcluded);
  return occurrences.some(
    (occurrence) => !isNegatedOccurrence(rawField, canonicalField, occurrence),
  );
}

function getExcludeTokensFromReq(req) {
  const raw = req?.exclude_tokens;
  if (!Array.isArray(raw)) return [];

  return Array.from(
    new Set(
      raw
        .map((value) => normalizeToken(typeof value === "string" ? value : String(value || "")))
        .filter(Boolean),
    ),
  );
}

function sanitizeExcludeTokens(excludeTokens, searchValues = []) {
  const values = Array.isArray(searchValues) ? searchValues : [searchValues];
  return (excludeTokens || []).filter((excluded) => {
    const target = normalizeTokenForSimilarity(excluded);
    return !values.some((value) => {
      const polarity = extractPolarityTargets(value);
      return polarity.negated.some((negatedTarget) => tokenSimilarity(negatedTarget, target) >= 0.9);
    });
  });
}

function filterRowsByExcludeTokens(rows, excludeTokens) {
  if (!Array.isArray(rows) || !rows.length) return rows || [];
  if (!Array.isArray(excludeTokens) || !excludeTokens.length) return rows;

  return rows.filter((row) => {
    const fields = [row?.name || "", row?.display_name_en || ""];
    return !excludeTokens.some((excluded) =>
      fields.some((field) => containsPositiveExcludedToken(field, excluded)),
    );
  });
}

module.exports = {
  NEGATION_CANONICAL,
  tokenImportance,
  tokenizeName,
  tokenizeForMatching,
  tokenSimilarity,
  damerauLevenshtein,
  normalizeToken,
  normalizeTokenForSimilarity,
  isNumericToken,
  isUnitToken,
  isNegationToken,
  extractPolarityTargets,
  candidateHasTargetWithPolarity,
  containsPositiveExcludedToken,
  getExcludeTokensFromReq,
  sanitizeExcludeTokens,
  filterRowsByExcludeTokens,
};

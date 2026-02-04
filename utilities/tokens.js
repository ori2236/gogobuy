function normalizeToken(t) {
  return String(t || "")
    .normalize("NFKC")
    .replace(/['’"]/g, "")
    .trim();
}

function normForContains(s) {
  return normalizeToken(String(s || "").toLowerCase());
}

function filterRowsByExcludeTokens(rows, excludeTokens) {
  if (!rows || !rows.length || !excludeTokens.length) return rows || [];

  const ex = excludeTokens.map(normForContains).filter(Boolean);

  return rows.filter((r) => {
    const name = normForContains(r.name || "");
    const en = normForContains(r.display_name_en || "");
    return !ex.some((t) => (t && name.includes(t)) || (t && en.includes(t)));
  });
}

const HEBREW_NOISE_TOKENS = new Set([
  "רגיל",
  "רגילה",
  "רגילים",
  "רגילות",
  "קטן",
  "קטנה",
  "קטנים",
  "קטנות",
  "גדול",
  "גדולה",
  "גדולים",
  "גדולות",
]);

const ENGLISH_NOISE_TOKENS = new Set([
  "regular",
  "normal",
  "plain",
  "classic",
  "small",
  "large",
  "big",
]);

function isNoiseToken(t) {
  return HEBREW_NOISE_TOKENS.has(t) || ENGLISH_NOISE_TOKENS.has(t);
}

function tokenImportance(token) {
  const t = String(token || "").toLowerCase();

  if (/^\d+(\.\d+)?$/.test(t)) return 0.5;

  if (/\d/.test(t)) return 0.7;

  return 1;
}

function tokenizeName(str) {
  if (!str) return [];

  const baseTokens = String(str)
    .toLowerCase()
    .replace(/[^\w\u0590-\u05FF]+/g, " ")
    .split(/\s+/)
    .map((t) => normalizeToken(t))
    .filter(Boolean);

  if (baseTokens.length <= 1) return baseTokens;

  const filtered = baseTokens.filter((t) => !isNoiseToken(t));
  return filtered.length ? filtered : baseTokens;
}

function getExcludeTokensFromReq(req) {
  const raw = req && req.exclude_tokens;
  if (!Array.isArray(raw)) return [];
  return raw
    .map((x) => normalizeToken(typeof x === "string" ? x : String(x || "")))
    .map((x) => x.toLowerCase())
    .filter(Boolean);
}

module.exports = {
  tokenImportance,
  tokenizeName,
  getExcludeTokensFromReq,
  filterRowsByExcludeTokens,
};

const assert = require("assert");
const fs = require("fs");
const path = require("path");
const Module = require("module");

const originalLoad = Module._load;
Module._load = function patchedLoad(request, parent, isMain) {
  if (request === "mysql2/promise") {
    return {
      createPool() {
        return {
          query: async () => [[], []],
          getConnection: async () => {
            throw new Error("Unexpected connection request in regression test");
          },
          end: async () => {},
        };
      },
    };
  }
  return originalLoad.call(this, request, parent, isMain);
};

const {
  tokenizeName,
  tokenizeForMatching,
  tokenSimilarity,
  containsPositiveExcludedToken,
  filterRowsByExcludeTokens,
} = require("../utilities/tokens");
const {
  buildProductSearchTerms,
  extractHardConstraintTokens,
  buildTokenMatchMeta,
  findBestByTermGroups,
  minimumMatchedTokenCount,
  tokenCoverageThreshold,
} = require("../services/products");

Module._load = originalLoad;

function row(overrides) {
  return {
    id: 1,
    name: "",
    display_name_en: null,
    price: 10,
    stock_amount: 10,
    is_default: 0,
    is_consignment: 0,
    customer_default: 0,
    has_active_promotion: 0,
    category: "Test",
    sub_category: "Primary",
    ...overrides,
  };
}

async function choose({ rows, req, primarySub = "Primary", relatedSubs = [] }) {
  return findBestByTermGroups({
    rows,
    termGroups: buildProductSearchTerms(req),
    excludeTokens: Array.isArray(req.exclude_tokens) ? req.exclude_tokens : [],
    primarySub,
    relatedSubs,
  });
}

async function main() {
  const productsSource = fs.readFileSync(
    path.join(__dirname, "../services/products.js"),
    "utf8",
  );

  const modifySource = fs.readFileSync(
    path.join(__dirname, "../categoryHandlers/ORD/MODIFY.js"),
    "utf8",
  );

  assert(
    productsSource.includes("const MATCH_DEBUG = true;"),
    "product matching logs must stay enabled in production",
  );
  assert(
    modifySource.includes("const MATCH_DEBUG = true;"),
    "modify-order matching logs must stay enabled in production",
  );
  assert(
    !productsSource.includes("stripLeadingOrderQuantity"),
    "the matcher must trust the AI-separated product name and amount",
  );

  assert(!productsSource.includes("shopWide"), "whole-shop matching must not exist");
  assert(
    !/[\u05d1][\u05d9][\u05e6][\u05d4\u05d9\u05d9\u05dd]/.test(productsSource),
    "the matcher must not contain product-specific egg logic",
  );
  assert(
    !productsSource.includes("תבנית") &&
      !productsSource.includes("מגש") &&
      !productsSource.includes("קרטון"),
    "the matcher must not contain product-specific packaging exceptions",
  );

  assert.deepStrictEqual(
    tokenizeForMatching("מגבונים ביביסיטר 9יח ללא בישום"),
    ["מגבונימ", "ביביסיטר", "9", "unit", "__neg__", "בישומ"],
    "attached digits and units must be separated and canonicalized",
  );
  assert.deepStrictEqual(
    tokenizeName("מגבונים 9יח ללא בישום"),
    ["מגבונים", "9", "יח", "ללא", "בישום"],
    "general SQL-facing tokenization must not emit internal matcher tokens",
  );
  assert.deepStrictEqual(
    tokenizeForMatching("משקה 1.5ליטר"),
    ["משקה", "1.5", "l"],
    "decimal package sizes must stay intact",
  );

  assert.deepStrictEqual(
    extractHardConstraintTokens(tokenizeForMatching("סלמון 4 מנות 500 גרם")),
    ["4", "portion", "500", "g"],
    "numeric package constraints must be paired with generic units",
  );
  assert.deepStrictEqual(
    extractHardConstraintTokens(tokenizeForMatching("2 קולה")),
    [],
    "a bare quantity must not become a hard catalog constraint",
  );
  assert(
    tokenizeForMatching("מלפפון גדול").includes("גדול"),
    "size and variant words must not be silently removed",
  );

  assert(tokenSimilarity("מלפפונים", "מלפפון") >= 0.9);
  assert(tokenSimilarity("פירכיות", "פריכיות") >= 0.9);
  assert.strictEqual(
    tokenSimilarity("חלב", "כלב"),
    0,
    "short unrelated one-letter substitutions must not fuzzy-match",
  );

  assert.strictEqual(
    containsPositiveExcludedToken("מגבונים ללא בישום", "בישום"),
    false,
  );
  assert.strictEqual(
    containsPositiveExcludedToken("מגבונים עם בישום", "בישום"),
    true,
  );
  assert.deepStrictEqual(
    filterRowsByExcludeTokens(
      [
        row({ id: 1, name: "מגבונים ללא בישום" }),
        row({ id: 2, name: "מגבונים עם בישום" }),
        row({ id: 3, name: "מגבונים רגילים" }),
      ],
      ["בישום"],
    ).map((item) => item.id),
    [1, 3],
    "exclude tokens must reject positive occurrences but keep explicit free-from products",
  );

  assert.strictEqual(minimumMatchedTokenCount(["a", "b", "c"]), 3);
  assert.strictEqual(minimumMatchedTokenCount(["a", "b", "c", "d"]), 3);
  assert.strictEqual(tokenCoverageThreshold(["a", "b", "c"]), 1);
  assert(tokenCoverageThreshold(["a", "b", "c", "d"]) < 1);

  const fourTokenMeta = buildTokenMatchMeta(
    tokenizeForMatching("סלמון פרוסות ללא עור"),
    tokenizeForMatching("פילה סלמון מנות ללא עור"),
  );
  assert(
    fourTokenMeta.matchedCount >= 3 && fourTokenMeta.coverage >= 0.74,
    "a long specific request may tolerate one non-critical wording mismatch",
  );

  const threeTokenMeta = buildTokenMatchMeta(
    tokenizeForMatching("קפה טורקי עלית"),
    tokenizeForMatching("קפה נמס עלית"),
  );
  assert(
    threeTokenMeta.coverage < 1,
    "a three-token request must not silently accept a different variant",
  );

  const genericCornflakesWithSeparatedAmount = await choose({
    rows: [
      row({
        id: 1,
        name: "קורנפלקס ללא גלוטן 500 גרם",
        category: "Pantry",
        sub_category: "Breakfast Cereal",
        price: 27.9,
      }),
      row({
        id: 2,
        name: "קורנפלקס אלופים",
        category: "Pantry",
        sub_category: "Breakfast Cereal",
        price: 14.9,
      }),
      row({
        id: 3,
        name: "קליק קורנפלקס ופצפוצים (מעיין)",
        category: "Pantry",
        sub_category: "Breakfast Cereal",
        price: 7.9,
        has_active_promotion: 1,
      }),
    ],
    req: {
      original_user_text: "4 קורנפלקס",
      name: "קורנפלקס",
      search_terms: ["קורנפלקס"],
      amount: 4,
      category: "Pantry",
      "sub-category": "Breakfast Cereal",
      exclude_tokens: [],
    },
    primarySub: "Breakfast Cereal",
  });
  assert(
    genericCornflakesWithSeparatedAmount,
    "a one-word structured product term must remain searchable when the raw text also contains an order quantity",
  );
  assert.strictEqual(
    Number(genericCornflakesWithSeparatedAmount.id),
    2,
    "a promotion must not make a more specific variant beat the simplest catalog match for a generic one-word request",
  );

  const configuredDefaultForGenericRequest = await choose({
    rows: [
      row({
        id: 20,
        name: "קורנפלקס אלופים",
        category: "Pantry",
        sub_category: "Breakfast Cereal",
        price: 14.9,
      }),
      row({
        id: 21,
        name: "קורנפלקס תלמה משפחתי 750 גרם",
        category: "Pantry",
        sub_category: "Breakfast Cereal",
        price: 24.9,
        is_default: 1,
      }),
    ],
    req: {
      original_user_text: "קורנפלקס",
      name: "קורנפלקס",
      search_terms: ["קורנפלקס"],
      amount: 1,
      category: "Pantry",
      "sub-category": "Breakfast Cereal",
      exclude_tokens: [],
    },
    primarySub: "Breakfast Cereal",
  });
  assert.strictEqual(
    Number(configuredDefaultForGenericRequest.id),
    21,
    "an explicitly configured default must still control a generic one-word request",
  );

  const genericBambaWithSeparatedAmount = await choose({
    rows: [
      row({
        id: 10,
        name: "במבה פרו 80 גרם",
        category: "Snacks",
        sub_category: "Chips & Crisps",
        price: 5.9,
      }),
      row({
        id: 11,
        name: "במבה מאנצ' צ'דר 150 גרם",
        category: "Snacks",
        sub_category: "Chips & Crisps",
        price: 13.9,
      }),
    ],
    req: {
      original_user_text: "3 במבה",
      name: "במבה",
      search_terms: ["במבה"],
      amount: 3,
      category: "Snacks",
      "sub-category": "Chips & Crisps",
      exclude_tokens: [],
    },
    primarySub: "Chips & Crisps",
  });
  assert(
    genericBambaWithSeparatedAmount,
    "a one-word structured snack name must not be rejected as overly broad merely because the raw text contains a quantity",
  );
  assert(
    String(genericBambaWithSeparatedAmount.name).includes("במבה"),
    "generic Bamba must resolve only to a Bamba product inside the bounded category",
  );

  const cucumber = await choose({
    rows: [
      row({ id: 35128, name: "מלפפון (ביכורי שדה)", is_default: 1 }),
      row({
        id: 35906,
        name: "מלפפון בייבי",
        is_default: 0,
        customer_default: 0,
        has_active_promotion: 1,
      }),
    ],
    req: {
      original_user_text: "מלפפונים",
      name: "מלפפון",
      search_terms: ["מלפפונים", "מלפפון"],
      category: "Produce",
      "sub-category": "Vegetables",
      exclude_tokens: [],
    },
    primarySub: "Vegetables",
  });
  assert.strictEqual(Number(cucumber.id), 35128);

  const salmon = await choose({
    rows: [
      row({
        id: 35444,
        name: "פילה סלמון מנות ללא עור (רוזנרס)",
        category: "Fish & Seafood",
        sub_category: "Fresh Fish",
      }),
      row({
        id: 36973,
        name: "פילה סלמון",
        is_default: 1,
        category: "Fish & Seafood",
        sub_category: "Fresh Fish",
      }),
    ],
    req: {
      original_user_text: "סלמון פרוסות ללא עור",
      name: "סלמון",
      search_terms: ["סלמון פרוסות ללא עור", "סלמון ללא עור", "סלמון"],
      category: "Fish & Seafood",
      "sub-category": "Fresh Fish",
      exclude_tokens: ["עור"],
    },
    primarySub: "Fresh Fish",
  });
  assert.strictEqual(
    Number(salmon.id),
    35444,
    "an explicit free-from candidate must beat a generic default product",
  );

  const wipes = await choose({
    rows: [
      row({
        id: 38657,
        name: "מגבונים ביביסיטר 9יח ללא בישום",
        category: "Personal Care",
        sub_category: "Baby Care",
      }),
      row({
        id: 38658,
        name: "מגבונים בניחוח פרחים עם בישום",
        is_default: 1,
        category: "Personal Care",
        sub_category: "Baby Care",
      }),
    ],
    req: {
      original_user_text: "מגבונים ללא בישום",
      name: "מגבונים",
      search_terms: ["מגבונים ללא בישום", "מגבונים"],
      category: "Personal Care",
      "sub-category": "Baby Care",
      exclude_tokens: ["בישום"],
    },
    primarySub: "Baby Care",
  });
  assert.strictEqual(Number(wipes.id), 38657);


  const wrongCategoryWipes = await choose({
    rows: [
      row({
        id: 38657,
        name: "מגבונים לחים סוויטי 64X4 יח",
        category: "Personal Care",
        sub_category: "Bath & Body",
        is_default: 1,
      }),
    ],
    req: {
      original_user_text: "מגבונים ללא בישום",
      name: "מגבונים",
      search_terms: ["מגבונים ללא בישום", "מגבונים"],
      category: "Personal Care",
      "sub-category": "Bath & Body",
      exclude_tokens: ["בישום"],
    },
    primarySub: "Bath & Body",
  });
  assert.strictEqual(
    wrongCategoryWipes,
    null,
    "a strong free-from request must return not found instead of a generic wrong product",
  );

  const genericEggWording = await choose({
    rows: [
      row({ id: 35190, name: "ביצים 30 יחידות L", is_default: 1, category: "Dairy & Eggs", sub_category: "Eggs" }),
      row({ id: 35361, name: "ביצים קינדר 3 יח", is_default: 0, category: "Dairy & Eggs", sub_category: "Eggs" }),
    ],
    req: {
      original_user_text: "תבנית ביצים",
      name: "ביצים",
      search_terms: ["תבנית ביצים", "ביצים"],
      category: "Dairy & Eggs",
      "sub-category": "Eggs",
      exclude_tokens: [],
    },
    primarySub: "Eggs",
  });
  assert.strictEqual(
    Number(genericEggWording.id),
    35190,
    "generic model search terms and the configured default must handle wording differences without a product-specific rule",
  );

  const colaWithoutLemon = await choose({
    rows: [
      row({ id: 1, name: "קוקה קולה זירו", category: "Beverages", sub_category: "Soft Drinks" }),
      row({ id: 2, name: "קוקה קולה זירו לימון", category: "Beverages", sub_category: "Soft Drinks" }),
    ],
    req: {
      original_user_text: "קולה בלי לימון",
      name: "קוקה קולה",
      search_terms: ["קולה בלי לימון", "קוקה קולה"],
      category: "Beverages",
      "sub-category": "Soft Drinks",
      exclude_tokens: ["לימון"],
    },
    primarySub: "Soft Drinks",
  });
  assert.strictEqual(
    Number(colaWithoutLemon.id),
    1,
    "when no explicit free-from name exists, a generic product without the rejected variant may match",
  );

  const hallucinatedNegativeVariant = await choose({
    rows: [
      row({ id: 1, name: "מגבונים רגילים", is_default: 1 }),
      row({ id: 2, name: "מגבונים ללא בישום" }),
    ],
    req: {
      original_user_text: "מגבונים",
      name: "מגבונים",
      search_terms: ["מגבונים ללא בישום"],
      category: "Baby",
      "sub-category": "Primary",
      exclude_tokens: [],
    },
  });
  assert.strictEqual(
    Number(hallucinatedNegativeVariant.id),
    1,
    "a model-generated alternative must not invent a mandatory negative attribute",
  );

  const noSearchTerms = await choose({
    rows: [row({ id: 1, name: "מוצר ברירת מחדל", is_default: 1 })],
    req: {
      original_user_text: null,
      name: null,
      search_terms: [],
      category: "Test",
      "sub-category": "Primary",
      exclude_tokens: [],
    },
  });
  assert.strictEqual(
    noSearchTerms,
    null,
    "an empty request must never return an arbitrary default product",
  );

  const exactOutsidePrimary = await choose({
    rows: [
      row({ id: 1, name: "פריכיות תירס", sub_category: "Primary", is_default: 1 }),
      row({ id: 2, name: "פריכיות אורז", sub_category: "Other" }),
    ],
    req: {
      original_user_text: "פריכיות אורז",
      name: "פריכיות אורז",
      search_terms: ["פריכיות אורז"],
      category: "Snacks",
      "sub-category": "Primary",
      exclude_tokens: [],
    },
    primarySub: "Primary",
  });
  assert.strictEqual(
    Number(exactOutsidePrimary.id),
    2,
    "semantic quality must beat a wrong model sub-category",
  );

  const mixedLanguageExactName = await choose({
    rows: [
      row({ id: 1, name: "גומי HARIBO", display_name_en: "Gummy candy" }),
      row({ id: 2, name: "גומי אחר", display_name_en: "Other gummy" }),
    ],
    req: {
      original_user_text: "גומי HARIBO",
      name: "גומי HARIBO",
      search_terms: ["גומי HARIBO"],
      category: "Snacks",
      "sub-category": "Primary",
      exclude_tokens: [],
    },
  });
  assert.strictEqual(
    Number(mixedLanguageExactName.id),
    1,
    "mixed Hebrew-English catalog names must be searchable by their exact text",
  );

  const packageSize = await choose({
    rows: [
      row({ id: 1, name: "משקה קולה 1.5 ליטר", category: "Beverages" }),
      row({ id: 2, name: "משקה קולה 500 מל", is_default: 1, category: "Beverages" }),
    ],
    req: {
      original_user_text: "קולה 1.5 ליטר",
      name: "קולה",
      search_terms: ["קולה 1.5 ליטר", "קולה"],
      category: "Beverages",
      "sub-category": "Primary",
      exclude_tokens: [],
    },
  });
  assert.strictEqual(
    Number(packageSize.id),
    1,
    "explicit numeric package constraints must not be overridden by defaults",
  );

  console.log("product matching regression checks passed");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

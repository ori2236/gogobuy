/*
  Translate products in product_import_staging into English using Azure OpenAI.

  Safe behavior:
  - Reads only rows from product_import_staging for one import_batch_id.
  - Writes translation back to product_import_staging.
  - Does NOT insert/update product.

  Usage from project root:
    node scripts/translateStagingProducts.js --batchId=leshem_2026_05_27
    node scripts/translateStagingProducts.js --batchId=leshem_2026_05_27 --limit=100 --batchSize=10
    node scripts/translateStagingProducts.js --batchId=leshem_2026_05_27 --dryRun

  Re-run problematic translations:
    node scripts/translateStagingProducts.js --batchId=leshem_2026_05_27 --translationStatus=needs_review --batchSize=10

  Supported translation statuses:
    pending
    translated
    needs_review
*/

require("dotenv").config();

const crypto = require("crypto");
const { OpenAI } = require("openai");
const db = require("../config/db");

function getArg(name, defaultValue = null) {
  const prefix = `--${name}=`;
  const arg = process.argv.find((x) => x.startsWith(prefix));
  if (!arg) return defaultValue;
  return arg.slice(prefix.length);
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function parseJsonFromText(text) {
  if (!text || typeof text !== "string") {
    throw new Error("Empty model response");
  }

  const trimmed = text.trim();

  try {
    return JSON.parse(trimmed);
  } catch (_) {
    const start = trimmed.indexOf("{");
    const end = trimmed.lastIndexOf("}");

    if (start === -1 || end === -1 || end <= start) {
      throw new Error("Model response is not JSON");
    }

    return JSON.parse(trimmed.slice(start, end + 1));
  }
}

function clampConfidence(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function normalizeTranslationStatus(status) {
  const value = String(status || "pending").trim();

  const allowed = new Set(["pending", "translated", "needs_review"]);

  if (!allowed.has(value)) {
    throw new Error(
      `Invalid --translationStatus=${value}. Allowed: pending, translated, needs_review`,
    );
  }

  return value;
}

function makeAzureClient() {
  const endpoint = (process.env.AZURE_OPENAI_ENDPOINT || "").replace(
    /\/+$/,
    "",
  );
  const apiKey = process.env.AZURE_OPENAI_KEY;
  const deployment =
    process.env.AZURE_OPENAI_DEPLOYMENT_MAIN ||
    process.env.AZURE_OPENAI_DEPLOYMENT;
  const apiVersion =
    process.env.AZURE_OPENAI_API_VERSION_MAIN ||
    process.env.AZURE_OPENAI_API_VERSION;

  if (!endpoint || !apiKey || !deployment || !apiVersion) {
    throw new Error(
      "Missing Azure OpenAI env vars. Need AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT_MAIN/AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION_MAIN/AZURE_OPENAI_API_VERSION.",
    );
  }

  const client = new OpenAI({
    apiKey,
    baseURL: `${endpoint}/openai/deployments/${deployment}`,
    defaultHeaders: { "api-key": apiKey },
    defaultQuery: { "api-version": apiVersion },
  });

  return { client, deployment };
}

const SYSTEM_PROMPT = `You translate Israeli supermarket product names into clean English display names.
Return JSON only. Do not explain outside JSON.

TASK
For each product, create one English display name for product_import_staging.display_name_en.

OUTPUT FORMAT
Return exactly:
{
  "items": [
    {
      "row_id": 123,
      "display_name_en": "English product name",
      "confidence": 0.92,
      "reason": "short reason"
    }
  ]
}

RULES
- Include every input product exactly once.
- row_id must match an input row_id.
- display_name_en must be English only, concise, and suitable for a supermarket product page.
- Preserve brand names when clear. Transliterate Hebrew brand names if needed.
- Preserve product size, quantity, weight, volume and unit.
- Convert common Hebrew units:
  - ק"ג / קילו -> kg
  - גרם / גר' -> g
  - ליטר -> L
  - מ"ל -> ml
  - יח / יח' -> units
  - מידה -> size
- Keep useful descriptors: frozen, fresh, sliced, whole, diet, zero, spicy, sweet, family pack, etc.
- Do not add marketing text.
- Do not invent details that are not in the Hebrew name.
- If the name is unclear, produce the best short English transliteration/translation and set confidence below 0.75.
- If the Hebrew name contains only a brand/code/ambiguous phrase, keep a clean transliteration and set low confidence.
- Do not include category names unless they clarify the product.
- Do not use quotes unless part of a brand name.
- Keep names under 120 characters when possible.

EXAMPLES
- "חלה מתוקה ויזניץ" -> "Vizhnitz Sweet Challah"
- "ביצים M קדומים 30 יח" -> "Kedumim Eggs Size M, 30 Units"
- "גזר מגורד 1 ק\\"ג חסלט" -> "Hasalat Grated Carrots 1 kg"
- "פילה אמנון עם עור" -> "Tilapia Fillet with Skin"
- "בורגול דק 5 ק\\"ג שמש" -> "Shemesh Fine Bulgur 5 kg"
- "סטרצ' משטחים 3 ק\\"ג 17 מיקרון" -> "Pallet Stretch Film 3 kg, 17 Micron"`;

async function loadRows(batchId, translationStatus, limit) {
  const params = [batchId, translationStatus];

  let limitSql = "";
  if (limit && Number(limit) > 0) {
    limitSql = "LIMIT ?";
    params.push(Number(limit));
  }

  const [rows] = await db.query(
    `
    SELECT
      s.id,
      s.barcode,
      s.name,
      s.display_name_en,
      s.translation_status,
      s.translation_confidence,
      s.translation_reason,
      s.ai_subcategory_id,
      COALESCE(pc.name, s.ai_category) AS category,
      COALESCE(ps.name, s.ai_sub_category) AS sub_category
    FROM product_import_staging s
    LEFT JOIN product_subcategory ps ON ps.id = s.ai_subcategory_id
    LEFT JOIN product_category pc ON pc.id = ps.category_id
    WHERE s.import_batch_id = ?
      AND s.translation_status = ?
      AND s.name IS NOT NULL
      AND TRIM(s.name) <> ''
    ORDER BY s.id ASC
    ${limitSql}
    `,
    params,
  );

  return rows.map((r) => ({
    row_id: Number(r.id),
    barcode: String(r.barcode || ""),
    name_he: String(r.name || ""),
    previous_display_name_en: r.display_name_en || null,
    previous_translation_status: String(r.translation_status || ""),
    previous_translation_confidence:
      r.translation_confidence === null ||
      r.translation_confidence === undefined
        ? null
        : Number(r.translation_confidence),
    previous_translation_reason: r.translation_reason || null,
    category: r.category || null,
    sub_category: r.sub_category || null,
  }));
}

async function callModel({ client, deployment, products }) {
  const userPayload = {
    PRODUCTS: products.map((p) => ({
      row_id: p.row_id,
      barcode: p.barcode,
      name_he: p.name_he,
      category: p.category,
      sub_category: p.sub_category,
      previous_display_name_en: p.previous_display_name_en,
      previous_translation_status: p.previous_translation_status,
      previous_translation_confidence: p.previous_translation_confidence,
      previous_translation_reason: p.previous_translation_reason,
    })),
  };

  const response = await client.chat.completions.create(
    {
      model: deployment,
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        { role: "user", content: JSON.stringify(userPayload) },
      ],
      response_format: {
        type: "json_schema",
        json_schema: {
          name: "product_translation_batch",
          strict: true,
          schema: {
            type: "object",
            additionalProperties: false,
            properties: {
              items: {
                type: "array",
                items: {
                  type: "object",
                  additionalProperties: false,
                  properties: {
                    row_id: { type: "integer" },
                    display_name_en: { type: "string" },
                    confidence: { type: "number" },
                    reason: { type: "string" },
                  },
                  required: [
                    "row_id",
                    "display_name_en",
                    "confidence",
                    "reason",
                  ],
                },
              },
            },
            required: ["items"],
          },
        },
      },
    },
    {
      headers: { "X-Client-Request-Id": crypto.randomUUID() },
    },
  );

  const text = response?.choices?.[0]?.message?.content || "";
  return parseJsonFromText(text);
}

function cleanEnglishName(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .trim()
    .slice(0, 255);
}

function looksLikeUsefulEnglishName(value) {
  const text = cleanEnglishName(value);

  if (!text) return false;
  if (text.length < 2) return false;

  const latinLetters = (text.match(/[A-Za-z]/g) || []).length;
  const hebrewLetters = (text.match(/[\u0590-\u05FF]/g) || []).length;

  if (latinLetters < 2) return false;
  if (hebrewLetters > 0) return false;

  return true;
}

async function updateRows({
  translations,
  products,
  dryRun,
  sourceTranslationStatus,
}) {
  const productIds = new Set(products.map((p) => p.row_id));
  const byRowId = new Map();

  for (const item of translations.items || []) {
    const rowId = Number(item.row_id);
    if (!productIds.has(rowId)) continue;
    byRowId.set(rowId, item);
  }

  const missing = products.filter((p) => !byRowId.has(p.row_id));

  if (missing.length > 0) {
    console.warn(
      `[warn] Missing model results for ${missing.length} rows: ${missing
        .map((p) => p.row_id)
        .join(", ")}`,
    );
  }

  if (dryRun) {
    for (const product of products) {
      const item = byRowId.get(product.row_id);

      console.log(
        JSON.stringify(
          {
            product,
            translation: item || null,
          },
          null,
          2,
        ),
      );
    }

    return { updated: 0, missing: missing.length };
  }

  const conn = await db.getConnection();

  try {
    await conn.beginTransaction();

    let updated = 0;

    for (const product of products) {
      const item = byRowId.get(product.row_id);

      if (!item) {
        await conn.query(
          `
          UPDATE product_import_staging
          SET translation_status = 'needs_review',
              translation_reason = ?,
              updated_at = CURRENT_TIMESTAMP(6)
          WHERE id = ?
          `,
          [
            `AI did not return this row while translating from ${sourceTranslationStatus}`,
            product.row_id,
          ],
        );
        continue;
      }

      const displayNameEn = cleanEnglishName(item.display_name_en);
      const confidence = clampConfidence(item.confidence);
      const reason = String(item.reason || "").slice(0, 255);
      const validName = looksLikeUsefulEnglishName(displayNameEn);

      const translationStatus =
        validName && confidence >= 0.7 ? "translated" : "needs_review";

      await conn.query(
        `
        UPDATE product_import_staging
        SET display_name_en = ?,
            translation_confidence = ?,
            translation_reason = ?,
            translation_raw_json = CAST(? AS JSON),
            translation_status = ?,
            updated_at = CURRENT_TIMESTAMP(6)
        WHERE id = ?
        `,
        [
          validName ? displayNameEn : null,
          confidence,
          reason,
          JSON.stringify(item),
          translationStatus,
          product.row_id,
        ],
      );

      updated += 1;
    }

    await conn.commit();

    return { updated, missing: missing.length };
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

async function markFailedBatchAsNeedsReview({
  ids,
  maxRetries,
  sourceTranslationStatus,
}) {
  if (!ids || ids.length === 0) return;

  await db.query(
    `
    UPDATE product_import_staging
    SET translation_status = 'needs_review',
        translation_reason = ?,
        updated_at = CURRENT_TIMESTAMP(6)
    WHERE id IN (?)
    `,
    [
      `AI translation failed after ${maxRetries} retries from status ${sourceTranslationStatus}`,
      ids,
    ],
  );
}

async function printSummary(batchId) {
  const [summary] = await db.query(
    `
    SELECT translation_status, COUNT(*) AS rows_count
    FROM product_import_staging
    WHERE import_batch_id = ?
    GROUP BY translation_status
    ORDER BY translation_status
    `,
    [batchId],
  );

  console.table(summary);

  const [readySummary] = await db.query(
    `
    SELECT
      SUM(CASE WHEN status IN ('classified', 'approved') THEN 1 ELSE 0 END) AS category_ready,
      SUM(CASE WHEN status = 'needs_review' THEN 1 ELSE 0 END) AS category_needs_review,
      SUM(CASE WHEN status = 'raw' THEN 1 ELSE 0 END) AS category_remaining,
      SUM(CASE WHEN translation_status = 'translated' THEN 1 ELSE 0 END) AS translated,
      SUM(CASE WHEN translation_status = 'needs_review' THEN 1 ELSE 0 END) AS translation_needs_review,
      SUM(CASE WHEN translation_status = 'pending' THEN 1 ELSE 0 END) AS translation_remaining,
      SUM(
        CASE
          WHEN status IN ('classified', 'approved')
           AND translation_status = 'translated'
          THEN 1 ELSE 0
        END
      ) AS ready_for_product
    FROM product_import_staging
    WHERE import_batch_id = ?
    `,
    [batchId],
  );

  console.table(readySummary);
}

async function main() {
  const batchId = getArg("batchId", "leshem_2026_05_27");
  const translationStatus = normalizeTranslationStatus(
    getArg("translationStatus", "pending"),
  );
  const limit = Number(getArg("limit", "0"));
  const batchSize = Number(getArg("batchSize", "35"));
  const dryRun = hasFlag("dryRun");
  const delayMs = Number(getArg("delayMs", "700"));
  const maxRetries = Number(getArg("retries", "3"));

  if (!batchId) throw new Error("Missing --batchId");

  if (!Number.isFinite(batchSize) || batchSize < 1 || batchSize > 80) {
    throw new Error("batchSize must be between 1 and 80");
  }

  if (!Number.isFinite(limit) || limit < 0) {
    throw new Error("limit must be 0 or a positive number");
  }

  if (!Number.isFinite(delayMs) || delayMs < 0) {
    throw new Error("delayMs must be 0 or a positive number");
  }

  if (!Number.isFinite(maxRetries) || maxRetries < 1 || maxRetries > 10) {
    throw new Error("retries must be between 1 and 10");
  }

  const { client, deployment } = makeAzureClient();

  const rows = await loadRows(batchId, translationStatus, limit);

  console.log(
    `[start] batchId=${batchId}, translationStatus=${translationStatus}, rows=${rows.length}, batchSize=${batchSize}, dryRun=${dryRun}`,
  );
  console.log(`[model] deployment=${deployment}`);

  if (rows.length === 0) {
    console.log(
      `No rows with translation_status "${translationStatus}" to translate.`,
    );
    await printSummary(batchId);
    return;
  }

  const chunks = chunkArray(rows, batchSize);
  let totalUpdated = 0;

  for (let i = 0; i < chunks.length; i += 1) {
    const products = chunks[i];

    let attempt = 0;
    let done = false;

    while (!done && attempt < maxRetries) {
      attempt += 1;

      try {
        console.log(
          `[batch ${i + 1}/${chunks.length}] rows ${products[0].row_id}-${
            products[products.length - 1].row_id
          }, attempt ${attempt}`,
        );

        const translations = await callModel({
          client,
          deployment,
          products,
        });

        const result = await updateRows({
          translations,
          products,
          dryRun,
          sourceTranslationStatus: translationStatus,
        });

        totalUpdated += result.updated;
        done = true;
      } catch (err) {
        console.error(
          `[batch ${i + 1}] failed attempt ${attempt}:`,
          err.message,
        );

        if (attempt >= maxRetries) {
          if (!dryRun) {
            const ids = products.map((p) => p.row_id);

            await markFailedBatchAsNeedsReview({
              ids,
              maxRetries,
              sourceTranslationStatus: translationStatus,
            });
          }
        } else {
          await sleep(1500 * attempt);
        }
      }
    }

    if (delayMs > 0) await sleep(delayMs);
  }

  console.log(`[done] total updated=${totalUpdated}`);
  await printSummary(batchId);
}

main()
  .catch((err) => {
    console.error("[fatal]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await db.end();
    } catch (_) {}
  });

/*
  Classify products in product_import_staging using Azure OpenAI.

  Safe behavior:
  - Reads only rows from product_import_staging for one import_batch_id.
  - Writes AI classification back to product_import_staging.
  - Does NOT insert/update product.

  Usage from project root:
    node scripts/classifyStagingProducts.js --batchId=leshem_2026_05_27
    node scripts/classifyStagingProducts.js --batchId=leshem_2026_05_27 --limit=100 --batchSize=10
    node scripts/classifyStagingProducts.js --batchId=leshem_2026_05_27 --dryRun

  New status mode:
    node scripts/classifyStagingProducts.js --batchId=leshem_2026_05_27 --status=needs_review --batchSize=10
    node scripts/classifyStagingProducts.js --batchId=leshem_2026_05_27 --status=needs_review --limit=20 --batchSize=10 --dryRun

  Supported statuses:
    raw
    classified
    needs_review
    approved
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
  for (let i = 0; i < arr.length; i += size)
    chunks.push(arr.slice(i, i + size));
  return chunks;
}

function parseJsonFromText(text) {
  if (!text || typeof text !== "string")
    throw new Error("Empty model response");

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

function normalizeStatus(status) {
  const value = String(status || "raw").trim();

  const allowed = new Set(["raw", "classified", "needs_review", "approved"]);

  if (!allowed.has(value)) {
    throw new Error(
      `Invalid --status=${value}. Allowed: raw, classified, needs_review, approved`,
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

const SYSTEM_PROMPT = `You classify supermarket products into the allowed Gogobuy taxonomy.
Return JSON only. Do not explain outside JSON.

TASK
For each product, choose exactly one allowed subcategory id.
Use the product name as the main evidence.
Do not invent new categories or subcategories.
If uncertain, still choose the closest allowed subcategory, but set confidence below 0.85.

IMPORTANT
Some subcategory names appear under more than one category, for example:
- Fruits: Produce vs Frozen
- Vegetables: Produce vs Frozen
- Cookies & Biscuits: Bakery vs Snacks
- Canned Fish: Fish & Seafood vs Pantry

Therefore you must return subcategory_id, not only subcategory name.

OUTPUT FORMAT
Return exactly:
{
  "items": [
    {
      "row_id": 123,
      "subcategory_id": 95,
      "confidence": 0.92,
      "reason": "short Hebrew or English reason"
    }
  ]
}

RULES
- Include every input product exactly once.
- row_id must match an input row_id.
- subcategory_id must be one of ALLOWED_SUBCATEGORIES ids.
- confidence must be a number from 0 to 1.
- reason must be short.
- Do not classify cigarettes/tobacco into food. Use the closest non-food household/personal/home category only if the taxonomy lacks a better option and use low confidence.
- For baby formula / מטרנה / סימילאק, use Baby > Formula.
- For vitamins, supplements and medicines, use Health & Wellness.
- For disposable plates/cups/cutlery, use Home & Leisure > Disposable Tableware.
- For fresh bakery breads/challah/rolls, use Bakery.
- For frozen dough, frozen jachnun/malawach/bourekas, use Frozen > Pizza & Dough or Frozen > Ready Meals depending on product.
- For ready salads/spreads like hummus, tahini salads, matbucha, use Deli & Ready Meals > Salads & Spreads unless it is a shelf-stable jar/can.
- If a product is a store/industrial/private tool, decoration, seasonal item, packaging item, office item, Judaica item, toy, or non-supermarket special item, choose the closest allowed non-food category and use low confidence if uncertain.
- Prefer low confidence over a confident wrong answer.
- If a newly added category or subcategory is a better match, use it.`;

async function loadAllowedSubcategories() {
  const [rows] = await db.query(`
    SELECT
      ps.id,
      pc.name AS category,
      ps.name AS subcategory
    FROM product_subcategory ps
    JOIN product_category pc ON pc.id = ps.category_id
    ORDER BY pc.sort_order, ps.sort_order, ps.id
  `);

  return rows.map((r) => ({
    id: Number(r.id),
    category: r.category,
    subcategory: r.subcategory,
  }));
}

async function loadRows(batchId, status, limit) {
  const params = [batchId, status];

  let limitSql = "";
  if (limit && Number(limit) > 0) {
    limitSql = "LIMIT ?";
    params.push(Number(limit));
  }

  const [rows] = await db.query(
    `
    SELECT id, barcode, name, status, ai_subcategory_id, ai_confidence, ai_reason
    FROM product_import_staging
    WHERE import_batch_id = ?
      AND status = ?
    ORDER BY id ASC
    ${limitSql}
    `,
    params,
  );

  return rows.map((r) => ({
    row_id: Number(r.id),
    barcode: String(r.barcode || ""),
    name: String(r.name || ""),
    previous_status: String(r.status || ""),
    previous_ai_subcategory_id:
      r.ai_subcategory_id === null || r.ai_subcategory_id === undefined
        ? null
        : Number(r.ai_subcategory_id),
    previous_ai_confidence:
      r.ai_confidence === null || r.ai_confidence === undefined
        ? null
        : Number(r.ai_confidence),
    previous_ai_reason: r.ai_reason || null,
  }));
}

async function callModel({
  client,
  deployment,
  allowedSubcategories,
  products,
}) {
  const userPayload = {
    ALLOWED_SUBCATEGORIES: allowedSubcategories,
    PRODUCTS: products.map((p) => ({
      row_id: p.row_id,
      barcode: p.barcode,
      name: p.name,
      previous_status: p.previous_status,
      previous_ai_subcategory_id: p.previous_ai_subcategory_id,
      previous_ai_confidence: p.previous_ai_confidence,
      previous_ai_reason: p.previous_ai_reason,
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
          name: "product_classification_batch",
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
                    subcategory_id: { type: "integer" },
                    confidence: { type: "number" },
                    reason: { type: "string" },
                  },
                  required: [
                    "row_id",
                    "subcategory_id",
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

async function updateRows({
  classifications,
  products,
  allowedIds,
  dryRun,
  sourceStatus,
}) {
  const productIds = new Set(products.map((p) => p.row_id));
  const byRowId = new Map();

  for (const item of classifications.items || []) {
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
            classification: item || null,
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
          SET status = 'needs_review',
              review_note = ?,
              updated_at = CURRENT_TIMESTAMP(6)
          WHERE id = ?
          `,
          [
            `AI did not return this row while reclassifying from ${sourceStatus}`,
            product.row_id,
          ],
        );
        continue;
      }

      const subcategoryId = Number(item.subcategory_id);
      const confidence = clampConfidence(item.confidence);
      const reason = String(item.reason || "").slice(0, 255);
      const validSubcategory = allowedIds.has(subcategoryId);

      const status =
        validSubcategory && confidence >= 0.65 ? "classified" : "needs_review";

      const reviewNote = validSubcategory
        ? null
        : `Invalid subcategory_id from AI: ${item.subcategory_id}`;

      await conn.query(
        `
        UPDATE product_import_staging
        SET ai_subcategory_id = ?,
            ai_confidence = ?,
            ai_reason = ?,
            ai_raw_json = CAST(? AS JSON),
            status = ?,
            review_note = ?,
            updated_at = CURRENT_TIMESTAMP(6)
        WHERE id = ?
        `,
        [
          validSubcategory ? subcategoryId : null,
          confidence,
          reason,
          JSON.stringify(item),
          status,
          reviewNote,
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

async function markFailedBatchAsNeedsReview({ ids, maxRetries, sourceStatus }) {
  if (!ids || ids.length === 0) return;

  await db.query(
    `
    UPDATE product_import_staging
    SET status = 'needs_review',
        review_note = ?,
        updated_at = CURRENT_TIMESTAMP(6)
    WHERE id IN (?)
    `,
    [
      `AI classification failed after ${maxRetries} retries from status ${sourceStatus}`,
      ids,
    ],
  );
}

async function printSummary(batchId) {
  const [summary] = await db.query(
    `
    SELECT status, COUNT(*) AS rows_count
    FROM product_import_staging
    WHERE import_batch_id = ?
    GROUP BY status
    ORDER BY status
    `,
    [batchId],
  );

  console.table(summary);
}

async function main() {
  const batchId = getArg("batchId", "leshem_2026_05_27");
  const status = normalizeStatus(getArg("status", "raw"));
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

  const allowedSubcategories = await loadAllowedSubcategories();
  const allowedIds = new Set(allowedSubcategories.map((x) => Number(x.id)));

  const rows = await loadRows(batchId, status, limit);

  console.log(
    `[start] batchId=${batchId}, status=${status}, rows=${rows.length}, batchSize=${batchSize}, dryRun=${dryRun}`,
  );
  console.log(`[model] deployment=${deployment}`);
  console.log(
    `[taxonomy] allowed subcategories=${allowedSubcategories.length}`,
  );

  if (rows.length === 0) {
    console.log(`No rows with status "${status}" to classify.`);
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

        const classifications = await callModel({
          client,
          deployment,
          allowedSubcategories,
          products,
        });

        const result = await updateRows({
          classifications,
          products,
          allowedIds,
          dryRun,
          sourceStatus: status,
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
              sourceStatus: status,
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

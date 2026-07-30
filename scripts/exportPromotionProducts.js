require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function assertPositiveInt(value, fieldName) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${fieldName} must be a positive integer`);
  }
  return parsed;
}

function cleanProductName(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

async function exportPromotionProducts({ shopId, outFile }) {
  const [shopRows] = await db.query(
    `SELECT id, name FROM shop WHERE id = ? LIMIT 1`,
    [shopId],
  );
  const shop = shopRows?.[0];
  if (!shop) throw new Error(`Shop ${shopId} was not found`);

  const [rows] = await db.query(
    `
    SELECT id, name
    FROM product
    WHERE shop_id = ?
      AND name IS NOT NULL
      AND TRIM(name) <> ''
    ORDER BY name ASC, id ASC
    `,
    [shopId],
  );

  const products = rows.map((row) => {
    const name = cleanProductName(row.name);
    const productId = Number(row.id);
    return {
      product_id: productId,
      name,
      selector: `${name} [ID:${productId}]`,
    };
  });

  const payload = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    shop_id: Number(shop.id),
    shop_name: String(shop.name || ""),
    product_count: products.length,
    products,
  };

  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return payload;
}

async function main() {
  const shopId = assertPositiveInt(
    argValue("--shopId", process.env.PROMOTION_EXCEL_SHOP_ID || 2),
    "shopId",
  );
  const outFile = path.resolve(
    argValue(
      "--out",
      path.join(__dirname, "..", "data", `promotion_products_shop_${shopId}.json`),
    ),
  );

  try {
    const payload = await exportPromotionProducts({ shopId, outFile });
    console.log(JSON.stringify({
      mode: "export_products",
      shop_id: payload.shop_id,
      shop_name: payload.shop_name,
      product_count: payload.product_count,
      out_file: outFile,
    }, null, 2));
  } finally {
    await db.end();
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || error);
    process.exitCode = 1;
  });
}

module.exports = {
  exportPromotionProducts,
};

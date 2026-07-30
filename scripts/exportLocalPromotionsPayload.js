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

const SHOP_ID = Number(argValue("--shopId", process.env.PROMO_SYNC_SHOP_ID || 2));
const OUT_FILE = path.resolve(
  argValue(
    "--out",
    path.join(__dirname, "..", "data", `promotions_prod_payload_shop${SHOP_ID}.json`)
  )
);

function qid(id) {
  return `\`${id}\``;
}

async function tableExists(conn, tableName) {
  const [rows] = await conn.query(
    `SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
    [tableName]
  );
  return Array.isArray(rows) && rows.length > 0;
}

async function fetchProductsLookup(conn, shopId) {
  const [rows] = await conn.query(
    `SELECT id, barcode, chain_product_key, name FROM product WHERE shop_id = ?`,
    [shopId]
  );
  const map = new Map();
  for (const r of rows || []) {
    map.set(Number(r.id), {
      id: Number(r.id),
      barcode: r.barcode || null,
      chain_product_key: r.chain_product_key || null,
      name: r.name || null,
    });
  }
  return map;
}

async function main() {
  console.log(`====================================================`);
  console.log(`🚀 Exporting Local Promotions Payload for Shop ID: ${SHOP_ID}`);
  console.log(`====================================================`);

  const productMap = await fetchProductsLookup(db, SHOP_ID);
  console.log(`📦 Loaded ${productMap.size} products for shop ID ${SHOP_ID}`);

  const payload = {
    metadata: {
      exported_at: new Date().toISOString(),
      shop_id: SHOP_ID,
      source_db: process.env.DB_NAME || "local",
    },
    product_group_promotions: [],
    cart_promotion_rules: [],
    product_promotions: [],
  };

  // 1. Export product_group_promotion + product_group_promotion_item
  if (await tableExists(db, "product_group_promotion")) {
    const [groups] = await db.query(
      `SELECT * FROM product_group_promotion WHERE shop_id = ? ORDER BY id ASC`,
      [SHOP_ID]
    );

    for (const g of groups || []) {
      let items = [];
      if (await tableExists(db, "product_group_promotion_item")) {
        const [rawItems] = await db.query(
          `SELECT * FROM product_group_promotion_item WHERE group_promotion_id = ? ORDER BY id ASC`,
          [g.id]
        );
        items = (rawItems || []).map((item) => {
          const prodInfo = productMap.get(Number(item.product_id)) || null;
          return {
            ...item,
            _product_lookup: prodInfo,
          };
        });
      }

      payload.product_group_promotions.push({
        group: g,
        items,
      });
    }
    console.log(`✅ Exported ${payload.product_group_promotions.length} product group promotions`);
  }

  // 2. Export cart_promotion_rule
  if (await tableExists(db, "cart_promotion_rule")) {
    const [rules] = await db.query(
      `SELECT * FROM cart_promotion_rule WHERE shop_id = ? ORDER BY id ASC`,
      [SHOP_ID]
    );

    payload.cart_promotion_rules = (rules || []).map((r) => {
      const rewardProd = r.reward_product_id ? productMap.get(Number(r.reward_product_id)) || null : null;
      return {
        rule: r,
        _reward_product_lookup: rewardProd,
      };
    });
    console.log(`✅ Exported ${payload.cart_promotion_rules.length} cart promotion rules`);
  }

  // 3. Export standard promotion
  if (await tableExists(db, "promotion")) {
    const [promos] = await db.query(
      `SELECT * FROM promotion WHERE shop_id = ? ORDER BY id ASC`,
      [SHOP_ID]
    );

    payload.product_promotions = (promos || []).map((p) => {
      const prodInfo = p.product_id ? productMap.get(Number(p.product_id)) || null : null;
      return {
        promotion: p,
        _product_lookup: prodInfo,
      };
    });
    console.log(`✅ Exported ${payload.product_promotions.length} standard product promotions`);
  }

  // Ensure output directory exists
  const outDir = path.dirname(OUT_FILE);
  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true });
  }

  fs.writeFileSync(OUT_FILE, JSON.stringify(payload, null, 2), "utf8");
  console.log(`\n💾 Saved payload to: ${OUT_FILE}`);
  console.log(`====================================================\n`);

  process.exit(0);
}

main().catch((err) => {
  console.error("❌ Fatal error exporting promotions payload:", err);
  process.exit(1);
});

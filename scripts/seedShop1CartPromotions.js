require("dotenv").config({ quiet: true });
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");

const SHOP_ID = Number(process.env.SEED_CART_PROMO_SHOP_ID || 1);
const SOURCE = "shop1_cart_promo_logic_test";

async function getOrCreateProduct(conn, { name, price, stock, category, subCategory }) {
  const [[existing]] = await conn.query(
    `SELECT id, name FROM product WHERE shop_id = ? AND name = ? LIMIT 1`,
    [SHOP_ID, name],
  );
  if (existing) return Number(existing.id);

  const [ins] = await conn.query(
    `
    INSERT INTO product
      (shop_id, name, display_name_en, price, stock_amount, category, sub_category, description, created_at, updated_at)
    VALUES (?, ?, NULL, ?, ?, ?, ?, 'מוצר בדיקה למבצעי סל', NOW(), NOW())
    `,
    [SHOP_ID, name, Number(price), Number(stock), category || "Other", subCategory || "Other"],
  );
  return Number(ins.insertId);
}

async function upsertRule(conn, data) {
  await conn.query(
    `
    INSERT INTO cart_promotion_rule
      (shop_id, rule_type, title, description, threshold_amount, delivery_fee_override,
       reward_product_id, reward_qty, reward_fixed_price, reward_max_qty, threshold_base_mode,
       priority, is_active, source, external_reward_id, start_at, end_at, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NOW() - INTERVAL 1 DAY, NOW() + INTERVAL 30 DAY, NOW(), NOW())
    ON DUPLICATE KEY UPDATE
      rule_type = VALUES(rule_type),
      title = VALUES(title),
      description = VALUES(description),
      threshold_amount = VALUES(threshold_amount),
      delivery_fee_override = VALUES(delivery_fee_override),
      reward_product_id = VALUES(reward_product_id),
      reward_qty = VALUES(reward_qty),
      reward_fixed_price = VALUES(reward_fixed_price),
      reward_max_qty = VALUES(reward_max_qty),
      threshold_base_mode = VALUES(threshold_base_mode),
      priority = VALUES(priority),
      is_active = 1,
      start_at = VALUES(start_at),
      end_at = VALUES(end_at),
      updated_at = NOW()
    `,
    [
      SHOP_ID,
      data.rule_type,
      data.title,
      data.description || data.title,
      data.threshold_amount || 0,
      data.delivery_fee_override ?? null,
      data.reward_product_id ?? null,
      data.reward_qty ?? null,
      data.reward_fixed_price ?? null,
      data.reward_max_qty ?? null,
      data.threshold_base_mode || "ITEMS_SUBTOTAL",
      data.priority || 100,
      SOURCE,
      data.external_reward_id,
    ],
  );
}

async function upsertLimitedProductPromotion(conn, productId) {
  const description = "בדיקה: מחיר קבוע עד 2 יחידות";
  const [[existing]] = await conn.query(
    `SELECT id FROM promotion WHERE shop_id = ? AND product_id = ? AND description = ? LIMIT 1`,
    [SHOP_ID, productId, description],
  );

  if (existing) {
    await conn.query(
      `
      UPDATE promotion
         SET kind = 'FIXED_PRICE',
             percent_off = NULL,
             amount_off = NULL,
             fixed_price = 4.90,
             bundle_buy_qty = NULL,
             bundle_pay_price = NULL,
             max_discounted_qty = 2,
             start_at = NOW() - INTERVAL 1 DAY,
             end_at = NOW() + INTERVAL 30 DAY,
             updated_at = NOW()
       WHERE id = ?
      `,
      [existing.id],
    );
    return;
  }

  await conn.query(
    `
    INSERT INTO promotion
      (shop_id, product_id, kind, percent_off, amount_off, fixed_price,
       bundle_buy_qty, bundle_pay_price, max_discounted_qty, description,
       start_at, end_at, created_at, updated_at)
    VALUES (?, ?, 'FIXED_PRICE', NULL, NULL, 4.90, NULL, NULL, 2, ?,
            NOW() - INTERVAL 1 DAY, NOW() + INTERVAL 30 DAY, NOW(), NOW())
    `,
    [SHOP_ID, productId, description],
  );
}

async function main() {
  await ensureCartPromotionSchema();
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const basketDealProductId = await getOrCreateProduct(conn, {
      name: "מוצר בדיקה מבצע סל",
      price: 19.90,
      stock: 999,
      category: "Other",
      subCategory: "Other",
    });

    const giftProductId = await getOrCreateProduct(conn, {
      name: "מתנת בדיקה מבצע סל",
      price: 5.00,
      stock: 999,
      category: "Other",
      subCategory: "Other",
    });

    const limitedPromoProductId = await getOrCreateProduct(conn, {
      name: "מוצר בדיקה מקסימום מבצע",
      price: 9.90,
      stock: 999,
      category: "Other",
      subCategory: "Other",
    });

    await upsertRule(conn, {
      external_reward_id: "delivery_30_to_10",
      rule_type: "DELIVERY_FEE_OVERRIDE",
      title: "בדיקה: בקנייה מעל 30 משלוח ב-10",
      threshold_amount: 30,
      delivery_fee_override: 10,
      priority: 10,
    });

    await upsertRule(conn, {
      external_reward_id: "delivery_60_free",
      rule_type: "DELIVERY_FEE_OVERRIDE",
      title: "בדיקה: בקנייה מעל 60 משלוח חינם",
      threshold_amount: 60,
      delivery_fee_override: 0,
      priority: 9,
    });

    await upsertRule(conn, {
      external_reward_id: "gift_40",
      rule_type: "GIFT_PRODUCT",
      title: "בדיקה: בקנייה מעל 40 מתנת בדיקה",
      threshold_amount: 40,
      reward_product_id: giftProductId,
      reward_qty: 1,
      priority: 20,
    });

    await upsertRule(conn, {
      external_reward_id: "product_50_fixed",
      rule_type: "THRESHOLD_PRODUCT_FIXED_PRICE",
      title: "בדיקה: בקנייה מעל 50 מוצר בדיקה מבצע סל ב-4.90",
      threshold_amount: 50,
      reward_product_id: basketDealProductId,
      reward_fixed_price: 4.90,
      reward_max_qty: 1,
      threshold_base_mode: "EXCLUDING_REWARD_PRODUCTS",
      priority: 30,
    });

    await upsertLimitedProductPromotion(conn, limitedPromoProductId);

    await conn.commit();

    console.log("Seeded shop cart promotion test data:");
    console.log({
      shop_id: SHOP_ID,
      basketDealProductId,
      giftProductId,
      limitedPromoProductId,
    });
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });

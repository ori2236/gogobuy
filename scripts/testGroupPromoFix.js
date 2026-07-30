require("dotenv").config();
const { applyProductGroupPromotionsToItems } = require("../services/productGroupPromotions");

async function testGroupPromo() {
  console.log("=== Testing Product Group Promotion Calculation for Weighted Items ===");

  const sampleOrderItems = [
    {
      order_item_id: 1,
      product_id: 101,
      name: "מלפפון (ביכורי שדה)",
      amount: 3, // 3 KG
      unit_price: 8.90,
      price: 26.70,
      current_line_total: 26.70,
      sold_by_weight: 1,
      is_gift: 0,
    }
  ];

  const sampleGroupPromo = {
    id: 1,
    shop_id: 1,
    title: "ירק ביכורי שדה",
    bundle_buy_qty: 3,
    bundle_pay_price: 8.90,
    is_active: 1,
    products: [
      { product_id: 101, name: "מלפפון (ביכורי שדה)" }
    ]
  };

  const fakeConn = {
    query: async (sql, params) => {
      if (sql.includes("INFORMATION_SCHEMA.COLUMNS")) {
        return [[{ 1: 1 }]];
      }
      if (sql.includes("FROM product_group_promotion") && !sql.includes("JOIN")) {
        return [[sampleGroupPromo]];
      }
      if (sql.includes("FROM product_group_promotion_item")) {
        return [[{ group_promotion_id: 1, product_id: 101, name: "מלפפון (ביכורי שדה)" }]];
      }
      return [[]];
    }
  };

  const res = await applyProductGroupPromotionsToItems(fakeConn, {
    order_id: 1,
    shop_id: 1,
    items: sampleOrderItems,
  });

  console.log("\nCalculation Result:");
  console.log("Applications:", JSON.stringify(res, null, 2));

  if (res.length > 0 && res[0].discount_amount > 0) {
    console.log("\n✅ SUCCESS: Group promotion discount (₪" + res[0].discount_amount + ") was calculated and applied to 3 KG of cucumber!");
  } else {
    console.log("\n❌ FAILED: Discount was NOT applied.");
  }

  process.exit(0);
}

testGroupPromo().catch(console.error);

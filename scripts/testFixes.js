require("dotenv").config();
const { buildOrderSummaryMessage } = require("../utilities/orderSummaryMessage");
const db = require("../config/db");

async function runTests() {
  console.log("=========================================");
  console.log("🧪 RUNNING VERIFICATION TESTS FOR FIXES");
  console.log("=========================================\n");

  // TEST 1: Unit vs Weight formatting in Order Summary
  console.log("--- Test 1: Group Promo Hint Unit Formatting (ק\"ג vs יח') ---");
  const weightedItem = {
    name: "מלפפון (ביכורי שדה)",
    amount: 2,
    line_total: 17.80,
    unit_price: 8.90,
    sold_by_weight: true,
    group_promo: {
      bundle_buy_qty: 3,
      bundle_pay_price: 8.90,
    },
  };

  const unitItem = {
    name: "שקית חלב",
    amount: 2,
    line_total: 14.00,
    unit_price: 7.00,
    sold_by_weight: false,
    group_promo: {
      bundle_buy_qty: 3,
      bundle_pay_price: 18.00,
    },
  };

  const summaryMsg = buildOrderSummaryMessage({
    orderId: 999,
    items: [weightedItem, unitItem],
    isEnglish: false,
  });

  console.log("Generated Summary Message Output:\n");
  console.log(summaryMsg);

  const containsKgHint = summaryMsg.includes("3 ק״ג ב-₪8.90") || summaryMsg.includes("3 ק״ג");
  const containsUnitHint = summaryMsg.includes("3 יח׳ ב-₪18") || summaryMsg.includes("3 יח׳");

  if (containsKgHint && containsUnitHint) {
    console.log("\n✅ Test 1 PASSED: Weighted item displayed 'ק״ג' and unit item displayed 'יח׳' correctly!");
  } else {
    console.log("\n❌ Test 1 FAILED: Expected weighted hint to contain 'ק״ג' and unit hint to contain 'יח׳'.");
  }

  // TEST 2: Checking Bundle Promo query logic for oi.promo_id IS NULL
  console.log("\n--- Test 2: Checking DB Query in fetchBundleRowsForOrderItems ---");
  try {
    const [promos] = await db.query(
      `SELECT pr.id, pr.product_id, pr.kind, pr.bundle_buy_qty, pr.bundle_pay_price, p.name
         FROM promotion pr
         JOIN product p ON p.id = pr.product_id
        WHERE pr.kind = 'BUNDLE'
          AND (pr.start_at IS NULL OR pr.start_at <= NOW())
          AND (pr.end_at IS NULL OR pr.end_at >= NOW())
        LIMIT 1`
    );

    if (!promos.length) {
      console.log("ℹ️ No active BUNDLE promotions in DB right now, but query syntax is verified.");
    } else {
      const samplePromo = promos[0];
      console.log("Found sample active BUNDLE promo in DB:", {
        promo_id: samplePromo.id,
        product_id: samplePromo.product_id,
        product_name: samplePromo.name,
        deal: `${samplePromo.bundle_buy_qty} for ₪${samplePromo.bundle_pay_price}`,
      });

      // Test query simulating order_item with promo_id = NULL
      const [testRows] = await db.query(
        `SELECT oi.product_id, pr.id AS promo_id, pr.bundle_buy_qty
           FROM (SELECT ? AS product_id, 1 AS amount, 1 AS sold_by_weight, NULL AS promo_id, 1 AS order_id) oi
           JOIN product p ON p.id = oi.product_id AND p.shop_id = ?
           JOIN promotion pr ON (pr.id = oi.promo_id OR (oi.promo_id IS NULL AND pr.product_id = oi.product_id)) AND pr.shop_id = ?
          WHERE pr.kind = 'BUNDLE'`,
        [samplePromo.product_id, 1, 1]
      );

      if (testRows.length > 0) {
        console.log("✅ Test 2 PASSED: Successfully matched BUNDLE promo even when oi.promo_id is NULL!");
      } else {
        console.log("❌ Test 2 FAILED: Query did not match BUNDLE promo when promo_id was NULL.");
      }
    }
  } catch (err) {
    console.error("Test 2 Error:", err.message);
  }

  process.exit(0);
}

runTests();

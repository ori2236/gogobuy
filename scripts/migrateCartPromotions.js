require("dotenv").config({ quiet: true });
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");

async function main() {
  await ensureCartPromotionSchema();
  console.log("Cart promotion schema is ready.");
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });

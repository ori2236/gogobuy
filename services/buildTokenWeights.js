const db = require("../config/db");
const { tokenizeName } = require("../utilities/tokens");

async function rebuildTokenWeightsForShop(shop_id) {
  const [rows] = await db.query(
    `
    SELECT id, name, display_name_en
    FROM product
    WHERE shop_id = ?
    `,
    [shop_id],
  );

  const df = new Map();

  for (const r of rows) {
    const tokens = new Set(tokenizeName(r.name || ""));
    for (const t of tokens) {
      df.set(t, (df.get(t) || 0) + 1);
    }
  }

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    await conn.query(`DELETE FROM product_token_weight WHERE shop_id = ?`, [
      shop_id,
    ]);

    const chunkSize = 500;
    const entries = Array.from(df.entries()); // [token, docFreq]

    for (let i = 0; i < entries.length; i += chunkSize) {
      const chunk = entries.slice(i, i + chunkSize);

      const values = [];
      const params = [];

      for (const [token, docFreq] of chunk) {
        values.push("(?, ?, ?, ?)");
        params.push(shop_id, token, docFreq, docFreq > 0 ? 1 / docFreq : 1);
      }

      await conn.query(
        `
        INSERT INTO product_token_weight (shop_id, token, doc_freq, inv_df)
        VALUES ${values.join(",")}
        `,
        params,
      );
    }

    await conn.commit();
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

module.exports = {
  rebuildTokenWeightsForShop
};

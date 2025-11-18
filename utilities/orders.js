const db = require("../config/db");
const { addDec, addMoney, mulMoney } = require("./decimal");

function mergeDuplicateLineItems(lineItems) {
  const byId = new Map();
  for (const item of lineItems) {
    const id = Number(item.product_id);
    if (!id) continue;
    const amount = Number(item.amount);
    if (!(amount > 0)) continue;

    const prev = byId.get(id);
    if (!prev) {
      byId.set(id, { ...item, amount: addDec(0, amount) });
    } else {
      byId.set(id, {
        ...prev,
        amount: addDec(prev.amount, amount),
      });
    }
  }
  return Array.from(byId.values());
}

async function fetchAlternativesWithStock(
  shop_id,
  category,
  subCategory,
  neededQty,
  excludeIds = [],
  limit = 3,
  conn = db
) {
  if (!category && !subCategory) return [];
  const params = [shop_id];
  let sql = `
    SELECT id, name, price, stock_amount, category, sub_category
    FROM product
    WHERE shop_id = ?
  `;
  if (category) {
    sql += ` AND category = ?`;
    params.push(category);
  }
  if (subCategory) {
    sql += ` AND sub_category = ?`;
    params.push(subCategory);
  }
  if (excludeIds.length) {
    sql += ` AND id NOT IN (${excludeIds.map(() => "?").join(",")})`;
    params.push(...excludeIds);
  }
  sql += ` AND stock_amount >= ? ORDER BY updated_at DESC, id DESC LIMIT ?`;
  params.push(Number(neededQty), Number(limit));
  const [rows] = await conn.query(sql, params);
  return rows || [];
}

async function createOrderWithStockReserve({
  shop_id,
  customer_id,
  lineItems, // [{product_id, amount}]
  status = "pending",
  payment_method = "other",
  delivery_address = null,
}) {
  const merged = mergeDuplicateLineItems(lineItems || []);

  //new empty order
  if (!merged.length) {
    const [insOrder] = await db.query(
      `INSERT INTO orders
           (shop_id, customer_id, status, price, payment_method, delivery_address, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, NOW(6), NOW(6))`,
      [shop_id, customer_id, status, 0, payment_method, delivery_address]
    );
    return {
      ok: true,
      partial: false,
      order_id: insOrder.insertId,
      totalPrice: 0,
      items: [],
      insufficient: [],
    };
  }

  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const ids = merged.map((li) => Number(li.product_id)).filter(Boolean);
    const placeholders = ids.map(() => "?").join(",");
    const [prodRows] = await conn.query(
      `
        SELECT id, shop_id, name, price, stock_amount, category, sub_category
        FROM product
        WHERE shop_id = ? AND id IN (${placeholders})
        ORDER BY id
        FOR UPDATE
      `,
      [shop_id, ...ids]
    );

    const byId = new Map(prodRows.map((p) => [Number(p.id), p]));

    //is in stock
    const insufficient = [];
    const okItems = [];

    for (const li of merged) {
      const p = byId.get(Number(li.product_id));
      if (!p) {
        insufficient.push({
          product_id: li.product_id,
          requested_amount: Number(li.amount),
          reason: "NOT_FOUND",
          missing: Number(li.amount),
          alternatives: [],
        });
        continue;
      }

      const reqQty = Number(li.amount);
      const stock = Number(p.stock_amount);

      if (stock < reqQty) {
        const missing = reqQty - stock;
        const excludeSet = new Set([
          p.id,
          ...ids,
          ...okItems.map((o) => o.product_id),
        ]);
        const exclude = Array.from(excludeSet);

        const alts = await fetchAlternativesWithStock(
          shop_id,
          p.category,
          p.sub_category,
          reqQty,
          exclude,
          3,
          conn
        );

        insufficient.push({
          product_id: p.id,
          matched_name: p.name,
          requested_name: li.requested_name || null,
          requested_amount: reqQty,
          in_stock: stock,
          missing: Number(missing.toFixed(3)),
          alternatives: alts.map((a) => ({
            for_product_id: p.id,
            for_requested_name: li.requested_name || null,
            product_id: a.id,
            name: a.name,
            price: Number(a.price),
            stock_amount: Number(a.stock_amount),
            category: a.category,
            sub_category: a.sub_category,
          })),
        });
      } else {
        const newStock = Number((stock - reqQty).toFixed(3));
        okItems.push({
          product_id: p.id,
          name: p.name,
          price: Number(p.price),
          amount: reqQty,
          category: p.category,
          sub_category: p.sub_category,
          new_stock: newStock,
        });
      }
    }

    //if there is no products in stock
    if (!okItems.length) {
      const [insOrder] = await conn.query(
        `INSERT INTO orders
             (shop_id, customer_id, status, price, payment_method, delivery_address, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, NOW(6), NOW(6))`,
        [shop_id, customer_id, status, 0, payment_method, delivery_address]
      );
      await conn.commit();
      return {
        ok: true,
        partial: true,
        order_id: insOrder.insertId,
        totalPrice: 0,
        items: [],
        insufficient,
      };
    }

    // change the amount in stock
    for (const it of okItems) {
      await conn.query(
        `UPDATE product
           SET stock_amount = ?
         WHERE id = ? AND shop_id = ?`,
        [it.new_stock, it.product_id, shop_id]
      );
    }

    let total = 0;
    for (const it of okItems) {
      total = addMoney(total, mulMoney(it.amount, it.price));
    }

    //create the order
    const [insOrder] = await conn.query(
      `INSERT INTO orders
           (shop_id, customer_id, status, price, payment_method, delivery_address, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, NOW(6), NOW(6))`,
      [shop_id, customer_id, status, total, payment_method, delivery_address]
    );
    const order_id = insOrder.insertId;

    const valuesSql = okItems.map(() => `(?, ?, ?, ?, NOW(6))`).join(", ");
    const params = [];
    for (const it of okItems) {
      params.push(order_id, it.product_id, it.amount, it.price);
    }
    await conn.query(
      `INSERT INTO order_item (order_id, product_id, amount, price, created_at)
         VALUES ${valuesSql}`,
      params
    );

    await conn.commit();

    return {
      ok: true,
      partial: insufficient.length > 0,
      order_id,
      totalPrice: total,
      items: okItems.map(({ product_id, name, amount, price }) => ({
        product_id,
        name,
        amount,
        price,
      })),
      insufficient,
    };
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

async function expireStalePendingOrders({ hours = 24, shop_id = null } = {}) {
  const conn = await db.getConnection();
  try {
    await conn.beginTransaction();

    const params = [hours];
    let sql = `
      SELECT id, shop_id
      FROM orders
      WHERE status = 'pending'
        AND TIMESTAMPDIFF(HOUR, GREATEST(created_at, updated_at), NOW()) >= ?`;
    if (shop_id) {
      sql += ` AND shop_id = ?`;
      params.push(shop_id);
    }

    const [orders] = await conn.query(sql + ` FOR UPDATE`, params);

    if (!orders.length) {
      await conn.commit();
      return { ok: true, expired: 0 };
    }

    for (const row of orders) {
      const orderId = Number(row.id);
      const orderShopId = Number(row.shop_id);

      const [[ord]] = await conn.query(
        `SELECT id, shop_id, status
           FROM orders
          WHERE id = ? FOR UPDATE`,
        [orderId]
      );
      if (!ord || ord.status !== "pending") {
        continue;
      }

      const [items] = await conn.query(
        `SELECT product_id, amount
           FROM order_item
          WHERE order_id = ?
          FOR UPDATE`,
        [orderId]
      );

      if (items.length) {
        const ids = items.map((i) => Number(i.product_id));
        const placeholders = ids.map(() => "?").join(",");

        await conn.query(
          `SELECT id
             FROM product
            WHERE id IN (${placeholders}) AND shop_id = ?
            FOR UPDATE`,
          [...ids, orderShopId]
        );

        for (const it of items) {
          await conn.query(
            `UPDATE product
                SET stock_amount = COALESCE(stock_amount,0) + ?
              WHERE id = ? AND shop_id = ?`,
            [Number(it.amount), Number(it.product_id), orderShopId]
          );
        }
      }

      await conn.query(`DELETE FROM order_item WHERE order_id = ?`, [orderId]);

      await conn.query(
        `DELETE FROM orders WHERE id = ? AND status = 'pending'`,
        [orderId]
      );
    }

    await conn.commit();
    return { ok: true, expired: orders.length };
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}


async function getOrder(order_id) {
  const [rows] = await db.query(
    `SELECT *
       FROM orders
      WHERE id = ?`,
    [order_id]
  );
  return rows[0] || null;
}

async function getActiveOrder(customer_id, shop_id) {
  const [rows] = await db.query(
    `SELECT *
       FROM orders
      WHERE customer_id = ? AND shop_id = ?
      ORDER BY updated_at DESC, id DESC
      LIMIT 1`,
    [customer_id, shop_id]
  );
  return rows[0] || null;
}

async function getOrderItems(order_id) {
  const [rows] = await db.query(
    `SELECT oi.*, p.name, p.display_name_en, p.category, p.sub_category AS 'sub-category'
       FROM order_item oi
       LEFT JOIN product p ON p.id = oi.product_id
      WHERE oi.order_id = ?`,
    [order_id]
  );
  return rows;
}

function buildActiveOrderSignals(order, items) {
  if (!order) {
    return {
      ACTIVE_ORDER_EXISTS: false,
      ACTIVE_ORDER_SUMMARY: "none",
    };
  }
  const examples = (items || [])
    .slice(0, 3)
    .map((i) => `${i.name} ×${(+i.amount).toString().replace(/\.0+$/, "")}`);
  return {
    ACTIVE_ORDER_EXISTS: true,
    ACTIVE_ORDER_SUMMARY: `order_id=${order.id}; items=${
      items.length
    }; examples=[${examples.join(", ")}]`,
  };
}

module.exports = {
  createOrderWithStockReserve,
  expireStalePendingOrders,
  getOrder,
  getActiveOrder,
  getOrderItems,
  buildActiveOrderSignals,
};

const { chat } = require("../../config/openai");
const db = require("../../config/db");
const { getPromptFromDB } = require("../../repositories/prompt");
const {
  getOrder,
  fetchActivePromotionsMap,
  calcLineTotalWithPromo,
  formatOrderStatus,
} = require("../../utilities/orders");
const { addMoney, roundTo } = require("../../utilities/decimal");
const { isEnglishMessage } = require("../../utilities/lang");
const {
  pickAltTemplate,
  findBestProductForRequest,
  fetchAlternatives,
} = require("../../services/products");
const {
  saveOpenQuestions,
  closeQuestionsByIds,
  deleteQuestionsByIds,
} = require("../../utilities/openQuestions");
const { normalizeIncomingQuestions } = require("../../utilities/normalize");
const { buildModifyOrderSchema } = require("./schemas/modify.schema");
const { getExcludeTokensFromReq } = require("../../utilities/tokens");
const { parseModelAnswer } = require("../../utilities/jsonParse");
const {
  buildItemsBlock,
  buildQuestionsBlock,
} = require("../../utilities/messageBuilders");

const PROMPT_CAT = "ORD";
const PROMPT_SUB = "MODIFY";

async function repriceOrderItemsWithPromos(
  conn,
  { shop_id, order_id, orderItemIds },
) {
  const ids = Array.from(
    new Set(Array.from(orderItemIds).map(Number).filter(Boolean)),
  );
  if (!ids.length) return;

  const placeholders = ids.map(() => "?").join(",");
  const [rows] = await conn.query(
    `SELECT
    oi.id AS order_item_id,
    oi.product_id,
    oi.amount,
    oi.sold_by_weight,
    oi.promo_id AS locked_promo_id,
    p.price AS unit_price,
    pr.id AS promo_id,
    pr.kind,
    pr.percent_off,
    pr.amount_off,
    pr.fixed_price,
    pr.bundle_buy_qty,
    pr.bundle_pay_price
    FROM order_item oi
    JOIN product p ON p.id = oi.product_id
    LEFT JOIN promotion pr ON pr.id = oi.promo_id
    WHERE oi.order_id = ?
    AND oi.id IN (${placeholders})
  `,
    [Number(order_id), ...ids],
  );

  for (const r of rows) {
    const promo =
      r.locked_promo_id && r.promo_id
        ? {
            id: r.promo_id,
            kind: r.kind,
            percent_off: r.percent_off,
            amount_off: r.amount_off,
            fixed_price: r.fixed_price,
            bundle_buy_qty: r.bundle_buy_qty,
            bundle_pay_price: r.bundle_pay_price,
          }
        : null;

    const { lineTotal, promo_id } = calcLineTotalWithPromo({
      unitPrice: r.unit_price,
      amount: r.amount,
      soldByWeight: r.sold_by_weight === 1 || r.sold_by_weight === true,
      promo,
    });

    const fallback =
      Math.round(Number(r.unit_price) * Number(r.amount) * 100) / 100;

    await conn.query(
      `UPDATE order_item
        SET price = ?,
        price_locked = CASE WHEN promo_id IS NULL THEN price_locked ELSE 1 END
      WHERE id = ? AND order_id = ?`,
      [lineTotal ?? fallback, Number(r.order_item_id), Number(order_id)],
    );
  }
}

async function applyOrderPatch({
  shop_id,
  order_id,
  ops,
  isEnglish,
  maxPerProduct,
}) {
  const conn = await db.getConnection();
  const stockQuestions = [];
  let altTemplateIdx = 0;

  const removedApplied = [];
  const qtyIncreased = [];
  const qtyDecreased = [];
  const addedApplied = [];
  const notFoundAdds = [];
  const insufficientExistingIncreases = [];
  const insufficientNewAdds = [];
  const cappedWarnings = [];

  const display = (rowOrName) => {
    if (!rowOrName) return "";
    if (typeof rowOrName === "string") return rowOrName;
    const he = rowOrName.name;
    const en = rowOrName.display_name_en;
    return isEnglish ? (en && en.trim() ? en : he) : he;
  };

  const subjectForReq = (p) =>
    isEnglish
      ? (p && typeof p.outputName === "string" && p.outputName.trim()) ||
        (p && typeof p.name === "string" && p.name.trim()) ||
        ""
      : (p && typeof p.name === "string" && p.name.trim()) || "";

  const hasOwn = (obj, key) => Object.prototype.hasOwnProperty.call(obj, key);

  const stockToNumber = (raw) => {
    if (raw === null || raw === undefined) return Infinity; // unlimited stock
    const n = Number(raw);
    return Number.isFinite(n) ? n : 0;
  };

  const decStock = async (pid, delta) => {
    await conn.query(
      `UPDATE product
         SET stock_amount = CASE
           WHEN stock_amount IS NULL THEN NULL
           ELSE stock_amount - ?
         END
       WHERE id = ? AND shop_id = ?`,
      [Number(delta), Number(pid), Number(shop_id)],
    );
  };

  const incStock = async (pid, delta) => {
    await conn.query(
      `UPDATE product
         SET stock_amount = CASE
           WHEN stock_amount IS NULL THEN NULL
           ELSE stock_amount + ?
         END
       WHERE id = ? AND shop_id = ?`,
      [Number(delta), Number(pid), Number(shop_id)],
    );
  };

  const touchedOrderItemIds = new Set();
  const updateOrderItemAmountAndMaybeMeta = async (
    orderItemId,
    newQty,
    modelP,
  ) => {
    const setParts = [`amount = ?`];
    const params = [Number(newQty)];

    if (modelP && hasOwn(modelP, "sold_by_weight")) {
      const sbw = !!modelP.sold_by_weight;
      setParts.push(`sold_by_weight = ?`);
      params.push(sbw ? 1 : 0);

      if (!sbw) {
        setParts.push(`requested_units = NULL`);
      } else {
        if (hasOwn(modelP, "units")) {
          const u = Number(modelP.units);
          const uInt = Number.isFinite(u) ? Math.trunc(u) : NaN;
          if (Number.isFinite(uInt) && uInt > 0) {
            setParts.push(`requested_units = ?`);
            params.push(uInt);
          } else {
            setParts.push(`requested_units = NULL`);
          }
        } else {
          setParts.push(`requested_units = NULL`);
        }
      }
    }

    await conn.query(
      `UPDATE order_item
      SET ${setParts.join(", ")}
      WHERE id = ? AND order_id = ?`,
      [...params, Number(orderItemId), Number(order_id)],
    );
  };

  const getOrderItemIdFromOp = (x) => {
    const v = x?.["order_item.id"];
    return typeof v === "number" && Number.isFinite(v) ? Number(v) : null;
  };

  try {
    await conn.beginTransaction();

    const [origItems] = await conn.query(
      `SELECT
         oi.id,
         oi.product_id,
         oi.amount,
         oi.price,
         oi.sold_by_weight,
         oi.requested_units,
         p.name,
         p.display_name_en,
         p.stock_amount,
         p.category,
         p.sub_category
       FROM order_item oi
       JOIN product p ON p.id = oi.product_id
       WHERE oi.order_id = ?
       FOR UPDATE`,
      [Number(order_id)],
    );

    const byOrderItemId = new Map(origItems.map((it) => [Number(it.id), it]));
    const byProdId = new Map(
      origItems.map((it) => [Number(it.product_id), it]),
    );

    //REMOVE
    for (const rem of ops.remove || []) {
      const orderItemId = getOrderItemIdFromOp(rem);
      if (!orderItemId) continue;

      const row = byOrderItemId.get(orderItemId);
      if (!row) continue;

      await incStock(Number(row.product_id), Number(row.amount));

      await conn.query(
        `DELETE FROM order_item WHERE id = ? AND order_id = ? LIMIT 1`,
        [Number(orderItemId), Number(order_id)],
      );

      removedApplied.push({
        order_item_id: Number(orderItemId),
        product_id: Number(row.product_id),
        name: row.name,
        amount: Number(row.amount),
        mode: "explicit",
      });

      byOrderItemId.delete(Number(orderItemId));
      byProdId.delete(Number(row.product_id));
    }

    //SET (update existing)
    for (const s of ops.set || []) {
      const orderItemId = getOrderItemIdFromOp(s);
      if (!orderItemId) continue;

      const row = byOrderItemId.get(orderItemId);
      if (!row) continue;

      const prevQty = Number(row.amount);
      let newQty = Number(s.amount);

      if (!Number.isFinite(newQty) || newQty <= 0) continue;

      const sbw = hasOwn(s, "sold_by_weight")
        ? !!s.sold_by_weight
        : !!row.sold_by_weight;

      //new quantity
      if (!sbw) {
        if (newQty > maxPerProduct) {
          cappedWarnings.push({
            name: subjectForReq(s) || display(row),
            original: newQty,
            capped: maxPerProduct,
          });
          newQty = maxPerProduct;
        }
        newQty = Math.trunc(newQty);
        if (newQty <= 0) continue;
      } else {
        newQty = roundTo(newQty, 3);
        if (newQty <= 0) continue;
      }

      const delta = roundTo(newQty - prevQty, 3);

      if (delta === 0) {
        // might still need to update meta (sold_by_weight / units)
        await updateOrderItemAmountAndMaybeMeta(orderItemId, prevQty, s);
        touchedOrderItemIds.add(Number(orderItemId));
        continue;
      }

      if (delta > 0) {
        const [[prod]] = await conn.query(
          `SELECT stock_amount, category, sub_category
             FROM product
             WHERE id = ? AND shop_id = ?
            FOR UPDATE`,
          [Number(row.product_id), Number(shop_id)],
        );

        const stock = stockToNumber(prod?.stock_amount);

        if (stock >= delta) {
          await decStock(Number(row.product_id), delta);
          await updateOrderItemAmountAndMaybeMeta(orderItemId, newQty, s);
          touchedOrderItemIds.add(Number(orderItemId));

          qtyIncreased.push({
            product_id: Number(row.product_id),
            name: display(row),
            before: prevQty,
            after: newQty,
            delta,
          });
          row.amount = newQty;
          row.sold_by_weight = sbw ? 1 : 0;
        } else {
          const tpl = pickAltTemplate(isEnglish, altTemplateIdx++);
          const mainName = display(row);

          const excludeTokens = getExcludeTokensFromReq(s);

          const alts = await fetchAlternatives(
            shop_id,
            row.category || prod?.category || null,
            row.sub_category || prod?.sub_category || null,
            [Number(row.product_id)],
            3,
            mainName,
            excludeTokens,
          );

          const altNames = alts.map((a) => display(a));

          insufficientExistingIncreases.push({
            name: mainName,
            requested_delta: delta,
            available: stock === Infinity ? null : stock,
            alternatives: alts,
          });

          stockQuestions.push({
            name: mainName,
            question: altNames.length
              ? isEnglish
                ? `${mainName} is short on stock for the requested increase (requested +${delta}, available ${
                    stock === Infinity ? "unlimited" : stock
                  }). ${tpl(mainName, altNames)}`
                : `${mainName} חסר במלאי להגדלה שביקשת (תוספת ${delta}, זמינות ${
                    stock === Infinity ? "לא מוגבל" : stock
                  }). ${tpl(mainName, altNames)}`
              : isEnglish
                ? `${mainName} is short on stock for the requested increase (requested +${delta}, available ${
                    stock === Infinity ? "unlimited" : stock
                  }). Would you like a replacement or should I keep the current quantity?`
                : `${mainName} חסר במלאי להגדלה שביקשת (תוספת ${delta}, זמינות ${
                    stock === Infinity ? "לא מוגבל" : stock
                  }). להציע חלופה או להשאיר את הכמות הנוכחית?`,
            options: altNames,
          });
        }
      } else {
        // delta < 0 (decrease) -> return stock
        await incStock(Number(row.product_id), Math.abs(delta));
        await updateOrderItemAmountAndMaybeMeta(orderItemId, newQty, s);
        touchedOrderItemIds.add(Number(orderItemId));

        qtyDecreased.push({
          product_id: Number(row.product_id),
          name: display(row),
          before: prevQty,
          after: newQty,
          delta,
        });

        row.amount = newQty;
        row.sold_by_weight = sbw ? 1 : 0;
      }
    }

    //ADD
    for (const p of ops.add || []) {
      const rawAmount = Number(p.amount) || 0;
      if (!(rawAmount > 0)) continue;

      const sbw = p.sold_by_weight === true;

      let desiredAdd = sbw ? roundTo(rawAmount, 3) : Math.trunc(rawAmount);
      if (!(desiredAdd > 0)) continue;

      if (
        !sbw &&
        Number.isFinite(Number(maxPerProduct)) &&
        desiredAdd > maxPerProduct
      ) {
        cappedWarnings.push({
          name: subjectForReq(p) || p.name || "",
          original: desiredAdd,
          capped: maxPerProduct,
        });
        desiredAdd = maxPerProduct;
      }

      const row = await findBestProductForRequest(shop_id, p);

      // product not found in shop -> ask alternatives
      if (!row) {
        const cat = (p.category || "").trim() || null;
        const sub = (p["sub-category"] || p.sub_category || "").trim() || null;

        const excludeTokens = getExcludeTokensFromReq(p);
        const alts = await fetchAlternatives(
          shop_id,
          cat,
          sub,
          [],
          3,
          p.name,
          excludeTokens,
        );

        notFoundAdds.push({
          requested: {
            name: p.name,
            amount: desiredAdd,
            category: cat,
            sub_category: sub,
          },
          alternatives: alts,
        });

        const altNames = alts.map((a) => display(a));
        const tpl = pickAltTemplate(isEnglish, altTemplateIdx++);
        const subject = subjectForReq(p);

        stockQuestions.push({
          name: p.name,
          question: altNames.length
            ? tpl(subject, altNames)
            : isEnglish
              ? `Couldn't find "${subject}". Would you like a replacement or should I skip it?`
              : `לא מצאתי "${p.name}". להציע חלופה או לדלג?`,
          options: altNames,
        });

        continue;
      }

      const pid = Number(row.id);

      // lock product row
      const [[prod]] = await conn.query(
        `SELECT stock_amount, price, category, sub_category
           FROM product
          WHERE id = ? AND shop_id = ?
          FOR UPDATE`,
        [pid, Number(shop_id)],
      );

      const stock = stockToNumber(prod?.stock_amount);

      // lock existing order item for this product (if any)
      const [[existingOrderItem]] = await conn.query(
        `SELECT id, amount, sold_by_weight
           FROM order_item
          WHERE order_id = ? AND product_id = ?
          FOR UPDATE`,
        [Number(order_id), pid],
      );

      const prevAmount = existingOrderItem
        ? Number(existingOrderItem.amount) || 0
        : 0;

      // compute finalQty + actualDelta BEFORE touching stock
      let finalQty = prevAmount;
      let actualDelta = 0;

      const existingIsWeight =
        !!existingOrderItem &&
        (existingOrderItem.sold_by_weight === 1 ||
          existingOrderItem.sold_by_weight === true);

      if (existingOrderItem) {
        desiredAdd = existingIsWeight
          ? roundTo(rawAmount, 3)
          : Math.trunc(rawAmount);
        if (!(desiredAdd > 0)) continue;
        const mergeAsWeight = existingIsWeight;

        if (mergeAsWeight) {
          finalQty = roundTo(prevAmount + desiredAdd, 3);
          actualDelta = roundTo(finalQty - prevAmount, 3);
        } else {
          const wanted = prevAmount + desiredAdd;
          if (
            Number.isFinite(Number(maxPerProduct)) &&
            wanted > maxPerProduct
          ) {
            cappedWarnings.push({
              name: subjectForReq(p) || display(row),
              original: wanted,
              capped: maxPerProduct,
            });
            finalQty = maxPerProduct;
          } else {
            finalQty = wanted;
          }
          finalQty = Math.trunc(finalQty);
          actualDelta = finalQty - prevAmount;
        }
      } else {
        finalQty = sbw ? roundTo(desiredAdd, 3) : Math.trunc(desiredAdd);
        actualDelta = sbw ? roundTo(finalQty, 3) : finalQty;

        if (
          !sbw &&
          Number.isFinite(Number(maxPerProduct)) &&
          finalQty > maxPerProduct
        ) {
          cappedWarnings.push({
            name: subjectForReq(p) || display(row),
            original: finalQty,
            capped: maxPerProduct,
          });
          finalQty = maxPerProduct;
          actualDelta = finalQty; // prev is 0
        }
      }

      if (!(actualDelta > 0)) {
        // nothing to add (already at cap etc.)
        continue;
      }

      if (stock >= actualDelta) {
        await decStock(pid, actualDelta);

        if (existingOrderItem) {
          const metaForExisting = {
            ...p,
            sold_by_weight: existingIsWeight,
            units: existingIsWeight
              ? typeof p.units === "number"
                ? p.units
                : null
              : null,
          };

          await updateOrderItemAmountAndMaybeMeta(
            Number(existingOrderItem.id),
            finalQty,
            metaForExisting,
          );
          touchedOrderItemIds.add(Number(existingOrderItem.id));

          qtyIncreased.push({
            product_id: pid,
            name: isEnglish
              ? (row.display_name_en && row.display_name_en.trim()) || row.name
              : row.name,
            before: prevAmount,
            after: finalQty,
            delta: roundTo(finalQty - prevAmount, 3),
          });
        } else {
          const unitPrice = Number(prod?.price ?? row.price);

          const promoMap = await fetchActivePromotionsMap(conn, shop_id, [pid]);
          const promo = promoMap.get(pid) || null;

          const { lineTotal, promo_id } = calcLineTotalWithPromo({
            unitPrice,
            amount: finalQty,
            soldByWeight: sbw,
            promo,
          });

          const linePrice = lineTotal ?? roundTo(unitPrice * finalQty, 2);

          const [ins] = await conn.query(
            `INSERT INTO order_item
                (order_id, product_id, amount, sold_by_weight, requested_units, price, price_locked, promo_id, created_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))`,
            [
              Number(order_id),
              pid,
              finalQty,
              sbw ? 1 : 0,
              sbw && typeof p.units === "number" ? p.units : null,
              linePrice,
              1, // price_locked
              promo_id ?? null,
            ],
          );

          touchedOrderItemIds.add(Number(ins.insertId));

          addedApplied.push({
            product_id: pid,
            name: row.name,
            amount: finalQty,
            price: Number(prod?.price ?? row.price),
          });
        }
      } else {
        const mainName = isEnglish
          ? (row.display_name_en && row.display_name_en.trim()) || row.name
          : row.name;

        const excludeTokens = getExcludeTokensFromReq(p);

        const alts = await fetchAlternatives(
          shop_id,
          prod?.category || row.category || null,
          prod?.sub_category || row.sub_category || null,
          [pid],
          3,
          mainName,
          excludeTokens,
        );

        insufficientNewAdds.push({
          name: row.name,
          requested: actualDelta,
          available: stock === Infinity ? null : stock,
          alternatives: alts,
        });

        const altNames = alts.map((a) => display(a));
        const tpl = pickAltTemplate(isEnglish, altTemplateIdx++);
        const subject = subjectForReq(p);

        stockQuestions.push({
          name: p.name,
          question: altNames.length
            ? isEnglish
              ? `${mainName} is short on stock (requested ${actualDelta}, available ${
                  stock === Infinity ? "unlimited" : stock
                }). ${tpl(subject, altNames)}`
              : `${mainName} חסר במלאי (התבקשה כמות ${actualDelta}, זמינות ${
                  stock === Infinity ? "לא מוגבל" : stock
                }). ${tpl(p.name, altNames)}`
            : isEnglish
              ? `${mainName} is short on stock (requested ${actualDelta}, available ${
                  stock === Infinity ? "unlimited" : stock
                }). Would you like a replacement or should I skip it?`
              : `${mainName} חסר במלאי (התבקשה כמות ${actualDelta}, זמינות ${
                  stock === Infinity ? "לא מוגבל" : stock
                }). להציע חלופה או לדלג?`,
          options: altNames,
        });
      }
    }

    await repriceOrderItemsWithPromos(conn, {
      shop_id,
      order_id,
      orderItemIds: touchedOrderItemIds,
    });

    //Recompute total
    const [[sumRow]] = await conn.query(
      `SELECT COALESCE(ROUND(SUM(price), 2), 0) AS total
        FROM order_item
        WHERE order_id = ?`,
      [Number(order_id)],
    );

    const total = Number(sumRow.total || 0);

    await conn.query(
      `UPDATE orders SET price = ?, updated_at = NOW(6) WHERE id = ?`,
      [total, Number(order_id)],
    );

    const [curItems] = await conn.query(
      `SELECT
        oi.product_id,
        oi.amount,
        oi.sold_by_weight,
        oi.requested_units,
        oi.price AS line_total,
        oi.promo_id,

        pr.kind AS promo_kind,
        pr.percent_off,
        pr.amount_off,
        pr.fixed_price,
        pr.bundle_buy_qty,
        pr.bundle_pay_price,

        p.price AS unit_price,
        p.name,
        p.display_name_en
      FROM order_item oi
      JOIN product p ON p.id = oi.product_id
      LEFT JOIN promotion pr ON pr.id = oi.promo_id
      WHERE oi.order_id = ?`,
      [Number(order_id)],
    );

    await conn.commit();

    return {
      ok: true,
      total,
      items: curItems.map((it) => {
        const heName = it.name;
        const enName =
          (it.display_name_en && it.display_name_en.trim()) || it.name;
        const displayName = isEnglish ? enName : heName;

        const units = Number(it.requested_units);
        const hasUnits = Number.isFinite(units) && units > 0;

        return {
          name: displayName,
          amount: Number(it.amount),
          price: Number(it.unit_price),
          line_total: Number(it.line_total),
          promo_id: it.promo_id ? Number(it.promo_id) : null,
          promo: it.promo_id
            ? {
                kind: it.promo_kind,
                percent_off: it.percent_off,
                amount_off: it.amount_off,
                fixed_price: it.fixed_price,
                bundle_buy_qty: it.bundle_buy_qty,
                bundle_pay_price: it.bundle_pay_price,
              }
            : null,

          ...(it.sold_by_weight ? { sold_by_weight: true } : {}),
          ...(hasUnits ? { units } : {}),
        };
      }),
      questions: stockQuestions,
      meta: {
        removedApplied,
        qtyIncreased,
        qtyDecreased,
        addedApplied,
        notFoundAdds,
        insufficientExistingIncreases,
        insufficientNewAdds,
        cappedWarnings,
      },
    };
  } catch (e) {
    console.error("[ORD-MODIFY/applyPatch] TX error:", e);
    console.error("[ORD-MODIFY/applyPatch] TX context:", {
      shop_id,
      order_id,
      ops,
    });
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

function validateModelOutput(obj) {
  if (!obj || typeof obj !== "object")
    throw new Error("Bad output: not object");

  if (!obj.ops || typeof obj.ops !== "object")
    throw new Error("Missing key: ops");
  if (!Array.isArray(obj.ops.set)) throw new Error("ops.set must be array");
  if (!Array.isArray(obj.ops.remove))
    throw new Error("ops.remove must be array");
  if (!Array.isArray(obj.ops.add)) throw new Error("ops.add must be array");

  if (!Array.isArray(obj.questions)) throw new Error("questions must be array");

  if (!obj.question_updates || typeof obj.question_updates !== "object") {
    throw new Error("question_updates must be object");
  }
  if (!Array.isArray(obj.question_updates.close_ids)) {
    throw new Error("question_updates.close_ids must be array");
  }
  if (!Array.isArray(obj.question_updates.delete_ids)) {
    throw new Error("question_updates.delete_ids must be array");
  }

  for (const x of obj.ops.set) {
    const oid = x?.["order_item.id"];
    if (typeof oid !== "number")
      throw new Error("ops.set missing order_item.id");
    if (typeof x?.amount !== "number")
      throw new Error("ops.set missing amount");
    if (typeof x?.sold_by_weight !== "boolean")
      throw new Error("ops.set missing sold_by_weight");
    if (!("units" in x)) throw new Error("ops.set missing units");
  }

  for (const x of obj.ops.remove) {
    const oid = x?.["order_item.id"];
    if (typeof oid !== "number")
      throw new Error("ops.remove missing order_item.id");
  }

  for (const x of obj.ops.add) {
    if (!x || typeof x.name !== "string")
      throw new Error("ops.add missing name");
    if (!("outputName" in x)) throw new Error("ops.add missing outputName");
    if (typeof x.amount !== "number") throw new Error("ops.add missing amount");
    if (typeof x.sold_by_weight !== "boolean")
      throw new Error("ops.add missing sold_by_weight");
    if (!("units" in x)) throw new Error("ops.add missing units");
    if (!Array.isArray(x.exclude_tokens))
      throw new Error("ops.add missing exclude_tokens");
    if (typeof x.category !== "string")
      throw new Error("ops.add missing category");
    const sub = x["sub-category"] ?? x.sub_category;
    if (typeof sub !== "string")
      throw new Error("ops.add missing sub-category");
  }

  for (const q of obj.questions) {
    if (!q || typeof q !== "object") throw new Error("Bad question");
    if (!("name" in q)) throw new Error("Question missing name");
    if (typeof q.question !== "string" || !q.question.trim())
      throw new Error("Question missing text");
    if (!Array.isArray(q.options))
      throw new Error("Question missing options array");
  }

  return true;
}

module.exports = {
  async modifyOrder({
    message,
    customer_id,
    shop_id,
    order_id,
    activeOrder = null,
    items = [],
    history,
    openQsCtx = [],
    maxPerProduct,
  }) {
    if (!message || !customer_id || !shop_id) {
      throw new Error("modifyOrder: missing message/customer_id/shop_id");
    }
    const order = activeOrder || (await getOrder(order_id));
    if (!order) {
      return "כדי לערוך הזמנה צריך להיות הזמנה פעילה. להתחיל הזמנה חדשה?";
    }

    const isEnglishEarly = isEnglishMessage(message);
    if (!["pending", "confirmed"].includes(String(order.status))) {
      return isEnglishEarly
        ? `Order (#${order.id}) can't be modified at this stage.`
        : `אי אפשר לערוך את ההזמנה (#${order.id}) בשלב הזה.`;
    }

    const orderItems =
      Array.isArray(items) && items.length
        ? items
        : (
            await db.query(
              `SELECT oi.*, p.name, p.display_name_en, p.category, p.sub_category AS 'sub-category'
           FROM order_item oi
           LEFT JOIN product p ON p.id = oi.product_id
           WHERE oi.order_id = ?`,
              [order.id],
            )
          )[0];

    const basePrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);

    const systemWithInputs = [
      basePrompt,
      "",
      "=== STRUCTURED CONTEXT ===",
      `- OPEN_QUESTIONS: ${JSON.stringify(openQsCtx)}`,
      `- ORDER: ${JSON.stringify(order)}`,
      `- ORDER_ITEMS: ${JSON.stringify(orderItems)}`,
    ].join("\n");

    const answer = await chat({
      message,
      history,
      systemPrompt: systemWithInputs,
      response_format: {
        type: "json_schema",
        json_schema: await buildModifyOrderSchema(),
      },
    });

    let isEnglish = isEnglishMessage(message);

    let parsed;
    try {
      parsed = JSON.parse(answer);
    } catch (e1) {
      try {
        parsed = parseModelAnswer(answer);
      } catch (e2) {
        console.error("Failed to parse model JSON:", e2?.message, answer);
        return isEnglish
          ? "Sorry, I had a problem applying the changes. Please rephrase briefly."
          : "מצטערים, הייתה תקלה בעיבוד הבקשה. אפשר לנסח שוב בקצרה מה תרצה לשנות בהזמנה?";
      }
    }

    const modelQuestions = normalizeIncomingQuestions(parsed?.questions, {
      preserveOptions: true,
    });

    try {
      validateModelOutput(parsed);
    } catch (e) {
      const summaryLine = isEnglish
        ? "To complete your order, I need a few clarifications:"
        : "כדי להשלים את ההזמנה חסרות כמה הבהרות:";
      const questionsBlock = buildQuestionsBlock({
        questions: modelQuestions,
        isEnglish,
      });
      return [summaryLine, questionsBlock].filter(Boolean).join("\n");
    }

    const qUpdates = parsed?.question_updates || {};
    if (Array.isArray(qUpdates.close_ids) && qUpdates.close_ids.length) {
      await closeQuestionsByIds(qUpdates.close_ids);
    }
    if (Array.isArray(qUpdates.delete_ids) && qUpdates.delete_ids.length) {
      await deleteQuestionsByIds(qUpdates.delete_ids);
    }

    const ops = parsed?.ops || {};
    const patchOps = {
      set: Array.isArray(ops.set) ? ops.set : [],
      remove: Array.isArray(ops.remove) ? ops.remove : [],
      add: Array.isArray(ops.add) ? ops.add : [],
    };

    let combinedQuestions = [];
    let limitWarningsBlock = "";

    try {
      const txRes = await applyOrderPatch({
        shop_id,
        order_id: order.id,
        ops: patchOps,
        isEnglish,
        maxPerProduct,
      });

      const hasItems = Array.isArray(txRes.items) && txRes.items.length > 0;
      combinedQuestions = normalizeIncomingQuestions(
        [...modelQuestions, ...(txRes.questions || [])],
        { preserveOptions: true },
      );

      await saveOpenQuestions({
        customer_id,
        shop_id,
        order_id: order.id,
        questions: combinedQuestions,
      });

      const cappedWarnings = (txRes.meta && txRes.meta.cappedWarnings) || [];
      if (cappedWarnings.length) {
        limitWarningsBlock = isEnglish
          ? `Note: you can order up to ${maxPerProduct} units per product.`
          : `שימו לב: ניתן להזמין עד ${maxPerProduct} יחידות מכל מוצר.`;
      }

      const hasQuestions = combinedQuestions.length > 0;

      if (!hasItems) {
        const orderIdPart = isEnglish
          ? `(Order: #${order.id})`
          : `(הזמנה מספר: #${order.id})`;

        let summaryLine;
        if (isEnglish) {
          summaryLine = hasQuestions
            ? `Your order is currently empty ${orderIdPart}.\nTo build an order that fits what you want, I need your answers to a few questions:`
            : `Your order is currently empty ${orderIdPart}.`;
        } else {
          summaryLine = hasQuestions
            ? `ההזמנה שלך כרגע ריקה ${orderIdPart}.\nכדי שאוכל לבנות עבורך הזמנה שמתאימה לך, אני צריך תשובה לכמה שאלות:`
            : `ההזמנה שלך כרגע ריקה ${orderIdPart}.`;
        }

        const questionsLines = (combinedQuestions || [])
          .map((q) => `• ${q.question}`)
          .join("\n");

        return [summaryLine, questionsLines].filter(Boolean).join("\n\n");
      }

      //there is items in the order
      const itemsBlock = buildItemsBlock({ items: txRes.items, isEnglish });
      const summaryLine =
        typeof parsed?.summary_line === "string" && parsed.summary_line.trim()
          ? parsed.summary_line.trim()
          : isEnglish
            ? "Here is your updated order:"
            : "זוהי ההזמנה המעודכנת שלך:";

      let totalNoPromos = 0;
      for (const it of txRes.items || []) {
        const unit = Number(it.price);
        const qty = Number(it.amount);
        if (!Number.isFinite(unit) || !Number.isFinite(qty)) continue;
        totalNoPromos = addMoney(totalNoPromos, roundTo(unit * qty, 2));
      }

      const totalWithPromos = Number(txRes.total || 0);
      const savings = roundTo(totalNoPromos - totalWithPromos, 2);
      const hasSavings = Number.isFinite(savings) && savings >= 0.01;

      const statusText = formatOrderStatus(order.status, isEnglish);

      const headerBlock = isEnglish
        ? [
            `Order: #${order.id}`,
            `Status: ${statusText}`,
            hasSavings
              ? `Subtotal: *₪${totalWithPromos.toFixed(
                  2,
                )}* instead of ₪${totalNoPromos.toFixed(2)}`
              : `Subtotal: *₪${totalWithPromos.toFixed(2)}*`,
          ].join("\n")
        : [
            `מספר הזמנה: #${order.id}`,
            `סטטוס: ${statusText}`,
            hasSavings
              ? `סה״כ ביניים: *₪${totalWithPromos.toFixed(
                  2,
                )}* במקום ₪${totalNoPromos.toFixed(2)}`
              : `סה״כ ביניים: *₪${totalWithPromos.toFixed(2)}*`,
          ].join("\n");

      console.log(
        "[ORD-MODIFY] Qty decreased:",
        JSON.stringify(txRes.meta.qtyDecreased, null, 2),
      );
      console.log(
        "[ORD-MODIFY] Not-found adds (with alts):",
        JSON.stringify(txRes.meta.notFoundAdds, null, 2),
      );
      console.log(
        "[ORD-MODIFY] Insufficient existing increases:",
        JSON.stringify(txRes.meta.insufficientExistingIncreases, null, 2),
      );
      console.log(
        "[ORD-MODIFY] Insufficient new adds:",
        JSON.stringify(txRes.meta.insufficientNewAdds, null, 2),
      );

      const questionsBlock = buildQuestionsBlock({
        questions: combinedQuestions,
        isEnglish,
      });

      return [
        summaryLine,
        limitWarningsBlock,
        itemsBlock,
        " ",
        headerBlock,
        questionsBlock,
      ]
        .filter(Boolean)
        .join("\n");
    } catch (e) {
      console.error("[ORD-MODIFY] Fatal apply error:", e);
      console.error("[ORD-MODIFY] Error context:", {
        order_id: order && order.id,
        shop_id,
        patchOps,
      });

      // fallback: show current order state + technical question
      const [curItems] = await db.query(
        `SELECT
          oi.product_id,
          oi.amount,
          oi.sold_by_weight,
          oi.requested_units,
          oi.price AS line_total,
          oi.promo_id,
          p.price AS unit_price,
          p.name,
          p.display_name_en
        FROM order_item oi
        JOIN product p ON p.id = oi.product_id
        WHERE oi.order_id = ?`,
        [order.id],
      );

      const itemsForView = (curItems || []).map((it) => ({
        name: isEnglish
          ? (it.display_name_en && it.display_name_en.trim()) || it.name
          : it.name,
        amount: Number(it.amount),
        price: Number(it.unit_price),
        ...(it.sold_by_weight ? { sold_by_weight: true } : {}),
        ...(Number.isFinite(Number(it.requested_units)) &&
        Number(it.requested_units) > 0
          ? { units: Number(it.requested_units) }
          : {}),
      }));

      const summaryLine = isEnglish
        ? "To complete your order, I need a few clarifications:"
        : "כדי להשלים את ההזמנה חסרות כמה הבהרות:";

      const techQuestion = {
        name: null,
        question: isEnglish
          ? "We hit a technical issue applying all changes. Would you like me to try again or specify the change in a short message?"
          : "נתקלנו בתקלה טכנית ביישום כל השינויים. לנסות שוב או לפרט בקצרה את השינוי המבוקש?",
        options: [],
      };

      const itemsBlock = buildItemsBlock({ items: itemsForView, isEnglish });

      let total = 0;
      for (const it of curItems || []) {
        total = addMoney(total, Number(it.line_total));
      }

      const headerBlock = isEnglish
        ? [
            `Order: #${order.id}`,
            `Subtotal: *₪${Number(total || 0).toFixed(2)}*`,
          ].join("\n")
        : [
            `מספר הזמנה: #${order.id}`,
            `סה״כ ביניים: *₪${Number(total || 0).toFixed(2)}*`,
          ].join("\n");

      combinedQuestions = [...combinedQuestions, techQuestion];

      const questionsBlock = buildQuestionsBlock({
        questions: combinedQuestions,
        isEnglish,
      });

      return [
        summaryLine,
        itemsBlock,
        " ",
        headerBlock,
        limitWarningsBlock,
        questionsBlock,
      ]
        .filter(Boolean)
        .join("\n");
    }
  },
};

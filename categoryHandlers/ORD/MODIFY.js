const { chat } = require("../../config/openai");
const db = require("../../config/db");
const { getPromptFromDB } = require("../../repositories/prompt");
const {
  getOrder,
  mapOrderItemRowToDisplay,
} = require("../../utilities/orders");
const { addMoney, mulMoney, roundTo } = require("../../utilities/decimal");
const { isEnglishSummary } = require("../../utilities/lang");
const {
  parseModelAnswer,
  pickAltTemplate,
  findBestProductForRequest,
  fetchAlternatives,
  buildItemsBlock,
  buildQuestionsBlock,
} = require("../../services/products");
const {
  saveOpenQuestions,
  closeQuestionsByIds,
  deleteQuestionsByIds,
} = require("../../utilities/openQuestions");
const { normalizeIncomingQuestions } = require("../../utilities/normalize");

const PROMPT_CAT = "ORD";
const PROMPT_SUB = "MODIFY";

async function applyOrderModifications({
  shop_id,
  order_id,
  modelProducts,
  removedProducts,
  isEnglish,
  maxPerProduct,
}) {
  const conn = await db.getConnection();
  const stockQuestions = [];
  let altTemplateIdx = 0;

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

  const removedApplied = []; // [{product_id,name,amount,mode:'explicit'|'implicit'}]
  const qtyIncreased = []; // [{product_id,name,before,after,delta}]
  const qtyDecreased = []; // [{product_id,name,before,after,delta}]
  const addedApplied = []; // [{product_id,name,amount,price}]
  const notFoundAdds = []; // [{requested:{name,amount,category,sub_category}, alternatives:[...]}]
  const insufficientExistingIncreases = []; // [{name,requested_delta,available,alternatives:[...]}]
  const insufficientNewAdds = []; // [{name,requested,available,alternatives:[...]}]

  const unitsByProductId = new Map();
  const weightFlagByProductId = new Map();

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
     p.stock_amount
      FROM order_item oi
      JOIN product p ON p.id = oi.product_id
      WHERE oi.order_id = ?
      FOR UPDATE`,
      [order_id]
    );

    const byProdId = new Map(
      origItems.map((it) => [Number(it.product_id), it])
    );

    for (const rem of removedProducts || []) {
      const row = byProdId.get(Number(rem.product_id));
      if (!row) continue;
      await conn.query(
        `UPDATE product SET stock_amount = stock_amount + ? WHERE id = ? AND shop_id = ?`,
        [Number(row.amount), Number(row.product_id), shop_id]
      );
      await conn.query(
        `DELETE FROM order_item WHERE id = ? AND order_id = ? LIMIT 1`,
        [Number(row.id || rem["order_item.id"] || row.id), order_id]
      );
      removedApplied.push({
        product_id: Number(row.product_id),
        name: row.name,
        amount: Number(row.amount),
        mode: "explicit",
      });

      byProdId.delete(Number(row.product_id));
    }

    const existingNames = new Set();
    for (const it of origItems) {
      existingNames.add(it.name);
      if (it.display_name_en && it.display_name_en.trim()) {
        existingNames.add(it.display_name_en.trim());
      }
    }

    const cappedWarnings = [];

    const afterMap = new Map();
    for (const p of modelProducts) {
      const rawAmount = Number(p.amount) || 0;
      const capped = rawAmount > maxPerProduct ? maxPerProduct : rawAmount;

      if (rawAmount > maxPerProduct) {
        const label = subjectForReq(p);
        cappedWarnings.push({
          name: label || "",
          original: rawAmount,
          capped,
        });
      }

      afterMap.set(p.name, capped);
    }

    const metaByName = new Map();

    for (const p of modelProducts) {
      if (p && typeof p.name === "string" && p.name.trim()) {
        metaByName.set(p.name.trim(), p);
      }
      if (p && typeof p.outputName === "string" && p.outputName.trim()) {
        metaByName.set(p.outputName.trim(), p);
      }
    }

    const hasOwn = (obj, key) => Object.prototype.hasOwnProperty.call(obj, key);

    const updateOrderItemAmountAndMaybeMeta = async (
      orderItemId,
      newQty,
      modelP
    ) => {
      const setParts = [`amount = ?`];
      const params = [newQty];

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

      params.push(Number(orderItemId));

      await conn.query(
        `UPDATE order_item SET ${setParts.join(", ")} WHERE id = ?`,
        params
      );
    };

    for (const orig of origItems) {
      const keyHe = orig.name;
      const keyEn = (orig.display_name_en || "").trim();
      const inAfter = afterMap.has(keyHe) || (keyEn && afterMap.has(keyEn));

      if (!inAfter) {
        await conn.query(
          `UPDATE product SET stock_amount = stock_amount + ? WHERE id = ? AND shop_id = ?`,
          [Number(orig.amount), Number(orig.product_id), shop_id]
        );
        await conn.query(
          `DELETE FROM order_item WHERE id = ? AND order_id = ? LIMIT 1`,
          [Number(orig.id), order_id]
        );
        removedApplied.push({
          product_id: Number(orig.product_id),
          name: orig.name,
          amount: Number(orig.amount),
          mode: "implicit",
        });
        continue;
      }
      const newQty = Number(afterMap.get(keyHe) ?? afterMap.get(keyEn));
      const delta = roundTo(newQty - Number(orig.amount), 3);

      if (delta === 0) continue;

      if (delta > 0) {
        const [[prod]] = await conn.query(
          `SELECT stock_amount, price, category, sub_category FROM product WHERE id = ? AND shop_id = ? FOR UPDATE`,
          [Number(orig.product_id), shop_id]
        );
        const stock = Number(prod?.stock_amount ?? 0);
        if (stock >= delta) {
          await conn.query(
            `UPDATE product SET stock_amount = stock_amount - ? WHERE id = ? AND shop_id = ?`,
            [delta, Number(orig.product_id), shop_id]
          );
          const modelP =
            metaByName.get(keyHe) || (keyEn ? metaByName.get(keyEn) : null);

          await updateOrderItemAmountAndMaybeMeta(
            Number(orig.id),
            newQty,
            modelP
          );

          qtyIncreased.push({
            product_id: Number(orig.product_id),
            name: isEnglish
              ? (orig.display_name_en && orig.display_name_en.trim()) ||
                orig.name
              : orig.name,
            before: Number(orig.amount),
            after: newQty,
            delta,
          });
        } else {
          const tpl = pickAltTemplate(isEnglish, altTemplateIdx++);
          const mainName = isEnglish
            ? (orig.display_name_en && orig.display_name_en.trim()) || orig.name
            : orig.name;

          const alts = await fetchAlternatives(
            shop_id,
            orig.category || prod?.category || null,
            orig.sub_category || prod?.sub_category || null,
            [Number(orig.product_id)],
            3,
            mainName
          );

          const altNames = alts.map((a) =>
            isEnglish
              ? (a.display_name_en && a.display_name_en.trim()) || a.name
              : a.name
          );

          insufficientExistingIncreases.push({
            name: mainName,
            requested_delta: delta,
            available: stock,
            alternatives: alts,
          });

          stockQuestions.push({
            name: mainName,
            question: altNames.length
              ? isEnglish
                ? `${mainName} is short on stock for the requested increase (requested +${delta}, available ${stock}). ${tpl(
                    mainName,
                    altNames
                  )}`
                : `${mainName} חסר במלאי להגדלה שביקשת (תוספת ${delta}, זמינות ${stock}). ${tpl(
                    mainName,
                    altNames
                  )}`
              : isEnglish
              ? `${mainName} is short on stock for the requested increase (requested +${delta}, available ${stock}). Would you like a replacement or should I keep the current quantity?`
              : `${mainName} חסר במלאי להגדלה שביקשת (תוספת ${delta}, זמינות ${stock}). להציע חלופה או להשאיר את הכמות הנוכחית?`,
            options: altNames.length ? altNames : undefined,
          });
        }
      } else {
        await conn.query(
          `UPDATE product SET stock_amount = stock_amount + ? WHERE id = ? AND shop_id = ?`,
          [Math.abs(delta), Number(orig.product_id), shop_id]
        );
        const modelP =
          metaByName.get(keyHe) || (keyEn ? metaByName.get(keyEn) : null);

        await updateOrderItemAmountAndMaybeMeta(
          Number(orig.id),
          newQty,
          modelP
        );

        qtyDecreased.push({
          product_id: Number(orig.product_id),
          name: isEnglish
            ? (orig.display_name_en && orig.display_name_en.trim()) || orig.name
            : orig.name,
          before: Number(orig.amount),
          after: newQty,
          delta,
        });
      }
    }

    // add new products to the order
    for (const p of modelProducts) {
      if (existingNames.has(p.name)) {
        console.log(
          "[ORD-MODIFY/apply] skip add – name already in existingNames:",
          p.name
        );
        continue;
      }
      const row = await findBestProductForRequest(shop_id, p);

      //the product not selling at the shop
      if (!row) {
        const cat = (p.category || "").trim() || null;
        const sub = (p["sub-category"] || p.sub_category || "").trim() || null;

        const alts = await fetchAlternatives(shop_id, cat, sub, [], 3, p.name);

        notFoundAdds.push({
          requested: {
            name: p.name,
            amount: Number(p.amount),
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
          options: altNames.length ? altNames : undefined,
        });

        continue;
      }

      //checking stock
      const [lock] = await conn.query(
        `SELECT stock_amount, price, category, sub_category FROM product WHERE id = ? AND shop_id = ? FOR UPDATE`,
        [Number(row.id), shop_id]
      );
      const stock = Number(lock?.[0]?.stock_amount ?? 0);
      const rawAmount = Number(p.amount) || 0;
      const need = rawAmount > maxPerProduct ? maxPerProduct : rawAmount;

      if (rawAmount > maxPerProduct) {
        const label =
          subjectForReq(p) ||
          (isEnglish
            ? (row.display_name_en && row.display_name_en.trim()) || row.name
            : row.name);
        cappedWarnings.push({
          name: label || "",
          original: rawAmount,
          capped: need,
        });
      }

      if (stock >= need) {
        const unitsFromReq = Number(p.units);
        const pid = Number(row.id);

        if (Number.isFinite(unitsFromReq) && unitsFromReq > 0) {
          unitsByProductId.set(
            pid,
            (unitsByProductId.get(pid) || 0) + unitsFromReq
          );
        }

        if (p.sold_by_weight === true) {
          weightFlagByProductId.set(pid, true);
        }

        await conn.query(
          `UPDATE product SET stock_amount = stock_amount - ? WHERE id = ? AND shop_id = ?`,
          [need, Number(row.id), shop_id]
        );
        await conn.query(
          `INSERT INTO order_item (order_id, product_id, amount, sold_by_weight, requested_units, price, created_at)
          VALUES (?, ?, ?, ?, ?, ?, NOW(6))`,
          [
            order_id,
            Number(row.id),
            need,
            p.sold_by_weight ? 1 : 0,
            p.sold_by_weight && typeof p.units === "number" ? p.units : null,
            Number(lock?.[0]?.price ?? row.price),
          ]
        );
        addedApplied.push({
          product_id: Number(row.id),
          name: row.name,
          amount: need,
          price: Number(lock?.[0]?.price ?? row.price),
        });
      } else {
        const mainName = isEnglish
          ? (row.display_name_en && row.display_name_en.trim()) || row.name
          : row.name;

        const alts = await fetchAlternatives(
          shop_id,
          lock?.[0]?.category || row.category || null,
          lock?.[0]?.sub_category || row.sub_category || null,
          [Number(row.id)],
          3,
          mainName
        );

        insufficientNewAdds.push({
          name: row.name,
          requested: need,
          available: stock,
          alternatives: alts,
        });

        const altNames = alts.map((a) =>
          isEnglish
            ? (a.display_name_en && a.display_name_en.trim()) || a.name
            : a.name
        );
        const tpl = pickAltTemplate(isEnglish, altTemplateIdx++);

        const subject = subjectForReq(p);

        stockQuestions.push({
          name: p.name,
          question: altNames.length
            ? isEnglish
              ? `${mainName} is short on stock (requested ${need}, available ${stock}). ${tpl(
                  subject,
                  altNames
                )}`
              : `${mainName} חסר במלאי (התבקשה כמות ${need}, זמינות ${stock}). ${tpl(
                  p.name,
                  altNames
                )}`
            : isEnglish
            ? `${mainName} is short on stock (requested ${need}, available ${stock}). Would you like a replacement or should I skip it?`
            : `${mainName} חסר במלאי (התבקשה כמות ${need}, זמינות ${stock}). להציע חלופה או לדלג?`,
          options: altNames.length ? altNames : undefined,
        });
      }
    }

    const [curItems] = await conn.query(
      `SELECT
     oi.product_id,
     oi.amount,
     oi.sold_by_weight,
     oi.requested_units,
     p.price,
     p.name,
     p.display_name_en
      FROM order_item oi
      JOIN product p ON p.id = oi.product_id
      WHERE oi.order_id = ?`,
      [order_id]
    );

    let total = 0;
    for (const it of curItems) {
      total = addMoney(total, mulMoney(Number(it.amount), Number(it.price)));
    }
    await conn.query(
      `UPDATE orders SET price = ?, updated_at = NOW(6) WHERE id = ?`,
      [total, order_id]
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
          price: Number(it.price),
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
    console.error("[ORD-MODIFY/apply] TX error:", e);
    console.error("[ORD-MODIFY/apply] TX context:", {
      shop_id,
      order_id,
      modelProducts,
      removedProducts,
    });
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

function validateModelOutput(obj) {
  const must = [
    "summary_line",
    "products",
    "added_products",
    "removed_products",
    "questions",
  ];
  for (const k of must) if (!(k in obj)) throw new Error(`Missing key: ${k}`);
  if (
    !Array.isArray(obj.products) ||
    !Array.isArray(obj.added_products) ||
    !Array.isArray(obj.removed_products) ||
    !Array.isArray(obj.questions)
  ) {
    throw new Error("Bad array types");
  }

  for (const p of obj.products) {
    if (!p || typeof p.name !== "string") throw new Error("Bad product.name");
    if (typeof p.amount !== "number") throw new Error("Bad product.amount");
    if (
      typeof p.category !== "string" ||
      typeof p["sub-category"] !== "string"
    ) {
      throw new Error("Bad product category/sub-category");
    }
  }

  for (const r of obj.removed_products) {
    if (
      typeof r?.["order_item.id"] !== "number" ||
      typeof r?.product_id !== "number"
    ) {
      throw new Error("Bad removed_products entry");
    }
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
      return "כדי לערוך הזמנה צריך להיות סל פעיל. להתחיל הזמנה חדשה?";
    }

    const orderItems =
      items ||
      (
        await db.query(
          `SELECT oi.*, p.name, p.display_name_en, p.category, p.sub_category AS 'sub-category'
          FROM order_item oi
          LEFT JOIN product p ON p.id = oi.product_id
          WHERE oi.order_id = ?`,
          [order.id]
        )
      )[0];

    const basePrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);

    const systemWithInputs = [
      basePrompt,
      "",
      "=== STRUCTURED CONTEXT ===",
      openQsCtx,
      "",
      `- ORDER: ${JSON.stringify(order)}`,
      `- ORDER_ITEMS: ${JSON.stringify(orderItems)}`,
    ].join("\n");

    const answer = await chat({
      message,
      history,
      systemPrompt: systemWithInputs,
    });

    let parsed;
    try {
      parsed = parseModelAnswer(answer);
    } catch (e) {
      return "מצטערים, לא הצלחתי להבין את הבקשה לעריכת ההזמנה. אפשר לנסח שוב בקצרה?";
    }

    console.log("[ORD-MODIFY] parsed answer:", JSON.stringify(parsed, null, 2));

    const qUpdates = parsed?.question_updates || {};
    if (Array.isArray(qUpdates.close_ids) && qUpdates.close_ids.length) {
      await closeQuestionsByIds(qUpdates.close_ids);
    }
    if (Array.isArray(qUpdates.delete_ids) && qUpdates.delete_ids.length) {
      await deleteQuestionsByIds(qUpdates.delete_ids);
    }

    const modelProducts = Array.isArray(parsed.products) ? parsed.products : [];
    const removedProducts = Array.isArray(parsed.removed_products)
      ? parsed.removed_products
      : [];
    let isEnglish = isEnglishSummary(parsed?.summary_line);
    const modelQuestions = normalizeIncomingQuestions(parsed?.questions, {
      preserveOptions: true,
    });

    try {
      validateModelOutput(parsed);
    } catch (e) {
      const summaryLine = isEnglish
        ? "To complete your order, I need a few clarifications:"
        : "כדי להשלים את ההזמנה חסרות כמה הבהרות:";
      const itemsBlock = "";
      const headerBlock = "";
      const questionsBlock = buildQuestionsBlock({
        questions: modelQuestions,
        isEnglish,
      });
      return [summaryLine, itemsBlock, headerBlock, questionsBlock]
        .filter(Boolean)
        .join("\n");
    }

    let combinedQuestions = [];
    let limitWarningsBlock = "";
    try {
      const txRes = await applyOrderModifications({
        shop_id,
        order_id,
        modelProducts,
        removedProducts,
        isEnglish,
        maxPerProduct,
      });

      const hasItems = Array.isArray(txRes.items) && txRes.items.length > 0;
      combinedQuestions = [
        ...modelQuestions,
        ...(Array.isArray(txRes.questions) ? txRes.questions : []),
      ];

      await saveOpenQuestions({
        customer_id,
        shop_id,
        order_id: order.id,
        questions: combinedQuestions,
      });

      const cappedWarnings = (txRes.meta && txRes.meta.cappedWarnings) || [];

      if (cappedWarnings.length) {
        if (isEnglish) {
          limitWarningsBlock = `Note: you can order up to ${maxPerProduct} units per product.`;
        } else {
          limitWarningsBlock = `שימו לב: ניתן להזמין עד ${maxPerProduct} יחידות מכל מוצר.`;
        }
      }

      const hasQuestions = combinedQuestions.length > 0;

      let summaryLine;
      let itemsBlock = "";
      let headerBlock = "";

      if (!hasItems) {
        //empty order
        const orderIdPart = isEnglish
          ? `(Order: #${order.id})`
          : `(הזמנה מספר: #${order.id})`;

        if (isEnglish) {
          if (hasQuestions) {
            summaryLine =
              `Your order is currently empty ${orderIdPart}.` +
              `\nTo build an order that fits what you want, I need your answers to a few questions:`;
          } else {
            summaryLine = `Your order is currently empty ${orderIdPart}.`;
          }
        } else {
          if (hasQuestions) {
            summaryLine =
              `ההזמנה שלך כרגע ריקה ${orderIdPart}.` +
              `\nכדי שאוכל לבנות עבורך הזמנה שמתאימה לך, אני צריך תשובה לכמה שאלות:`;
          } else {
            summaryLine = `ההזמנה שלך כרגע ריקה ${orderIdPart}.`;
          }
        }

        const questionsLines = (combinedQuestions || [])
          .map((q) => `• ${q.question}`)
          .join("\n");

        return [summaryLine, questionsLines].filter(Boolean).join("\n\n");
      }

      //there is items in the order
      itemsBlock = buildItemsBlock({ items: txRes.items, isEnglish });
      summaryLine =
        typeof parsed?.summary_line === "string" && parsed.summary_line.trim()
          ? parsed.summary_line.trim()
          : isEnglish
          ? "Here is your updated order:"
          : "זוהי ההזמנה המעודכנת שלך:";

      headerBlock = isEnglish
        ? [
            `Order: #${order.id}`,
            `Subtotal: *₪${Number(txRes.total || 0).toFixed(2)}*`,
          ].join("\n")
        : [
            `מספר הזמנה: #${order.id}`,
            `סה״כ ביניים: *₪${Number(txRes.total || 0).toFixed(2)}*`,
          ].join("\n");

      console.log(
        "[ORD-MODIFY] Current items:",
        JSON.stringify(txRes.items, null, 2)
      );
      console.log(
        "[ORD-MODIFY] Removed (applied):",
        JSON.stringify(txRes.meta.removedApplied, null, 2)
      );
      console.log(
        "[ORD-MODIFY] Qty increased:",
        JSON.stringify(txRes.meta.qtyIncreased, null, 2)
      );
      console.log(
        "[ORD-MODIFY] Qty decreased:",
        JSON.stringify(txRes.meta.qtyDecreased, null, 2)
      );
      console.log(
        "[ORD-MODIFY] Added (applied):",
        JSON.stringify(txRes.meta.addedApplied, null, 2)
      );
      console.log(
        "[ORD-MODIFY] Not-found adds (with alts):",
        JSON.stringify(txRes.meta.notFoundAdds, null, 2)
      );
      console.log(
        "[ORD-MODIFY] Insufficient existing increases:",
        JSON.stringify(txRes.meta.insufficientExistingIncreases, null, 2)
      );
      console.log(
        "[ORD-MODIFY] Insufficient new adds:",
        JSON.stringify(txRes.meta.insufficientNewAdds, null, 2)
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
        modelProducts,
        removedProducts,
      });
      const [curItems] = await db.query(
        `SELECT oi.product_id, oi.amount, p.price, p.name, p.display_name_en
          FROM order_item oi
          JOIN product p ON p.id = oi.product_id
          WHERE oi.order_id = ?`,
        [order.id]
      );

      const itemsForView = (curItems || []).map((it) => ({
        name: isEnglish
          ? (it.display_name_en && it.display_name_en.trim()) || it.name
          : it.name,
        amount: Number(it.amount),
        price: Number(it.price),
      }));

      const summaryLine = isEnglish
        ? "To complete your order, I need a few clarifications:"
        : "כדי להשלים את ההזמנה חסרות כמה הבהרות:";

      const techQuestion = {
        name: null,
        question: isEnglish
          ? "We hit a technical issue applying all changes. Would you like me to try again or specify the change in a short message?"
          : "נתקלנו בתקלה טכנית ביישום כל השינויים. לנסות שוב או לפרט בקצרה את השינוי המבוקש?",
      };

      const itemsBlock = buildItemsBlock({ items: itemsForView, isEnglish });
      let total = 0;
      for (const it of curItems) {
        total = addMoney(total, mulMoney(Number(it.amount), Number(it.price)));
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

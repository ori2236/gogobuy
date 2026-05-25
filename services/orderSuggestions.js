const db = require("../config/db");
const { chat } = require("../config/openai");
const { saveChat } = require("../repositories/chat");
const { sendWhatsAppText } = require("../utilities/whatsapp");
const { isEnglishMessage } = require("../utilities/lang");
const {
  saveOpenQuestions,
  closeQuestionsByIds,
} = require("../utilities/openQuestions");
const {
  fetchActivePromotionsMap,
  calcLineTotalWithPromo,
} = require("../utilities/orders");
const {
  findBestProductForRequest,
} = require("./products");
const { buildProductRecommendationSchema } = require("../categoryHandlers/ORD/schemas/productRecommendations.schema");
const { parseModelAnswer } = require("../utilities/jsonParse");
const { roundTo } = require("../utilities/decimal");
const { getPromptFromDB } = require("../repositories/prompt");

const SUGGESTION_SOURCE = {
  GPT: "GPT_RECOMMENDATION",
  BUNDLE: "BUNDLE_PROMO",
};

const OPEN_Q_TYPES = {
  PRODUCT_RECOMMENDATION: "PRODUCT_RECOMMENDATION",
  BUNDLE_PROMO_ADD: "BUNDLE_PROMO_ADD",
  MULTI_BUNDLE_PROMO_ADD: "MULTI_BUNDLE_PROMO_ADD",
};

const MAX_PRODUCT_RECOMMENDATIONS_TO_SEND = 1;
const MAX_BUNDLE_NUDGES_PER_MESSAGE = 3;

const PRODUCT_RECOMMENDATION_PROMPT_CAT = "ORD";
const PRODUCT_RECOMMENDATION_PROMPT_SUB = "PRODUCT_RECOMMENDATIONS";

const DEFAULT_REPLY_CONFIG = {
  positive: [
    "כן",
    "כן תודה",
    "כן בבקשה",
    "תוסיף",
    "תוסיפי",
    "תוסיף לי",
    "יאללה",
    "סבבה",
    "אוקיי",
    "ok",
    "okay",
    "yes",
    "y",
    "add",
    "sure",
  ],
  negative: [
    "לא",
    "לא תודה",
    "בלי",
    "עזוב",
    "עזבי",
    "דלג",
    "תדלג",
    "לא צריך",
    "no",
    "n",
    "skip",
    "no thanks",
  ],
  all: ["הכל", "כולם", "שניהם", "את כולם", "all", "both"],
};

function safeJsonParse(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function displayName(row, isEnglish) {
  if (!row) return "";
  if (isEnglish) {
    return (row.display_name_en && String(row.display_name_en).trim()) || row.name || "";
  }
  return row.name || row.display_name_en || "";
}

function qtyText(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return "";
  return x.toFixed(3).replace(/\.?0+$/, "");
}

function normalizeReplyConfig(options) {
  const cfg = (options && options.reply_config) || {};
  const pick = (key) => {
    const custom = Array.isArray(cfg[key]) ? cfg[key] : [];
    const defaults = Array.isArray(DEFAULT_REPLY_CONFIG[key])
      ? DEFAULT_REPLY_CONFIG[key]
      : [];
    return Array.from(
      new Set([...custom, ...defaults].map((x) => String(x || "").trim().toLowerCase()).filter(Boolean)),
    );
  };

  return {
    positive: pick("positive"),
    negative: pick("negative"),
    all: pick("all"),
  };
}

function isPositiveReply(message, options) {
  const m = String(message || "").trim().toLowerCase();
  if (!m) return false;
  return normalizeReplyConfig(options).positive.includes(m);
}

function isNegativeReply(message, options) {
  const m = String(message || "").trim().toLowerCase();
  if (!m) return false;
  return normalizeReplyConfig(options).negative.includes(m);
}

function selectedActionIndexes(message, count, options) {
  const m = String(message || "").trim().toLowerCase();
  if (!m || !count) return [];

  const replyConfig = normalizeReplyConfig(options);
  if (replyConfig.all.includes(m)) {
    return Array.from({ length: count }, (_, i) => i);
  }

  const digits = Array.from(m.matchAll(/\d+/g))
    .map((x) => Number(x[0]) - 1)
    .filter((x) => Number.isInteger(x) && x >= 0 && x < count);

  if (digits.length) return Array.from(new Set(digits));

  const words = [
    ["ראשון", "הראשון", "first"],
    ["שני", "השני", "second"],
    ["שלישי", "השלישי", "third"],
  ];

  const out = [];
  for (let i = 0; i < Math.min(words.length, count); i++) {
    if (words[i].some((w) => m.includes(w))) out.push(i);
  }
  return Array.from(new Set(out));
}

async function ensureSuggestionTable() {
  // The table is created by the SQL migration and should not be created on runtime.
  // This function is kept as a harmless compatibility export for older imports/tests.
  return true;
}


async function hasShownSuggestion({ order_id, product_id, source }) {
  const [rows] = await db.query(
    `SELECT id, status
       FROM order_product_suggestion
      WHERE order_id = ?
        AND suggested_product_id = ?
        AND source = ?
      LIMIT 1`,
    [Number(order_id), Number(product_id), source],
  );
  return rows && rows.length ? rows[0] : null;
}

async function recordShownSuggestion({
  customer_id,
  shop_id,
  order_id,
  product_id,
  product_name,
  source,
}) {
  await db.query(
    `INSERT IGNORE INTO order_product_suggestion
       (customer_id, shop_id, order_id, suggested_product_id, suggested_product_name, source, status)
     VALUES (?, ?, ?, ?, ?, ?, 'shown')`,
    [
      Number(customer_id),
      Number(shop_id),
      Number(order_id),
      Number(product_id),
      String(product_name || "").slice(0, 500),
      source,
    ],
  );
}

async function updateSuggestionStatus({ order_id, product_id, source, status }) {
  await db.query(
    `UPDATE order_product_suggestion
        SET status = ?, updated_at = NOW()
      WHERE order_id = ?
        AND suggested_product_id = ?
        AND source = ?`,
    [status, Number(order_id), Number(product_id), source],
  );
}

function buildActionLabel({ action, isEnglish }) {
  const name = action.display_name || action.product_name || action.name || "";
  const addQty = qtyText(action.amount_to_add || action.amount || 1);
  if (isEnglish) return `${addQty} × ${name}`;
  return `${name} × ${addQty}`;
}

async function addSuggestionActionsToOrder({
  customer_id,
  shop_id,
  order_id,
  actions,
  maxPerProduct,
}) {
  const cleanActions = (Array.isArray(actions) ? actions : [])
    .map((a) => ({
      ...a,
      product_id: Number(a.product_id),
      amount_to_add: Number(a.amount_to_add ?? a.amount ?? 1),
      source: a.source || SUGGESTION_SOURCE.GPT,
    }))
    .filter(
      (a) =>
        Number.isFinite(a.product_id) &&
        a.product_id > 0 &&
        Number.isFinite(a.amount_to_add) &&
        a.amount_to_add > 0,
    );

  if (!cleanActions.length) {
    return { ok: false, added: [], skipped: [] };
  }

  const conn = await db.getConnection();
  const added = [];
  const skipped = [];

  try {
    await conn.beginTransaction();

    const [[order]] = await conn.query(
      `SELECT id, status
         FROM orders
        WHERE id = ? AND shop_id = ? AND customer_id = ?
        FOR UPDATE`,
      [Number(order_id), Number(shop_id), Number(customer_id)],
    );

    if (!order || !["pending", "confirmed"].includes(String(order.status))) {
      await conn.rollback();
      return { ok: false, added: [], skipped: cleanActions, reason: "ORDER_NOT_EDITABLE" };
    }

    for (const action of cleanActions) {
      const [[product]] = await conn.query(
        `SELECT id, name, display_name_en, price, stock_amount, category, sub_category
           FROM product
          WHERE id = ? AND shop_id = ?
          FOR UPDATE`,
        [Number(action.product_id), Number(shop_id)],
      );

      if (!product) {
        skipped.push({ ...action, reason: "PRODUCT_NOT_FOUND" });
        continue;
      }

      const [[existingItem]] = await conn.query(
        `SELECT id, amount, sold_by_weight
           FROM order_item
          WHERE order_id = ? AND product_id = ?
          FOR UPDATE`,
        [Number(order_id), Number(product.id)],
      );

      const existingQty = existingItem ? Number(existingItem.amount) || 0 : 0;
      const existingIsWeight =
        existingItem &&
        (existingItem.sold_by_weight === 1 || existingItem.sold_by_weight === true);
      const isWeight = existingIsWeight || action.sold_by_weight === true;

      let delta = isWeight
        ? roundTo(action.amount_to_add, 3)
        : Math.trunc(action.amount_to_add);

      if (!(delta > 0)) {
        skipped.push({ ...action, reason: "BAD_AMOUNT" });
        continue;
      }

      if (!isWeight && Number.isFinite(Number(maxPerProduct))) {
        const maxQty = Number(maxPerProduct);
        const remainingToCap = Math.max(0, maxQty - existingQty);
        if (remainingToCap <= 0) {
          skipped.push({ ...action, reason: "MAX_PER_PRODUCT" });
          continue;
        }
        delta = Math.min(delta, remainingToCap);
      }

      const stock = Number(product.stock_amount);
      if (!Number.isFinite(stock) || stock < delta) {
        skipped.push({ ...action, reason: "INSUFFICIENT_STOCK", available: stock });
        continue;
      }

      await conn.query(
        `UPDATE product
            SET stock_amount = stock_amount - ?
          WHERE id = ? AND shop_id = ?`,
        [Number(delta), Number(product.id), Number(shop_id)],
      );

      const finalQty = isWeight
        ? roundTo(existingQty + delta, 3)
        : Math.trunc(existingQty + delta);

      const promoMap = await fetchActivePromotionsMap(conn, shop_id, [product.id]);
      const promo = promoMap.get(Number(product.id)) || null;
      const { lineTotal, promo_id } = calcLineTotalWithPromo({
        unitPrice: product.price,
        amount: finalQty,
        soldByWeight: isWeight,
        promo,
      });
      const fallback = roundTo(Number(product.price) * finalQty, 2);
      const linePrice = lineTotal ?? fallback;

      if (existingItem) {
        await conn.query(
          `UPDATE order_item
              SET amount = ?,
                  sold_by_weight = ?,
                  requested_units = CASE WHEN ? = 1 THEN requested_units ELSE NULL END,
                  price = ?,
                  price_locked = 1,
                  promo_id = ?
            WHERE id = ? AND order_id = ?`,
          [
            finalQty,
            isWeight ? 1 : 0,
            isWeight ? 1 : 0,
            linePrice,
            promo_id ?? null,
            Number(existingItem.id),
            Number(order_id),
          ],
        );
      } else {
        await conn.query(
          `INSERT INTO order_item
             (order_id, product_id, amount, sold_by_weight, requested_units, price, price_locked, promo_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, 1, ?, NOW(6))`,
          [
            Number(order_id),
            Number(product.id),
            finalQty,
            isWeight ? 1 : 0,
            null,
            linePrice,
            promo_id ?? null,
          ],
        );
      }

      added.push({
        ...action,
        product_id: Number(product.id),
        product_name: product.name,
        display_name_en: product.display_name_en,
        amount_added: delta,
        final_amount: finalQty,
      });
    }

    const [[sumRow]] = await conn.query(
      `SELECT COALESCE(ROUND(SUM(price), 2), 0) AS total
         FROM order_item
        WHERE order_id = ?`,
      [Number(order_id)],
    );

    await conn.query(
      `UPDATE orders SET price = ?, updated_at = NOW(6) WHERE id = ?`,
      [Number(sumRow?.total || 0), Number(order_id)],
    );

    await conn.commit();

    for (const a of added) {
      if (a.source) {
        await updateSuggestionStatus({
          order_id,
          product_id: a.product_id,
          source: a.source,
          status: "accepted",
        }).catch((err) => console.error("[suggestions status accepted]", err));
      }
    }

    return { ok: true, added, skipped };
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

async function handleSuggestionReply({
  message,
  customer_id,
  shop_id,
  activeOrder,
  openQs,
  maxPerProduct,
}) {
  if (!activeOrder || !Array.isArray(openQs) || !openQs.length) return null;

  const candidates = openQs
    .map((q) => ({ ...q, options: safeJsonParse(q.option_set) }))
    .filter((q) => {
      const type = q.options && q.options.type;
      return Object.values(OPEN_Q_TYPES).includes(type);
    })
    .sort((a, b) => Number(b.id) - Number(a.id));

  if (!candidates.length) return null;

  const q = candidates[0];
  const data = q.options || {};
  const isEnglish = isEnglishMessage(message || q.question_text || "");

  if (isNegativeReply(message, data)) {
    await closeQuestionsByIds([q.id]);
    const actions = data.actions || (data.product_id ? [data] : []);
    for (const action of actions) {
      if (action.product_id && action.source) {
        await updateSuggestionStatus({
          order_id: activeOrder.id,
          product_id: action.product_id,
          source: action.source,
          status: "declined",
        }).catch((err) => console.error("[suggestions status declined]", err));
      }
    }
    return isEnglish ? "No problem, I won’t add it." : "אין בעיה, לא הוספתי את זה להזמנה.";
  }

  const actions = Array.isArray(data.actions)
    ? data.actions
    : data.product_id
      ? [data]
      : [];

  if (!actions.length) return null;

  let selected = [];
  if (data.type === OPEN_Q_TYPES.MULTI_BUNDLE_PROMO_ADD) {
    const idxs = selectedActionIndexes(message, actions.length, data);
    if (idxs.length) {
      selected = idxs.map((i) => actions[i]).filter(Boolean);
    } else if (isPositiveReply(message, data)) {
      selected = actions;
    } else {
      return null;
    }
  } else {
    if (!isPositiveReply(message, data)) return null;
    selected = actions;
  }

  await closeQuestionsByIds([q.id]);

  const res = await addSuggestionActionsToOrder({
    customer_id,
    shop_id,
    order_id: activeOrder.id,
    actions: selected,
    maxPerProduct,
  });

  if (!res.added.length) {
    return isEnglish
      ? "I tried to add it, but it is not available right now."
      : "ניסיתי להוסיף, אבל כרגע זה לא זמין במלאי.";
  }

  const addedText = res.added.map((a) => buildActionLabel({ action: a, isEnglish })).join("\n• ");
  return isEnglish
    ? `Added to your order:\n• ${addedText}`
    : `הוספתי להזמנה:\n• ${addedText}`;
}

async function fetchBundleRowsForOrderItems({ shop_id, order_id, productIds }) {
  const ids = Array.from(new Set((productIds || []).map(Number).filter(Boolean)));
  if (!ids.length || !order_id) return [];

  const [rows] = await db.query(
    `SELECT
       oi.product_id,
       oi.amount,
       oi.sold_by_weight,
       p.name,
       p.display_name_en,
       p.stock_amount,
       pr.id AS promo_id,
       pr.kind,
       pr.bundle_buy_qty,
       pr.bundle_pay_price,
       pr.description
     FROM order_item oi
     JOIN product p ON p.id = oi.product_id AND p.shop_id = ?
     JOIN promotion pr ON pr.id = oi.promo_id AND pr.shop_id = ?
     WHERE oi.order_id = ?
       AND oi.product_id IN (${ids.map(() => "?").join(",")})
       AND pr.kind = 'BUNDLE'
       AND (pr.start_at IS NULL OR pr.start_at <= NOW())
       AND (pr.end_at IS NULL OR pr.end_at >= NOW())`,
    [Number(shop_id), Number(shop_id), Number(order_id), ...ids],
  );

  return rows || [];
}

function buildBundleNudgeActions(rows, { isEnglish, maxPerProduct }) {
  const nudges = [];

  for (const r of rows || []) {
    const buyQty = Number(r.bundle_buy_qty);
    const qty = Number(r.amount);
    const stock = Number(r.stock_amount);
    const isWeight = r.sold_by_weight === 1 || r.sold_by_weight === true;

    if (!Number.isFinite(buyQty) || buyQty < 2) continue;
    if (!Number.isFinite(qty) || qty <= 0) continue;

    const remainder = isWeight
      ? qty % buyQty
      : Math.ceil(qty) % buyQty;
    if (!Number.isFinite(remainder) || remainder === 0) continue;

    let amountToAdd = roundTo(buyQty - remainder, isWeight ? 3 : 0);
    if (!isWeight) amountToAdd = Math.trunc(amountToAdd);
    if (!(amountToAdd > 0)) continue;

    if (Number.isFinite(stock) && stock < amountToAdd) continue;

    if (!isWeight && Number.isFinite(Number(maxPerProduct))) {
      const maxQty = Number(maxPerProduct);
      if (qty + amountToAdd > maxQty) continue;
    }

    const productName = displayName(r, isEnglish);
    const buyText = qtyText(buyQty);
    const payText = Number(r.bundle_pay_price).toFixed(2);
    const addText = qtyText(amountToAdd);

    nudges.push({
      product_id: Number(r.product_id),
      product_name: r.name,
      display_name: productName,
      amount_to_add: amountToAdd,
      sold_by_weight: isWeight,
      promo_id: Number(r.promo_id),
      source: SUGGESTION_SOURCE.BUNDLE,
      line: isEnglish
        ? `You have ${qtyText(qty)} ${productName}. Add ${addText} more to use the ${buyText} for ₪${payText} deal?`
        : `לקחת ${qtyText(qty)} ${productName}. להוסיף עוד ${addText} כדי לנצל מבצע ${buyText} ב-₪${payText}?`,
    });
  }

  nudges.sort((a, b) => Number(a.amount_to_add) - Number(b.amount_to_add));
  return nudges.slice(0, MAX_BUNDLE_NUDGES_PER_MESSAGE);
}

async function buildBundlePromotionFollowUps({
  customer_id,
  shop_id,
  order_id,
  productIds,
  isEnglish,
  maxPerProduct,
}) {
  if (!order_id || !Array.isArray(productIds) || !productIds.length) {
    return [];
  }

  const rows = await fetchBundleRowsForOrderItems({ shop_id, order_id, productIds });
  const actions = buildBundleNudgeActions(rows, { isEnglish, maxPerProduct });
  if (!actions.length) return [];

  for (const action of actions) {
    await recordShownSuggestion({
      customer_id,
      shop_id,
      order_id,
      product_id: action.product_id,
      product_name: action.product_name,
      source: SUGGESTION_SOURCE.BUNDLE,
    }).catch((err) => console.error("[bundle suggestion log]", err));
  }

  const questionText =
    actions.length === 1
      ? actions[0].line
      : isEnglish
        ? [
            "Small saving tip:",
            ...actions.map((a, i) => `${i + 1}. ${a.line}`),
            "Reply with the number, 'all', or 'no'.",
          ].join("\n")
        : [
            "טיפ קטן לחיסכון:",
            ...actions.map((a, i) => `${i + 1}. ${a.line}`),
            "אפשר לענות עם מספר, 'הכל', או 'לא'.",
          ].join("\n");

  await saveOpenQuestions({
    customer_id,
    shop_id,
    order_id,
    questions: [
      {
        name: isEnglish ? "bundle promotion" : "מבצע באנדל",
        question: questionText,
        options: {
          type:
            actions.length === 1
              ? OPEN_Q_TYPES.BUNDLE_PROMO_ADD
              : OPEN_Q_TYPES.MULTI_BUNDLE_PROMO_ADD,
          actions,
          reply_config: {
            positive: ["כן", "תוסיף", "תוסיף לי", "yes", "add"],
            negative: ["לא", "לא תודה", "no", "skip"],
            all: ["הכל", "כולם", "all", "both"],
          },
        },
      },
    ],
  });

  return [questionText];
}

function buildRecommendationUserContext({ cartItems, isEnglish }) {
  const lite = (cartItems || []).map((it) => ({
    name: displayName(it, isEnglish),
    category: it.category || null,
    sub_category: it.sub_category || it["sub-category"] || null,
    amount: Number(it.amount),
  }));

  return [
    `USER_LANGUAGE=${isEnglish ? "English" : "Hebrew"}`,
    "CURRENT_CART_JSON:",
    JSON.stringify(lite).slice(0, 4000),
  ].join("\n");
}

async function getCurrentCartItems({ order_id, shop_id }) {
  if (!order_id) return [];
  const [rows] = await db.query(
    `SELECT
       oi.product_id,
       oi.amount,
       p.name,
       p.display_name_en,
       p.category,
       p.sub_category
     FROM order_item oi
     JOIN product p ON p.id = oi.product_id AND p.shop_id = ?
     WHERE oi.order_id = ?
     ORDER BY oi.id ASC`,
    [Number(shop_id), Number(order_id)],
  );
  return rows || [];
}

async function getExistingProductIdsInOrder({ order_id }) {
  const [rows] = await db.query(
    `SELECT product_id FROM order_item WHERE order_id = ?`,
    [Number(order_id)],
  );
  return new Set((rows || []).map((r) => Number(r.product_id)).filter(Boolean));
}

async function findAvailableRecommendationMatch({ shop_id, suggestion }) {
  const req = {
    name: suggestion.name,
    outputName: suggestion.name,
    amount: 1,
    units: null,
    sold_by_weight: false,
    exclude_tokens: [],
    category: suggestion.category || "",
    "sub-category": suggestion["sub-category"] || suggestion.sub_category || "",
  };

  const row = await findBestProductForRequest(shop_id, req);
  if (!row) return null;

  const stock = Number(row.stock_amount);
  if (!Number.isFinite(stock) || stock < 1) return null;

  return row;
}

async function runProductRecommendationsAndSend({
  customer_id,
  shop_id,
  order_id,
  phone_number,
  isEnglish,
}) {
  try {
    if (!customer_id || !shop_id || !order_id || !phone_number) return;
    const cartItems = await getCurrentCartItems({ order_id, shop_id });
    if (!cartItems.length) return;

    const systemPrompt = await getPromptFromDB(
      PRODUCT_RECOMMENDATION_PROMPT_CAT,
      PRODUCT_RECOMMENDATION_PROMPT_SUB,
    );

    if (!systemPrompt) {
      console.warn(
        `[product recommendations] Missing DB prompt ${PRODUCT_RECOMMENDATION_PROMPT_CAT}.${PRODUCT_RECOMMENDATION_PROMPT_SUB}`,
      );
      return;
    }

    const answer = await chat({
      message: "Suggest complementary supermarket products for this cart.",
      history: [],
      systemPrompt,
      userContext: buildRecommendationUserContext({ cartItems, isEnglish }),
      response_format: {
        type: "json_schema",
        json_schema: await buildProductRecommendationSchema(),
      },
      prompt_cache_key: "ord_product_recommendations_v2",
    });

    let parsed;
    try {
      parsed = JSON.parse(answer);
    } catch (e1) {
      try {
        parsed = parseModelAnswer(answer);
      } catch (e2) {
        console.error("[product recommendations] Failed to parse JSON", e2?.message, answer);
        return;
      }
    }

    const suggestions = Array.isArray(parsed?.suggestions) ? parsed.suggestions : [];
    if (!suggestions.length) return;

    const existingIds = await getExistingProductIdsInOrder({ order_id });
    const chosen = [];
    const seenIds = new Set();

    for (const s of suggestions) {
      if (!s || !s.name || chosen.length >= MAX_PRODUCT_RECOMMENDATIONS_TO_SEND) break;
      const row = await findAvailableRecommendationMatch({ shop_id, suggestion: s });
      if (!row) continue;

      const pid = Number(row.id);
      if (!pid || existingIds.has(pid) || seenIds.has(pid)) continue;
      seenIds.add(pid);

      const alreadyShown = await hasShownSuggestion({
        order_id,
        product_id: pid,
        source: SUGGESTION_SOURCE.GPT,
      });
      if (alreadyShown) continue;

      chosen.push({ suggestion: s, row });
    }

    if (!chosen.length) return;

    const first = chosen[0];
    const productName = displayName(first.row, isEnglish);
    const cartNames = cartItems.map((it) => displayName(it, isEnglish)).filter(Boolean);
    const contextNames = cartNames.slice(0, 2).join(isEnglish ? " and " : " ו");

    const message = isEnglish
      ? `By the way, I noticed you have ${contextNames}. ${productName} could fit well with that. Want me to add one?`
      : `אגב, ראיתי שלקחת ${contextNames}. יכול להתאים גם ${productName}. להוסיף לך אחד להזמנה?`;

    await recordShownSuggestion({
      customer_id,
      shop_id,
      order_id,
      product_id: Number(first.row.id),
      product_name: first.row.name,
      source: SUGGESTION_SOURCE.GPT,
    });

    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id,
      questions: [
        {
          name: first.row.name,
          question: message,
          options: {
            type: OPEN_Q_TYPES.PRODUCT_RECOMMENDATION,
            actions: [
              {
                product_id: Number(first.row.id),
                product_name: first.row.name,
                display_name: productName,
                amount_to_add: 1,
                sold_by_weight: false,
                source: SUGGESTION_SOURCE.GPT,
              },
            ],
            reply_config: {
              positive: ["כן", "תוסיף", "תוסיף לי", "yes", "add"],
              negative: ["לא", "לא תודה", "no", "skip"],
            },
          },
        },
      ],
    });

    await sendWhatsAppText(phone_number, message);
    await saveChat({
      customer_id,
      shop_id,
      sender: "bot",
      status: "classified",
      message,
    });
  } catch (err) {
    console.error("[product recommendations async]", err?.response?.data || err);
  }
}

function shouldStartProductRecommendations(payload) {
  if (!payload || typeof payload !== "object") return false;
  const ctx = payload.productRecommendationContext;
  if (!ctx || !ctx.order_id) return false;
  if (Array.isArray(payload.followUpMessages) && payload.followUpMessages.length) {
    // bundle savings are higher priority; avoid two marketing follow-ups for the same user action
    return false;
  }
  if (ctx.hasOpenQuestions) return false;
  return true;
}

module.exports = {
  SUGGESTION_SOURCE,
  OPEN_Q_TYPES,
  ensureSuggestionTable,
  buildBundlePromotionFollowUps,
  handleSuggestionReply,
  runProductRecommendationsAndSend,
  shouldStartProductRecommendations,
};

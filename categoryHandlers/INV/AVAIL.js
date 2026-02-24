const { chat } = require("../../config/openai");
const { getPromptFromDB } = require("../../repositories/prompt");
const {
  buildAlternativeQuestions,
  searchVariants,
} = require("../../services/products");
const {
  searchProductsAvailability,
} = require("../../services/productsAvailability");
const db = require("../../config/db");
const { saveOpenQuestions } = require("../../utilities/openQuestions");
const { buildInvAvailSchema } = require("./schemas/avail.schema");
const { parseModelAnswer } = require("../../utilities/jsonParse");
const { buildQuestionsBlock } = require("../../utilities/messageBuilders");

const PROMPT_CAT = "INV";
const PROMPT_SUB = "AVAIL";

function joinNames(names, isEnglish) {
  if (!names || !names.length) return "";
  if (names.length === 1) return names[0];
  if (names.length === 2) {
    return isEnglish
      ? `${names[0]} and ${names[1]}`
      : `${names[0]} ו${names[1]}`;
  }
  const allButLast = names.slice(0, -1).join(", ");
  const last = names[names.length - 1];
  return isEnglish ? `${allButLast} and ${last}` : `${allButLast} ו${last}`;
}

async function fetchProductsByNameKeyword(shop_id, keyword, limit = 50) {
  if (!keyword || !keyword.trim()) return [];

  const raw = keyword.trim();
  const tokens = raw.split(/\s+/).filter(Boolean);

  let sql = `
      SELECT id, name, display_name_en, stock_amount
      FROM product
      WHERE shop_id = ?
        AND (stock_amount IS NULL OR stock_amount > 0)
  `;
  const params = [shop_id];

  for (const t of tokens) {
    sql += `
      AND (
        name COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
        OR display_name_en COLLATE utf8mb4_general_ci LIKE CONCAT('%', ?, '%')
      )
    `;
    params.push(t, t);
  }

  sql += `
      ORDER BY name ASC, id DESC
      LIMIT ?
  `;
  params.push(limit);

  const [rows] = await db.query(sql, params);
  return rows || [];
}

async function saveOpenQuestionsAvail(botPayload, customer_id, shop_id) {
  const secondSentence = botPayload.split(". ")[1];
  const question = secondSentence || botPayload;

  await saveOpenQuestions({
    customer_id,
    shop_id,
    order_id: null,
    questions: [
      {
        name: null,
        question,
        options: null,
      },
    ],
  });
}

async function checkAvailability({
  message,
  customer_id,
  shop_id,
  history,
  isEnglish,
  maxPerProduct,
}) {
  if (typeof message !== "string" || !message.trim()) {
    throw new Error("INV.AVAIL: message is required and must be a string");
  }
  if (!shop_id) {
    throw new Error("INV.AVAIL: shop_id is required");
  }
  if (!customer_id) {
    throw new Error("INV.AVAIL: customer_id is required");
  }

  const systemPrompt = await getPromptFromDB(PROMPT_CAT, PROMPT_SUB);

  const answer = await chat({
    message,
    history,
    systemPrompt,
    response_format: {
      type: "json_schema",
      json_schema: await buildInvAvailSchema(),
    },
    prompt_cache_key: "inv_avail_v1",
  });

  let parsed;
  try {
    parsed = JSON.parse(answer);
  } catch (e1) {
    try {
      parsed = parseModelAnswer(answer);
    } catch (e2) {
      console.error("[INV.AVAIL] parseModelAnswer failed:", e2);
      const botPayload = isEnglish
        ? "Sorry, I couldn’t understand which products you want me to check. Can you please write again which product you’re asking about?"
        : "מצטערים, לא הבנו על איזה מוצר אתה שואל. תוכל לכתוב שוב בקצרה על איזה מוצר לבדוק מלאי?";
      await saveFallbackOpenQuestion(botPayload, customer_id, shop_id);
      return botPayload;
    }
  }

  console.log("[INV.AVAIL] parsed answer:", JSON.stringify(parsed, null, 2));

  const productRequests = Array.isArray(parsed.products) ? parsed.products : [];
  const clarifyQuestionsFromModel = Array.isArray(parsed.questions)
    ? parsed.questions
    : [];

  console.log(
    "[INV.AVAIL] productRequests:",
    productRequests.map((p, idx) => ({
      idx,
      name: p.name,
      searchTerm: p.searchTerm,
      outputName: p.outputName,
      availability_intent: p.availability_intent,
      category: p.category,
      subCategory: p["sub-category"] || p.sub_category,
      requested_amount: p.requested_amount,
    })),
  );

  console.log(
    "[INV.AVAIL] clarifyQuestionsFromModel:",
    clarifyQuestionsFromModel,
  );

  const hasProducts = productRequests.length > 0;
  const hasClarify = clarifyQuestionsFromModel.length > 0;

  if (!hasProducts && !hasClarify) {
    const botPayload = isEnglish
      ? "I couldn’t identify any specific product in your question. Can you please write which product you want me to check?"
      : "לא הצלחתי לזהות על איזה מוצר בדיוק אתה שואל. תוכל לכתוב את שם המוצר שתרצה שאבדוק?";

    await saveOpenQuestionsAvail(botPayload, customer_id, shop_id);
    return botPayload;
  }

  let availabilityLines = [];
  let variantLines = [];
  let found = [];
  let notFound = [];

  if (hasProducts) {
    const searchRequests = productRequests.map((p) => {
      const n = Number(p?.requested_amount);
      const amount = Number.isFinite(n) && n > 0 ? n : 1;
      return { ...p, amount };
    });

    const res = await searchProductsAvailability(shop_id, searchRequests);

    found = res.found || [];
    notFound = res.notFound || [];
    console.log("[INV.AVAIL] found:", found);
    console.log("[INV.AVAIL] notFound:", notFound);
    console.log("[INV.AVAIL] searchProducts result:", {
      found: found.length,
      notFound: notFound.length,
    });
  }

  const foundByIndex = new Map();
  for (const f of found) {
    foundByIndex.set(f.originalIndex, f);
  }

  const quantityNames = [];
  const EXCLUDE_FOR_ALT = new Set([
    "ASK_QUANTITY",
    "ASK_VARIANTS",
    "ASK_BRANDS",
    "ASK_BOTH",
  ]);

  let notFoundForAlternatives = notFound.filter((nf) => {
    const req = productRequests[nf.originalIndex] || {};
    const intentRaw = String(req.availability_intent || "")
      .trim()
      .toUpperCase();
    return !EXCLUDE_FOR_ALT.has(intentRaw);
  });

  for (let i = 0; i < productRequests.length; i++) {
    const req = productRequests[i] || {};
    const intentRaw = String(req.availability_intent || "")
      .trim()
      .toUpperCase();

    let intent = "CHECK_AVAILABILITY";
    if (
      intentRaw === "ASK_VARIANTS" ||
      intentRaw === "ASK_BRANDS" ||
      intentRaw === "ASK_BOTH" ||
      intentRaw === "ASK_QUANTITY"
    ) {
      intent = intentRaw;
    }

    const f = foundByIndex.get(i) || null;

    const rawName = (req.name || "").trim();
    const searchTerm = (req.searchTerm || "").trim();
    const outputSearchTerm = (req.outputSearchTerm || "").trim();

    const searchTermForText = isEnglish
      ? outputSearchTerm || searchTerm
      : searchTerm;

    let heName = rawName;
    if (!heName && intent === "CHECK_AVAILABILITY" && f && f.matched_name) {
      heName = String(f.matched_name || "").trim();
    }

    const enNameCandidate = (
      req.outputName ||
      (f && f.matched_display_name_en) ||
      ""
    ).trim();

    const displayName = isEnglish
      ? enNameCandidate || heName || "this product"
      : heName || enNameCandidate || "המוצר הזה";

    let displayLabel;

    if (isEnglish) {
      const st = (outputSearchTerm || searchTerm || "").trim();
      if (rawName && searchTerm && searchTerm !== rawName) {
        const base = (req.outputName || enNameCandidate || rawName).trim();
        displayLabel = st ? `${base} by ${st}` : base;
      } else if (req.outputName && req.outputName.trim()) {
        displayLabel = req.outputName.trim();
      } else if (enNameCandidate) {
        displayLabel = enNameCandidate;
      } else if (st) {
        displayLabel = st;
      } else if (rawName) {
        displayLabel = rawName;
      } else {
        displayLabel = displayName;
      }
    } else {
      if (rawName && searchTerm && searchTerm !== rawName) {
        displayLabel = `${rawName} של ${searchTerm}`;
      } else if (rawName) {
        displayLabel = rawName;
      } else if (searchTerm) {
        displayLabel = searchTerm;
      } else {
        displayLabel = displayName; // fallback
      }
    }

    // ASK_QUANTITY - user asks "how many units do you have in stock?"
    if (intent === "ASK_QUANTITY") {
      if (!heName && !searchTerm) {
        availabilityLines.push(
          isEnglish
            ? "I can’t tell how many units are in stock because I’m not sure which product you mean. Can you write its name?"
            : "אני לא יכול לענות כמה יחידות יש במלאי כי לא ברור על איזה מוצר אתה שואל. תוכל לכתוב את שם המוצר?",
        );
        continue;
      }

      if (!f) {
        availabilityLines.push(
          isEnglish
            ? `${displayLabel} is currently out of stock.`
            : `${displayLabel} כרגע חסר במלאי.`,
        );
        continue;
      }

      const stock = Number.isFinite(Number(f.stock_amount))
        ? Number(f.stock_amount)
        : null;

      if (stock === null) {
        quantityNames.push(displayLabel);
      } else if (stock <= 0) {
        availabilityLines.push(
          isEnglish
            ? `Right now we don’t have any ${displayLabel} in stock.`
            : `כרגע אין לנו במלאי בכלל ${displayLabel}.`,
        );
      } else if (stock < maxPerProduct) {
        availabilityLines.push(
          isEnglish
            ? `Right now we have ${stock} units of ${displayLabel} in stock.`
            : `כרגע יש לנו במלאי ${stock} יחידות של ${displayLabel}.`,
        );
      } else {
        quantityNames.push(displayLabel);
      }

      continue;
    }

    if (intent === "CHECK_AVAILABILITY") {
      // Simple "do you have X?"

      if (!f) continue;
      const stock = Number.isFinite(Number(f.stock_amount))
        ? Number(f.stock_amount)
        : 0;
      const requestedAmount =
        Number.isFinite(Number(f.requested_amount)) &&
        Number(f.requested_amount) > 0
          ? Number(f.requested_amount)
          : 1;

      if (stock <= 0) {
        availabilityLines.push(
          isEnglish
            ? `${displayLabel} is currently out of stock.`
            : `${displayLabel} כרגע חסר במלאי.`,
        );

        notFoundForAlternatives.push({
          originalIndex: i,
          requested_name: req.name || f.matched_name || null,
          requested_output_name: req.outputName || enNameCandidate || null,
          requested_amount: requestedAmount,
          category: f.category,
          sub_category: f.sub_category,
        });
      } else if (requestedAmount > maxPerProduct) {
        availabilityLines.push(
          isEnglish
            ? `This is a large quantity request. For orders of more than ${maxPerProduct} units, it’s better to talk directly with a store representative so they can check the stock and help you.`
            : `זו כמות גדולה. להזמנות של יותר מ-${maxPerProduct} יחידות עדיף לדבר עם נציג מהסופר כדי לבדוק את המלאי ולעזור לך.`,
        );
      } else if (requestedAmount <= 1) {
        // Simple: "Do you have X?" → answer with product + price
        const price = Number.isFinite(Number(f.price)) ? Number(f.price) : null;

        const productNameForUser = isEnglish
          ? (f.matched_display_name_en && f.matched_display_name_en.trim()) ||
            displayLabel
          : (f.matched_name && f.matched_name.trim()) || displayLabel;

        if (isEnglish) {
          if (price !== null) {
            availabilityLines.push(
              `Yes, we currently have ${displayLabel} in stock. The product is: ${productNameForUser}, and it costs ₪${price.toFixed(
                2,
              )}.`,
            );
          } else {
            availabilityLines.push(
              `Yes, we currently have ${displayLabel} in stock. The product is: ${productNameForUser}.`,
            );
          }
        } else {
          if (price !== null) {
            availabilityLines.push(
              `כן, יש לנו במלאי ${displayLabel}. המוצר הוא: ${productNameForUser}, והוא עולה ₪${price.toFixed(
                2,
              )}.`,
            );
          } else {
            availabilityLines.push(
              `כן, יש לנו במלאי ${displayLabel}. המוצר הוא: ${productNameForUser}.`,
            );
          }
        }
      } else if (stock >= requestedAmount) {
        if (stock < maxPerProduct) {
          availabilityLines.push(
            isEnglish
              ? `Right now we have ${stock} units of ${displayLabel} in stock, which is enough for the quantity you asked for.`
              : `כרגע יש לנו במלאי ${stock} יחידות של ${displayLabel}, ויש מספיק לכמות שביקשת.`,
          );
        } else {
          availabilityLines.push(
            isEnglish
              ? `Yes, we have enough ${displayLabel} in stock for the quantity you asked for.`
              : `כן, יש לנו מספיק ${displayLabel} במלאי לכמות שביקשת.`,
          );
        }
      } else {
        // Partial availability
        availabilityLines.push(
          isEnglish
            ? `Right now we only have ${stock} units of ${displayLabel} in stock, which is less than you asked for.`
            : `כרגע יש לנו במלאי רק ${stock} יחידות של ${displayLabel}, פחות מהכמות שביקשת.`,
        );

        notFoundForAlternatives.push({
          originalIndex: i,
          requested_name: req.name || f.matched_name || null,
          requested_output_name: req.outputName || enNameCandidate || null,
          requested_amount: requestedAmount,
          category: f.category,
          sub_category: f.sub_category,
        });
      }
    } else if (
      intent === "ASK_VARIANTS" ||
      intent === "ASK_BRANDS" ||
      intent === "ASK_BOTH"
    ) {
      // "Which variants/brands/options do you have for X?"
      const cat = (req.category || (f && f.category) || "").trim();
      const subCategory = (
        req["sub-category"] ||
        req.sub_category ||
        (f && f.sub_category) ||
        ""
      ).trim();

      if (!cat && !subCategory && searchTerm && !heName) {
        const rows = await fetchProductsByNameKeyword(shop_id, searchTerm, 50);

        if (!rows || !rows.length) {
          variantLines.push(
            isEnglish
              ? `I couldn’t find any in-stock products matching "${searchTermForText}".`
              : `לא מצאתי מוצרים במלאי שמתאימים ל-"${searchTermForText}".`,
          );
        } else {
          const listNames = Array.from(
            new Set(
              rows.map((r) =>
                isEnglish
                  ? (r.display_name_en && r.display_name_en.trim()) || r.name
                  : r.name,
              ),
            ),
          );

          if (listNames.length === 1) {
            const single = listNames[0];
            variantLines.push(
              isEnglish
                ? `We currently have the following product for "${searchTermForText}": ${single}.`
                : `כרגע יש לנו במלאי את המוצר הבא עבור "${searchTermForText}": ${single}.`,
            );
          } else if (listNames.length <= 4) {
            variantLines.push(
              isEnglish
                ? `We currently have the following products for "${searchTermForText}": ${listNames.join(
                    ", ",
                  )}.`
                : `כרגע יש לנו במלאי את המוצרים הבאים עבור "${searchTermForText}": ${listNames.join(
                    ", ",
                  )}.`,
            );
          } else {
            const itemsBlock = listNames.join("\n");

            variantLines.push(
              isEnglish
                ? `We currently have the following products for "${searchTermForText}":\n${itemsBlock}`
                : `כרגע יש לנו במלאי את המוצרים הבאים עבור "${searchTermForText}":\n${itemsBlock}`,
            );
          }
        }
        continue;
      }

      if (!cat && !subCategory) {
        availabilityLines.push(
          isEnglish
            ? `I’m not sure which type of product you mean for "${displayLabel}". Can you clarify?`
            : `לא ברור לי בדיוק לאיזה סוג מוצר אתה מתכוון לגבי "${displayLabel}". תוכל לחדד?`,
        );
        continue;
      }

      const effectiveSearchTerm =
        rawName && searchTerm && searchTerm !== rawName
          ? `${rawName} ${searchTerm}`
          : rawName || searchTerm || enNameCandidate || null;

      const variantsRows = await searchVariants(shop_id, {
        category: cat || null,
        subCategory: subCategory || null,
        searchTerm: effectiveSearchTerm,
        limit: 50,
      });

      const norm = (s) =>
        String(s || "")
          .replace(/\s+/g, " ")
          .trim();

      if (!variantsRows || !variantsRows.length) {
        let nameForAlt = rawName || null;
        const nameN = norm(nameForAlt);
        const stN = norm(searchTermForText || searchTerm);

        if (stN) {
          if (nameN && stN !== nameN) {
            nameForAlt = isEnglish
              ? `${nameN} by ${stN}`
              : `${nameN} של ${stN}`;
          } else if (nameN) {
            nameForAlt = nameN;
          } else {
            nameForAlt = stN;
          }
        } else {
          nameForAlt = nameN || null;
        }

        if (!nameForAlt) nameForAlt = heName || null;

        notFoundForAlternatives.push({
          originalIndex: i,
          requested_name: nameForAlt,
          requested_output_name: req.outputName || null,
          requested_amount: 1,
          category: cat || null,
          sub_category: subCategory || null,
        });
      } else {
        const listNames = Array.from(
          new Set(
            variantsRows.map((r) =>
              isEnglish
                ? (r.display_name_en && r.display_name_en.trim()) || r.name
                : r.name,
            ),
          ),
        );

        if (listNames.length === 1) {
          const single = listNames[0];

          if (intent === "ASK_VARIANTS") {
            variantLines.push(
              isEnglish
                ? `We have the following option for ${displayLabel}: ${single}.`
                : `יש לנו את האפשרות הבאה עבור ${displayLabel}: ${single}.`,
            );
          } else if (intent === "ASK_BRANDS") {
            variantLines.push(
              isEnglish
                ? `We carry the following product of ${displayLabel}: ${single}.`
                : `יש לנו במלאי את המוצר הבא מסוג ${displayLabel}: ${single}.`,
            );
          } else if (intent === "ASK_BOTH") {
            variantLines.push(
              isEnglish
                ? `Yes, we have ${displayLabel}. The product in stock is: ${single}.`
                : `כן, יש לנו ${displayLabel}. המוצר במלאי הוא: ${single}.`,
            );
          }
        } else if (listNames.length <= 4) {
          const inlineList = listNames.join(", ");

          if (intent === "ASK_VARIANTS") {
            variantLines.push(
              isEnglish
                ? `We have the following options for ${displayLabel}: ${inlineList}.`
                : `יש לנו את האפשרויות הבאות עבור ${displayLabel}: ${inlineList}.`,
            );
          } else if (intent === "ASK_BRANDS") {
            variantLines.push(
              isEnglish
                ? `We carry the following options for ${displayLabel}: ${inlineList}.`
                : `יש לנו במלאי את המוצרים הבאים מסוג ${displayLabel}: ${inlineList}.`,
            );
          } else if (intent === "ASK_BOTH") {
            variantLines.push(
              isEnglish
                ? `Yes, we have ${displayLabel}. Available options: ${inlineList}.`
                : `כן, יש לנו ${displayLabel}. האפשרויות במלאי: ${inlineList}.`,
            );
          }
        } else {
          const itemsBlock = listNames.join("\n");

          if (intent === "ASK_VARIANTS") {
            variantLines.push(
              isEnglish
                ? `We have the following options for ${displayLabel}:\n${itemsBlock}`
                : `יש לנו את האפשרויות הבאות עבור ${displayLabel}:\n${itemsBlock}`,
            );
          } else if (intent === "ASK_BRANDS") {
            variantLines.push(
              isEnglish
                ? `We carry the following options for ${displayLabel}:\n${itemsBlock}`
                : `יש לנו במלאי את המוצרים הבאים מסוג ${displayLabel}:\n${itemsBlock}`,
            );
          } else if (intent === "ASK_BOTH") {
            variantLines.push(
              isEnglish
                ? `Yes, we have ${displayLabel}. Available options:\n${itemsBlock}`
                : `כן, יש לנו ${displayLabel}. האפשרויות במלאי:\n${itemsBlock}`,
            );
          }
        }
      }
    }
  }

  if (quantityNames.length) {
    const namesJoined = joinNames(quantityNames, isEnglish);
    availabilityLines.push(
      isEnglish
        ? `I can’t tell exactly how many units of ${namesJoined} we have in stock.`
        : `אני לא יכול לענות כמה יחידות של ${namesJoined} יש במלאי.`,
    );
  }

  let altQuestions = [];
  if (notFoundForAlternatives.length) {
    const foundIdsSet = new Set(
      found.filter((f) => Number(f.stock_amount) > 0).map((f) => f.product_id),
    );

    const altRes = await buildAlternativeQuestions(
      shop_id,
      notFoundForAlternatives,
      foundIdsSet,
      isEnglish,
      "availability",
    );
    altQuestions = altRes.altQuestions || [];
  }

  const allQuestions = [...clarifyQuestionsFromModel, ...altQuestions];
  if (allQuestions.length) {
    await saveOpenQuestions({
      customer_id,
      shop_id,
      order_id: null,
      questions: allQuestions,
    });
  }

  const allAvailabilityLines = [...availabilityLines, ...variantLines].filter(
    (line) => line && line.trim(),
  );

  const hasAvailability = allAvailabilityLines.length > 0;

  let body = "";
  if (hasAvailability) {
    body = allAvailabilityLines.join("\n");
  }

  let questionsBlock = "";

  if (allQuestions.length === 1) {
    questionsBlock = allQuestions[0].question || "";
  } else if (allQuestions.length) {
    questionsBlock = buildQuestionsBlock({
      questions: allQuestions,
      isEnglish,
    });

    if (questionsBlock) {
      const header = isEnglish ? "Questions:" : "שאלות:";
      const lines = questionsBlock.split("\n");

      const cleaned = [];
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        if (trimmed === header) continue;
        cleaned.push(line);
      }

      questionsBlock = cleaned.join("\n");
    }
  }

  if (questionsBlock && questionsBlock.trim()) {
    body = body ? [body, questionsBlock].join("\n\n") : questionsBlock;
  }

  if (!body || !body.trim()) {
    body = isEnglish
      ? "I tried to understand which products to check in the inventory, but I’m not sure. Can you please write again which product you want me to check?"
      : "ניסיתי להבין על איזה מוצר לבדוק מלאי, אבל לא הצלחתי. תוכל לכתוב שוב על איזה מוצר אתה שואל?";
  }

  return body;
}

module.exports = {
  checkAvailability,
};

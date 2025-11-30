const { orderProducts } = require("./ORD/CREATE");
const { modifyOrder } = require("./ORD/MODIFY");
const { orderReview } = require("./ORD/REVIEW");
const { askToCancelOrder } = require("./ORD/CANCEL");
const { checkAvailability } = require("./INV/AVAIL");

const CATEGORY_HANDLERS = {
  ORD: {
    CREATE: async (ctx) =>
      orderProducts({
        message: ctx.message,
        customer_id: ctx.customer_id,
        shop_id: ctx.shop_id,
        history: ctx.history,
        openQsCtx: ctx.openQsCtx,
        maxPerProduct: ctx.maxPerProduct,
      }),

    MODIFY: async (ctx) =>
      modifyOrder({
        message: ctx.message,
        customer_id: ctx.customer_id,
        shop_id: ctx.shop_id,
        order_id: ctx.order_id,
        activeOrder: ctx.activeOrder,
        items: ctx.items,
        history: ctx.history,
        openQsCtx: ctx.openQsCtx,
        maxPerProduct: ctx.maxPerProduct,
      }),

    REVIEW: async (ctx) =>
      orderReview(
        ctx.activeOrder,
        ctx.items,
        ctx.isEnglish,
        ctx.customer_id,
        ctx.shop_id
      ),

    CANCEL: async (ctx) =>
      askToCancelOrder(
        ctx.activeOrder,
        ctx.isEnglish,
        ctx.customer_id,
        ctx.shop_id
      ),

    CHECKOUT: async (ctx) =>
      ctx.isEnglish
        ? "At the moment you can only create, modify, cancel and review orders. Checkout will be available soon."
        : "כרגע יש לנו תמיכה רק ביצירת, עריכת, ביטול וצפייה בהזמנות. סיום הזמנה יתווסף בהמשך.",
  },
  INV: {
    AVAIL: async (ctx) =>
      checkAvailability({
        message: ctx.message,
        customer_id: ctx.customer_id,
        shop_id: ctx.shop_id,
        history: ctx.history,
        isEnglish: ctx.isEnglish,
        maxPerProduct: ctx.maxPerProduct,
      }),
  },
};

const ALLOWED = Object.fromEntries(
  Object.entries(CATEGORY_HANDLERS).map(([cat, subs]) => [
    cat,
    new Set(Object.keys(subs)),
  ])
);

function isValidCategorySub(category, subcategory) {
  if (!category || !subcategory) return false;
  const cat = String(category).toUpperCase().trim();
  const sub = String(subcategory).toUpperCase().trim();
  return !!(ALLOWED[cat] && ALLOWED[cat].has(sub));
}

async function routeByCategory(category, subcategory, ctx) {
  if (!category || !subcategory) return null;

  const cat = String(category).toUpperCase().trim();
  const sub = String(subcategory).toUpperCase().trim();

  const catHandlers = CATEGORY_HANDLERS[cat];
  if (!catHandlers) return null;

  const handler = catHandlers[sub];
  if (!handler) return null;

  return handler(ctx);
}

module.exports = {
  CATEGORY_HANDLERS,
  routeByCategory,
  isValidCategorySub,
};

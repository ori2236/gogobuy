const { parseShopId } = require("../utilities/dashboardUtils");
const {
  fetchDashboardMarketDayPromotions,
  listMarketDayRecipients,
  createMarketDayRecipient,
  updateMarketDayRecipient,
  deleteMarketDayRecipient,
  sendMarketDayTemplateToRecipient,
  sendMarketDayTemplateToRecipients,
} = require("../services/marketDayPromotions");

function handleError(res, label, err) {
  const status = Number(err?.status || 500);
  if (status >= 500) console.error(label, err);
  return res.status(status).json({ ok: false, message: err?.message || "Server error" });
}

exports.listPromotions = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const data = await fetchDashboardMarketDayPromotions(shopId);
    return res.json({ ok: true, ...data });
  } catch (err) {
    return handleError(res, "[marketDay.listPromotions]", err);
  }
};

exports.listRecipients = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const includeInactive = String(req.query.include_inactive || req.query.includeInactive || "").toLowerCase() === "true";
    const data = await listMarketDayRecipients(shopId, { sync: true, includeInactive });
    return res.json({ ok: true, ...data });
  } catch (err) {
    return handleError(res, "[marketDay.listRecipients]", err);
  }
};

exports.createRecipient = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const recipient = await createMarketDayRecipient(shopId, req.body || {});
    return res.status(201).json({ ok: true, recipient });
  } catch (err) {
    return handleError(res, "[marketDay.createRecipient]", err);
  }
};

exports.updateRecipient = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const recipient = await updateMarketDayRecipient(shopId, req.params.recipientId, req.body || {});
    return res.json({ ok: true, recipient });
  } catch (err) {
    return handleError(res, "[marketDay.updateRecipient]", err);
  }
};

exports.deleteRecipient = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const recipient = await deleteMarketDayRecipient(shopId, req.params.recipientId);
    return res.json({ ok: true, recipient });
  } catch (err) {
    return handleError(res, "[marketDay.deleteRecipient]", err);
  }
};

exports.sendTemplate = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const result = await sendMarketDayTemplateToRecipients(shopId, req.body || {});
    return res.json(result);
  } catch (err) {
    return handleError(res, "[marketDay.sendTemplate]", err);
  }
};

exports.sendTemplateToRecipient = async (req, res) => {
  try {
    const shopId = parseShopId(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const result = await sendMarketDayTemplateToRecipient(shopId, req.params.recipientId);
    return res.json(result);
  } catch (err) {
    return handleError(res, "[marketDay.sendTemplateToRecipient]", err);
  }
};

const { parseShopId } = require("../utilities/dashboardUtils");
const {
  listStaffRecipients,
  createStaffRecipient,
  updateStaffRecipient,
  deleteStaffRecipient,
  sendStaffRecipientTestAlert,
} = require("../services/staffOrderAlerts");

function shopIdForRequest(req) {
  return req.dashboardUser?.shop_id ? Number(req.dashboardUser.shop_id) : parseShopId(req);
}

function handleError(res, label, err) {
  console.error(`[${label}]`, err?.response?.data || err);
  return res.status(err.status || 500).json({
    ok: false,
    message: err.status ? err.message : "Server error",
    details: err?.response?.data || undefined,
  });
}

exports.listRecipients = async (req, res) => {
  try {
    const shopId = shopIdForRequest(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const recipients = await listStaffRecipients(shopId);
    return res.json({ ok: true, recipients });
  } catch (err) {
    return handleError(res, "dashboardStaffWhatsapp.listRecipients", err);
  }
};

exports.createRecipient = async (req, res) => {
  try {
    const shopId = shopIdForRequest(req);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }

    const recipient = await createStaffRecipient(shopId, req.body || {});
    return res.status(201).json({ ok: true, recipient });
  } catch (err) {
    return handleError(res, "dashboardStaffWhatsapp.createRecipient", err);
  }
};

exports.updateRecipient = async (req, res) => {
  try {
    const shopId = shopIdForRequest(req);
    const recipientId = Number(req.params.recipientId);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isFinite(recipientId) || recipientId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid recipient id" });
    }

    const recipient = await updateStaffRecipient(shopId, recipientId, req.body || {});
    return res.json({ ok: true, recipient });
  } catch (err) {
    return handleError(res, "dashboardStaffWhatsapp.updateRecipient", err);
  }
};

exports.deleteRecipient = async (req, res) => {
  try {
    const shopId = shopIdForRequest(req);
    const recipientId = Number(req.params.recipientId);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isFinite(recipientId) || recipientId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid recipient id" });
    }

    const deleted = await deleteStaffRecipient(shopId, recipientId);
    if (!deleted) {
      return res.status(404).json({ ok: false, message: "המספר לא נמצא." });
    }

    return res.json({ ok: true });
  } catch (err) {
    return handleError(res, "dashboardStaffWhatsapp.deleteRecipient", err);
  }
};

exports.sendTest = async (req, res) => {
  try {
    const shopId = shopIdForRequest(req);
    const recipientId = Number(req.params.recipientId);
    if (!Number.isFinite(shopId) || shopId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid shop_id" });
    }
    if (!Number.isFinite(recipientId) || recipientId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid recipient id" });
    }

    const result = await sendStaffRecipientTestAlert({ shopId, recipientId });
    return res.json({ ok: true, result });
  } catch (err) {
    return handleError(res, "dashboardStaffWhatsapp.sendTest", err);
  }
};

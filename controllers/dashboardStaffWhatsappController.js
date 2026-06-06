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

function normalizeMetaError(err) {
  const metaError = err?.response?.data?.error;
  if (!metaError) return null;

  const details = String(metaError.details || metaError.message || "").toLowerCase();
  const code = Number(metaError.code || 0);

  if (
    code === 132001 ||
    details.includes("template name does not exist") ||
    details.includes("does not exist in he") ||
    details.includes("does not exist in he_il")
  ) {
    return {
      status: 400,
      message:
        "תבנית ההתראה עדיין לא אושרה על ידי Meta, או שהיא לא קיימת בחשבון ה-WhatsApp של הסניף. אחרי שהאישור יושלם נסה שוב.",
    };
  }

  if (code === 131030 || details.includes("allowed list")) {
    return {
      status: 400,
      message:
        "המספר הזה עדיין לא מורשה לקבל הודעות ממספר בדיקה של Meta. אם שולחים ממספר עסק אמיתי בפרודקשן, הבעיה הזו לא אמורה להופיע.",
    };
  }

  if (code === 132000 || details.includes("parameter") || details.includes("translation")) {
    return {
      status: 400,
      message:
        "יש חוסר התאמה בין משתני התבנית בקוד לבין התבנית שמוגדרת ב-Meta. צריך לבדוק את מבנה המשתנים של התבנית.",
    };
  }

  return null;
}

function handleError(res, label, err) {
  console.error(`[${label}]`, err?.response?.data || err);

  const normalizedMeta = normalizeMetaError(err);
  if (normalizedMeta) {
    return res.status(normalizedMeta.status).json({
      ok: false,
      message: normalizedMeta.message,
      details: err?.response?.data || undefined,
    });
  }

  return res.status(err.status || 500).json({
    ok: false,
    message: err.message || "Server error",
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

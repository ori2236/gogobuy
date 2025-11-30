const { processMessage } = require("../services/messageFlow");
const { normalizeOutboundMessage } = require("../utilities/normalize");

async function handleMessage(req, res, next) {
  const { message, phone_number, shop_id } = req.body;
  if (!message || typeof message !== "string" || !phone_number || !shop_id) {
    return res.status(400).json({
      success: false,
      message: "message, phone_number and shop_id are required",
    });
  }

  try {
    const responseMessage = await processMessage(
      message,
      phone_number,
      shop_id
    );

    if (responseMessage && responseMessage.skipSend) {
      return res.json({ success: true, message: null, skipSend: true });
    }

    const messageText = normalizeOutboundMessage(responseMessage);
    res.json({ success: true, message: messageText });
  } catch (error) {
    console.error(error);
    next(error);
  }
}

module.exports = {
  handleMessage,
};

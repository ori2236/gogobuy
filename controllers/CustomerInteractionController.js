const { processMessage } = require("../services/messageFlow");
const { normalizeOutboundMessage } = require("../utilities/normalize");
const { getShopById } = require("../repositories/shopWhatsappPhone");

function parseShopId(raw) {
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : null;
}

async function handleMessage(req, res, next) {
  const { message, phone_number } = req.body;
  const shop_id = parseShopId(req.body?.shop_id);

  if (!message || typeof message !== "string" || !phone_number || !shop_id) {
    return res.status(400).json({
      success: false,
      message: "message, phone_number and a valid numeric shop_id are required",
    });
  }

  try {
    const shop = await getShopById(shop_id);

    if (!shop?.shop_id || !shop?.chain_id) {
      return res.status(400).json({
        success: false,
        message: `Invalid shop_id: ${shop_id}`,
      });
    }

    const responseMessage = await processMessage(
      message,
      phone_number,
      shop_id,
    );

    if (responseMessage && responseMessage.skipSend) {
      return res.json({
        success: true,
        shop_id,
        chain_id: Number(shop.chain_id),
        shop_name: shop.shop_name,
        message: null,
        skipSend: true,
      });
    }

    const messageText = normalizeOutboundMessage(responseMessage);
    res.json({
      success: true,
      shop_id,
      chain_id: Number(shop.chain_id),
      shop_name: shop.shop_name,
      message: messageText,
    });
  } catch (error) {
    console.error(error);
    next(error);
  }
}

module.exports = {
  handleMessage,
};

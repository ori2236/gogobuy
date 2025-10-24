const express = require("express");
const router = express.Router();

const { sendWhatsAppText } = require("../config/whatsapp");
const customerInteractionController = require("../controllers/CustomerInteractionController");

const VERIFY_TOKEN = (process.env.WHATSAPP_VERIFY_TOKEN || "").trim();

router.get("/webhooks", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = (req.query["hub.verify_token"] || "").trim();
  const challenge = req.query["hub.challenge"];

  console.log("VERIFY /webhooks", { mode, token, expected: VERIFY_TOKEN });
  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

router.post("/webhooks", async (req, res) => {
  try {
    const SHOP_ID = 1;
    const change = req.body?.entry?.[0]?.changes?.[0]?.value;
    const msg = change?.messages?.[0];

    if (!msg) return res.sendStatus(200);

    const from = msg.from; // מספר בווטסאפ (E.164 ללא +)
    let messageText = "";

    if (msg.type === "text") {
      messageText = msg.text?.body || "";
    } else {
      messageText = "";
    }

    let reply = "תודה";
    if (messageText) {
      const botResp = await customerInteractionController.processMessage(
        messageText,
        from,
        SHOP_ID
      );

      if (typeof botResp === "string") reply = botResp;
      else if (botResp?.message) reply = String(botResp.message);
      else if (botResp?.reply) reply = String(botResp.reply);
      else reply = "תודה";
    }

    await sendWhatsAppText(from, reply);
    console.log("Bot replied to", from);

    return res.sendStatus(200);
  } catch (e) {
    console.error("Webhook error", e?.response?.data || e.message);
    return res.sendStatus(200);
  }
});

module.exports = router;

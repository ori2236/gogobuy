const express = require("express");
const router = express.Router();
const db = require("../config/db");
const { sendWhatsAppText } = require("../config/whatsapp");
const { processMessage } = require("../controllers/CustomerInteractionController");

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

    const waMessageId = msg.id;
    const from = msg.from;
    let messageText = "";

    if (msg.type === "text") {
      messageText = msg.text?.body || "";
    } else {
      messageText = "";
    }

    if (waMessageId) {
      try {
        const [existing] = await db.query(
          "SELECT id FROM whatsapp_incoming WHERE wa_message_id = ? LIMIT 1",
          [waMessageId]
        );

        if (existing.length) {
          console.log("Duplicate WhatsApp message, skipping", waMessageId);
          return res.sendStatus(200);
        }

        await db.query(
          "INSERT INTO whatsapp_incoming (wa_message_id) VALUES (?)",
          [waMessageId]
        );
      } catch (err) {
        if (err && err.code === "ER_DUP_ENTRY") {
          console.log(
            "Duplicate WhatsApp message (race), skipping",
            waMessageId
          );
          return res.sendStatus(200);
        }
        console.error("Error when inserting whatsapp_incoming:", err);
        return res.sendStatus(200);
      }
    }

    res.sendStatus(200);

    (async () => {
      try {
        let reply = "תודה";

        if (messageText) {
          const botResp = await processMessage(
            messageText,
            from,
            SHOP_ID
          );

          if (botResp && botResp.skipSend) {
            console.log(
              "Skipping WhatsApp reply due to logical duplicate for waMessageId",
              waMessageId
            );
            return;
          }

          if (typeof botResp === "string") reply = botResp;
          else if (botResp?.message) reply = String(botResp.message);
          else if (botResp?.reply) reply = String(botResp.reply);
        }

        await sendWhatsAppText(from, reply);
        console.log("Bot replied to", from, "for waMessageId", waMessageId);
      } catch (err) {
        console.error(
          "Webhook async error:",
          err?.response?.data || err.message
        );
      }
    })();
  } catch (e) {
    console.error("Webhook error (outer):", e?.response?.data || e.message);
    return res.sendStatus(200);
  }
});

module.exports = router;

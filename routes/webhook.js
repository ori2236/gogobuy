const express = require("express");
const router = express.Router();
const db = require("../config/db");
const { sendWhatsAppText } = require("../utilities/whatsapp");
const { processMessage } = require("../services/messageFlow");
const {
  runProductRecommendationsAndSend,
  shouldStartProductRecommendations,
} = require("../services/orderSuggestions");
const { sendDeferredCheckoutNudge } = require("../utilities/checkoutNudge");
const {
  getShopByIncomingPhoneId,
} = require("../repositories/shopWhatsappPhone");
const { detectIsEnglish } = require("../utilities/lang");
const { botText } = require("../utilities/i18n");

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
    const receivedAt = Date.now();

    const change = req.body?.entry?.[0]?.changes?.[0]?.value;
    const msg = change?.messages?.[0];

    if (!msg) return res.sendStatus(200);

    const incomingPhoneNumberId = String(
      change?.metadata?.phone_number_id || "",
    ).trim();

    if (!incomingPhoneNumberId) {
      console.warn("[webhook] Missing metadata.phone_number_id. Message ignored to avoid routing to the wrong shop.");
      return res.sendStatus(200);
    }

    const phoneMapping = await getShopByIncomingPhoneId(incomingPhoneNumberId);

    if (!phoneMapping?.shop_id) {
      console.warn("[webhook] No active shop mapping for WhatsApp phone_number_id. Message ignored:", incomingPhoneNumberId);
      return res.sendStatus(200);
    }

    const SHOP_ID = Number(phoneMapping.shop_id);

    if (!Number.isInteger(SHOP_ID) || SHOP_ID <= 0) {
      console.warn("[webhook] Invalid mapped shop_id. Message ignored:", {
        incomingPhoneNumberId,
        shop_id: phoneMapping.shop_id,
      });
      return res.sendStatus(200);
    }

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
          [waMessageId],
        );

        if (existing.length) {
          console.log("Duplicate WhatsApp message, skipping", waMessageId);
          return res.sendStatus(200);
        }

        await db.query(
          "INSERT INTO whatsapp_incoming (wa_message_id) VALUES (?)",
          [waMessageId],
        );
      } catch (err) {
        if (err && err.code === "ER_DUP_ENTRY") {
          console.log(
            "Duplicate WhatsApp message (race), skipping",
            waMessageId,
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
        let reply = botText("webhookDefaultAck", detectIsEnglish(messageText));
        let botResp = null;

        if (messageText) {
          botResp = await processMessage(
            messageText,
            from,
            SHOP_ID,
            waMessageId,
            receivedAt,
            incomingPhoneNumberId,
          );

          if (botResp && botResp.skipSend) {
            console.log(
              "Skipping WhatsApp reply due to logical duplicate for waMessageId",
              waMessageId,
            );
            return;
          }

          if (typeof botResp === "string") reply = botResp;
          else if (botResp?.message) reply = String(botResp.message);
          else if (botResp?.reply) reply = String(botResp.reply);
        } else {
          return;
        }

        await sendWhatsAppText(from, reply, incomingPhoneNumberId);

        const followUpMessages = Array.isArray(botResp?.followUpMessages)
          ? botResp.followUpMessages
          : [];

        for (const followUp of followUpMessages) {
          const text = typeof followUp === "string" ? followUp.trim() : "";
          if (!text) continue;
          await sendWhatsAppText(from, text, incomingPhoneNumberId);
        }

        const deferredCheckoutNudge = botResp?.deferredCheckoutNudge || null;

        if (shouldStartProductRecommendations(botResp)) {
          const ctx = botResp.productRecommendationContext;
          setImmediate(async () => {
            const didSendRecommendation = await runProductRecommendationsAndSend({
              ...ctx,
              phone_number: from,
              businessPhoneNumberId: incomingPhoneNumberId,
            });

            // Checkout nudges should come only after async recommendations finish.
            // If a recommendation was sent, it is already an open question, so do not stack
            // a checkout question immediately after it.
            if (!didSendRecommendation && deferredCheckoutNudge) {
              await sendDeferredCheckoutNudge({
                checkoutNudge: deferredCheckoutNudge,
                phone_number: from,
                businessPhoneNumberId: incomingPhoneNumberId,
              });
            }
          });
        } else if (deferredCheckoutNudge) {
          setImmediate(() => {
            sendDeferredCheckoutNudge({
              checkoutNudge: deferredCheckoutNudge,
              phone_number: from,
              businessPhoneNumberId: incomingPhoneNumberId,
            });
          });
        }

        console.log("Bot replied to", from, "for waMessageId", waMessageId);
      } catch (err) {
        console.error(
          "Webhook async error:",
          err?.response?.data || err.message,
        );
      }
    })();
  } catch (e) {
    console.error("Webhook error (outer):", e?.response?.data || e.message);
    return res.sendStatus(200);
  }
});

module.exports = router;

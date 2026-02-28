const axios = require("axios");

const PHONE_ID = process.env.WHATSAPP_PHONE_ID.trim();
const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN.trim();

function getWhatsAppConfig() {
  if (!PHONE_ID || !ACCESS_TOKEN) {
    throw new Error("Missing WHATSAPP_PHONE_ID or WHATSAPP_TOKEN in env");
  }
  const url = `https://graph.facebook.com/v18.0/${PHONE_ID}/messages`;
  const headers = {
    Authorization: `Bearer ${ACCESS_TOKEN}`,
    "Content-Type": "application/json",
  };
  return { url, headers };
}

async function sendWhatsAppText(to, body) {
  const { url, headers } = getWhatsAppConfig();

  const payload = {
    messaging_product: "whatsapp",
    to, // מספר בפורמט E.164 ללא +
    type: "text",
    text: { body },
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

async function sendWhatsAppMarkAsRead(message_id) {
  if (!message_id) return null;

  const { url, headers } = getWhatsAppConfig();
  const payload = {
    messaging_product: "whatsapp",
    status: "read",
    message_id,
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

// Mark as read + typing indicator
async function sendWhatsAppTypingIndicator(message_id) {
  if (!message_id) return null;

  const { url, headers } = getWhatsAppConfig();
  const payload = {
    messaging_product: "whatsapp",
    status: "read",
    message_id,
    typing_indicator: { type: "text" },
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

module.exports = {
  sendWhatsAppText,
  sendWhatsAppMarkAsRead,
  sendWhatsAppTypingIndicator,
};
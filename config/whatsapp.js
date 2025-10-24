const axios = require("axios");

const PHONE_ID = (process.env.WHATSAPP_PHONE_ID).trim();
const ACCESS_TOKEN = (process.env.WHATSAPP_TOKEN).trim();

async function sendWhatsAppText(to, body) {
  if (!PHONE_ID || !ACCESS_TOKEN) {
    throw new Error("Missing WHATSAPP_PHONE_ID or WHATSAPP_TOKEN in env");
  }
  const url = `https://graph.facebook.com/v18.0/${PHONE_ID}/messages`;
  const payload = {
    messaging_product: "whatsapp",
    to, // מספר בפורמט E.164 ללא +
    type: "text",
    text: { body },
  };
  const headers = {
    Authorization: `Bearer ${ACCESS_TOKEN}`,
    "Content-Type": "application/json",
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

module.exports = { sendWhatsAppText }; 

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

module.exports = {
  getWhatsAppConfig,
};

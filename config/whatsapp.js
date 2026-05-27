const ACCESS_TOKEN = String(process.env.WHATSAPP_TOKEN || "").trim();
const DEFAULT_PHONE_ID = String(process.env.WHATSAPP_PHONE_ID || "").trim();
const GRAPH_VERSION = String(
  process.env.WHATSAPP_GRAPH_VERSION || "v18.0",
).trim();

function getWhatsAppConfig(phoneNumberId = DEFAULT_PHONE_ID) {
  const cleanPhoneNumberId = String(
    phoneNumberId || DEFAULT_PHONE_ID || "",
  ).trim();

  if (!cleanPhoneNumberId) {
    throw new Error("Missing WhatsApp phone number id");
  }

  if (!ACCESS_TOKEN) {
    throw new Error("Missing WHATSAPP_TOKEN");
  }

  return {
    url: `https://graph.facebook.com/${GRAPH_VERSION}/${cleanPhoneNumberId}/messages`,
    headers: {
      Authorization: `Bearer ${ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    phoneNumberId: cleanPhoneNumberId,
  };
}

module.exports = {
  getWhatsAppConfig,
};

const axios = require("axios");
const { getWhatsAppConfig } = require("../config/whatsapp");

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




/*
const axios = require("axios");
const { getWhatsAppConfig } = require("../config/whatsapp");

const isDryRun =
  String(process.env.WHATSAPP_DRY_RUN || "").toLowerCase() === "true";

async function sendWhatsAppText(to, body) {
  if (isDryRun) {
    console.log("\n================ WHATSAPP DRY RUN ================");
    console.log("TO:", to);
    console.log("BODY:\n" + body);
    console.log("===================================================\n");

    return {
      dryRun: true,
      to,
      body,
    };
  }

  const { url, headers } = getWhatsAppConfig();

  const payload = {
    messaging_product: "whatsapp",
    to,
    type: "text",
    text: { body },
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

async function sendWhatsAppMarkAsRead(message_id) {
  if (!message_id) return null;

  if (isDryRun) {
    console.log("[WHATSAPP DRY RUN] mark as read:", message_id);
    return { dryRun: true, message_id };
  }

  const { url, headers } = getWhatsAppConfig();
  const payload = {
    messaging_product: "whatsapp",
    status: "read",
    message_id,
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

async function sendWhatsAppTypingIndicator(message_id) {
  if (!message_id) return null;

  if (isDryRun) {
    console.log("[WHATSAPP DRY RUN] typing indicator:", message_id);
    return { dryRun: true, message_id };
  }

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
*/
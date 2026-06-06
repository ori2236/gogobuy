const axios = require("axios");
const { getWhatsAppConfig } = require("../config/whatsapp");

async function sendWhatsAppText(to, body, phoneNumberId = null) {
  const { url, headers } = getWhatsAppConfig(phoneNumberId);

  const payload = {
    messaging_product: "whatsapp",
    to,
    type: "text",
    text: { body },
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

async function sendWhatsAppMarkAsRead(message_id, phoneNumberId = null) {
  if (!message_id) return null;

  const { url, headers } = getWhatsAppConfig(phoneNumberId);

  const payload = {
    messaging_product: "whatsapp",
    status: "read",
    message_id,
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

async function sendWhatsAppTypingIndicator(message_id, phoneNumberId = null) {
  if (!message_id) return null;

  const { url, headers } = getWhatsAppConfig(phoneNumberId);

  const payload = {
    messaging_product: "whatsapp",
    status: "read",
    message_id,
    typing_indicator: { type: "text" },
  };

  const { data } = await axios.post(url, payload, { headers });
  return data;
}

async function sendWhatsAppTemplate(
  to,
  templateName,
  languageCode,
  bodyParams,
  phoneNumberId = null,
  tokenOverride = null,
) {
  const axios = require("axios");
  const { getWhatsAppConfig } = require("../config/whatsapp");

  const config = getWhatsAppConfig(phoneNumberId);

  const headers = tokenOverride
    ? {
        Authorization: `Bearer ${tokenOverride}`,
        "Content-Type": "application/json",
      }
    : config.headers;

  const payload = {
    messaging_product: "whatsapp",
    recipient_type: "individual",
    to,
    type: "template",
    template: {
      name: templateName,
      language: {
        code: languageCode,
      },
      components: [
        {
          type: "body",
          parameters: bodyParams,
        },
      ],
    },
  };

  const { data } = await axios.post(config.url, payload, { headers });
  return data;
}

module.exports = {
  sendWhatsAppText,
  sendWhatsAppMarkAsRead,
  sendWhatsAppTypingIndicator,
  sendWhatsAppTemplate,
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

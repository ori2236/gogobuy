const express = require('express');
const axios = require('axios');
require('dotenv').config();

const router = express.Router();

const VERIFY_TOKEN = process.env.WHATSAPP_VERIFY_TOKEN;
const ACCESS_TOKEN = process.env.WHATSAPP_TOKEN;
const PHONE_ID     = process.env.WHATSAPP_PHONE_ID;

router.get('/webhook/whatsapp', (req, res) => {
  const mode   = req.query['hub.mode'];
  const token  = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

router.post('/webhook/whatsapp', async (req, res) => {
  try {
    const change = req.body?.entry?.[0]?.changes?.[0]?.value;
    const msg    = change?.messages?.[0];

    if (msg && msg.type === 'text') {
      const to = msg.from;
      await axios.post(`https://graph.facebook.com/v18.0/${PHONE_ID}/messages`, {
        messaging_product: "whatsapp",
        to,
        type: "text",
        text: { body: "שלום, הגעת לעסק שלי" }
      }, {
        headers: {
          Authorization: `Bearer ${ACCESS_TOKEN}`,
          "Content-Type": "application/json"
        }
      });
      console.log('Replied to', to);
    }
    
    return res.sendStatus(200);
  } catch (e) {
    console.error('Webhook error', e?.response?.data || e.message);
    return res.sendStatus(200);
  }
});

module.exports = router;

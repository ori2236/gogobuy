
require("dotenv").config({ quiet: true });
const express = require("express");
const cors = require("cors");
const config = require("./config/config");
const db = require("./config/db");

const { expireStalePendingOrders } = require("./utilities/orders");

const customerInteractionRoutes = require("./routes/customerInteractionRoutes");

const whatsappRoutes = require('./routes/whatsapp');

let _expireJobRunning = false;

async function runExpireJob() {
  if (_expireJobRunning) return;
  _expireJobRunning = true;
  try {
    const res = await expireStalePendingOrders({ hours: 24 });
    console.log(`[EXPIRE] expired ${res.expired} stale pending orders`);
  } catch (e) {
    console.error("[EXPIRE] Error:", e);
  } finally {
    _expireJobRunning = false;
  }
}

const app = express();
const port = config.port || 3000;

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.use("/api/customer-interaction", customerInteractionRoutes);











app.get('/webhooks', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = (req.query['hub.verify_token'] || '').trim();
  const challenge = req.query['hub.challenge'];
  const EXPECTED = (process.env.WHATSAPP_VERIFY_TOKEN || '').trim();

  console.log('VERIFY /webhooks', { mode, token, expected: EXPECTED });

  if (mode === 'subscribe' && token === EXPECTED) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});




app.post('/webhooks', async (req, res) => {
  const axios = require('axios');
  try {
    const change = req.body?.entry?.[0]?.changes?.[0]?.value;
    const msg    = change?.messages?.[0];

    if (msg && msg.type === 'text') {
      const to = msg.from; // מספר שולח בפורמט E.164 ללא +
      await axios.post(`https://graph.facebook.com/v18.0/${process.env.WHATSAPP_PHONE_ID}/messages`, {
        messaging_product: "whatsapp",
        to,
        type: "text",
        text: { body: "שלום, הגעת לעסק שלי" }
      }, {
        headers: {
          Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
          "Content-Type": "application/json"
        }
      });
      console.log('Replied to', to);
    }

    // תמיד להחזיר 200 מהר
    return res.sendStatus(200);
  } catch (e) {
    console.error('Webhook error', e?.response?.data || e.message);
    return res.sendStatus(200);
  }
});





app.get('/health', (req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});



// START SERVER
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

db.getConnection()
  .then((connection) => {
    console.log("Database connected");
    connection.release(); // Release the connection back to the pool

    runExpireJob();
    setInterval(runExpireJob, 15 * 60 * 1000);
  })
  .catch((err) => {
    console.error("Failed to connect to the database:", err);
    process.exit(1);
  });

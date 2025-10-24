require("dotenv").config({ quiet: true });
const express = require("express");
const cors = require("cors");
const config = require("./config/config");
const db = require("./config/db");

const { expireStalePendingOrders } = require("./utilities/orders");

const customerInteractionRoutes = require("./routes/customerInteractionRoutes");
const webhooksRoutes = require("./routes/webhook");

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
app.use("/", webhooksRoutes);


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




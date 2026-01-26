require("dotenv").config({ quiet: true });
const express = require("express");
const cors = require("cors");
const config = require("./config/config");
const db = require("./config/db");

const customerInteractionRoutes = require("./routes/customerInteractionRoutes");
const webhooksRoutes = require("./routes/webhook");
const dashboardRoutes = require("./routes/dashboard");
const deleteLastData = require("./routes/deleteLastData");
const { rebuildTokenWeightsForShop } = require("./services/products");

const app = express();
const port = config.port || 3000;

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.use("/api/customer-interaction", customerInteractionRoutes);
app.use("/", webhooksRoutes);

//Dashboard routes
app.use("/api/dashboard", dashboardRoutes);

//temp
app.use("/api/deleteLastData", deleteLastData);

app.get("/health", (req, res) => {
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
  })
  .then(() => rebuildTokenWeightsForShop(1)) // shop_id
  .catch((err) => {
    console.error("Failed to connect to the database:", err);
    process.exit(1);
  });

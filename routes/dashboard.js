const express = require("express");
const router = express.Router();
const picker = require("../controllers/dashboardPickerController");

// GET orders for picker dashboard
router.get("/picker/orders", picker.getPickerOrders);

// PATCH order status (confirmed->preparing, preparing->ready)
router.patch("/picker/orders/:orderId/status", picker.updateOrderStatus);

module.exports = router;

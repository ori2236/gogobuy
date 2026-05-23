const express = require("express");
const router = express.Router();
const picker = require("../controllers/dashboardPickerController");
const stock = require("../controllers/dashboardStockController");
const promotions = require("../controllers/dashboardPromotionsController");

router.get("/picker/orders", picker.getPickerOrders);

router.patch("/picker/orders/:orderId/status", picker.updateOrderStatus);

router.get("/stock/categories", stock.getStockCategories);

router.get("/promotions", promotions.listPromotions);

router.post("/promotions", promotions.createPromotion);

router.patch("/promotions/:id", promotions.updatePromotion);

router.delete("/promotions/:id", promotions.deletePromotion);

router.get("/stock/products", stock.listStockProducts);

router.post("/stock/products", stock.createStockProduct);

router.patch("/stock/products/:id", stock.updateStockProduct);

router.delete("/stock/products/:id", stock.deleteStockProduct);

module.exports = router;

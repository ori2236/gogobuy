const express = require("express");
const router = express.Router();
const picker = require("../controllers/dashboardPickerController");
const stock = require("../controllers/dashboardStockController");
const promotions = require("../controllers/dashboardPromotionsController");
const auth = require("../controllers/dashboardAuthController");
const settings = require("../controllers/dashboardSettingsController");
const staffWhatsapp = require("../controllers/dashboardStaffWhatsappController");
const { requireDashboardAuth, requireDashboardAdmin } = require("../middleware/dashboardAuth");

router.post("/auth/login", auth.login);
router.get("/auth/me", requireDashboardAuth, auth.me);

router.use(requireDashboardAuth);

router.get("/settings/business", settings.getBusinessSettings);
router.patch("/settings/business", requireDashboardAdmin, settings.updateBusinessSettings);

router.get("/settings/staff-whatsapp-recipients", staffWhatsapp.listRecipients);
router.post("/settings/staff-whatsapp-recipients", requireDashboardAdmin, staffWhatsapp.createRecipient);
router.patch("/settings/staff-whatsapp-recipients/:recipientId", requireDashboardAdmin, staffWhatsapp.updateRecipient);
router.delete("/settings/staff-whatsapp-recipients/:recipientId", requireDashboardAdmin, staffWhatsapp.deleteRecipient);
router.post("/settings/staff-whatsapp-recipients/:recipientId/test", requireDashboardAdmin, staffWhatsapp.sendTest);

router.get("/picker/orders", picker.getPickerOrders);

router.patch("/picker/orders/:orderId/status", picker.updateOrderStatus);
router.patch("/picker/orders/:orderId/items/:itemId", picker.updateOrderItemPickerDetails);

router.get("/stock/categories", stock.getStockCategories);

router.get("/promotions", promotions.listPromotions);

router.post("/promotions", promotions.createPromotion);

router.patch("/promotions/:id", promotions.updatePromotion);

router.delete("/promotions/:id", promotions.deletePromotion);

router.get("/stock/products", stock.listStockProducts);

router.post("/stock/products", requireDashboardAdmin, stock.createStockProduct);

router.patch("/stock/products/:id", requireDashboardAdmin, stock.updateStockProduct);

router.delete("/stock/products/:id", requireDashboardAdmin, stock.deleteStockProduct);

module.exports = router;

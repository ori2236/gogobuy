const db = require('../config/db')
const { chat } = require("../config/openai");

module.exports = {
    async processMessage(message, phone_number, shop_id) {
        const reply = await chat({ message });
        return reply || "לא התקבלה תשובה מהמודל.";
    },

    async handleMessage(req, res, next) {
        const { message, phone_number, shop_id } = req.body
        if (!message || typeof message !== "string") {
          return res
            .status(400)
            .json({ success: false, message: "message is required" });
        }

        try {
            const responseMessage = await module.exports.processMessage(
              message,
              phone_number,
              shop_id
            );
            res.json({ success: true, message: responseMessage })
        } catch (error) {
            console.error(error);
            next(error)
        }
    }
}

const axios = require("axios");
require('dotenv').config()

module.exports = {
    whatsappKey: process.env.WHATSAPP_KEY,
    async sendWhatsAppMessage(to, message){
        const API_URL = `https://graph.facebook.com/v18.0/562506186943560/messages`;
        const accessToken = "EAASW9wn4O3QBO7t2KHGKREfv5BV9eCD7B7ZCDoB0ZAZAcBdXlKWJVu3Y93xlzua0xy7rhMZARhb9s9h9inL58gTXYva1njrErfBh8i9AnFbMpJVAyJNvqD0XZAtniy47uDlaHo2ZASfP33sFZAw25RhuKN2qxxptTDeoU3atGNQ0hgjhe8tkcSK6uthlibNQOOgiQZDZD";

        const payload = {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to: to, // Customer's phone number
            type: "text",
            text: { body: message }
        };

        try {
            const response = await axios.post(API_URL, payload, {
                headers: {
                    Authorization: `Bearer ${accessToken}`,
                    "Content-Type": "application/json"
                }
            });
            console.log("Message sent:", response.data);
        } catch (error) {
            console.error("Error sending message:", error.response ? error.response.data : error.message);
        }
    }
}
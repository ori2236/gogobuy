const express = require('express')
const router = express.Router()
const customerInteractionController = require('../controllers/CustomerInteractionController')

// Main endpoint for customer interaction
router.post('/', customerInteractionController.handleMessage)

module.exports = router

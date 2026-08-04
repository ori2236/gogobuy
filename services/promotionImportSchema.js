const db = require("../config/db");

async function ensurePromotionImportSchema(conn = db) {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS promotion_import_batch (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      shop_id INT UNSIGNED NOT NULL,
      source VARCHAR(80) NOT NULL,
      original_filename VARCHAR(255) DEFAULT NULL,
      file_sha256 CHAR(64) NOT NULL,
      plan_sha256 CHAR(64) NOT NULL,
      status VARCHAR(32) NOT NULL,
      total_source_rows INT UNSIGNED NOT NULL DEFAULT 0,
      ready_rows INT UNSIGNED NOT NULL DEFAULT 0,
      review_rows INT UNSIGNED NOT NULL DEFAULT 0,
      blocked_rows INT UNSIGNED NOT NULL DEFAULT 0,
      applied_actions INT UNSIGNED NOT NULL DEFAULT 0,
      report_json JSON DEFAULT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      applied_at DATETIME DEFAULT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_promotion_import_plan (shop_id, source, plan_sha256),
      KEY idx_promotion_import_shop_created (shop_id, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS promotion_source_link (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      shop_id INT UNSIGNED NOT NULL,
      source VARCHAR(80) NOT NULL,
      external_reward_id VARCHAR(120) NOT NULL,
      source_slot_key VARCHAR(255) NOT NULL,
      target_type VARCHAR(64) NOT NULL,
      target_id BIGINT UNSIGNED NOT NULL,
      identity_hash CHAR(64) NOT NULL,
      content_hash CHAR(64) NOT NULL,
      last_batch_id BIGINT UNSIGNED DEFAULT NULL,
      source_title VARCHAR(255) DEFAULT NULL,
      source_payload JSON DEFAULT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_promotion_source_slot (shop_id, source, external_reward_id, source_slot_key),
      KEY idx_promotion_source_target (shop_id, target_type, target_id),
      KEY idx_promotion_source_identity (shop_id, identity_hash)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS promotion_import_batch_item (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      batch_id BIGINT UNSIGNED NOT NULL,
      reward_id VARCHAR(120) NOT NULL,
      source_slot_key VARCHAR(255) DEFAULT NULL,
      action VARCHAR(64) NOT NULL,
      status VARCHAR(32) NOT NULL,
      target_type VARCHAR(64) DEFAULT NULL,
      target_id BIGINT UNSIGNED DEFAULT NULL,
      identity_hash CHAR(64) DEFAULT NULL,
      message VARCHAR(1000) DEFAULT NULL,
      source_payload JSON DEFAULT NULL,
      before_payload JSON DEFAULT NULL,
      after_payload JSON DEFAULT NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_promotion_import_item_batch (batch_id),
      KEY idx_promotion_import_item_reward (batch_id, reward_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  `);
}

module.exports = { ensurePromotionImportSchema };

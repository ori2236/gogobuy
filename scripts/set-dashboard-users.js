require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const db = require("../config/db");

function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const hash = crypto.scryptSync(String(password), salt, 64).toString("hex");
  return `scrypt$v1$${salt}$${hash}`;
}

function patchDashboardSeed() {
  const authPath = path.join(__dirname, "..", "middleware", "dashboardAuth.js");

  if (!fs.existsSync(authPath)) {
    console.warn("[WARN] middleware/dashboardAuth.js not found, skipping seed patch.");
    return;
  }

  const current = fs.readFileSync(authPath, "utf8");

  const oldBlock = `ON DUPLICATE KEY UPDATE
            shop_id = VALUES(shop_id),
            password_hash = VALUES(password_hash),
            role = VALUES(role),
            is_active = 1`;

  const newBlock = `ON DUPLICATE KEY UPDATE
            id = id`;

  if (current.includes(newBlock)) {
    console.log("[OK] Dashboard seed already patched: existing users will not be overwritten.");
    return;
  }

  if (!current.includes(oldBlock)) {
    console.warn("[WARN] Could not find the exact seed overwrite block. DB users will be updated, but check dashboardAuth.js manually.");
    return;
  }

  const backupPath = `${authPath}.bak-${Date.now()}`;
  fs.copyFileSync(authPath, backupPath);

  const updated = current.replace(oldBlock, newBlock);
  fs.writeFileSync(authPath, updated, "utf8");

  console.log(`[OK] Patched dashboard seed overwrite.`);
  console.log(`[OK] Backup created: ${backupPath}`);
}

async function ensureDashboardUserTable() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS dashboard_user (
      id INT UNSIGNED NOT NULL AUTO_INCREMENT,
      shop_id INT UNSIGNED NOT NULL DEFAULT 1,
      username VARCHAR(100) NOT NULL,
      password_hash VARCHAR(255) NOT NULL,
      role ENUM('user','admin') NOT NULL DEFAULT 'user',
      is_active TINYINT(1) NOT NULL DEFAULT 1,
      created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uniq_dashboard_user_username (username),
      KEY idx_dashboard_user_shop (shop_id),
      KEY idx_dashboard_user_shop_role (shop_id, role, is_active)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  `);

  try {
    await db.query(`
      ALTER TABLE dashboard_user
      MODIFY COLUMN role ENUM('user','admin') NOT NULL DEFAULT 'user'
    `);
  } catch (err) {
    await db.query(`
      ALTER TABLE dashboard_user
      MODIFY COLUMN role ENUM('picker','manager','owner','user','admin') NOT NULL DEFAULT 'user'
    `);

    await db.query(`
      UPDATE dashboard_user
      SET role = 'admin'
      WHERE role IN ('manager','owner')
    `);

    await db.query(`
      UPDATE dashboard_user
      SET role = 'user'
      WHERE role NOT IN ('admin')
    `);

    await db.query(`
      ALTER TABLE dashboard_user
      MODIFY COLUMN role ENUM('user','admin') NOT NULL DEFAULT 'user'
    `);
  }
}

async function upsertUsers() {
  const users = [
    {
      shopId: 1,
      username: "admin",
      password: "admin",
      role: "admin",
    },
    {
      shopId: 2,
      username: "glasneradmin",
      password: "glasneradmin!1",
      role: "admin",
    },
    {
      shopId: 2,
      username: "glasner",
      password: "glasner!1",
      role: "user",
    },
  ];

  for (const user of users) {
    await db.query(
      `
      INSERT INTO dashboard_user (shop_id, username, password_hash, role, is_active)
      VALUES (?, ?, ?, ?, 1)
      ON DUPLICATE KEY UPDATE
        shop_id = VALUES(shop_id),
        password_hash = VALUES(password_hash),
        role = VALUES(role),
        is_active = 1
      `,
      [user.shopId, user.username, hashPassword(user.password), user.role]
    );

    console.log(`[OK] ${user.username} -> shop_id=${user.shopId}, role=${user.role}`);
  }
}

async function printUsers() {
  const [rows] = await db.query(`
    SELECT id, shop_id, username, role, is_active, LEFT(password_hash, 10) AS hash_type
    FROM dashboard_user
    WHERE username IN ('admin', 'glasneradmin', 'glasner')
    ORDER BY shop_id, role, username
  `);

  console.table(rows);
}

async function main() {
  patchDashboardSeed();
  await ensureDashboardUserTable();
  await upsertUsers();
  await printUsers();

  console.log("");
  console.log("[DONE] Dashboard users were created/updated successfully.");
  console.log("[IMPORTANT] If the local server is currently running, restart it once.");
}

main()
  .catch((err) => {
    console.error("[ERROR]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });

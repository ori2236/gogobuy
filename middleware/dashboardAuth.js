const crypto = require("crypto");
const db = require("../config/db");

const DEFAULT_SECRET = "gogobuy-dashboard-dev-secret-change-me";
const TOKEN_TTL_SECONDS = 60 * 60 * 12; // 12 hours
let schemaReadyPromise = null;

function getAuthSecret() {
  return (
    process.env.DASHBOARD_AUTH_SECRET ||
    process.env.JWT_SECRET ||
    process.env.SCRAMBLER ||
    DEFAULT_SECRET
  );
}

function base64url(input) {
  return Buffer.from(input).toString("base64url");
}

function signPayload(encodedPayload) {
  return crypto
    .createHmac("sha256", getAuthSecret())
    .update(encodedPayload)
    .digest("base64url");
}

function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const hash = crypto.scryptSync(String(password), salt, 64).toString("hex");
  return `scrypt$v1$${salt}$${hash}`;
}

function safeEqual(a, b) {
  const ba = Buffer.from(String(a || ""));
  const bb = Buffer.from(String(b || ""));
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
}

function verifyPassword(password, storedHash) {
  const raw = String(storedHash || "");

  // Backwards-compatible fallback in case the DB was seeded manually with a plain text password.
  if (!raw.startsWith("scrypt$v1$")) return safeEqual(password, raw);

  const parts = raw.split("$");
  if (parts.length !== 4) return false;
  const [, version, salt, expectedHash] = parts;
  if (version !== "v1" || !salt || !expectedHash) return false;

  const actualHash = crypto.scryptSync(String(password), salt, 64).toString("hex");
  return safeEqual(actualHash, expectedHash);
}

function createDashboardToken(user) {
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    sub: Number(user.id),
    username: String(user.username),
    role: String(user.role),
    shop_id: Number(user.shop_id || 1),
    iat: now,
    exp: now + TOKEN_TTL_SECONDS,
  };

  const encodedPayload = base64url(JSON.stringify(payload));
  const signature = signPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

function verifyDashboardToken(token) {
  const raw = String(token || "").trim();
  const [encodedPayload, signature] = raw.split(".");
  if (!encodedPayload || !signature) return null;

  const expected = signPayload(encodedPayload);
  if (!safeEqual(signature, expected)) return null;

  let payload;
  try {
    payload = JSON.parse(Buffer.from(encodedPayload, "base64url").toString("utf8"));
  } catch {
    return null;
  }

  const now = Math.floor(Date.now() / 1000);
  if (!payload?.exp || Number(payload.exp) < now) return null;
  if (payload.role !== "picker") return null;
  if (!Number.isFinite(Number(payload.shop_id)) || Number(payload.shop_id) <= 0) {
    return null;
  }

  return payload;
}

async function ensureDashboardAuthTable() {
  if (!schemaReadyPromise) {
    schemaReadyPromise = (async () => {
      await db.query(`
        CREATE TABLE IF NOT EXISTS dashboard_user (
          id INT UNSIGNED NOT NULL AUTO_INCREMENT,
          shop_id INT UNSIGNED NOT NULL DEFAULT 1,
          username VARCHAR(100) NOT NULL,
          password_hash VARCHAR(255) NOT NULL,
          role ENUM('picker','manager','owner') NOT NULL DEFAULT 'picker',
          is_active TINYINT(1) NOT NULL DEFAULT 1,
          created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uniq_dashboard_user_username (username),
          KEY idx_dashboard_user_shop (shop_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
      `);

      const seedUsers = [
        {
          username: process.env.DASHBOARD_ADMIN_USERNAME || "admin",
          password: process.env.DASHBOARD_ADMIN_PASSWORD || "admin",
          shopId: Number(process.env.DASHBOARD_ADMIN_SHOP_ID || 1),
        },
        {
          username: process.env.DASHBOARD_BRANCH2_USERNAME || "glasner",
          password: process.env.DASHBOARD_BRANCH2_PASSWORD || "עglasner1!",
          shopId: Number(process.env.DASHBOARD_BRANCH2_SHOP_ID || 2),
        },
      ];

      for (const seed of seedUsers) {
        const shopId = Number.isFinite(seed.shopId) && seed.shopId > 0 ? seed.shopId : 1;
        const passwordHash = hashPassword(seed.password);

        await db.query(
          `
          INSERT INTO dashboard_user (shop_id, username, password_hash, role, is_active)
          VALUES (?, ?, ?, 'picker', 1)
          ON DUPLICATE KEY UPDATE
            shop_id = VALUES(shop_id),
            password_hash = VALUES(password_hash),
            role = 'picker',
            is_active = 1
          `,
          [shopId, seed.username, passwordHash],
        );
      }
    })().catch((err) => {
      schemaReadyPromise = null;
      throw err;
    });
  }

  return schemaReadyPromise;
}

function dashboardUserForClient(user) {
  return {
    id: Number(user.sub ?? user.id),
    username: String(user.username),
    role: String(user.role),
    shop_id: Number(user.shop_id || 1),
  };
}

async function requireDashboardAuth(req, res, next) {
  try {
    const header = String(req.headers.authorization || "");
    const token = header.toLowerCase().startsWith("bearer ")
      ? header.slice(7).trim()
      : "";

    const payload = verifyDashboardToken(token);
    if (!payload) {
      return res.status(401).json({ ok: false, message: "צריך להתחבר מחדש" });
    }

    req.dashboardUser = dashboardUserForClient(payload);
    return next();
  } catch (err) {
    console.error("[dashboardAuth.requireDashboardAuth]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
}

module.exports = {
  ensureDashboardAuthTable,
  hashPassword,
  verifyPassword,
  createDashboardToken,
  verifyDashboardToken,
  dashboardUserForClient,
  requireDashboardAuth,
};

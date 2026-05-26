const db = require("../config/db");
const {
  ensureDashboardAuthTable,
  verifyPassword,
  hashPassword,
  createDashboardToken,
  dashboardUserForClient,
} = require("../middleware/dashboardAuth");

exports.login = async (req, res) => {
  try {
    await ensureDashboardAuthTable();

    const username = String(req.body?.username || "").trim();
    const password = String(req.body?.password || "");

    if (!username || !password) {
      return res.status(400).json({ ok: false, message: "חובה להזין שם משתמש וסיסמה" });
    }

    const [rows] = await db.query(
      `
      SELECT id, shop_id, username, password_hash, role, is_active
      FROM dashboard_user
      WHERE username = ?
      LIMIT 1
      `,
      [username],
    );

    const user = rows[0];
    if (!user || !user.is_active || !verifyPassword(password, user.password_hash)) {
      return res.status(401).json({ ok: false, message: "שם משתמש או סיסמה לא נכונים" });
    }

    if (user.role !== "picker") {
      return res.status(403).json({ ok: false, message: "רק משתמש מסוג מלקט יכול להיכנס לדשבורד" });
    }

    // If a legacy plain-text password was used, transparently upgrade it after successful login.
    if (!String(user.password_hash || "").startsWith("scrypt$v1$")) {
      await db.query("UPDATE dashboard_user SET password_hash = ? WHERE id = ?", [
        hashPassword(password),
        user.id,
      ]);
    }

    const token = createDashboardToken(user);
    return res.json({
      ok: true,
      token,
      user: dashboardUserForClient(user),
    });
  } catch (err) {
    console.error("[dashboardAuth.login]", err);
    return res.status(500).json({ ok: false, message: "Server error" });
  }
};

exports.me = async (req, res) => {
  return res.json({ ok: true, user: dashboardUserForClient(req.dashboardUser) });
};

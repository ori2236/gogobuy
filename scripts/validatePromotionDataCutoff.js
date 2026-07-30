require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

const DATA_FILE = path.resolve(argValue("--data", "data/shop2_promotions_31752_august_plus.json"));
const AFTER_DATE = String(argValue("--after", "2026-07-31"));

function main() {
  if (!fs.existsSync(DATA_FILE)) throw new Error(`Data file was not found: ${DATA_FILE}`);

  const promotions = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  if (!Array.isArray(promotions)) throw new Error("Promotion data must be a JSON array");

  const violations = [];
  const byEndDate = {};
  const byType = {};

  for (const promo of promotions) {
    const endDate = promo && promo.end_date ? String(promo.end_date) : null;
    const active = promo && promo.active ? String(promo.active) : null;

    byEndDate[endDate || "missing"] = (byEndDate[endDate || "missing"] || 0) + 1;
    byType[promo && promo.type ? String(promo.type) : "missing"] = (byType[promo && promo.type ? String(promo.type) : "missing"] || 0) + 1;

    if (active !== "כן" || !endDate || endDate <= AFTER_DATE) {
      violations.push({
        reward_id: promo && promo.reward_id,
        title: promo && promo.title,
        active,
        end_date: endDate,
      });
    }
  }

  const report = {
    mode: "read_only_validation",
    data_file: DATA_FILE,
    rule: `active = כן AND end_date > ${AFTER_DATE}`,
    total: promotions.length,
    valid: promotions.length - violations.length,
    invalid: violations.length,
    by_end_date: byEndDate,
    by_type: byType,
    violations,
  };

  console.log(JSON.stringify(report, null, 2));
  if (violations.length) process.exitCode = 1;
}

try {
  main();
} catch (err) {
  console.error("[validate-promotion-data-cutoff]", err);
  process.exitCode = 1;
}

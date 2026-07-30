require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");
const db = require("../config/db");
const { exportPromotionProducts } = require("./exportPromotionProducts");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function assertPositiveInt(value, fieldName) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${fieldName} must be a positive integer`);
  }
  return parsed;
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function stamp() {
  const d = new Date();
  return [
    d.getFullYear(),
    pad2(d.getMonth() + 1),
    pad2(d.getDate()),
    "_",
    pad2(d.getHours()),
    pad2(d.getMinutes()),
    pad2(d.getSeconds()),
  ].join("");
}

function copyFile(source, target) {
  if (!fs.existsSync(source)) throw new Error(`Required tool file is missing: ${source}`);
  fs.copyFileSync(source, target);
}

function psQuote(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function createZipOnWindows(sourceDir, zipFile) {
  if (process.platform !== "win32") return { created: false, reason: "not_windows" };

  const command = [
    `$source=${psQuote(sourceDir)}`,
    `$zip=${psQuote(zipFile)}`,
    `if (Test-Path -LiteralPath $zip) { Remove-Item -LiteralPath $zip -Force }`,
    `Compress-Archive -Path (Join-Path $source '*') -DestinationPath $zip -CompressionLevel Optimal`,
  ].join("; ");

  const result = spawnSync("powershell.exe", [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    command,
  ], { encoding: "utf8" });

  if (result.status !== 0) {
    throw new Error(`Could not create ZIP: ${result.stderr || result.stdout || "PowerShell failed"}`);
  }

  return { created: true };
}

async function main() {
  const shopId = assertPositiveInt(
    argValue("--shopId", process.env.PROMOTION_EXCEL_SHOP_ID || 2),
    "shopId",
  );
  const rootDir = path.resolve(__dirname, "..");
  const dataFile = path.resolve(
    argValue("--productsOut", path.join(rootDir, "data", `promotion_products_shop_${shopId}.json`)),
  );
  const exportsDir = path.resolve(argValue("--exportsDir", path.join(rootDir, "exports")));
  const toolsDir = path.join(rootDir, "tools", "promotion_excel");
  const packageName = `promotion_excel_shop_${shopId}_${stamp()}`;
  const packageDir = path.join(exportsDir, packageName);
  const zipFile = `${packageDir}.zip`;

  try {
    const productPayload = await exportPromotionProducts({ shopId, outFile: dataFile });

    fs.mkdirSync(packageDir, { recursive: true });
    copyFile(dataFile, path.join(packageDir, "promotion_products.json"));

    const toolFiles = [
      "01_create_promotion_excel.bat",
      "02_validate_promotion_excel.bat",
      "CreatePromotionExcel.ps1",
      "ValidatePromotionExcel.ps1",
      "README.txt",
    ];
    for (const file of toolFiles) {
      copyFile(path.join(toolsDir, file), path.join(packageDir, file));
    }

    const zipResult = createZipOnWindows(packageDir, zipFile);

    console.log(JSON.stringify({
      mode: "prepare_promotion_excel_package",
      shop_id: productPayload.shop_id,
      shop_name: productPayload.shop_name,
      product_count: productPayload.product_count,
      products_json: dataFile,
      package_folder: packageDir,
      package_zip: zipResult.created ? zipFile : null,
      zip_created: zipResult.created,
      note: zipResult.created
        ? "Send the ZIP to the promotion supplier"
        : "The package folder was created. ZIP creation runs automatically on Windows.",
    }, null, 2));
  } finally {
    await db.end();
  }
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});

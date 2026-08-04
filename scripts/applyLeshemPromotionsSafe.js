require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");
const { ensureCartPromotionSchema } = require("../services/cartPromotions");
const { ensureProductGroupPromotionColumns } = require("../services/productGroupPromotions");
const { ensurePromotionImportSchema } = require("../services/promotionImportSchema");
const {
  contentHash,
  identityHash,
  productLookup,
  sha256,
  sourceSlotKey,
} = require("../services/promotionImportIdentity");
const {
  loadExistingEntities,
  loadProducts,
  loadSourceLinks,
  planEntity,
  resolveProductReference,
  tableExists,
} = require("./planLeshemPromotionsSafe");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function boolArg(name, fallback = false) {
  const value = argValue(name, fallback);
  if (value === true || value === false) return value;
  return ["1", "true", "yes", "y"].includes(String(value).trim().toLowerCase());
}

function loadJson(filePath) {
  if (!filePath || !fs.existsSync(filePath)) throw new Error(`File not found: ${filePath}`);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function verifiedPlanHash(plan) {
  const copy = JSON.parse(JSON.stringify(plan));
  const expected = copy.plan_sha256;
  delete copy.plan_sha256;
  const actual = sha256(copy);
  if (!expected || expected !== actual) {
    throw new Error(`Plan hash mismatch. Expected ${expected || "missing"}, calculated ${actual}`);
  }
  return actual;
}

function jsonValue(value) {
  return value == null ? null : JSON.stringify(value);
}

function targetType(entityType) {
  if (["promotion", "product_group_promotion", "cart_promotion_rule"].includes(entityType)) return entityType;
  throw new Error(`Unsupported entity type: ${entityType}`);
}

function addExistingEntity(existingStore, entity) {
  existingStore.list.push(entity);
  const values = existingStore.byIdentity.get(entity.identity_hash) || [];
  values.push(entity);
  existingStore.byIdentity.set(entity.identity_hash, values);
}

function replaceExistingEntity(existingStore, updated) {
  const index = existingStore.list.findIndex(
    (item) => item.entity_type === updated.entity_type && Number(item.target_id) === Number(updated.target_id),
  );
  if (index < 0) throw new Error(`Existing target disappeared: ${updated.entity_type}#${updated.target_id}`);
  const previous = existingStore.list[index];
  const oldGroup = existingStore.byIdentity.get(previous.identity_hash) || [];
  existingStore.byIdentity.set(
    previous.identity_hash,
    oldGroup.filter((item) => !(item.entity_type === previous.entity_type && Number(item.target_id) === Number(previous.target_id))),
  );
  existingStore.list[index] = updated;
  const newGroup = existingStore.byIdentity.get(updated.identity_hash) || [];
  newGroup.push(updated);
  existingStore.byIdentity.set(updated.identity_hash, newGroup);
}

function findExisting(existingStore, entityType, targetId) {
  return existingStore.list.find(
    (item) => item.entity_type === entityType && Number(item.target_id) === Number(targetId),
  ) || null;
}

async function rehydrateEntity(plannedEntity, productStore) {
  const entity = JSON.parse(JSON.stringify(plannedEntity));

  if (entity.entity_type === "promotion") {
    const resolved = resolveProductReference(entity.product_lookup, productStore);
    if (!resolved.ok) throw new Error(`Product resolution failed for promotion: ${JSON.stringify(resolved)}`);
    const plannedProductKey = entity.product_key || null;
    entity.product_id = resolved.product.id;
    entity.product_key = resolved.product.product_key;
    entity.product_lookup = productLookup(resolved.product);
    entity.percent_off = entity.percent_off ?? null;
    entity.amount_off = entity.amount_off ?? null;
    if (plannedProductKey && plannedProductKey !== entity.product_key) {
      throw new Error(
        `Product identity changed after resolution. Planned ${plannedProductKey}, resolved ${entity.product_key}`,
      );
    }
  } else if (entity.entity_type === "product_group_promotion") {
    if (!Array.isArray(entity.product_lookups) || entity.product_lookups.length < 2) {
      throw new Error("Group promotion must include at least two product lookups");
    }
    const resolved = entity.product_lookups.map((lookup) => resolveProductReference(lookup, productStore));
    const failed = resolved.filter((item) => !item.ok);
    if (failed.length) throw new Error(`Group product resolution failed: ${JSON.stringify(failed)}`);
    const products = [...new Map(resolved.map((item) => [item.product.id, item.product])).values()]
      .sort((a, b) => String(a.product_key).localeCompare(String(b.product_key)));
    if (products.length < 2) throw new Error("Group promotion resolved to fewer than two unique products");
    entity.product_ids = products.map((product) => product.id);
    entity.product_keys = products.map((product) => product.product_key);
    entity.product_lookups = products.map(productLookup);
  } else if (entity.entity_type === "cart_promotion_rule") {
    if (entity.reward_product_lookup) {
      const resolved = resolveProductReference(entity.reward_product_lookup, productStore);
      if (!resolved.ok) throw new Error(`Cart reward product resolution failed: ${JSON.stringify(resolved)}`);
      entity.reward_product_id = resolved.product.id;
      entity.reward_product_key = resolved.product.product_key;
      entity.reward_product_lookup = productLookup(resolved.product);
      if (entity.rule_type === "GIFT_PRODUCT") entity.gift_text = resolved.product.name;
    } else {
      entity.reward_product_id = null;
      entity.reward_product_key = null;
    }
  } else {
    throw new Error(`Unsupported entity type in plan: ${entity.entity_type}`);
  }

  entity.source_slot_key = sourceSlotKey(entity);
  entity.identity_hash = identityHash(entity);
  entity.content_hash = contentHash(entity);
  if (entity.identity_hash !== plannedEntity.identity_hash) {
    const expectedDescriptor = {
      entity_type: plannedEntity.entity_type,
      product_key: plannedEntity.product_key || null,
      product_keys: plannedEntity.product_keys || null,
      reward_product_key: plannedEntity.reward_product_key || null,
      kind: plannedEntity.kind || null,
      percent_off: plannedEntity.percent_off ?? null,
      amount_off: plannedEntity.amount_off ?? null,
      fixed_price: plannedEntity.fixed_price ?? null,
      bundle_buy_qty: plannedEntity.bundle_buy_qty ?? null,
      bundle_pay_price: plannedEntity.bundle_pay_price ?? null,
      rule_type: plannedEntity.rule_type || null,
      threshold_amount: plannedEntity.threshold_amount ?? null,
      delivery_fee_override: plannedEntity.delivery_fee_override ?? null,
      reward_qty: plannedEntity.reward_qty ?? null,
      reward_fixed_price: plannedEntity.reward_fixed_price ?? null,
      reward_max_qty: plannedEntity.reward_max_qty ?? null,
      threshold_base_mode: plannedEntity.threshold_base_mode || null,
      is_market_day: Boolean(plannedEntity.is_market_day),
    };
    const resolvedDescriptor = {
      ...expectedDescriptor,
      product_key: entity.product_key || null,
      product_keys: entity.product_keys || null,
      reward_product_key: entity.reward_product_key || null,
      percent_off: entity.percent_off ?? null,
      amount_off: entity.amount_off ?? null,
    };
    throw new Error(
      `Entity identity changed after product resolution for ${plannedEntity.entity_type}. ` +
      `Expected hash ${plannedEntity.identity_hash}, resolved hash ${entity.identity_hash}. ` +
      `Planned=${JSON.stringify(expectedDescriptor)} Resolved=${JSON.stringify(resolvedDescriptor)}`,
    );
  }
  if (entity.source_slot_key !== plannedEntity.source_slot_key) {
    throw new Error(`Entity source slot changed after product resolution for ${plannedEntity.entity_type}`);
  }
  return entity;
}

async function lockImportScope(conn, shopId) {
  await conn.query(`SELECT id FROM shop WHERE id = ? FOR UPDATE`, [shopId]);
  await conn.query(`SELECT id FROM promotion WHERE shop_id = ? FOR UPDATE`, [shopId]);
  if (await tableExists(conn, "product_group_promotion")) {
    await conn.query(`SELECT id FROM product_group_promotion WHERE shop_id = ? FOR UPDATE`, [shopId]);
    await conn.query(`SELECT id FROM product_group_promotion_item WHERE shop_id = ? FOR UPDATE`, [shopId]);
  }
  if (await tableExists(conn, "cart_promotion_rule")) {
    await conn.query(`SELECT id FROM cart_promotion_rule WHERE shop_id = ? FOR UPDATE`, [shopId]);
  }
  await conn.query(`SELECT id FROM promotion_source_link WHERE shop_id = ? FOR UPDATE`, [shopId]);
}

async function insertEntity(conn, shopId, entity) {
  if (entity.entity_type === "promotion") {
    if (!entity.start_at) throw new Error("Standard promotion requires start_at");
    const [result] = await conn.query(
      `
      INSERT INTO promotion (
        shop_id, product_id, kind, percent_off, amount_off, fixed_price,
        bundle_buy_qty, bundle_pay_price, max_discounted_qty, description, start_at, end_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        shopId,
        entity.product_id,
        entity.kind,
        entity.percent_off ?? null,
        entity.amount_off ?? null,
        entity.fixed_price ?? null,
        entity.bundle_buy_qty ?? null,
        entity.bundle_pay_price ?? null,
        entity.max_discounted_qty ?? null,
        entity.description ?? null,
        entity.start_at,
        entity.end_at ?? null,
      ],
    );
    return Number(result.insertId);
  }

  if (entity.entity_type === "product_group_promotion") {
    const [result] = await conn.query(
      `
      INSERT INTO product_group_promotion (
        shop_id, title, description, emoji, kind, bundle_buy_qty, bundle_pay_price,
        max_discounted_qty, priority, is_active, start_at, end_at
      ) VALUES (?, ?, ?, NULL, 'BUNDLE', ?, ?, ?, ?, 1, ?, ?)
      `,
      [
        shopId,
        entity.title,
        entity.description ?? null,
        entity.bundle_buy_qty,
        entity.bundle_pay_price,
        entity.max_discounted_qty ?? null,
        entity.priority ?? 100,
        entity.start_at ?? null,
        entity.end_at ?? null,
      ],
    );
    const groupId = Number(result.insertId);
    for (const productId of entity.product_ids) {
      await conn.query(
        `INSERT INTO product_group_promotion_item (group_promotion_id, shop_id, product_id) VALUES (?, ?, ?)`,
        [groupId, shopId, productId],
      );
    }
    return groupId;
  }

  if (entity.entity_type === "cart_promotion_rule") {
    const [result] = await conn.query(
      `
      INSERT INTO cart_promotion_rule (
        shop_id, rule_type, title, description, threshold_amount, delivery_fee_override,
        reward_product_id, gift_text, reward_qty, reward_fixed_price, reward_max_qty,
        threshold_base_mode, priority, is_active, notify_customer, start_at, end_at,
        source, external_reward_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
      `,
      [
        shopId,
        entity.rule_type,
        entity.title,
        entity.description ?? null,
        entity.threshold_amount ?? 0,
        entity.delivery_fee_override ?? null,
        entity.reward_product_id ?? null,
        entity.gift_text ?? null,
        entity.reward_qty ?? null,
        entity.reward_fixed_price ?? null,
        entity.reward_max_qty ?? null,
        entity.threshold_base_mode || "ITEMS_SUBTOTAL",
        entity.priority ?? 100,
        entity.notify_customer ?? 1,
        entity.start_at ?? null,
        entity.end_at ?? null,
        entity.source ?? null,
        entity.external_reward_id ?? null,
      ],
    );
    return Number(result.insertId);
  }

  throw new Error(`Unsupported entity type for insert: ${entity.entity_type}`);
}

async function extendEntityDates(conn, entityType, targetId, update) {
  const table = targetType(entityType);
  const isActiveSql = entityType === "promotion" ? "" : ", is_active = 1";
  await conn.query(
    `UPDATE ${table} SET start_at = ?, end_at = ?${isActiveSql} WHERE id = ?`,
    [update.start_at ?? null, update.end_at ?? null, targetId],
  );
}

function buildExistingFromInserted(entity, targetId) {
  return {
    ...JSON.parse(JSON.stringify(entity)),
    target_id: Number(targetId),
    raw: null,
  };
}

async function upsertSourceLink(conn, batchId, shopId, source, row, entity, targetId) {
  const externalRewardId = String(row.reward_id);
  const [currentRows] = await conn.query(
    `
    SELECT * FROM promotion_source_link
    WHERE shop_id = ? AND source = ? AND external_reward_id = ? AND source_slot_key = ?
    FOR UPDATE
    `,
    [shopId, source, externalRewardId, entity.source_slot_key],
  );
  const current = currentRows?.[0] || null;
  if (current) {
    if (current.target_type !== entity.entity_type || Number(current.target_id) !== Number(targetId)) {
      throw new Error(
        `Source link collision for reward ${externalRewardId}: points to ${current.target_type}#${current.target_id}`,
      );
    }
    if (current.identity_hash !== entity.identity_hash) {
      throw new Error(`Source link identity changed for reward ${externalRewardId}`);
    }
    await conn.query(
      `
      UPDATE promotion_source_link
      SET content_hash = ?, last_batch_id = ?, source_title = ?, source_payload = ?
      WHERE id = ?
      `,
      [entity.content_hash, batchId, row.title || null, jsonValue(row.source), current.id],
    );
    return Number(current.id);
  }

  const [result] = await conn.query(
    `
    INSERT INTO promotion_source_link (
      shop_id, source, external_reward_id, source_slot_key, target_type, target_id,
      identity_hash, content_hash, last_batch_id, source_title, source_payload
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      shopId,
      source,
      externalRewardId,
      entity.source_slot_key,
      entity.entity_type,
      targetId,
      entity.identity_hash,
      entity.content_hash,
      batchId,
      row.title || null,
      jsonValue(row.source),
    ],
  );
  return Number(result.insertId);
}

async function insertBatchItem(conn, batchId, row, action, status, targetId, message, beforePayload, afterPayload) {
  await conn.query(
    `
    INSERT INTO promotion_import_batch_item (
      batch_id, reward_id, source_slot_key, action, status, target_type, target_id,
      identity_hash, message, source_payload, before_payload, after_payload
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      batchId,
      String(row.reward_id),
      action?.source_slot_key || null,
      action?.action || "NONE",
      status,
      action?.target_type || action?.entity?.entity_type || null,
      targetId ?? null,
      action?.identity_hash || action?.entity?.identity_hash || null,
      message || null,
      jsonValue(row.source),
      jsonValue(beforePayload),
      jsonValue(afterPayload),
    ],
  );
}

function linkedTargetForAction(sourceLinks, row, entity) {
  const link = sourceLinks.find(
    (item) => String(item.external_reward_id) === String(row.reward_id)
      && item.source_slot_key === entity.source_slot_key,
  );
  return link ? Number(link.target_id) : null;
}

function allowedFreshAction(expected, fresh) {
  if (fresh.status !== "READY") return false;
  if (expected === "INSERT") return ["INSERT", "LINK_EXISTING"].includes(fresh.action);
  if (expected === "EXTEND_EXISTING_DATES") return ["EXTEND_EXISTING_DATES", "LINK_EXISTING"].includes(fresh.action);
  if (expected === "REFRESH_MARKET_DAY_DATES") {
    return ["REFRESH_MARKET_DAY_DATES", "LINK_EXISTING"].includes(fresh.action);
  }
  if (expected === "LINK_EXISTING") return fresh.action === "LINK_EXISTING";
  return false;
}

async function createOrLockBatch(conn, plan, planPath) {
  const metadata = plan.metadata;
  await conn.query(
    `
    INSERT INTO promotion_import_batch (
      shop_id, source, original_filename, file_sha256, plan_sha256, status,
      total_source_rows, ready_rows, review_rows, blocked_rows, report_json
    ) VALUES (?, ?, ?, ?, ?, 'APPLYING', ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      status = IF(status = 'APPLIED', status, 'APPLYING'),
      report_json = VALUES(report_json)
    `,
    [
      metadata.shop_id,
      metadata.source,
      metadata.original_filename || path.basename(planPath),
      metadata.file_sha256,
      plan.plan_sha256,
      plan.summary.total_source_rows || 0,
      plan.summary.ready_rows || 0,
      plan.summary.review_required_rows || 0,
      plan.summary.blocked_rows || 0,
      jsonValue({ plan_path: planPath, summary: plan.summary }),
    ],
  );
  const [rows] = await conn.query(
    `SELECT * FROM promotion_import_batch WHERE shop_id = ? AND source = ? AND plan_sha256 = ? FOR UPDATE`,
    [metadata.shop_id, metadata.source, plan.plan_sha256],
  );
  if (!rows?.length) throw new Error("Could not create or lock import batch");
  return rows[0];
}

async function recordFailedBatch(plan, planPath, error) {
  try {
    await ensurePromotionImportSchema(db);
    await db.query(
      `
      INSERT INTO promotion_import_batch (
        shop_id, source, original_filename, file_sha256, plan_sha256, status,
        total_source_rows, ready_rows, review_rows, blocked_rows, report_json
      ) VALUES (?, ?, ?, ?, ?, 'FAILED', ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE status = IF(status = 'APPLIED', status, 'FAILED'), report_json = VALUES(report_json)
      `,
      [
        plan.metadata.shop_id,
        plan.metadata.source,
        plan.metadata.original_filename || path.basename(planPath),
        plan.metadata.file_sha256,
        plan.plan_sha256,
        plan.summary.total_source_rows || 0,
        plan.summary.ready_rows || 0,
        plan.summary.review_required_rows || 0,
        plan.summary.blocked_rows || 0,
        jsonValue({ plan_path: planPath, error: error.message, failed_at: new Date().toISOString() }),
      ],
    );
  } catch (batchError) {
    console.error("[apply-safe] Could not persist failure audit:", batchError.message);
  }
}

async function main() {
  const planPath = path.resolve(argValue("--plan", ""));
  const confirm = boolArg("--confirm", false);
  const allowUnresolved = boolArg("--allowUnresolved", false);
  const dataPathArg = argValue("--data", null);
  const reportPath = path.resolve(argValue("--report", planPath.replace(/\.json$/i, "_apply_report.json")));

  if (!planPath || !fs.existsSync(planPath)) throw new Error("Use --plan=<path to a safe import plan>");
  const plan = loadJson(planPath);
  if (Number(plan.schema_version) !== 1) throw new Error(`Unsupported plan schema version: ${plan.schema_version}`);
  verifiedPlanHash(plan);

  const shopId = Number(plan.metadata?.shop_id);
  const source = String(plan.metadata?.source || "").trim();
  if (!Number.isInteger(shopId) || shopId <= 0 || !source) throw new Error("Plan metadata is invalid");

  const unresolved = (plan.rows || []).filter((row) => ["REVIEW_REQUIRED", "BLOCKED"].includes(row.status));
  if (unresolved.length && !allowUnresolved) {
    throw new Error(
      `Plan contains ${unresolved.length} unresolved rows. Approve/fix them and regenerate the plan, or use --allowUnresolved to apply READY rows only.`,
    );
  }

  const sourceDataPath = dataPathArg
    ? path.resolve(dataPathArg)
    : plan.metadata.data_file && fs.existsSync(plan.metadata.data_file)
      ? plan.metadata.data_file
      : null;
  if (sourceDataPath) {
    const actualFileHash = sha256(fs.readFileSync(sourceDataPath));
    if (actualFileHash !== plan.metadata.file_sha256) throw new Error("Source data file hash does not match the plan");
  }

  const readyRows = (plan.rows || []).filter((row) => row.status === "READY");
  const preview = {
    mode: confirm ? "confirm" : "dry_run",
    shop_id: shopId,
    source,
    plan_file: planPath,
    plan_sha256: plan.plan_sha256,
    ready_rows: readyRows.length,
    ready_actions: readyRows.flatMap((row) => row.actions || []).length,
    unresolved_rows: unresolved.length,
    note: confirm ? undefined : "No DB changes were made. Add --confirm to apply this exact plan.",
  };
  if (!confirm) {
    console.log(JSON.stringify(preview, null, 2));
    return;
  }

  await ensureCartPromotionSchema(db);
  await ensureProductGroupPromotionColumns(db);
  await ensurePromotionImportSchema(db);

  const lockName = `promotion_import_shop_${shopId}`;
  const [lockRows] = await db.query(`SELECT GET_LOCK(?, 0) AS acquired`, [lockName]);
  if (Number(lockRows?.[0]?.acquired) !== 1) throw new Error(`Another promotion import is already running for shop ${shopId}`);

  const conn = await db.getConnection();
  let committed = false;
  try {
    await conn.beginTransaction();
    await lockImportScope(conn, shopId);
    const batch = await createOrLockBatch(conn, plan, planPath);
    if (batch.status === "APPLIED") {
      await conn.rollback();
      console.log(JSON.stringify({ ...preview, mode: "already_applied", batch_id: Number(batch.id) }, null, 2));
      return;
    }

    const productStore = await loadProducts(conn, shopId);
    const existingStore = await loadExistingEntities(conn, shopId, productStore);
    const sourceLinks = await loadSourceLinks(conn, shopId, source);
    let appliedActions = 0;
    const applied = [];

    for (const row of plan.rows || []) {
      if (row.status !== "READY") {
        await insertBatchItem(conn, batch.id, row, null, "NOT_APPLIED", null, `Plan row status: ${row.status}`, null, null);
        continue;
      }

      for (const action of row.actions || []) {
        if (!["INSERT", "LINK_EXISTING", "EXTEND_EXISTING_DATES", "REFRESH_MARKET_DAY_DATES"].includes(action.action)) {
          throw new Error(`Unexpected action ${action.action} in READY row ${row.reward_id}`);
        }

        let entity;
        try {
          entity = await rehydrateEntity(action.entity, productStore);
        } catch (error) {
          throw new Error(`Reward ${row.reward_id} (${row.title}): ${error.message}`);
        }
        const linkedTargetId = linkedTargetForAction(sourceLinks, row, entity);
        const fresh = planEntity(entity, existingStore, linkedTargetId);
        if (!allowedFreshAction(action.action, fresh)) {
          throw new Error(
            `Production changed for reward ${row.reward_id}. Expected ${action.action}, now ${fresh.status}/${fresh.action}: ${fresh.reason}`,
          );
        }

        let targetId = fresh.target_id || null;
        let before = targetId ? findExisting(existingStore, entity.entity_type, targetId) : null;
        let actualAction = fresh.action;

        if (fresh.action === "INSERT") {
          targetId = await insertEntity(conn, shopId, entity);
          const inserted = buildExistingFromInserted(entity, targetId);
          addExistingEntity(existingStore, inserted);
        } else if (["EXTEND_EXISTING_DATES", "REFRESH_MARKET_DAY_DATES"].includes(fresh.action)) {
          targetId = Number(fresh.target_id);
          await extendEntityDates(conn, entity.entity_type, targetId, fresh.update);
          const current = findExisting(existingStore, entity.entity_type, targetId);
          if (!current) throw new Error(`Target missing before date update: ${entity.entity_type}#${targetId}`);
          const updated = {
            ...current,
            start_at: fresh.update.start_at ?? null,
            end_at: fresh.update.end_at ?? null,
            is_active: entity.entity_type === "promotion" ? current.is_active : 1,
          };
          updated.content_hash = contentHash(updated);
          replaceExistingEntity(existingStore, updated);
        } else {
          targetId = Number(fresh.target_id);
        }

        const after = findExisting(existingStore, entity.entity_type, targetId);
        if (!after) throw new Error(`Could not verify target after apply: ${entity.entity_type}#${targetId}`);

        const entityForLink = {
          ...entity,
          start_at: after.start_at,
          end_at: after.end_at,
          is_active: after.is_active,
        };
        entityForLink.content_hash = contentHash(entityForLink);
        if (action.record_source_link !== false) {
          await upsertSourceLink(conn, batch.id, shopId, source, row, entityForLink, targetId);
          const currentLink = {
            shop_id: shopId,
            source,
            external_reward_id: String(row.reward_id),
            source_slot_key: entity.source_slot_key,
            target_type: entity.entity_type,
            target_id: targetId,
            identity_hash: entity.identity_hash,
            content_hash: entityForLink.content_hash,
          };
          const linkIndex = sourceLinks.findIndex(
            (item) => String(item.external_reward_id) === String(row.reward_id)
              && item.source_slot_key === entity.source_slot_key,
          );
          if (linkIndex >= 0) sourceLinks[linkIndex] = currentLink;
          else sourceLinks.push(currentLink);
        }

        await insertBatchItem(
          conn,
          batch.id,
          row,
          action,
          "APPLIED",
          targetId,
          `${action.action} executed as ${actualAction}`,
          before,
          after,
        );
        appliedActions += 1;
        applied.push({
          reward_id: row.reward_id,
          title: row.title,
          planned_action: action.action,
          executed_action: actualAction,
          target_type: entity.entity_type,
          target_id: targetId,
          identity_hash: entity.identity_hash,
        });
      }
    }

    const report = {
      mode: "applied",
      batch_id: Number(batch.id),
      shop_id: shopId,
      source,
      plan_file: planPath,
      plan_sha256: plan.plan_sha256,
      applied_at: new Date().toISOString(),
      applied_actions: appliedActions,
      unresolved_rows_not_applied: unresolved.length,
      actions: applied,
    };

    await conn.query(
      `
      UPDATE promotion_import_batch
      SET status = 'APPLIED', applied_actions = ?, report_json = ?, applied_at = NOW()
      WHERE id = ?
      `,
      [appliedActions, jsonValue(report), batch.id],
    );
    await conn.commit();
    committed = true;

    fs.mkdirSync(path.dirname(reportPath), { recursive: true });
    fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), "utf8");
    console.log(JSON.stringify({ ...report, report_file: reportPath }, null, 2));
  } catch (error) {
    if (!committed) {
      try { await conn.rollback(); } catch {}
    }
    await recordFailedBatch(plan, planPath, error);
    throw error;
  } finally {
    conn.release();
    try { await db.query(`SELECT RELEASE_LOCK(?) AS released`, [lockName]); } catch {}
  }
}

main()
  .catch((error) => {
    console.error("[apply-leshem-promotions-safe]", error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.end();
  });

const db = require("../config/db");

async function fetchCategoriesMap() {
  const query = `
    SELECT c.name AS category, s.name AS subcategory
    FROM product_category c
    JOIN product_subcategory s ON s.category_id = c.id
    ORDER BY c.sort_order, s.sort_order;
  `;

  const [rows] = await db.query(query);

  const categoryMap = {};

  rows.forEach((row) => {
    if (!categoryMap[row.category]) {
      categoryMap[row.category] = [];
    }
    categoryMap[row.category].push(row.subcategory);
  });

  return categoryMap;
}

async function getSubCategoryCandidates(category, sub) {
  const c = String(category ?? "").trim();
  const s = String(sub ?? "").trim();

  if (!s) return [];
  if (!c) return [s];

  const [rows] = await db.query(
    `
    SELECT cand.name AS candidate_sub
    FROM subcategory_candidates m
    JOIN product_subcategory src ON src.id = m.source_subcategory_id
    JOIN product_category cat ON cat.id = src.category_id
    JOIN product_subcategory cand ON cand.id = m.candidate_subcategory_id
    WHERE cat.name = ? AND src.name = ?
    ORDER BY m.sort_order
    `,
    [c, s],
  );

  const list = (rows || [])
    .map((r) => String(r.candidate_sub || "").trim())
    .filter(Boolean);

  if (!list.length) return [s];

  if (list[0] === s) return list;
  return [s, ...list.filter((x) => x !== s)];
}

module.exports = {
  fetchCategoriesMap,
  getSubCategoryCandidates,
};
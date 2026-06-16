require("dotenv").config({ quiet: true });

const fs = require("fs");
const path = require("path");
const db = require("../config/db");

const SOURCE = "leshem_excel_2026_06_14";
const REPORTS_DIR = path.join(__dirname, "..", "reports");

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

const SHOP_ID = Number(argValue("--shopId", process.env.PROMO_IMPORT_SHOP_ID || 2));
const REPORT_ARG = argValue("--report", null);
const OUT_ARG = argValue("--out", null);

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

function normalizeText(value) {
  let s = String(value || "").toLowerCase();
  for (const ch of ["״", "”", "“", "׳", "’", "‘", "'", '"']) s = s.split(ch).join("");
  for (const ch of ["־", "–", "—", "-", "&", "+", "₪", "%", ".", ",", ":", ";", "(", ")", "[", "]", "/", "\\"]) {
    s = s.split(ch).join(" ");
  }
  return s.replace(/\s+/g, " ").trim();
}

function reasonLabel(reason) {
  const labels = {
    multiple_token_matches: "נמצאו כמה מוצרים אפשריים",
    no_match: "לא נמצאה התאמה אוטומטית",
    too_few_specific_tokens: "שם כללי מדי להתאמה בטוחה",
    cart_rule_requires_manual_reward_product_mapping: "מבצע סל שדורש בחירת מוצר הטבה",
    expired_in_excel: "פג תוקף באקסל",
    fractional_bundle_qty_requires_manual_mapping: "כמות חלקית / משקל — דורש החלטה ידנית",
    non_integer_bundle_qty_requires_manual_mapping: "כמות לא שלמה — דורש החלטה ידנית",
    unsupported_promotion_type: "סוג מבצע לא נתמך כרגע",
    inactive_in_excel: "לא פעיל באקסל",
  };
  return labels[reason] || reason || "לא ידוע";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function escapeAttr(value) {
  return escapeHtml(value).replace(/`/g, "&#096;");
}

function findLatestReport() {
  if (!fs.existsSync(REPORTS_DIR)) {
    throw new Error(`Reports folder was not found: ${REPORTS_DIR}`);
  }

  const files = fs
    .readdirSync(REPORTS_DIR)
    .filter((name) => /^leshem_promotions_import_report_.*\.json$/i.test(name))
    .map((name) => ({
      name,
      fullPath: path.join(REPORTS_DIR, name),
      mtime: fs.statSync(path.join(REPORTS_DIR, name)).mtimeMs,
    }))
    .sort((a, b) => b.mtime - a.mtime);

  if (!files.length) {
    throw new Error(`No leshem_promotions_import_report_*.json files were found in ${REPORTS_DIR}`);
  }

  return files[0].fullPath;
}

function extractExcelProductName(promo) {
  if (promo && promo.search_phrase && String(promo.search_phrase).trim()) return String(promo.search_phrase).trim();

  let s = String((promo && promo.title) || "").trim();
  s = s
    .replace(/בקנייה\s+מעל\s*\d+(?:\.\d+)?/g, "")
    .replace(/\d+(?:\.\d+)?\s*ב\s*-?\s*\d+(?:\.\d+)?/g, "")
    .replace(/רק\s*ב\s*-?\s*\d+(?:\.\d+)?/g, "")
    .replace(/ב\s*-?\s*\d+(?:\.\d+)?\s*(?:₪|שח|ש״ח)?/g, "")
    .replace(/(?:₪|שח|ש״ח|ללא עלות|עלות משלוח|משלוח חינם)/g, "")
    .replace(/\s+/g, " ")
    .trim();
  return s || String((promo && promo.title) || "").trim();
}

function isCartReward(promo) {
  return promo.reason === "cart_rule_requires_manual_reward_product_mapping" || promo.type === "קנה בסכום הוסף קבל";
}

function productLabel(product) {
  const price = product.price == null ? "" : ` · ₪${Number(product.price).toFixed(2)}`;
  const stock = product.stock_amount == null ? "" : ` · מלאי ${product.stock_amount}`;
  return `${product.id} | ${product.name || ""}${price}${stock}`;
}

function productLine(product) {
  const price = product.price == null ? "" : ` · ₪${Number(product.price).toFixed(2)}`;
  const stock = product.stock_amount == null ? "" : ` · מלאי ${product.stock_amount}`;
  const cat = [product.category, product.sub_category].filter(Boolean).join(" / ");
  return `<div><div class="product-name">${escapeHtml(product.name || "")}</div><div class="product-meta">ID ${escapeHtml(product.id)}${escapeHtml(price)}${escapeHtml(stock)}${cat ? " · " + escapeHtml(cat) : ""}</div></div>`;
}

function candidateHtml(promo) {
  const candidates = Array.isArray(promo.candidates) ? promo.candidates : [];
  if (!candidates.length) {
    return `<div class="empty">אין מועמדים אוטומטיים. אפשר לכתוב שם מוצר או ID בשדה ההשלמה האוטומטית.</div>`;
  }

  return candidates
    .map((c) => `
      <div class="candidate">
        ${productLine(c)}
        <button type="button" class="small secondary" onclick="promoToolAddCandidate(this)" data-product-id="${escapeAttr(c.id)}" data-product-name="${escapeAttr(c.name || "")}">בחר</button>
      </div>
    `)
    .join("\n");
}

function promoCardHtml(promo) {
  const reason = promo.reason || "unknown";
  const excelProductName = extractExcelProductName(promo);
  const action = isCartReward(promo) ? "cart_reward_product" : "product_promotion";
  const deal = promo.deal_text ? `<span class="badge ok">${escapeHtml(promo.deal_text)}</span>` : "";
  const search = normalizeText([promo.reward_id, promo.title, reasonLabel(reason), promo.search_phrase, excelProductName, promo.type].join(" "));

  return `
    <section class="card ${reason === "expired_in_excel" ? "expired" : ""}" data-card="${escapeAttr(promo.reward_id)}" data-reward-id="${escapeAttr(promo.reward_id)}" data-title="${escapeAttr(promo.title || "")}" data-type="${escapeAttr(promo.type || "")}" data-action="${escapeAttr(action)}" data-reason="${escapeAttr(reason)}" data-search="${escapeAttr(search)}">
      <div class="card-head">
        <div>
          <div class="title">${escapeHtml(promo.title || "")}</div>
          <div class="meta">
            <span class="badge">Reward ${escapeHtml(promo.reward_id)}</span>
            <span class="badge warn">${escapeHtml(reasonLabel(reason))}</span>
            <span class="badge">${escapeHtml(promo.type || "")}</span>
            ${deal}
            ${promo.start_date || promo.end_date ? `<span>תוקף: ${escapeHtml(promo.start_date || "?")} — ${escapeHtml(promo.end_date || "ללא")}</span>` : ""}
          </div>
        </div>
        <div class="badge state">לא מופה</div>
      </div>

      <div class="excel-grid">
        <div class="excel-panel"><strong>המבצע באקסל</strong><div class="value">${escapeHtml(promo.title || "")}</div><div class="hint">זה הטקסט המקורי של המבצע.</div></div>
        <div class="excel-panel"><strong>שם המוצר שמשויך למבצע באקסל</strong><div class="value">${escapeHtml(excelProductName)}</div><div class="hint">לפי שדה החיפוש/השם שחולץ מהמבצע.</div></div>
        <div class="excel-panel"><strong>פרטי מבצע</strong><div class="value">${escapeHtml([promo.type, promo.deal_text].filter(Boolean).join(" · ") || "מבצע סל / ללא כמות")}</div><div class="hint">פעולה: ${isCartReward(promo) ? "מוצר הטבה במבצע סל" : "מבצע מוצר"}</div></div>
      </div>

      <div class="cols">
        <div class="box">
          <div class="box-title">כתוב ובחר מוצרים מה־DB</div>
          <div class="autocomplete-row">
            <input list="productSuggestions" class="product-input" placeholder="כתוב שם מוצר או ID, למשל: 35479 | גלידת בן&גריס..." onkeydown="promoToolInputKeydown(event)" />
            <button type="button" class="small secondary" onclick="promoToolAddFromInput(this)">הוסף</button>
          </div>
          <div class="empty">אפשר לבחור כמה מוצרים לאותו מבצע. בחר מתוך ההשלמה האוטומטית או הקלד ID מדויק.</div>
        </div>

        <div class="box">
          <div class="box-title">מועמדים מהדוח</div>
          ${candidateHtml(promo)}
          ${Array.isArray(promo.candidates) && promo.candidates.length ? `<div class="card-actions"><button type="button" class="secondary small" onclick="promoToolAddAllCandidates(this)">בחר את כל המועמדים</button></div>` : ""}
        </div>

        <div class="box">
          <div class="box-title">מוצרים שנבחרו למבצע</div>
          <div class="selected-list"><div class="empty">עדיין לא נבחר מוצר.</div></div>
          <textarea class="note" placeholder="הערה לעצמך, לא חובה" style="margin-top:10px"></textarea>
          <div class="card-actions">
            <button type="button" class="danger small" onclick="promoToolClearCard(this)">נקה בחירה</button>
          </div>
        </div>
      </div>
    </section>
  `;
}

function makeHtml({ shopId, reportPath, report, products, skipped }) {
  const cardsHtml = skipped.map((promo) => promoCardHtml(promo)).join("\n");
  const datalistHtml = products.map((p) => `<option value="${escapeAttr(productLabel(p))}"></option>`).join("\n");
  const reasons = Array.from(new Set(skipped.map((p) => p.reason || "unknown"))).sort();
  const reasonOptions = reasons
    .map((reason) => `<option value="${escapeAttr(reason)}">${escapeHtml(reasonLabel(reason))} (${skipped.filter((p) => (p.reason || "unknown") === reason).length})</option>`)
    .join("\n");

  return `<!doctype html>
<html lang="he" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>מיפוי ידני למבצעי לשם</title>
  <style>
    :root { --bg:#f5f8ff; --card:#fff; --text:#162033; --muted:#667085; --border:#d8e2f3; --primary:#2563eb; --primary-dark:#1e40af; --danger:#b42318; --success:#087443; --warning:#b54708; --soft-green:#eafaf1; --soft-red:#fff1f0; --shadow:0 10px 30px rgba(37,99,235,.08); }
    * { box-sizing:border-box; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:Arial,"Noto Sans Hebrew",sans-serif; line-height:1.45; }
    header { position:sticky; top:0; z-index:20; background:rgba(245,248,255,.95); backdrop-filter:blur(10px); border-bottom:1px solid var(--border); padding:18px 22px; }
    .header-row { display:flex; align-items:center; justify-content:space-between; gap:16px; max-width:1400px; margin:0 auto; }
    h1 { margin:0 0 6px; font-size:24px; } .sub { color:var(--muted); font-size:14px; }
    .actions { display:flex; gap:10px; flex-wrap:wrap; justify-content:flex-start; }
    button,.file-label { border:0; border-radius:12px; padding:10px 14px; cursor:pointer; font-weight:700; background:var(--primary); color:#fff; box-shadow:0 3px 8px rgba(37,99,235,.16); }
    button:hover,.file-label:hover { background:var(--primary-dark); } button.secondary { background:#eef4ff; color:#1d4ed8; box-shadow:none; border:1px solid #bfdbfe; } button.danger { background:var(--soft-red); color:var(--danger); box-shadow:none; border:1px solid #fecaca; } button.small { padding:6px 10px; border-radius:9px; font-size:12px; }
    main { max-width:1400px; margin:22px auto; padding:0 22px 40px; }
    .stats { display:grid; grid-template-columns:repeat(4,minmax(160px,1fr)); gap:12px; margin-bottom:18px; }
    .stat { background:var(--card); border:1px solid var(--border); border-radius:18px; padding:14px; box-shadow:var(--shadow); } .stat strong { display:block; font-size:24px; } .stat span { color:var(--muted); font-size:13px; }
    .notice { background:#fff8e6; border:1px solid #fedf89; color:#7a4b00; border-radius:16px; padding:12px 14px; margin-bottom:18px; }
    .toolbar { display:grid; grid-template-columns:1.4fr 1fr 1fr; gap:10px; margin:16px 0 20px; }
    input,select,textarea { width:100%; border:1px solid var(--border); border-radius:12px; padding:10px 12px; font-size:14px; background:#fff; color:var(--text); }
    textarea { resize:vertical; min-height:38px; }
    .cards { display:grid; gap:14px; } .card { background:var(--card); border:1px solid var(--border); border-radius:20px; padding:16px; box-shadow:var(--shadow); } .card.mapped { border-color:#9ae6b4; background:linear-gradient(0deg,#fff 0%,#f5fff9 100%); } .card.expired { opacity:.72; }
    .card-head { display:grid; grid-template-columns:minmax(0,1fr) auto; gap:12px; align-items:start; } .title { font-size:18px; font-weight:800; margin-bottom:6px; } .meta { display:flex; flex-wrap:wrap; gap:8px; align-items:center; color:var(--muted); font-size:13px; }
    .badge { display:inline-flex; align-items:center; gap:4px; border-radius:999px; padding:4px 9px; background:#eef4ff; color:#1e40af; font-size:12px; font-weight:700; } .badge.warn { background:#fff7ed; color:var(--warning); } .badge.ok { background:var(--soft-green); color:var(--success); }
    .excel-grid { display:grid; grid-template-columns:minmax(260px,1fr) minmax(240px,.85fr) minmax(220px,.75fr); gap:10px; margin-top:14px; } .excel-panel { background:#f8fbff; border:1px solid var(--border); border-radius:15px; padding:11px 12px; } .excel-panel strong { display:block; margin-bottom:4px; font-size:13px; color:#1e40af; } .excel-panel .value { font-weight:800; font-size:15px; } .excel-panel .hint { color:var(--muted); font-size:12px; margin-top:4px; }
    .cols { display:grid; grid-template-columns:minmax(280px,.85fr) minmax(320px,1fr) minmax(320px,1fr); gap:14px; margin-top:14px; } .box { border:1px solid var(--border); border-radius:16px; padding:12px; background:#fbfdff; min-height:120px; } .box-title { font-weight:800; margin-bottom:8px; }
    .autocomplete-row { display:grid; grid-template-columns:minmax(0,1fr) auto; gap:8px; margin-bottom:8px; }
    .candidate,.selected-row { display:grid; grid-template-columns:minmax(0,1fr) auto; gap:8px; align-items:center; padding:8px; border:1px solid #e6edf8; border-radius:12px; background:#fff; margin-bottom:6px; } .selected-row { background:#effdf5; border-color:#b7ebc6; }
    .product-name { font-weight:700; } .product-meta { color:var(--muted); font-size:12px; margin-top:3px; } .empty { color:var(--muted); font-size:13px; padding:8px 2px; } .card-actions { display:flex; gap:8px; flex-wrap:wrap; margin-top:10px; } .hidden { display:none!important; }
    .footer-actions { position:sticky; bottom:14px; display:flex; justify-content:center; pointer-events:none; margin-top:24px; } .footer-actions>div { pointer-events:auto; display:flex; gap:10px; background:rgba(255,255,255,.94); border:1px solid var(--border); border-radius:18px; padding:10px; box-shadow:0 16px 38px rgba(16,24,40,.16); }
    .json-preview { direction:ltr; text-align:left; background:#0b1020; color:#dbeafe; border-radius:16px; padding:14px; overflow:auto; max-height:300px; font-size:12px; margin-top:16px; }
    .toast { position:fixed; left:24px; bottom:24px; z-index:9999; display:none; background:#0f172a; color:#fff; border-radius:16px; padding:12px 16px; box-shadow:0 18px 40px rgba(15,23,42,.25); font-weight:800; max-width:460px; } .toast.show { display:block; }
    @media (max-width:1100px) { .cols,.excel-grid,.toolbar { grid-template-columns:1fr; } .stats { grid-template-columns:repeat(2,1fr); } .header-row { align-items:stretch; flex-direction:column; } }
  </style>
  <script>
    function promoToolCard(el) { return el && el.closest ? el.closest('.card') : null; }
    function promoToolEscape(s) { return String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#039;'); }
    function promoToolToast(msg) { var el=document.getElementById('toast'); if(!el){ alert(msg); return; } el.textContent=msg; el.classList.add('show'); clearTimeout(window.__promoToastTimer); window.__promoToastTimer=setTimeout(function(){el.classList.remove('show');},2200); }
    function promoToolParseInputValue(value) { var raw=String(value||'').trim(); var m=raw.match(/^\s*(\d+)\s*(?:[|:—-]\s*)?(.*)$/); if(!m) return null; return { id:Number(m[1]), name:(m[2]||('ID '+m[1])).replace(/ · ₪.*$/,'').replace(/ · מלאי.*$/,'').trim() || ('ID '+m[1]) }; }
    function promoToolAddToCard(card, id, name) { if(!card || !id) return; var list=card.querySelector('.selected-list'); if(!list) return; var existing=list.querySelector('[data-selected-product-id="'+id+'"]'); if(existing) { promoToolToast('המוצר כבר נבחר למבצע הזה'); return; } var empty=list.querySelector('.empty'); if(empty) empty.remove(); var row=document.createElement('div'); row.className='selected-row'; row.setAttribute('data-selected-product-id', String(id)); row.setAttribute('data-selected-product-name', String(name||('ID '+id))); row.innerHTML='<div><div class="product-name">'+promoToolEscape(name||('ID '+id))+'</div><div class="product-meta">ID '+promoToolEscape(id)+'</div></div><button type="button" class="small danger" onclick="promoToolRemoveSelected(this)">הסר</button>'; list.appendChild(row); promoToolUpdateCard(card); promoToolRefreshStats(); promoToolToast('נבחר: '+(name||('ID '+id))); }
    function promoToolAddCandidate(btn) { var card=promoToolCard(btn); promoToolAddToCard(card, Number(btn.getAttribute('data-product-id')), btn.getAttribute('data-product-name')||''); }
    function promoToolAddFromInput(btn) { var card=promoToolCard(btn); var input=card && card.querySelector('.product-input'); var parsed=promoToolParseInputValue(input && input.value); if(!parsed) { alert('לא הצלחתי לזהות ID של מוצר. בחר מתוך ההשלמה האוטומטית או הקלד ID בתחילת השדה.'); return; } promoToolAddToCard(card, parsed.id, parsed.name); if(input) input.value=''; }
    function promoToolInputKeydown(ev) { if(ev && ev.key==='Enter') { ev.preventDefault(); var card=promoToolCard(ev.target); var btn=card && card.querySelector('.autocomplete-row button'); promoToolAddFromInput(btn); } }
    function promoToolRemoveSelected(btn) { var card=promoToolCard(btn); var row=btn && btn.closest ? btn.closest('.selected-row') : null; if(row) row.remove(); promoToolUpdateCard(card); promoToolRefreshStats(); }
    function promoToolClearCard(btn) { var card=promoToolCard(btn); var list=card && card.querySelector('.selected-list'); if(!list) return; list.innerHTML='<div class="empty">עדיין לא נבחר מוצר.</div>'; promoToolUpdateCard(card); promoToolRefreshStats(); }
    function promoToolAddAllCandidates(btn) { var card=promoToolCard(btn); if(!card) return; var buttons=card.querySelectorAll('.candidate button[data-product-id]'); buttons.forEach(function(b){ promoToolAddToCard(card, Number(b.getAttribute('data-product-id')), b.getAttribute('data-product-name')||''); }); }
    function promoToolUpdateCard(card) { if(!card) return; var count=card.querySelectorAll('.selected-row').length; card.classList.toggle('mapped', count>0); var state=card.querySelector('.state'); if(state) state.textContent=count>0 ? 'מופה: '+count+' מוצר'+(count>1?'ים':'') : 'לא מופה'; }
    function promoToolRefreshStats() { var cards=document.querySelectorAll('.card'); var mapped=0,total=0; cards.forEach(function(card){ var c=card.querySelectorAll('.selected-row').length; if(c>0) mapped++; total+=c; }); var mappedEl=document.getElementById('mappedCount'); var totalEl=document.getElementById('selectedProductsCount'); if(mappedEl) mappedEl.textContent=String(mapped); if(totalEl) totalEl.textContent=String(total); promoToolApplyFilters(); }
    function promoToolBuildMapping() { var mappings=[]; document.querySelectorAll('.card').forEach(function(card){ var ids=[]; card.querySelectorAll('.selected-row').forEach(function(row){ ids.push(Number(row.getAttribute('data-selected-product-id'))); }); if(!ids.length) return; var noteEl=card.querySelector('.note'); mappings.push({ reward_id:Number(card.getAttribute('data-reward-id')), title:card.getAttribute('data-title')||'', type:card.getAttribute('data-type')||'', action:card.getAttribute('data-action')||'product_promotion', product_ids:ids, note:(noteEl && noteEl.value)||'' }); }); return { source:'${SOURCE}', shop_id:${Number(shopId)}, based_on_report:${JSON.stringify(reportPath)}, generated_at:new Date().toISOString(), mappings:mappings }; }
    function promoToolDownloadJson() { var json=JSON.stringify(promoToolBuildMapping(),null,2); var blob=new Blob([json],{type:'application/json;charset=utf-8'}); var url=URL.createObjectURL(blob); var a=document.createElement('a'); a.href=url; a.download='leshem_manual_promo_mapping.json'; document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url); }
    function promoToolCopyJson() { var json=JSON.stringify(promoToolBuildMapping(),null,2); if(navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(json).then(function(){alert('ה־JSON הועתק');}); else alert(json); }
    function promoToolToggleJson() { var el=document.getElementById('jsonPreview'); if(!el) return; el.textContent=JSON.stringify(promoToolBuildMapping(),null,2); el.classList.toggle('hidden'); }
    function promoToolNorm(s) { return String(s||'').toLowerCase().replace(/\s+/g,' ').trim(); }
    function promoToolApplyFilters() { var q=promoToolNorm((document.getElementById('globalSearch')||{}).value); var reason=(document.getElementById('reasonFilter')||{}).value||'all'; var mapped=(document.getElementById('mappedFilter')||{}).value||'all'; document.querySelectorAll('.card').forEach(function(card){ var text=card.getAttribute('data-search')||''; var isMapped=card.querySelectorAll('.selected-row').length>0; var okText=!q || text.indexOf(q)>=0; var okReason=reason==='all' || card.getAttribute('data-reason')===reason; var okMapped=mapped==='all' || (mapped==='mapped'&&isMapped) || (mapped==='unmapped'&&!isMapped); card.classList.toggle('hidden', !(okText&&okReason&&okMapped)); }); }
    function promoToolLoadMappingFile(file) { var reader=new FileReader(); reader.onload=function(){ try { var parsed=JSON.parse(String(reader.result||'{}')); if(Array.isArray(parsed.skipped) && !Array.isArray(parsed.mappings)) { alert('זה נראה כמו דוח import ולא קובץ mapping. אין צורך לטעון אותו כאן.'); return; } document.querySelectorAll('.card').forEach(function(card){ var list=card.querySelector('.selected-list'); if(list) list.innerHTML='<div class="empty">עדיין לא נבחר מוצר.</div>'; var note=card.querySelector('.note'); if(note) note.value=''; }); (parsed.mappings||[]).forEach(function(m){ var card=document.querySelector('.card[data-reward-id="'+m.reward_id+'"]'); if(!card) return; (m.product_ids||[]).forEach(function(id){ promoToolAddToCard(card, Number(id), 'ID '+id); }); var note=card.querySelector('.note'); if(note) note.value=m.note||''; }); promoToolRefreshStats(); alert('ה־mapping נטען בהצלחה'); } catch(e) { alert('לא הצלחתי לקרוא את הקובץ: '+e.message); } }; reader.readAsText(file,'utf-8'); }
    window.addEventListener('DOMContentLoaded', function(){ promoToolRefreshStats(); ['globalSearch','reasonFilter','mappedFilter'].forEach(function(id){ var el=document.getElementById(id); if(el) { el.addEventListener('input', promoToolApplyFilters); el.addEventListener('change', promoToolApplyFilters); } }); var importFile=document.getElementById('importFile'); if(importFile) importFile.addEventListener('change', function(ev){ var f=ev.target.files && ev.target.files[0]; if(f) promoToolLoadMappingFile(f); }); });
  </script>
</head>
<body>
  <header>
    <div class="header-row">
      <div>
        <h1>מיפוי ידני למבצעי לשם</h1>
        <div class="sub">סניף ${escapeHtml(shopId)} · נוצר מתוך ${escapeHtml(path.basename(reportPath))} · קובץ יציאה מומלץ: <b>data/leshem_manual_promo_mapping.json</b></div>
      </div>
      <div class="actions">
        <label class="file-label">טען mapping קיים<input id="importFile" type="file" accept="application/json" style="display:none" /></label>
        <button type="button" class="secondary" onclick="promoToolCopyJson()">העתק JSON</button>
        <button type="button" onclick="promoToolDownloadJson()">הורד mapping JSON</button>
      </div>
    </div>
  </header>

  <main>
    <div class="stats">
      <div class="stat"><strong id="mappedCount">0</strong><span>מבצעים שמופו ידנית</span></div>
      <div class="stat"><strong>${escapeHtml(skipped.length)}</strong><span>מבצעים שדורשים החלטה</span></div>
      <div class="stat"><strong>${escapeHtml(products.length)}</strong><span>מוצרים זמינים ב־DB</span></div>
      <div class="stat"><strong id="selectedProductsCount">0</strong><span>בחירות מוצר סה״כ</span></div>
    </div>

    <div id="toast" class="toast"></div>
    <div class="notice">בכל כרטיס מופיע המבצע מהאקסל, שם המוצר/החיפוש שחולץ ממנו, ושדה כתיבה עם השלמה אוטומטית מתוך מוצרי ה־DB. אפשר לבחור כמה מוצרים לאותו מבצע.</div>

    <div class="toolbar">
      <input id="globalSearch" placeholder="חיפוש מבצע לפי שם / מזהה / סיבה" />
      <select id="reasonFilter"><option value="all">כל הסיבות</option>${reasonOptions}</select>
      <select id="mappedFilter"><option value="all">הכול</option><option value="mapped">רק מה שמופה</option><option value="unmapped">רק מה שלא מופה</option></select>
    </div>

    <datalist id="productSuggestions">${datalistHtml}</datalist>
    <div id="cards" class="cards">${cardsHtml}</div>
    <pre id="jsonPreview" class="json-preview hidden"></pre>
    <div class="footer-actions"><div><button type="button" class="secondary" onclick="promoToolToggleJson()">הצג/הסתר JSON</button><button type="button" onclick="promoToolDownloadJson()">הורד mapping JSON</button></div></div>
  </main>
</body>
</html>`;
}

async function loadProducts(shopId) {
  const [rows] = await db.query(
    `
    SELECT id, name, display_name_en, price, stock_amount, category, sub_category
    FROM product
    WHERE shop_id = ?
    ORDER BY name ASC, id ASC
    `,
    [shopId],
  );

  return (rows || []).map((row) => ({
    id: Number(row.id),
    name: row.name,
    display_name_en: row.display_name_en || null,
    price: row.price == null ? null : Number(row.price),
    stock_amount: row.stock_amount == null ? null : Number(row.stock_amount),
    category: row.category || null,
    sub_category: row.sub_category || null,
  }));
}

async function main() {
  if (!Number.isInteger(SHOP_ID) || SHOP_ID <= 0) throw new Error(`Invalid --shopId: ${SHOP_ID}`);

  const reportPath = path.resolve(REPORT_ARG || findLatestReport());
  const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  const skipped = Array.isArray(report.skipped) ? report.skipped : [];
  const products = await loadProducts(SHOP_ID);

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const outputPath = path.resolve(OUT_ARG || path.join(REPORTS_DIR, `leshem_manual_promo_mapping_tool_${stamp()}.html`));
  fs.writeFileSync(outputPath, makeHtml({ shopId: SHOP_ID, reportPath, report, products, skipped }), "utf8");

  console.log(JSON.stringify({
    shop_id: SHOP_ID,
    products_in_shop: products.length,
    skipped_promotions: skipped.length,
    report_file: reportPath,
    html_file: outputPath,
  }, null, 2));
  console.log("\nOpen the html_file in your browser, choose products, then download leshem_manual_promo_mapping.json into the data folder.");
}

main()
  .catch((err) => {
    console.error("[generate-leshem-promo-mapping-tool]", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    if (db && typeof db.end === "function") await db.end();
  });

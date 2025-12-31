// timer-preload.js
const Module = require("module");
const path = require("path");
const { performance } = require("node:perf_hooks");

const thresholdMs = Number(process.env.PROFILE_THRESHOLD_MS || "0");

// תמדוד רק קבצים שמתאימים לרג׳קס הזה (כדי לא להציף)
const matchStr = process.env.PROFILE_MATCH || "";
const matcher = matchStr ? new RegExp(matchStr) : null;

function fmt(ms) {
  return ms < 1000 ? `${ms.toFixed(2)}ms` : `${(ms / 1000).toFixed(2)}s`;
}
function isPromiseLike(x) {
  return (
    x &&
    (typeof x === "object" || typeof x === "function") &&
    typeof x.then === "function"
  );
}

const WRAPPED = Symbol.for("__wrapped_for_timing__");
const originalLoad = Module._load;

function shouldInstrument(filename) {
  if (!filename) return false;

  // נרמול נתיב ל־/ כדי שהרג׳קס יעבוד גם ב-Windows
  const normalized = filename.replace(/\\/g, "/");

  if (normalized.includes("/node_modules/")) return false;
  if (matcher && !matcher.test(normalized)) return false;
  return true;
}


function wrapFn(prefix, name, fn) {
  return function wrapped(...args) {
    const start = performance.now();

    const log = (ok) => {
      const ms = performance.now() - start;
      if (ms < thresholdMs) return;
      console.log(`[TIMER] ${prefix}:${name} ${ok ? "✅" : "❌"} ${fmt(ms)}`);
    };

    try {
      const out = fn.apply(this, args);

      if (isPromiseLike(out)) {
        return out.then(
          (res) => {
            log(true);
            return res;
          },
          (err) => {
            log(false);
            throw err;
          }
        );
      }

      log(true);
      return out;
    } catch (err) {
      log(false);
      throw err;
    }
  };
}

Module._load = function patchedLoad(request, parent, isMain) {
  const exported = originalLoad.apply(this, arguments);

  let filename;
  try {
    filename = Module._resolveFilename(request, parent, isMain);
  } catch {
    return exported;
  }

  if (!shouldInstrument(filename)) return exported;
const prefix = path.relative(process.cwd(), filename).replace(/\\/g, "/");

  // module.exports = function ...
  if (typeof exported === "function") {
    if (exported[WRAPPED]) return exported;
    const w = wrapFn(prefix, "default", exported);
    w[WRAPPED] = true;
    return w;
  }

  // module.exports = { fn1, fn2, ... }
  if (exported && typeof exported === "object") {
    if (exported[WRAPPED]) return exported;
    const out = { ...exported };
    for (const [k, v] of Object.entries(out)) {
      if (typeof v === "function") out[k] = wrapFn(prefix, k, v);
    }
    Object.defineProperty(out, WRAPPED, { value: true, enumerable: false });
    return out;
  }

  return exported;
};

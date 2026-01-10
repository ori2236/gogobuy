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

  // built-in modules (e.g. "events", "path", "fs", "node:fs") הם לא נתיב קובץ
  if (filename.startsWith("node:")) return false;
  if (!path.isAbsolute(filename)) return false;

  const normalized = filename.replace(/\\/g, "/");
  if (normalized.includes("/node_modules/")) return false;
  if (matcher && !matcher.test(normalized)) return false;

  return true;
}

function wrapFn(prefix, name, fn) {
  function wrapped(...args) {
    const start = performance.now();

    const log = (ok) => {
      const ms = performance.now() - start;
      if (ms < thresholdMs) return;
      console.log(`[TIMER] ${prefix}:${name} ${ok ? "✅" : "❌"} ${fmt(ms)}`);
    };

    try {
      // ✅ אם קוראים עם new -> חייבים לבנות עם Reflect.construct
      if (new.target) {
        // משתמשים ב-fn כ-newTarget כדי לא לשבור built-ins כמו Promise
        const out = Reflect.construct(fn, args, fn);
        log(true);
        return out;
      }

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
  }

  // (מומלץ) לשמור prototype כדי לא לשבור instanceof
  try {
    wrapped.prototype = fn.prototype;
  } catch {}

  return wrapped;
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

    // לשמר properties של הפונקציה המקורית (events.EventEmitter וכו')
    for (const key of Reflect.ownKeys(exported)) {
      if (key === "length" || key === "name" || key === "prototype") continue;
      const desc = Object.getOwnPropertyDescriptor(exported, key);
      if (!desc) continue;
      try {
        Object.defineProperty(w, key, desc);
      } catch {}
    }

    // אם זה "self reference" כמו events.EventEmitter === events
    if (w.EventEmitter === exported) w.EventEmitter = w;

    Object.defineProperty(w, WRAPPED, { value: true, enumerable: false });
    return w;
  }

  // module.exports = { fn1, fn2, ... }
  if (exported && typeof exported === "object") {
    if (exported[WRAPPED]) return exported;

    const out = Object.create(Object.getPrototypeOf(exported));
    for (const key of Reflect.ownKeys(exported)) {
      const desc = Object.getOwnPropertyDescriptor(exported, key);
      if (!desc) continue;
      try {
        Object.defineProperty(out, key, desc);
      } catch {}
    }

    for (const key of Reflect.ownKeys(out)) {
      const desc = Object.getOwnPropertyDescriptor(out, key);
      if (!desc || typeof desc.value !== "function") continue;

      // אל תנסה לדרוס פונקציות שהן read-only
      if (desc.writable === false) continue;

      try {
        out[key] = wrapFn(prefix, String(key), desc.value);
      } catch {}
    }

    Object.defineProperty(out, WRAPPED, { value: true, enumerable: false });
    return out;
  }

  return exported;
};

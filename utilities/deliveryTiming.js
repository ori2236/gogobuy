const BUSINESS_DAYS = new Set([0, 1, 2, 3, 4]); // Sunday-Thursday. Friday/Saturday have no deliveries.

const HEBREW_WEEKDAYS = ["ראשון", "שני", "שלישי", "רביעי", "חמישי", "שישי", "שבת"];
const ENGLISH_WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

function normalizeTime(value, fallback = null) {
  const s = String(value ?? "").trim();
  const m = s.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (!m) return fallback;
  const h = Number(m[1]);
  const min = Number(m[2]);
  if (!Number.isInteger(h) || !Number.isInteger(min) || h < 0 || h > 23 || min < 0 || min > 59) {
    return fallback;
  }
  return `${String(h).padStart(2, "0")}:${String(min).padStart(2, "0")}`;
}

function getIsraelDateParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Jerusalem",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  })
    .formatToParts(date)
    .reduce((acc, p) => {
      if (p.type !== "literal") acc[p.type] = p.value;
      return acc;
    }, {});

  const isoDate = `${parts.year}-${parts.month}-${parts.day}`;
  const time = `${parts.hour}:${parts.minute}`;
  const weekday = new Date(`${isoDate}T12:00:00Z`).getUTCDay();
  return { isoDate, time, weekday };
}

function addDaysToIsoDate(isoDate, days) {
  const d = new Date(`${isoDate}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + Number(days || 0));
  return d.toISOString().slice(0, 10);
}

function weekdayForIsoDate(isoDate) {
  return new Date(`${isoDate}T12:00:00Z`).getUTCDay();
}

function nextBusinessDeliveryDate(isoDate) {
  let candidate = addDaysToIsoDate(isoDate, 1);
  for (let i = 0; i < 10; i += 1) {
    if (BUSINESS_DAYS.has(weekdayForIsoDate(candidate))) return candidate;
    candidate = addDaysToIsoDate(candidate, 1);
  }
  return candidate;
}

function formatIsoDate(isoDate, isEnglish = false) {
  if (!isoDate) return "";
  const weekday = weekdayForIsoDate(isoDate);
  const [year, month, day] = String(isoDate).split("-");
  if (isEnglish) return `${ENGLISH_WEEKDAYS[weekday]}, ${day}/${month}/${year}`;
  return `יום ${HEBREW_WEEKDAYS[weekday]}, ${day}/${month}/${year}`;
}

function formatTimeRange(start, end) {
  const s = normalizeTime(start);
  const e = normalizeTime(end);
  return s && e ? `${s}-${e}` : "";
}

function calculateDeliveryTiming({
  shop,
  now = new Date(),
  isEnglish = false,
} = {}) {
  const cutoff = normalizeTime(shop?.order_same_day_cutoff_time, "15:00");
  const arrivalStart = normalizeTime(shop?.delivery_arrival_start_time);
  const arrivalEnd = normalizeTime(shop?.delivery_arrival_end_time);
  const hasArrivalWindow = Boolean(arrivalStart && arrivalEnd);
  const current = getIsraelDateParts(now);
  const canDeliverToday = BUSINESS_DAYS.has(current.weekday) && current.time <= cutoff;
  const expectedDate = canDeliverToday ? current.isoDate : nextBusinessDeliveryDate(current.isoDate);

  return {
    expectedDate,
    expectedDateText: formatIsoDate(expectedDate, isEnglish),
    arrivalStart,
    arrivalEnd,
    arrivalWindowText: hasArrivalWindow ? formatTimeRange(arrivalStart, arrivalEnd) : "",
    cutoffTime: cutoff,
    isToday: expectedDate === current.isoDate,
    currentDate: current.isoDate,
    currentTime: current.time,
    currentWeekday: current.weekday,
    hasArrivalWindow,
  };
}

function buildDeliveryTimingMessage({ shop, isEnglish = false, now = new Date(), includeCutoff = false } = {}) {
  const timing = calculateDeliveryTiming({ shop, isEnglish, now });
  if (!timing.hasArrivalWindow) {
    return {
      text: "",
      timing,
    };
  }

  if (isEnglish) {
    const when = timing.isToday ? "today" : `on the next business delivery day (${timing.expectedDateText})`;
    const lines = [
      `🚚 The delivery is expected to arrive ${when} between ${timing.arrivalWindowText}.`,
      "📩 We'll notify you when the delivery leaves the store.",
    ];
    if (includeCutoff) {
      lines.push(`Orders confirmed after ${timing.cutoffTime} are scheduled for the next business delivery day. There are no deliveries on Friday or Saturday.`);
    }
    return { text: lines.join("\n"), timing };
  }

  const when = timing.isToday ? "היום" : `ביום העסקים הבא (${timing.expectedDateText})`;
  const lines = [
    `🚚 המשלוח צפוי להגיע ${when} בין ${timing.arrivalWindowText}.`,
    "📩 תקבל הודעה כשהמשלוח ייצא מהחנות.",
  ];
  if (includeCutoff) {
    lines.push(`הזמנות משלוח שמאושרות אחרי ${timing.cutoffTime} יעברו ליום העסקים הבא. אין משלוחים בשישי ושבת.`);
  }
  return { text: lines.join("\n"), timing };
}

module.exports = {
  BUSINESS_DAYS,
  normalizeTime,
  getIsraelDateParts,
  nextBusinessDeliveryDate,
  formatIsoDate,
  formatTimeRange,
  calculateDeliveryTiming,
  buildDeliveryTimingMessage,
};

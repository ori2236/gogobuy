require("dotenv").config();
const fs = require("fs");
const path = require("path");
const { chat } = require("../config/openai");

const BASE_URL = process.env.LOCAL_BOT_BASE_URL || "http://localhost:3000";
const ENDPOINT = `${BASE_URL}/api/customer-interaction`;
const SHOP_ID = Number(process.env.TEST_SHOP_ID || 1);
const MAX_TURNS = Number(process.env.AI_TEST_MAX_TURNS || 8);

const PERSONAS = [
  {
    name: "regular_customer",
    style: "דבר כמו לקוח אמיתי רגיל בווטצאפ. קצר, טבעי, לא רשמי מדי.",
  },
  {
    name: "confused_customer",
    style:
      "דבר כמו לקוח קצת מבולבל. לפעמים תענה חלקית, לפעמים תהיה עמום, אבל עדיין טבעי.",
  },
  {
    name: "tricky_customer",
    style:
      "נסה להכשיל את הבוט בעדינות: החלף כיוון, השתמש ב'זה', 'כאלה', 'אותו דבר', ושמור על שיחה הגיונית.",
  },
  {
    name: "impatient_customer",
    style: "דבר קצר ומהיר, קצת חסר סבלנות, אבל עדיין כמו לקוח אמיתי.",
  },
];

const GOALS = [
  "נסה ליצור הזמנה חדשה של כמה מוצרים.",
  "נסה להתחיל מבקשה עמומה כדי לבדוק אם הבוט שואל שאלת הבהרה.",
  "נסה ליצור הזמנה ואז לשנות אותה תוך כדי.",
  "נסה לשאול על מחיר או מלאי ואז לעבור להזמנה.",
  "נסה לסתור את עצמך מעט במהלך השיחה ולבדוק אם הבוט נשאר עקבי.",
  "נסה להשתמש בביטויים עמומים כמו 'זה', 'כזה', 'אותו דבר', כדי לבדוק שמירה על הקשר.",
];

const GENERATOR_PROMPT = `
אתה משחק לקוח אמיתי שמדבר עם בוט סופרמרקט בוואטסאפ.
המטרה שלך היא לבדוק את הבוט דרך שיחה אמיתית, לא דרך שפת QA.

חוקים:
- כתוב רק את הודעת הלקוח הבאה.
- שמור על ניסוח קצר, טבעי, בסגנון ווטצאפ.
- אל תסביר מה אתה בודק.
- אל תכתוב הערות, ניתוחים או reasoning.
- אל תחזור בדיוק על אותה הודעה שכבר שלחת קודם באותה שיחה.
- אם הושגה המטרה והשיחה מרגישה סגורה, אפשר לסמן done=true.
- החזר JSON תקין בלבד בפורמט:
- התמקד בבדיקת יכולות ליבה של בוט סופרמרקט: מחיר, מלאי, יצירה/שינוי/אישור הזמנה, ושאלות הבהרה.
- אל תבקש פיצ'רים חיצוניים כמו לינק תשלום, מעקב משלוח, זמן אספקה או שירות לקוחות, אלא אם מופיע במפורש בשיחה שהבוט תומך בזה.

{
  "message": "הודעת הלקוח הבאה",
  "done": false
}
`;

const JUDGE_PROMPT = `
אתה שופט QA לבוט סופרמרקט.

תן ציון רק לפי הרובריקה הבאה:
- הבנת הבקשה הראשית: 0-40
- טיפול נכון בעמימות / שאלות הבהרה: 0-20
- מענה לבקשות המשך של הלקוח: 0-20
- שמירה על הקשר ועקביות: 0-10
- ניסוח מועיל וטבעי: 0-10

חוקים חשובים:
- אל תעניש חזק על פיצ'ר שלא בטוח שהמערכת בכלל תומכת בו.
- אם הבוט לא תומך במשהו אבל ענה בצורה סבירה/מבהירה, תן קנס קטן בלבד.
- אם הודעת לקוח כוללת כמה כוונות יחד, מותר לבוט לטפל רק בכוונה הראשונה או המרכזית.
- במקרה כזה, אל תסמן כשל רק בגלל שהכוונות הנוספות לא טופלו עדיין.
- זה כן כשל אם הבוט טיפל לא נכון גם בכוונה הראשונה.
- זה כן כשל אם הבוט עשה פעולה שגויה או איבד הקשר.
- אם הבוט בחר ערך עמום בלי הבהרה כשנדרשה הבהרה, זה כשל אמיתי.
- החזר score כסכום כל הסעיפים, בין 0 ל-100.
- מותר לכתוב לפעמים הודעה עם יותר מכוונה אחת, אבל אל תחשיב את זה אוטומטית ככשל אם הבוט מטפל רק בכוונה הראשונה.

החזר JSON תקין בלבד בפורמט:
{
  "score": 0,
  "passed": true,
  "summary": "סיכום קצר",
  "worked": ["מה עבד"],
  "failed": ["מה לא עבד"]
}
`;

function randomPick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function makePhoneNumber() {
  return `qa_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
}

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function getLastCustomerMessage(transcript) {
  for (let i = transcript.length - 1; i >= 0; i--) {
    if (transcript[i].role === "customer") return transcript[i].content;
  }
  return "";
}

function normalizeText(s) {
  return String(s || "").trim();
}

function buildGeneratorInput({
  persona,
  goal,
  transcript,
  extraInstruction = "",
}) {
  return JSON.stringify({
    persona: persona.name,
    style: persona.style,
    goal,
    transcript,
    extraInstruction,
  });
}

async function getNextCustomerMessage({
  persona,
  goal,
  transcript,
  extraInstruction = "",
}) {
  const raw = await chat({
    systemPrompt: GENERATOR_PROMPT,
    message: buildGeneratorInput({
      persona,
      goal,
      transcript,
      extraInstruction,
    }),
    response_format: { type: "json_object" },
  });

  const parsed = safeJsonParse(raw);

  if (!parsed || typeof parsed.message !== "string") {
    return {
      message: "אני רוצה להזמין כמה דברים",
      done: false,
    };
  }

  return {
    message: normalizeText(parsed.message),
    done: Boolean(parsed.done),
  };
}

async function sendToBot(message, phoneNumber) {
  const res = await fetch(ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      phone_number: phoneNumber,
      shop_id: SHOP_ID,
      message,
    }),
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }

  return {
    ok: res.ok,
    status: res.status,
    data,
    botMessage: normalizeText(data?.message),
    skipSend: Boolean(data?.skipSend),
  };
}

async function judgeConversation({ persona, goal, transcript }) {
  const raw = await chat({
    systemPrompt: JUDGE_PROMPT,
    message: JSON.stringify({
      persona: persona.name,
      goal,
      transcript,
    }),
    response_format: { type: "json_object" },
  });

  const parsed = safeJsonParse(raw);
  if (!parsed) {
    return {
      score: 0,
      passed: false,
      summary: "לא הצלחתי לנתח את השיחה",
      worked: [],
      failed: ["פלט judge לא היה JSON תקין"],
    };
  }

  return {
    score: Number.isFinite(Number(parsed.score)) ? Number(parsed.score) : 0,
    passed: Boolean(parsed.passed),
    summary: normalizeText(parsed.summary),
    worked: Array.isArray(parsed.worked) ? parsed.worked : [],
    failed: Array.isArray(parsed.failed) ? parsed.failed : [],
  };
}

function printTranscript(transcript) {
  console.log("\n===== TRANSCRIPT =====\n");
  for (const msg of transcript) {
    const label =
      msg.role === "customer"
        ? "Customer"
        : msg.role === "bot"
          ? "Bot"
          : "System";
    console.log(`${label}: ${msg.content}\n`);
  }
}

function saveReport(report) {
  const reportsDir = path.join(__dirname, "..", "tmp", "ai-chat-tests");
  fs.mkdirSync(reportsDir, { recursive: true });

  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const filePath = path.join(reportsDir, `report-${ts}.json`);
  fs.writeFileSync(filePath, JSON.stringify(report, null, 2), "utf8");

  return filePath;
}

async function run() {
  const persona = randomPick(PERSONAS);
  const goal = randomPick(GOALS);
  const phoneNumber = makePhoneNumber();
  const transcript = [];

  console.log("===== AI CHAT TESTER =====");
  console.log("Persona:", persona.name);
  console.log("Goal:", goal);
  console.log("Phone:", phoneNumber);
  console.log("Endpoint:", ENDPOINT);

  for (let turn = 1; turn <= MAX_TURNS; turn++) {
    let extraInstruction = "";
    let next = await getNextCustomerMessage({
      persona,
      goal,
      transcript,
      extraInstruction,
    });

    let customerMessage = normalizeText(next.message);
    const lastCustomerMessage = getLastCustomerMessage(transcript);

    if (!customerMessage) {
      extraInstruction =
        "ההודעה הקודמת שלך הייתה ריקה. כתוב הודעה קצרה וטבעית.";
      next = await getNextCustomerMessage({
        persona,
        goal,
        transcript,
        extraInstruction,
      });
      customerMessage = normalizeText(next.message);
    }

    if (customerMessage && customerMessage === lastCustomerMessage) {
      extraInstruction =
        "אל תחזור על אותה הודעה בדיוק. נסח את ההודעה אחרת או קדם את השיחה.";
      next = await getNextCustomerMessage({
        persona,
        goal,
        transcript,
        extraInstruction,
      });
      customerMessage = normalizeText(next.message);
    }

    if (!customerMessage) break;

    transcript.push({ role: "customer", content: customerMessage });
    console.log(`\n[Turn ${turn}] Customer: ${customerMessage}`);

    const botRes = await sendToBot(customerMessage, phoneNumber);

    if (!botRes.ok) {
      transcript.push({
        role: "system",
        content: `HTTP ${botRes.status} from server`,
      });
      console.log(`Bot HTTP error: ${botRes.status}`);
      break;
    }

    if (botRes.skipSend) {
      transcript.push({
        role: "system",
        content: "Server returned skipSend בגלל dedup/duplicate handling",
      });
      console.log("System: skipSend returned by server");
      continue;
    }

    const botMessage = botRes.botMessage || "[empty bot message]";
    transcript.push({ role: "bot", content: botMessage });
    console.log(`Bot: ${botMessage}`);

    if (next.done) break;
  }

  const judgment = await judgeConversation({ persona, goal, transcript });

  printTranscript(transcript);

  console.log("===== REPORT =====\n");
  console.log(`Score: ${judgment.score}`);
  console.log(`Passed: ${judgment.passed}`);
  console.log(`Summary: ${judgment.summary}`);

  console.log("\nWhat worked:");
  if (judgment.worked.length) {
    for (const item of judgment.worked) console.log(`- ${item}`);
  } else {
    console.log("-");
  }

  console.log("\nWhat failed:");
  if (judgment.failed.length) {
    for (const item of judgment.failed) console.log(`- ${item}`);
  } else {
    console.log("-");
  }

  const fullReport = {
    meta: {
      persona: persona.name,
      goal,
      phoneNumber,
      endpoint: ENDPOINT,
      shopId: SHOP_ID,
      maxTurns: MAX_TURNS,
      createdAt: new Date().toISOString(),
    },
    transcript,
    judgment,
  };

  const reportPath = saveReport(fullReport);
  console.log(`\nSaved report: ${reportPath}`);
}

run().catch((err) => {
  console.error("\nFatal error:");
  console.error(err);
  process.exit(1);
});

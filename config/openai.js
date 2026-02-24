const { OpenAI } = require("openai");
const { performance } = require("perf_hooks");
const crypto = require("crypto");

const endpoint = (process.env.AZURE_OPENAI_ENDPOINT || "").replace(/\/+$/, "");
const apiKey = process.env.AZURE_OPENAI_KEY;

const deploymentMain = process.env.AZURE_OPENAI_DEPLOYMENT_MAIN;
const apiVersionMain = process.env.AZURE_OPENAI_API_VERSION_MAIN;

const deploymentClassifier = process.env.AZURE_OPENAI_DEPLOYMENT_CLASSIFIER;
const apiVersionClassifier = process.env.AZURE_OPENAI_API_VERSION_CLASSIFIER;

function normalizePrompt(s) {
  return typeof s === "string" ? s.replace(/\r\n/g, "\n").trimEnd() : s;
}

function makeClient(deployment, apiVersion) {
  return new OpenAI({
    apiKey,
    baseURL: `${endpoint}/openai/deployments/${deployment}`,
    defaultHeaders: { "api-key": apiKey },
    defaultQuery: { "api-version": apiVersion },
    logLevel: "debug",
  });
}

const clientMain = makeClient(deploymentMain, apiVersionMain);
const clientClassifier = makeClient(deploymentClassifier, apiVersionClassifier);

async function chat({
  message,
  history = [],
  systemPrompt,
  userContext,
  response_format,
  prompt_cache_key,
  use = "main",
}) {
  const normHistory = history.map((m) => ({
    ...m,
    content: normalizePrompt(m.content),
  }));
  const normMessage = normalizePrompt(message);

  const messages = [
    { role: "system", content: normalizePrompt(systemPrompt) },
    ...(userContext
      ? [{ role: "user", content: normalizePrompt(userContext) }]
      : []),
    ...normHistory,
    { role: "user", content: normMessage },
  ];

  const client = use === "classifier" ? clientClassifier : clientMain;
  const modelDeployment =
    use === "classifier" ? deploymentClassifier : deploymentMain;

  const clientReqId = crypto.randomUUID();
  const t0 = performance.now();

  const body = {
    model: modelDeployment,
    messages,
    ...(response_format ? { response_format } : {}),
    ...(prompt_cache_key ? { prompt_cache_key } : {}),
    top_p: 1,
  };

  if (use === "main") {
    body.reasoning_effort = "low";
  } else {
    body.temperature = 0;
    body.max_completion_tokens = 50;
    body.stop = ["\n"];
  };

  const req = client.chat.completions.create(body, {
    headers: { "X-Client-Request-Id": clientReqId },
  });

  // פה מקבלים גם data וגם raw Response (כולל headers)
  const { data, response, request_id } = await req.withResponse();

  const t1 = performance.now();
  const h = response.headers;

  console.log("AOAI timing/headers:", {
    ms_total: Math.round(t1 - t0),
    clientReqId,
    serverReqId: request_id,
    region: h.get("x-ms-region"),
    remainTokens: h.get("x-ratelimit-remaining-tokens"),
    remainReq: h.get("x-ratelimit-remaining-requests"),
    usage: data?.usage,
  });

  console.log("answer: ", data?.choices?.[0]?.message?.content?.trim());
  return (
    data?.choices?.[0]?.message?.content?.trim() || "לא התקבלה תשובה מהמודל."
  );
}

module.exports = { chat };

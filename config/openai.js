const { OpenAI } = require("openai");

const endpoint = (process.env.AZURE_OPENAI_ENDPOINT || "").replace(/\/+$/, "");
const deployment = process.env.AZURE_OPENAI_DEPLOYMENT;
const apiKey = process.env.AZURE_OPENAI_KEY;
const apiVersion = process.env.AZURE_OPENAI_API_VERSION || "2024-12-01-preview";

const client = new OpenAI({
  apiKey,
  baseURL: `${endpoint}/openai/deployments/${deployment}`,
  defaultHeaders: { "api-key": apiKey },
  defaultQuery: { "api-version": apiVersion },
});

async function chat({
  message,
  history = [],
  systemPrompt,
}) {
  const messages = [
    { role: "system", content: systemPrompt },
    ...history,
    { role: "user", content: message },
  ];
  
  const r = await client.chat.completions.create({
    model: deployment,
    messages,
  });
  return r.choices?.[0]?.message?.content?.trim() || "לא התקבלה תשובה מהמודל.";
}

module.exports = { chat };

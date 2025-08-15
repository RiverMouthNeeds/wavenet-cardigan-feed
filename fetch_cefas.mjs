import fs from "fs";
import { parse } from "csv-parse/sync";

const RECORDSET_ID = process.env.CEFAS_RECORDSET_ID || "12651";
const TARGET_INST  = String(process.env.INST_ID || "353").replace(/\D/g,""); // 353
const CSV_URL = `https://data-api.cefas.co.uk/api/export/${RECORDSET_ID}?format=csv`;

const norm = s => String(s ?? "").trim();
const lc = s => norm(s).toLowerCase();

function toISO(dt) {
  const s = norm(dt);
  // D/M/Y H:M[:S]
  const m1 = s.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m1) {
    const [, dd, MM, yyyy, hh="00", mm="00", ss="00"] = m1;
    return new Date(Date.UTC(+yyyy, +MM - 1, +dd, +hh, +mm, +ss)).toISOString();
  }
  const d = new Date(s.endsWith("Z") ? s : s + "Z");
  return isNaN(d) ? null : d.toISOString();
}

// Fuzzy classifiers for column names
const pick = (headers, exact=[], loose=[]) => {
  const H = headers.map(h => lc(h));
  for (const e of exact) { const i = H.indexOf(lc(e)); if (i>=0) return headers[i]; }
  for (const p of loose) { const i = H.findIndex(h => h.includes(lc(p))); if (i>=0) return headers[i]; }
  return null;
};

const resp = await fetch(CSV_URL, { cache: "no-store" });
if (!resp.ok) { console.error(`Download failed ${resp.status} ${resp.statusText}`); process.exit(1); }
const text = await resp.text();
const rows = parse(text, { columns: true, skip_empty_lines: true });

fs.mkdirSync("public", { recursive: true });

if (!rows.length) {
  fs.writeFileSync("public/latest.json", JSON.stringify({ latest:null }, null, 2));
  fs.writeFileSync("public/history.json", "[]");
  fs.writeFileSync("public/diagnostics.json", JSON.stringify({ note:"CSV empty" }, null, 2));
  process.exit(0);
}

const headers = Object.keys(rows[0]);

// Detect relevant columns present in YOUR file (from your diagnostics)
const timeCol   = pick(headers, ["Date/Time","DateTime","SampleDateTime","Timestamp","Time","Datetime"], ["date","time"]);
const instCol   = pick(headers, ["InstId","InstID","InstrumentID","InstrumentId"], ["inst","instrument"]);
const depCol    = pick(headers, ["Deployment","DeploymentName"], ["deployment","location","site","name"]);
const paramCol  = pick(headers, ["Parameter","ParameterCode","ParamCode"], ["parameter","param","variable","observed","property","name"]);
const valueCol  = pick(headers, ["ResultMean","Value","Result","Reading","DataValue","NumericValue"], ["value","result","reading"]);

// Filter to Cardigan by InstId or Deployment text
function isCardigan(row) {
  const inst = instCol ? String(row[instCol] ?? "") : "";
  if (inst && inst.replace(/\D/g,"") === TARGET_INST) return true;
  const dep  = depCol ? lc(row[depCol]) : "";
  return dep.includes("cardigan");
}

// Build: ts -> { rawLabel: value, ... }, keep all param labels we see
const byTs = new Map();
const labelsSeen = new Set();

for (const r of rows) {
  if (!isCardigan(r)) continue;

  const ts = toISO(r[timeCol]);
  if (!ts) continue;

  const label = norm(r[paramCol]);
  const rawVal = norm(r[valueCol]).replace(",", ".");
  const val = parseFloat(rawVal);
  if (!Number.isFinite(val) || !label) continue;

  labelsSeen.add(label);
  if (!byTs.has(ts)) byTs.set(ts, { ts, raw: {} });
  byTs.get(ts).raw[label] = val;
}

// Choose canonical fields from raw labels via fuzzy rules
function pickField(raw, keys) {
  const entries = Object.entries(raw);
  const find = (reArr) =>
    entries.find(([k]) => reArr.some(re => re.test(k.toLowerCase().replace(/[^a-z0-9]+/g,""))));
  const m = find(keys);
  return m ? m[1] : undefined;
}

function canonicalize(entry) {
  const raw = entry.raw || {};
  const mk = (arr) => arr.map(s => new RegExp(s));

  return {
    ts: entry.ts,
    hm0: pickField(raw, mk([
      "^hm0$", "significantwaveheight", "^hs$", "hm0m"
    ])),
    tp: pickField(raw, mk([
      "^tpeak$", "^tp$", "peakperiod", "^tpp$"
    ])),
    tz: pickField(raw, mk([
      "^tz$", "^t02$", "zerocross"
    ])),
    dir: pickField(raw, mk([
      "^w_pdir$", "^dp$", "peakdirection", "^mwd$", "meandirection", "^direction$"
    ])),
    raw
  };
}

const series = [...byTs.values()].map(canonicalize).sort((a,b)=>a.ts.localeCompare(b.ts));
const latest = series.at(-1) ?? null;

// Write outputs
fs.writeFileSync("public/history.json", JSON.stringify(series, null, 2));
fs.writeFileSync("public/latest.json", JSON.stringify({ latest }, null, 2));
fs.writeFileSync("public/diagnostics.json", JSON.stringify({
  headers, timeCol, instCol, depCol, paramCol, valueCol,
  targetInst: TARGET_INST,
  labelSamples: Array.from(labelsSeen).slice(0, 30),
  countTimestamps: series.length,
  latestSummary: latest ? { ts: latest.ts, keys: Object.keys(latest.raw) } : null
}, null, 2));
fs.writeFileSync("public/index.html",
  `<!doctype html><meta charset="utf-8"><title>Cardigan (EXT)</title>
   <h1>Cardigan Bay (EXT) – WaveNet feed</h1>
   <ul>
     <li><a href="./latest.json">latest.json</a></li>
     <li><a href="./history.json">history.json</a></li>
     <li><a href="./diagnostics.json">diagnostics.json</a></li>
   </ul>`);

console.log(`Built ${series.length} records; latest =`, latest);

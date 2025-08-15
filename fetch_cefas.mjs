import fs from "fs";
import { parse } from "csv-parse/sync";

const RECORDSET_ID = process.env.CEFAS_RECORDSET_ID || "12651";
const CSV_URL = `https://data-api.cefas.co.uk/api/export/${RECORDSET_ID}?format=csv`;
const PREFERRED_INST = (process.env.INST_ID || "").replace(/\D/g, ""); // optional override, e.g. "353"

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

const resp = await fetch(CSV_URL, { cache: "no-store" });
if (!resp.ok) {
  console.error(`Download failed ${resp.status} ${resp.statusText}`);
  process.exit(1);
}
const text = await resp.text();
const rows = parse(text, { columns: true, skip_empty_lines: true });

fs.mkdirSync("public", { recursive: true });

if (!rows.length) {
  fs.writeFileSync("public/latest.json", JSON.stringify({ latest: null }, null, 2));
  fs.writeFileSync("public/history.json", "[]");
  fs.writeFileSync("public/diagnostics.json", JSON.stringify({ note: "CSV empty" }, null, 2));
  process.exit(0);
}

const headers = Object.keys(rows[0]);
// Column detection (from your diagnostics)
const timeCol  = ["Date/Time","DateTime","SampleDateTime","Timestamp","Time","Datetime"].find(h => headers.includes(h)) || headers.find(h => lc(h).includes("time") || lc(h).includes("date"));
const instCol  = ["InstId","InstID","InstrumentID","InstrumentId"].find(h => headers.includes(h)) || headers.find(h => lc(h).includes("inst"));
const depCol   = ["Deployment","DeploymentName"].find(h => headers.includes(h)) || headers.find(h => lc(h).includes("deploy") || lc(h).includes("location") || lc(h).includes("site") || lc(h).includes("name"));
const paramCol = ["Parameter","ParameterCode","ParamCode"].find(h => headers.includes(h)) || headers.find(h => lc(h).includes("param") || lc(h).includes("variable") || lc(h).includes("observed") || lc(h).includes("property") || lc(h).includes("name"));
const valueCol = ["ResultMean","Value","Result","Reading","DataValue","NumericValue"].find(h => headers.includes(h)) || headers.find(h => lc(h).includes("value") || lc(h).includes("result") || lc(h).includes("reading"));

function instKeyOf(r) {
  const inst = instCol ? norm(r[instCol]) : "";
  const dep  = depCol  ? norm(r[depCol])  : "";
  const instDigits = inst.replace(/\D/g, "");
  return `${instDigits || "no-inst"}|${dep || "no-deployment"}`;
}

// Build a summary of instruments present
const stationMap = new Map();
for (const r of rows) {
  const key = instKeyOf(r);
  if (!stationMap.has(key)) stationMap.set(key, {
    instId: (instCol ? norm(r[instCol]).replace(/\D/g,"") : "") || null,
    deployment: depCol ? norm(r[depCol]) : null,
    rowCount: 0,
    lastTs: null,
    params: new Set(),
  });
  const s = stationMap.get(key);
  s.rowCount++;
  const ts = toISO(r[timeCol]);
  if (ts && (!s.lastTs || ts > s.lastTs)) s.lastTs = ts;
  s.params.add(norm(r[paramCol]));
}

// Pick Cardigan instrument
function pickStation() {
  const all = [...stationMap.values()].map(s => ({ ...s, params: [...s.params] }));
  // Prefer Deployment containing "Cardigan"
  const cardigan = all.filter(s => lc(s.deployment || "").includes("cardigan"));
  if (cardigan.length) return cardigan.sort((a,b) => b.rowCount - a.rowCount)[0];
  // Else prefer explicit INST_ID env
  if (PREFERRED_INST) {
    const byInst = all.filter(s => (s.instId || "") === PREFERRED_INST);
    if (byInst.length) return byInst.sort((a,b) => b.rowCount - a.rowCount)[0];
  }
  // Else take the most-populated instrument
  return all.sort((a,b) => b.rowCount - a.rowCount)[0];
}

const chosen = pickStation();

// Pivot long -> wide for the chosen instrument
const byTs = new Map();
const labelSet = new Set();

function matchesChosen(r) {
  const instDigits = instCol ? norm(r[instCol]).replace(/\D/g,"") : "";
  const depTxt = depCol ? lc(r[depCol]) : "";
  if (chosen.instId && instDigits === chosen.instId) return true;
  if (chosen.deployment && lc(chosen.deployment) && depTxt === lc(chosen.deployment)) return true;
  return false;
}

for (const r of rows) {
  if (!matchesChosen(r)) continue;
  const ts = toISO(r[timeCol]);
  if (!ts) continue;

  const label = norm(r[paramCol]);
  const rawVal = norm(r[valueCol]).replace(",", ".");
  const val = parseFloat(rawVal);
  if (!Number.isFinite(val) || !label) continue;

  labelSet.add(label);
  if (!byTs.has(ts)) byTs.set(ts, { ts, raw: {} });
  byTs.get(ts).raw[label] = val;
}

// Fuzzy pick canonical fields from raw labels
function pickField(raw, patterns) {
  const keyz = Object.keys(raw);
  const canon = k => k.toLowerCase().replace(/[^a-z0-9]/g,"");
  for (const re of patterns) {
    const m = keyz.find(k => re.test(canon(k)));
    if (m) return raw[m];
  }
  return undefined;
}
const mk = arr => arr.map(s => new RegExp(s));

const series = [...byTs.values()].map(e => {
  const obj = {
    ts: e.ts,
    hm0: pickField(e.raw, mk(["^hm0$", "significantwaveheight", "^hs$", "hm0m"])),
    tp:  pickField(e.raw, mk(["^tpeak$", "^tp$", "peakperiod", "^tpp$"])),
    tz:  pickField(e.raw, mk(["^tz$", "^t02$", "zerocross"])),
    dir: pickField(e.raw, mk(["^w_pdir$", "^wpdir$", "^dp$", "peakdirection", "^mwd$", "meandirection", "^direction$"])),
    raw: e.raw
  };
  if (obj.dir == null && e.raw["W_PDIR"] != null) obj.dir = e.raw["W_PDIR"];
  return obj;
}).sort((a,b)=>a.ts.localeCompare(b.ts));

const latest = series.at(-1) ?? null;

// Write outputs
fs.writeFileSync("public/history.json", JSON.stringify(series, null, 2));
fs.writeFileSync("public/latest.json", JSON.stringify({ latest }, null, 2));

// Stations summary for debugging/confirmation
const stations = [...stationMap.values()].map(s => ({
  instId: s.instId, deployment: s.deployment, rowCount: s.rowCount, lastTs: s.lastTs, params: [...s.params].slice(0,40)
})).sort((a,b)=>b.rowCount - a.rowCount);
fs.writeFileSync("public/stations.json", JSON.stringify(stations, null, 2));

// Diagnostics
fs.writeFileSync("public/diagnostics.json", JSON.stringify({
  headers, timeCol, instCol, depCol, paramCol, valueCol, recordsetId: RECORDSET_ID,
  chosen, paramLabelSamples: Array.from(labelSet).slice(0, 40),
  countTimestamps: series.length,
  latestSummary: latest ? { ts: latest.ts, keys: Object.keys(latest.raw || {}) } : null
}, null, 2));

// Simple index
fs.writeFileSync("public/index.html",
  `<!doctype html><meta charset="utf-8"><title>WaveNet – Cardigan feed</title>
   <h1>WaveNet – Cardigan feed</h1>
   <ul>
     <li><a href="./latest.json">latest.json</a></li>
     <li><a href="./history.json">history.json</a></li>
     <li><a href="./diagnostics.json">diagnostics.json</a></li>
     <li><a href="./stations.json">stations.json</a></li>
   </ul>`);

console.log(`Built ${series.length} records for chosen instrument`, chosen);

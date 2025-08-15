import fs from "fs";
import { parse } from "csv-parse/sync";

const RECORDSET_ID = process.env.CEFAS_RECORDSET_ID || "12651";
const STATION_CODE = (process.env.STATION_CODE || "EXT").toUpperCase();
const PLATFORM_ID  = (process.env.PLATFORM_ID  || "353~EXT").toUpperCase();
const INST_ID_ENV  = process.env.INST_ID ? String(process.env.INST_ID).trim() : "353"; // NEW: instrument id

const CSV_URL = `https://data-api.cefas.co.uk/api/export/${RECORDSET_ID}?format=csv`;

const norm = s => String(s ?? "").trim();
const lc = s => norm(s).toLowerCase();

function toISO(dt) {
  const s = norm(dt);
  // ISO already
  if (/^\d{4}-\d{2}-\d{2}T/.test(s)) {
    const d = new Date(s);
    return isNaN(d) ? null : d.toISOString();
  }
  // D/M/Y H:M[:S]
  const m1 = s.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m1) {
    const [, dd, MM, yyyy, hh="00", mm="00", ss="00"] = m1;
    const t = Date.UTC(+yyyy, +MM - 1, +dd, +hh, +mm, +ss);
    return new Date(t).toISOString();
  }
  // Y-M-D H:M[:S] (assume UTC)
  const m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m2) {
    const [, yyyy, MM, dd, hh="00", mm="00", ss="00"] = m2;
    const t = Date.UTC(+yyyy, +MM - 1, +dd, +hh, +mm, +ss);
    return new Date(t).toISOString();
  }
  const d = new Date(s);
  return isNaN(d) ? null : d.toISOString();
}

function pickExact(headers, names) {
  const H = headers.map(h => lc(h));
  for (const n of names) {
    const i = H.indexOf(lc(n));
    if (i >= 0) return headers[i];
  }
  return null;
}
function pickLoose(headers, needles) {
  const H = headers.map(h => lc(h));
  for (const n of needles) {
    const i = H.findIndex(h => h.includes(lc(n)));
    if (i >= 0) return headers[i];
  }
  return null;
}

function classifyParam(v) {
  const t = lc(v).replace(/[^a-z0-9]/g, "");
  if (t.includes("hm0") || t.includes("hs") || t.includes("significantwaveheight")) return "hm0";
  if (t === "tpeak" || t === "tp" || t.includes("peakperiod") || t === "tpp") return "tp";
  if (t === "tz" || t === "t02" || t.includes("zerocross")) return "tz";
  if (t.includes("w_pdir") || t === "dp" || t.includes("peakdirection") || t.includes("mwd") || t.includes("direction")) return "dir";
  return null;
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
  fs.writeFileSync("public/latest.json", JSON.stringify({ site:"cardigan", station:STATION_CODE, platform:PLATFORM_ID, latest:null }, null, 2));
  fs.writeFileSync("public/history.json", "[]");
  fs.writeFileSync("public/diagnostics.json", JSON.stringify({ note:"CSV empty" }, null, 2));
  process.exit(0);
}

const headers = Object.keys(rows[0]);

// Columns present in your diagnostics
const timeCol   = pickExact(headers, ["Date/Time","DateTime","SampleDateTime","Timestamp","Time","Datetime"]) || pickLoose(headers, ["date","time"]);
const paramCol  = pickExact(headers, ["Parameter","ParameterCode","ParamCode"]) || pickLoose(headers, ["parameter","param","variable","observed","property","name"]);
const valueCol  = pickExact(headers, ["ResultMean","Value","Result","Reading","DataValue","NumericValue"]) || pickLoose(headers, ["value","result","reading"]);
const instCol   = pickExact(headers, ["InstId","InstID","InstrumentID","InstrumentId"]) || pickLoose(headers, ["inst","instrument"]);
const deployCol = pickExact(headers, ["Deployment","DeploymentName"]) || pickLoose(headers, ["deployment","location","site","name"]);

// Match Cardigan rows by InstId or Deployment text
const TARGET_INST = INST_ID_ENV; // "353" by default
function isCardigan(row) {
  const instVal = instCol ? norm(row[instCol]) : "";
  const depVal  = deployCol ? lc(row[deployCol]) : "";
  if (instVal && instVal.replace(/\D/g,"") === TARGET_INST.replace(/\D/g,"")) return true;
  if (depVal.includes("cardigan")) return true;
  if (depVal.includes(lc(STATION_CODE))) return true; // EXT
  return false;
}

// Build time series
const byTs = new Map();
for (const r of rows) {
  if (!isCardigan(r)) continue;
  const ts = toISO(r[timeCol]);
  if (!ts) continue;
  const cls = classifyParam(r[paramCol]);
  if (!cls) continue;
  const raw = norm(r[valueCol]).replace(",", ".");
  const val = parseFloat(raw);
  if (!Number.isFinite(val)) continue;

  if (!byTs.has(ts)) byTs.set(ts, { ts });
  byTs.get(ts)[cls] = val;
}

const series = [...byTs.values()].sort((a,b)=>a.ts.localeCompare(b.ts));
const latest = series.at(-1) ?? null;

// Write outputs
fs.writeFileSync("public/history.json", JSON.stringify(series, null, 2));
fs.writeFileSync("public/latest.json", JSON.stringify({ site:"cardigan", station:STATION_CODE, platform:PLATFORM_ID, latest }, null, 2));
fs.writeFileSync("public/diagnostics.json", JSON.stringify({
  headers, timeCol, paramCol, valueCol, instCol, deployCol, targetInst: TARGET_INST,
  rowCount: rows.length, matchedRows: series.length ? "ok" : 0, sampleFirst: rows[0], sampleMatch: rows.find(isCardigan) || null,
  note: "Shape: long (Parameter/ResultMean). Matched by InstId or Deployment."
}, null, 2));

// Simple index
fs.writeFileSync("public/index.html",
  `<!doctype html><meta charset="utf-8"><title>Cardigan (EXT)</title>
   <h1>Cardigan Bay (EXT) – WaveNet feed</h1>
   <ul>
     <li><a href="./latest.json">latest.json</a></li>
     <li><a href="./history.json">history.json</a></li>
     <li><a href="./diagnostics.json">diagnostics.json</a></li>
   </ul>
   <p>Source: Cefas Data Hub recordset ${RECORDSET_ID}. Station ${STATION_CODE}, Platform ${PLATFORM_ID}, InstId ${TARGET_INST}.</p>`);
console.log(`Built ${series.length} records; latest =`, latest);

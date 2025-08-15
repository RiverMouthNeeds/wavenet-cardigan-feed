import fs from "fs";
import { parse } from "csv-parse/sync";

const RECORDSET_ID = process.env.CEFAS_RECORDSET_ID || "12651";
const STATION_CODE  = process.env.STATION_CODE  || "EXT";
const PLATFORM_ID   = process.env.PLATFORM_ID   || "353~EXT";

// ---- helpers ----
const CSV_URL = `https://data-api.cefas.co.uk/api/export/${RECORDSET_ID}?format=csv`;

function toISO(s) {
  // Try native parse; if not, try appending Z
  let d = new Date(s);
  if (isNaN(d.getTime())) d = new Date(s + "Z");
  return isNaN(d.getTime()) ? null : d.toISOString();
}

function pickHeader(headers, exact=[], loose=[]) {
  const H = headers.map(h => h.toLowerCase());
  for (const e of exact) {
    const i = H.indexOf(e.toLowerCase());
    if (i >= 0) return headers[i];
  }
  for (const part of loose) {
    const i = H.findIndex(h => h.includes(part.toLowerCase()));
    if (i >= 0) return headers[i];
  }
  return null;
}

// Normalize a parameter key for matching (letters+numbers only, lowercase)
function normParam(v) {
  return String(v ?? "").toLowerCase().replace(/[^a-z0-9]+/g, "");
}

// Decide if a row belongs to Cardigan (EXT / 353~EXT / Cardigan Bay)
function rowIsCardigan(r, stationCols, platformCols, nameCols) {
  const needles = [
    STATION_CODE.toUpperCase(),            // "EXT"
    PLATFORM_ID.toUpperCase(),            // "353~EXT"
    "353",                                // numeric platform id often present
    "CARDIGAN",                           // station name
  ];
  const pool = [];

  for (const c of stationCols) pool.push(String(r[c] ?? ""));
  for (const c of platformCols) pool.push(String(r[c] ?? ""));
  for (const c of nameCols)     pool.push(String(r[c] ?? ""));

  const hay = pool.join("|").toUpperCase();
  return needles.some(n => hay.includes(n));
}

// Map many possible parameter labels to our canonical keys
function classifyParam(raw) {
  const p = normParam(raw);

  // Significant wave height
  if (["hm0","hs","significantwaveheight","swh","hm0m","hsig"].some(k => p.includes(k))) return "hm0";

  // Peak period
  if (["tpeak","tp","peakperiod","tppeak","tpp"].some(k => p.includes(k) || p === k)) return "tp";

  // Mean zero-crossing period
  if (["tz","t02","meanzero","zerocross"].some(k => p.includes(k))) return "tz";

  // Direction (degrees true, "from")
  if (
    ["w_pdir","dp","peakdirection","dirpeak","swelldirection","direction","mwd","meandirection"].some(k => p.includes(k))
  ) return "dir";

  return null;
}

// ---- fetch & parse ----
const resp = await fetch(CSV_URL, { cache: "no-store" });
if (!resp.ok) {
  console.error(`Download failed ${resp.status}: ${resp.statusText}`);
  process.exit(1);
}
const text = await resp.text();
const rows = parse(text, { columns: true, skip_empty_lines: true });

if (!rows.length) {
  fs.mkdirSync("public", { recursive: true });
  fs.writeFileSync("public/latest.json", JSON.stringify({ site:"cardigan", station:STATION_CODE, platform:PLATFORM_ID, latest:null }, null, 2));
  fs.writeFileSync("public/history.json", "[]");
  fs.writeFileSync("public/diagnostics.json", JSON.stringify({ note:"CSV returned 0 rows" }, null, 2));
  process.exit(0);
}

const headers = Object.keys(rows[0]);

// Identify likely columns
const timeCol     = pickHeader(headers, ["DateTime","SampleDateTime","Timestamp","Time","Datetime"], ["time"]);
const valueCol    = pickHeader(headers, ["Value","Result","Reading","DataValue","NumericValue"], ["value","result","reading"]);
const paramCols   = [
  pickHeader(headers, ["Parameter","ParameterCode","ParamCode"], ["parameter","param"]),
  pickHeader(headers, [], ["name","shortname","observed","property","variable"])
].filter(Boolean);

// Possible station/platform/name columns
const stationCols  = [
  pickHeader(headers, ["Station","StationCode","Site","SiteCode","StationID"], ["station","site"])
].filter(Boolean);
const platformCols = [
  pickHeader(headers, ["Platform","PlatformCode","PlatformID","PlatformNumber"], ["platform"])
].filter(Boolean);
const nameCols     = [
  pickHeader(headers, ["StationName","SiteName","PlatformName","Location"], ["name","location"])
].filter(Boolean);

const diag = {
  rowCount: rows.length,
  headers,
  timeCol, valueCol, paramCols, stationCols, platformCols, nameCols,
  samples: rows.slice(0, 3)
};

// Build frequency of parameter labels to see what’s present
const paramFreq = {};
for (const r of rows) {
  for (const pc of paramCols) {
    const raw = r[pc];
    if (!raw) continue;
    const k = String(raw).trim();
    paramFreq[k] = (paramFreq[k] || 0) + 1;
  }
}
diag.paramFrequency = paramFreq;

// Filter to Cardigan rows
const cardiganRows = rows.filter(r => rowIsCardigan(r, stationCols, platformCols, nameCols));

// Pivot long → wide
const byTs = new Map();
for (const r of cardiganRows) {
  const ts = toISO(r[timeCol]);
  if (!ts) continue;

  // Pick best param label available in this row
  let pRaw = null;
  for (const pc of paramCols) { if (r[pc]) { pRaw = r[pc]; break; } }
  const cls = classifyParam(pRaw);
  if (!cls) continue;

  const val = parseFloat(String(r[valueCol]).replace(",", "."));
  if (!Number.isFinite(val)) continue;

  if (!byTs.has(ts)) byTs.set(ts, { ts });
  byTs.get(ts)[cls] = val;
}

const series = [...byTs.values()].sort((a,b)=>a.ts.localeCompare(b.ts));
const latest = series.at(-1) ?? null;

// ---- write outputs ----
fs.mkdirSync("public", { recursive: true });
fs.writeFileSync("public/history.json", JSON.stringify(series, null, 2));
fs.writeFileSync("public/latest.json", JSON.stringify({ site:"cardigan", station:STATION_CODE, platform:PLATFORM_ID, latest }, null, 2));

// Diagnostics to help us tune mappings, visible at /diagnostics.json
diag.filteredRowCount = cardiganRows.length;
diag.seriesCount = series.length;
diag.latest = latest;
fs.writeFileSync("public/diagnostics.json", JSON.stringify(diag, null, 2));

// Human index
const html = `<!doctype html><meta charset="utf-8"><title>Cardigan (EXT)</title>
<h1>Cardigan Bay (EXT) – WaveNet feed</h1>
<ul>
  <li><a href="./latest.json">latest.json</a></li>
  <li><a href="./history.json">history.json</a></li>
  <li><a href="./diagnostics.json">diagnostics.json</a> (debug)</li>
</ul>
<p>Source: Cefas Data Hub recordset ${RECORDSET_ID}. Station ${STATION_CODE}, Platform ${PLATFORM_ID}.</p>`;
fs.writeFileSync("public/index.html", html);

console.log(`Rows: ${rows.length}, Cardigan rows: ${cardiganRows.length}, Series: ${series.length}`);

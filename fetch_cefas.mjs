import fs from "fs";
import { parse } from "csv-parse/sync";

const RECORDSET_ID = process.env.CEFAS_RECORDSET_ID || "12651";
const STATION_CODE = (process.env.STATION_CODE || "EXT").toUpperCase();
const PLATFORM_ID  = (process.env.PLATFORM_ID  || "353~EXT").toUpperCase();

const CSV_URL = `https://data-api.cefas.co.uk/api/export/${RECORDSET_ID}?format=csv`;

const norm = s => String(s ?? "").trim();
const lc = s => norm(s).toLowerCase();

function toISO(s) {
  // Try native parse; if NaN, try appending Z (assume UTC)
  let d = new Date(s);
  if (isNaN(d)) d = new Date(`${s}Z`);
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
  for (const needle of needles) {
    const i = H.findIndex(h => h.includes(lc(needle)));
    if (i >= 0) return headers[i];
  }
  return null;
}

function findTimeCol(headers) {
  return (
    pickExact(headers, ["DateTime", "SampleDateTime", "Timestamp", "Time", "Datetime"]) ||
    pickLoose(headers, ["date", "time"])
  );
}

function stationMatchPool(row, stationCols) {
  return stationCols.map(c => lc(row[c] ?? "")).join(" | ");
}

function looksLikeCardigan(pool) {
  return (
    pool.includes(lc(STATION_CODE)) ||
    pool.includes(lc(PLATFORM_ID)) ||
    /(^|[^0-9])353([^0-9]|$)/.test(pool) ||  // numeric id appears alone
    pool.includes("cardigan")
  );
}

// map headers like "Hm0", "Hm0 (m)", "Hs", "Significant wave height" → "hm0"
function classifyWideHeader(h) {
  const t = lc(h).replace(/[^a-z0-9]/g, "");
  if (/(^|)hm0($|)/.test(t) || t.includes("hs") || t.includes("significantwaveheight")) return "hm0";
  if (t === "tpeak" || t === "tp" || t.includes("peakperiod")) return "tp";
  if (t === "tz" || t === "t02" || t.includes("zerocross")) return "tz";
  if (t.includes("w_pdir") || t === "dp" || t.includes("peakdirection") || t.includes("mwd") || t === "direction" || t.includes("meandirection")) return "dir";
  return null;
}

function classifyLongParam(v) {
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

// Find likely station/platform/name cols for filtering
const stationCols = [
  pickExact(headers, ["Station","StationCode","Site","SiteCode","StationID"]),
  pickLoose(headers, ["station","site","buoy"])
].filter(Boolean);

const platformCols = [
  pickExact(headers, ["Platform","PlatformCode","PlatformID","PlatformNumber"]),
  pickLoose(headers, ["platform"])
].filter(Boolean);

const nameCols = [
  pickExact(headers, ["StationName","SiteName","PlatformName","Location"]),
  pickLoose(headers, ["name","location"])
].filter(Boolean);

const filterCols = [...new Set([...stationCols, ...platformCols, ...nameCols])];

const timeCol = findTimeCol(headers);

// --- Detect WIDE vs LONG ---
const wideMap = {};
for (const h of headers) {
  const k = classifyWideHeader(h);
  if (k) wideMap[k] = h;
}
const isWide = Object.keys(wideMap).length >= 2;  // need at least two of hm0/tp/tz/dir

let series = [];
let diagnostics = {
  shape: isWide ? "wide" : "long",
  headers,
  timeCol,
  stationCols: filterCols,
  wideColumnsDetected: wideMap,
};

if (isWide) {
  // ----- WIDE: one row has Hm0/Tpeak/Tz/W_PDIR in separate columns -----
  for (const r of rows) {
    const pool = stationMatchPool(r, filterCols);
    if (!looksLikeCardigan(pool)) continue;

    const ts = toISO(r[timeCol]);
    if (!ts) continue;

    const obj = { ts };
    for (const key of ["hm0","tp","tz","dir"]) {
      const col = wideMap[key];
      if (!col) continue;
      const raw = norm(r[col]).replace(",", ".");
      const val = parseFloat(raw);
      if (Number.isFinite(val)) obj[key] = val;
    }
    if (obj.hm0 != null || obj.tp != null || obj.tz != null || obj.dir != null) series.push(obj);
  }
} else {
  // ----- LONG: rows like Parameter=Hm0, Value=..., DateTime=... -----
  const valueCol = pickExact(headers, ["Value","Result","Reading","DataValue","NumericValue"]) || pickLoose(headers, ["value","result","reading"]);
  const paramCol = pickExact(headers, ["Parameter","ParameterCode","ParamCode"]) || pickLoose(headers, ["parameter","param","variable","observed","property","name"]);

  diagnostics.valueCol = valueCol;
  diagnostics.paramCol = paramCol;

  const byTs = new Map();
  for (const r of rows) {
    const pool = stationMatchPool(r, filterCols);
    if (!looksLikeCardigan(pool)) continue;

    const ts = toISO(r[timeCol]);
    if (!ts) continue;

    const cls = classifyLongParam(r[paramCol]);
    if (!cls) continue;

    const raw = norm(r[valueCol]).replace(",", ".");
    const val = parseFloat(raw);
    if (!Number.isFinite(val)) continue;

    if (!byTs.has(ts)) byTs.set(ts, { ts });
    byTs.get(ts)[cls] = val;
  }
  series = [...byTs.values()].sort((a,b)=>a.ts.localeCompare(b.ts));
}

series.sort((a,b)=>a.ts.localeCompare(b.ts));
const latest = series.at(-1) ?? null;

// Write outputs
fs.writeFileSync("public/history.json", JSON.stringify(series, null, 2));
fs.writeFileSync("public/latest.json", JSON.stringify({
  site: "cardigan",
  station: STATION_CODE,
  platform: PLATFORM_ID,
  latest
}, null, 2));

diagnostics.rowCount = rows.length;
diagnostics.filteredSeries = series.length;
diagnostics.latest = latest;
fs.writeFileSync("public/diagnostics.json", JSON.stringify(diagnostics, null, 2));

// Simple index
fs.writeFileSync("public/index.html",
  `<!doctype html><meta charset="utf-8"><title>Cardigan (EXT)</title>
   <h1>Cardigan Bay (EXT) – WaveNet feed</h1>
   <ul>
     <li><a href="./latest.json">latest.json</a></li>
     <li><a href="./history.json">history.json</a></li>
     <li><a href="./diagnostics.json">diagnostics.json</a></li>
   </ul>
   <p>Source: Cefas Data Hub recordset ${RECORDSET_ID}. Station ${STATION_CODE}, Platform ${PLATFORM_ID}.</p>`);
console.log(`Built ${series.length} records; latest =`, latest);

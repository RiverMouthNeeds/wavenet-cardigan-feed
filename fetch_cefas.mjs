import fs from "fs";
import { parse } from "csv-parse/sync";

const RECORDSET_ID = process.env.CEFAS_RECORDSET_ID || "12651"; // WaveNet 48h feed
const STATION_CODE  = process.env.STATION_CODE  || "EXT";       // Cardigan Bay station
const PLATFORM_ID   = process.env.PLATFORM_ID   || "353~EXT";   // Cardigan platform

const CSV_URL = `https://data-api.cefas.co.uk/api/export/${RECORDSET_ID}?format=csv`;

function toISO(s) {
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d.toISOString();
}
function findCol(headers, exact, loose=[]) {
  const h = headers.map(x => x.toLowerCase());
  for (const e of exact) {
    const i = h.indexOf(e.toLowerCase());
    if (i >= 0) return headers[i];
  }
  for (const p of loose) {
    const i = h.findIndex(x => x.includes(p.toLowerCase()));
    if (i >= 0) return headers[i];
  }
  return null;
}

// 1) Download CSV
const resp = await fetch(CSV_URL, { cache: "no-store" });
if (!resp.ok) {
  console.error(`Download failed ${resp.status}: ${resp.statusText}`);
  process.exit(1);
}
const text = await resp.text();
const rows = parse(text, { columns: true, skip_empty_lines: true });
if (!rows.length) {
  console.error("CSV had no rows.");
  process.exit(1);
}

// 2) Detect columns (Cefas headers can vary slightly)
const headers = Object.keys(rows[0]);
const timeCol     = findCol(headers, ["DateTime","SampleDateTime","Timestamp","Time","Datetime"], ["time"]);
const stationCol  = findCol(headers, ["Station","StationCode","Site","SiteCode","StationID"], ["station","site"]);
const platformCol = findCol(headers, ["Platform","PlatformCode","PlatformID"], ["platform"]);
const paramCol    = findCol(headers, ["Parameter","ParameterCode","ParamCode"], ["parameter"]);
const valueCol    = findCol(headers, ["Value","Result","Reading","DataValue","NumericValue"], ["value","result","reading"]);
if (!timeCol || !paramCol || !valueCol) {
  console.error("Could not identify required columns.\nHeaders:", headers);
  process.exit(1);
}

// 3) Filter to Cardigan Bay (EXT)
function matchesStation(r) {
  const s = stationCol ? String(r[stationCol] ?? "").trim().toUpperCase() : "";
  const p = platformCol ? String(r[platformCol] ?? "").trim().toUpperCase() : "";
  return s === STATION_CODE.toUpperCase() || p === PLATFORM_ID.toUpperCase();
}
const filtered = rows.filter(matchesStation);

// 4) Pivot long → wide by timestamp, keeping key parameters
const wanted = { Hm0:"hm0", Tpeak:"tp", Tz:"tz", W_PDIR:"dir" };
const byTs = new Map();
for (const r of filtered) {
  const ts = toISO(r[timeCol]);
  if (!ts) continue;
  const pcode = String(r[paramCol] ?? "").trim();
  const key = wanted[pcode];
  if (!key) continue;
  const val = parseFloat(String(r[valueCol]).replace(",", "."));
  if (!Number.isFinite(val)) continue;
  if (!byTs.has(ts)) byTs.set(ts, { ts });
  byTs.get(ts)[key] = val;
}
const series = [...byTs.values()].sort((a,b)=>a.ts.localeCompare(b.ts));
const latest = series.at(-1) ?? null;

// 5) Write public files
fs.mkdirSync("public", { recursive: true });
fs.writeFileSync("public/history.json", JSON.stringify(series, null, 2));
fs.writeFileSync("public/latest.json", JSON.stringify({
  site: "cardigan",
  station: STATION_CODE,
  platform: PLATFORM_ID,
  latest
}, null, 2));
fs.writeFileSync("public/index.html",
  `<!doctype html><meta charset="utf-8"><title>Cardigan (EXT)</title>
   <h1>Cardigan Bay (EXT) – WaveNet feed</h1>
   <ul><li><a href="./latest.json">latest.json</a></li><li><a href="./history.json">history.json</a></li></ul>
   <p>Source: Cefas Data Hub recordset ${RECORDSET_ID}. Station ${STATION_CODE}, Platform ${PLATFORM_ID}.</p>`);

console.log(`Wrote latest.json + history.json with ${series.length} rows.`);

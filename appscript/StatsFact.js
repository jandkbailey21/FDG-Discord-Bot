/***********************
 * FDG-14 + FDG-16: StatsFact (Normalized + Upsert)
 *
 * FDG-14:
 * - Creates StatsFact tab with normalized schema (no event columns, no formulas)
 * - Composite key defines the “fact grain”
 * - Manual uniqueness repair tool (dedupe)
 *
 * FDG-16:
 * - Idempotent upsert into StatsFact using composite key
 * - Safe handling of missing statIds (skip by default)
 * - Batch-friendly reads/writes for scale
 ***********************/

const STATSFACT_SHEET_NAME = "StatsFact";

// Normalized “fact” grain (uniqueness):
// Season + EventCode + TournID + Division + PdgaNumber + StatId
const STATSFACT_HEADERS = [
  "Season",     // e.g. 2025, 2026
  "EventCode",  // e.g. SFO, PCCO (your internal code)
  "TournID",    // PDGA Live event id used for API calls (string/number)
  "Division",   // MPO / FPO
  "PdgaNumber", // numeric/string
  "StatId",     // PDGA statId (string/number)
  "StatName",   // optional label (human/debug)
  "Value",      // numeric where possible; if text, store as text
  "Units",      // optional: %, strokes, feet, etc.
  "Source",     // e.g. "PDGA_LIVE_DIVISION_STATS"
  "PulledAt",   // ISO timestamp of insert/update
  "Key"         // composite key string for uniqueness + fast upsert
];

/**
 * Run once to create/repair the StatsFact sheet schema.
 * Safe to re-run.
 */
function setupStatsFactSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(STATSFACT_SHEET_NAME) || ss.insertSheet(STATSFACT_SHEET_NAME);

  // Write headers
  sh.getRange(1, 1, 1, STATSFACT_HEADERS.length).setValues([STATSFACT_HEADERS]);
  sh.setFrozenRows(1);

  // Header formatting
  sh.getRange(1, 1, 1, STATSFACT_HEADERS.length).setFontWeight("bold");

  // Optional: column widths
  sh.setColumnWidth(1, 70);   // Season
  sh.setColumnWidth(2, 90);   // EventCode
  sh.setColumnWidth(3, 80);   // TournID
  sh.setColumnWidth(4, 70);   // Division
  sh.setColumnWidth(5, 90);   // PdgaNumber
  sh.setColumnWidth(6, 80);   // StatId
  sh.setColumnWidth(7, 160);  // StatName
  sh.setColumnWidth(8, 90);   // Value
  sh.setColumnWidth(9, 70);   // Units
  sh.setColumnWidth(10, 180); // Source
  sh.setColumnWidth(11, 160); // PulledAt
  sh.setColumnWidth(12, 340); // Key

  SpreadsheetApp.flush();
}

/**
 * Builds the composite key used to enforce uniqueness.
 * IMPORTANT: This key defines the “grain” of your StatsFact table.
 */
function buildStatsFactKey_(season, eventCode, tournId, division, pdgaNumber, statId) {
  const s = String(season || "").trim();
  const e = String(eventCode || "").trim().toUpperCase();
  const t = String(tournId || "").trim();
  const d = String(division || "").trim().toUpperCase();
  const p = String(pdgaNumber || "").trim();
  const st = String(statId || "").trim();

  return [s, e, t, d, p, st].join("|");
}

/**
 * Manual repair tool:
 * Enforces uniqueness by Key (keeps the last occurrence).
 * Use only as an emergency fix; normal operation should rely on upsert.
 */
function enforceStatsFactUniqueness() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(STATSFACT_SHEET_NAME);
  if (!sh) throw new Error("StatsFact sheet not found. Run setupStatsFactSheet() first.");

  const lastRow = sh.getLastRow();
  if (lastRow <= 1) return; // headers only

  const lastCol = STATSFACT_HEADERS.length;
  const values = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();

  const keyColIndex = STATSFACT_HEADERS.indexOf("Key");
  if (keyColIndex < 0) throw new Error("Key column missing from STATSFACT_HEADERS.");

  // Keep last occurrence
  const keyToRow = new Map();
  for (let i = 0; i < values.length; i++) {
    const key = String(values[i][keyColIndex] || "").trim();
    if (!key) continue;
    keyToRow.set(key, i); // last wins
  }

  const keep = [];
  for (let i = 0; i < values.length; i++) {
    const key = String(values[i][keyColIndex] || "").trim();
    if (!key) {
      keep.push(values[i]); // keep blank-key rows (debug/optional cleanup later)
      continue;
    }
    if (keyToRow.get(key) === i) keep.push(values[i]);
  }

  if (keep.length !== values.length) {
    sh.getRange(2, 1, lastRow - 1, lastCol).clearContent();
    sh.getRange(2, 1, keep.length, lastCol).setValues(keep);

    const newLast = keep.length + 1;
    if (newLast < lastRow) {
      sh.getRange(newLast + 1, 1, lastRow - newLast, lastCol).clearContent();
    }
  }

  SpreadsheetApp.flush();
}

/***********************
 * FDG-16: StatsFact Upsert (Idempotent)
 ***********************/

/**
 * Upsert behavior:
 * - Uses composite Key column to enforce uniqueness
 * - Existing key => overwrite row
 * - New key => append row
 * - Missing required fields => skip safely
 *
 * opts:
 * - skipMissingStatId: boolean (default true). If false, will allow empty statId rows.
 *
 * Input: array of objects:
 * { Season, EventCode, TournID, Division, PdgaNumber, StatId, StatName, Value, Units, Source }
 */
function upsertStatsFactRows_(rows, opts) {
  opts = opts || {};
  const skipMissingStatId = opts.skipMissingStatId !== false; // default true
  const nowIso = new Date().toISOString();

  if (!rows || !rows.length) {
    return { inserted: 0, updated: 0, skipped: 0, total: 0 };
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(STATSFACT_SHEET_NAME);
  if (!sh) throw new Error("StatsFact sheet not found. Run setupStatsFactSheet() first.");

  const headers = STATSFACT_HEADERS;
  const lastCol = headers.length;

  const idx = {
    Season: headers.indexOf("Season"),
    EventCode: headers.indexOf("EventCode"),
    TournID: headers.indexOf("TournID"),
    Division: headers.indexOf("Division"),
    PdgaNumber: headers.indexOf("PdgaNumber"),
    StatId: headers.indexOf("StatId"),
    StatName: headers.indexOf("StatName"),
    Value: headers.indexOf("Value"),
    Units: headers.indexOf("Units"),
    Source: headers.indexOf("Source"),
    PulledAt: headers.indexOf("PulledAt"),
    Key: headers.indexOf("Key"),
  };

  // Validate headers once (fail fast)
  for (const [k, v] of Object.entries(idx)) {
    if (v < 0) throw new Error(`StatsFact missing required column: ${k}`);
  }

  const lastRow = sh.getLastRow();
  const existingCount = Math.max(0, lastRow - 1);

  // Read ONLY existing keys for speed
  const existingKeys = existingCount
    ? sh.getRange(2, idx.Key + 1, existingCount, 1).getValues()
    : [];

  // Map key => actual sheet row number
  const keyToSheetRow = new Map();
  for (let i = 0; i < existingKeys.length; i++) {
    const k = String(existingKeys[i][0] || "").trim();
    if (k) keyToSheetRow.set(k, i + 2);
  }

  const updatesByRow = new Map(); // rowNumber -> rowData[]
  const appends = [];

  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  for (const r of rows) {
    const season = r.Season ?? r.season ?? "";
    const eventCode = r.EventCode ?? r.eventCode ?? "";
    const tournId = r.TournID ?? r.tournId ?? r.tournID ?? "";
    const division = r.Division ?? r.division ?? "";
    const pdgaNumber = r.PdgaNumber ?? r.pdgaNumber ?? r.pdga ?? "";
    const statId = r.StatId ?? r.statId ?? "";
    const statName = r.StatName ?? r.statName ?? "";
    const value = r.Value ?? r.value ?? "";
    const units = r.Units ?? r.units ?? "";
    const source = r.Source ?? r.source ?? "";

    // Required fields
    if (!season || !eventCode || !tournId || !division || !pdgaNumber) {
      skipped++;
      continue;
    }
    if (skipMissingStatId && !statId) {
      skipped++;
      continue;
    }

    const key = buildStatsFactKey_(season, eventCode, tournId, division, pdgaNumber, statId);

    const rowData = new Array(lastCol).fill("");
    rowData[idx.Season] = String(season).trim();
    rowData[idx.EventCode] = String(eventCode).trim().toUpperCase();
    rowData[idx.TournID] = String(tournId).trim();
    rowData[idx.Division] = String(division).trim().toUpperCase();
    rowData[idx.PdgaNumber] = String(pdgaNumber).trim();
    rowData[idx.StatId] = String(statId).trim();
    rowData[idx.StatName] = statName;
    rowData[idx.Value] = value;
    rowData[idx.Units] = units;
    rowData[idx.Source] = source;
    rowData[idx.PulledAt] = nowIso;
    rowData[idx.Key] = key;

    const existingRowNum = keyToSheetRow.get(key);
    if (existingRowNum) {
      updatesByRow.set(existingRowNum, rowData);
      updated++;
    } else {
      appends.push(rowData);
      inserted++;
      // prevent duplicates within the same batch
      keyToSheetRow.set(key, (lastRow + 1) + (appends.length - 1));
    }
  }

  // Write updates in contiguous blocks
  if (updatesByRow.size) {
    const rowsSorted = Array.from(updatesByRow.keys()).sort((a, b) => a - b);

    let blockStart = rowsSorted[0];
    let block = [updatesByRow.get(blockStart)];

    for (let i = 1; i < rowsSorted.length; i++) {
      const rnum = rowsSorted[i];
      const prev = rowsSorted[i - 1];

      if (rnum === prev + 1) {
        block.push(updatesByRow.get(rnum));
      } else {
        sh.getRange(blockStart, 1, block.length, lastCol).setValues(block);
        blockStart = rnum;
        block = [updatesByRow.get(rnum)];
      }
    }
    sh.getRange(blockStart, 1, block.length, lastCol).setValues(block);
  }

  // Append new rows in one write
  if (appends.length) {
    sh.getRange(sh.getLastRow() + 1, 1, appends.length, lastCol).setValues(appends);
  }

  SpreadsheetApp.flush();

  return { inserted, updated, skipped, total: inserted + updated + skipped, existingBefore: existingCount };
}

/***********************
 * FDG-16: Test Helpers (DEV-only)
 * Remove or ignore in PROD (safe if never called).
 ***********************/

function test_FDG16_upsert_basic() {
  const r1 = {
    Season: 2025,
    EventCode: "SFO",
    TournID: 97685,
    Division: "MPO",
    PdgaNumber: 35449,
    StatId: "C1X_PCT",
    StatName: "Circle1X Putting %",
    Value: 78.3,
    Units: "%",
    Source: "TEST"
  };

  const result1 = upsertStatsFactRows_([r1]);
  const result2 = upsertStatsFactRows_([r1]); // run twice; second should update

  Logger.log(JSON.stringify({ result1, result2 }, null, 2));
}

function test_FDG16_missing_statId_skips() {
  const bad = {
    Season: 2025,
    EventCode: "SFO",
    TournID: 97685,
    Division: "MPO",
    PdgaNumber: 35449,
    StatId: "", // missing
    StatName: "Broken Stat",
    Value: 1,
    Units: "",
    Source: "TEST"
  };

  const result = upsertStatsFactRows_([bad]); // default skipMissingStatId=true
  Logger.log(JSON.stringify(result, null, 2));
}

function test_FDG16_bulk_5000() {
  const rows = [];
  for (let i = 0; i < 5000; i++) {
    rows.push({
      Season: 2025,
      EventCode: "SFO",
      TournID: 97685,
      Division: "MPO",
      PdgaNumber: 100000 + i,
      StatId: "C1X_PCT",
      StatName: "Circle1X Putting %",
      Value: 70 + (i % 30),
      Units: "%",
      Source: "BULK_TEST"
    });
  }
  const res = upsertStatsFactRows_(rows);
  Logger.log(JSON.stringify(res, null, 2));
}
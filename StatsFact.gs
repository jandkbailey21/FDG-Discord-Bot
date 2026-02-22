/***********************
 * FDG-14: StatsFact (Normalized)
 * - Creates StatsFact tab with normalized schema (no event columns, no formulas)
 * - Enforces uniqueness of a composite key via script
 ***********************/

const STATSFACT_SHEET_NAME = "StatsFact";

// Normalized “fact” grain:
// Season + EventCode + TournID + Division + PDGA + StatId  => unique row
const STATSFACT_HEADERS = [
  "Season",        // e.g. 2025, 2026
  "EventCode",     // e.g. SFO, PCCO (your internal code)
  "TournID",       // PDGA Live event id used for API calls (string or number)
  "Division",      // MPO / FPO (or other PDGA divisions if ever needed)
  "PdgaNumber",    // numeric id for the player
  "StatId",        // PDGA statId (string/number) - can be blank if unknown
  "StatName",      // optional label for debugging/human read; not required for uniqueness
  "Value",         // numeric where possible; if text, store as text
  "Units",         // optional: %, strokes, feet, etc.
  "Source",        // e.g. "PDGA_LIVE_DIVISION_STATS"
  "PulledAt",      // ISO timestamp of when row was inserted/updated
  "Key"            // composite key string for uniqueness + fast upsert
];

/**
 * Run this once to create/repair the StatsFact sheet schema.
 */
function setupStatsFactSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(STATSFACT_SHEET_NAME) || ss.insertSheet(STATSFACT_SHEET_NAME);

  // Write headers
  sh.getRange(1, 1, 1, STATSFACT_HEADERS.length).setValues([STATSFACT_HEADERS]);
  sh.setFrozenRows(1);

  // Basic formatting (no formulas)
  sh.getRange(1, 1, 1, STATSFACT_HEADERS.length).setFontWeight("bold");

  // Optional: set reasonable column widths
  sh.setColumnWidth(1, 70);  // Season
  sh.setColumnWidth(2, 90);  // EventCode
  sh.setColumnWidth(3, 80);  // TournID
  sh.setColumnWidth(4, 70);  // Division
  sh.setColumnWidth(5, 90);  // PdgaNumber
  sh.setColumnWidth(6, 80);  // StatId
  sh.setColumnWidth(7, 160); // StatName
  sh.setColumnWidth(8, 90);  // Value
  sh.setColumnWidth(9, 70);  // Units
  sh.setColumnWidth(10, 180);// Source
  sh.setColumnWidth(11, 160);// PulledAt
  sh.setColumnWidth(12, 340);// Key

  // Ensure there's no accidental filters/formulas
  sh.getDataRange().clearFormat();
  sh.getRange(1, 1, 1, STATSFACT_HEADERS.length).setFontWeight("bold");

  SpreadsheetApp.flush();
}

/**
 * Builds the composite key used to enforce uniqueness.
 * IMPORTANT: This key defines the “grain” of your StatsFact table.
 */
function buildStatsFactKey_(season, eventCode, tournId, division, pdgaNumber, statId) {
  // normalize to strings to avoid 123 vs "123" mismatches
  const s = String(season || "").trim();
  const e = String(eventCode || "").trim().toUpperCase();
  const t = String(tournId || "").trim();
  const d = String(division || "").trim().toUpperCase();
  const p = String(pdgaNumber || "").trim();
  const st = String(statId || "").trim();

  // If StatId is missing, we STILL form a key (ends with empty statId).
  // FDG-16/15 will decide whether to skip writing these rows.
  return [s, e, t, d, p, st].join("|");
}

/**
 * Enforces uniqueness of StatsFact based on the Key column.
 * - Removes duplicate rows (keeps the *last* occurrence by default)
 * - Safe for 100k+ rows (one read, one write)
 *
 * Run manually as a “repair” tool; FDG-16 will use this concept during upserts.
 */
function enforceStatsFactUniqueness() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(STATSFACT_SHEET_NAME);
  if (!sh) throw new Error("StatsFact sheet not found. Run setupStatsFactSheet() first.");

  const lastRow = sh.getLastRow();
  if (lastRow <= 1) return; // headers only

  const lastCol = STATSFACT_HEADERS.length;
  const values = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();

  const keyColIndex = STATSFACT_HEADERS.indexOf("Key"); // 0-based in our header array
  if (keyColIndex < 0) throw new Error("Key column missing from STATSFACT_HEADERS.");

  // Keep last occurrence: iterate forward, record latest row index
  const keyToRow = new Map();
  for (let i = 0; i < values.length; i++) {
    const key = String(values[i][keyColIndex] || "").trim();
    if (!key) continue;
    keyToRow.set(key, i); // overwrites, so last wins
  }

  // Rebuild the deduped list in original order, but only keeping the last rows
  const keep = [];
  for (let i = 0; i < values.length; i++) {
    const key = String(values[i][keyColIndex] || "").trim();
    if (!key) {
      // Keep rows with blank keys (you may later decide to purge them)
      keep.push(values[i]);
      continue;
    }
    if (keyToRow.get(key) === i) keep.push(values[i]);
  }

  // Only rewrite if something changed
  if (keep.length !== values.length) {
    // Clear old data rows (not headers)
    sh.getRange(2, 1, lastRow - 1, lastCol).clearContent();

    // Write back deduped
    sh.getRange(2, 1, keep.length, lastCol).setValues(keep);

    // Optional: clear any trailing rows formatting/content if sheet had more rows
    const newLast = keep.length + 1;
    if (newLast < lastRow) {
      sh.getRange(newLast + 1, 1, lastRow - newLast, lastCol).clearContent();
    }
  }

  SpreadsheetApp.flush();
}
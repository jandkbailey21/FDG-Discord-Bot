/***********************
 * FDG BACKEND (Apps Script) — CLEANED + UPDATED (TWILIO SMS)
 *
 * Implements 4 SMS alert types via Twilio:
 * 1) FreeAgents: DROP or SWAP drop-leg -> league-wide subscribers
 * 2) WaiverAwards: after WAIVER_RUN -> only teams with awards + subscribed
 * 3) Withdrawals: updateAllEvents diff ✅/WL -> — for owned player -> that team if subscribed
 * 4) LineupReminders: LINEUP_REMINDER_RUN -> all subscribed teams
 *
 * Sheet schemas (MUST MATCH):
 * AlertSubscriptions Columns:
 *   TeamName, PhoneE164, Enabled, FreeAgents, WaiverAwards, Withdrawals, LineupReminders,
 *   CreatedAt, UpdatedAt, LastSmsAt, OptOut
 *
 * SmsLog Columns:
 *   Timestamp, Team, ToPhone, AlertType, Message, Status (SENT/ERROR), ProviderId (Twilio SID), Error
 ***********************/

/***********************
 * 0) GLOBAL CONFIGF
 ***********************/

// Spreadsheet ID (from your URL)
function getSS_() {
  const id = getScriptProp_("SPREADSHEET_ID");
  if (!id) throw new Error("Missing Script Property SPREADSHEET_ID");
  return SpreadsheetApp.openById(id);
}

// Canonical team names (must match DraftBoard + Teams tab values)
const CANON_TEAMS = [
  "Sir Krontzalot",
  "Exalted Evil",
  "Tree Ninja Disc Golf",
  "The Abba Zabba",
  "Ryan Morgan",
  "SPY Dyes",
  "Eddie Speidel",
  "Webb Webb Webb",
  "Hughes Moves",
  "Matthew Lopez",
  "Free Agent",
];

const MAX_ROSTER = 10;
const FREE_AGENT = "Free Agent";

// Sheets
const SHEET_TRANSACTIONS = "Transactions";
const SHEET_DRAFTBOARD = "DraftBoard";
const SHEET_ROSTERS = "Rosters";
const SHEET_WEBHOOKLOG = "WebhookLog";

const ALERTS_SUBS_SHEET = "AlertSubscriptions";
const SMS_LOG_SHEET = "SmsLog";
const LINEUP_LOG_SHEET = "LineupRemindersLog";

// AlertSubscriptions headers (MUST match your sheet)
const ALERTS_SUBS_HEADERS = [
  "TeamName",
  "PhoneE164",
  "Enabled",
  "FreeAgents",
  "WaiverAwards",
  "Withdrawals",
  "LineupReminders",
  "CreatedAt",
  "UpdatedAt",
  "LastSmsAt",
  "OptOut",
];

// SmsLog headers (MUST match your sheet)
const SMS_LOG_HEADERS = [
  "Timestamp",
  "Team",
  "ToPhone",
  "AlertType",
  "Message",
  "Status (SENT/ERROR)",
  "ProviderId (Twilio SID)",
  "Error",
];

// LineupRemindersLog headers
const LINEUP_LOG_HEADERS = [
  "CycleId",
  "EventName",
  "RunAt",
  "CreatedAt",
  "Status",
  "MetaJson",
];

// DGS / PDGA parsing + backfill controls
const DGS_PDGA_PROFILE_NAME_CACHE_HOURS = 12;
const DGS_NAME_BACKFILL_LIMIT = 250;

// Shared fetch headers
const FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
  Accept: "text/html,application/xhtml+xml",
};

// SMS safety controls (tight caps)
const SMS_MAX_SENDS_PER_INVOCATION = 15; // hard cap per single execution/run
const SMS_MAX_SENDS_PER_HOUR = 40;       // across all invocations in the hour
const SMS_MAX_SENDS_PER_DAY = 100;       // across all invocations in the day
const SMS_DEDUPE_MINUTES = 30;           // dedupe identical key for 30 minutes

/***********************
 * 1) CORE HELPERS
 ***********************/

function safeJson_(s, fallbackObj) {
  try {
    return JSON.parse(s);
  } catch (e) {
    return fallbackObj || {};
  }
}

function coerceBool_(v) {
  if (v === true || v === false) return v;
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return false;
  return s === "true" || s === "1" || s === "yes" || s === "y" || s === "on";
}

function newSmsBudget_() {
  return { sent: 0, max: SMS_MAX_SENDS_PER_INVOCATION };
}

function normalizeTeam_(t) {
  const raw = String(t || "").trim();
  if (!raw) return "";

  const key = raw.toUpperCase();

  for (const c of CANON_TEAMS) {
    if (c.toUpperCase() === key) return c;
  }

  const shorthand = {
    SIR: "Sir Krontzalot",
    EXA: "Exalted Evil",
    TRE: "Tree Ninja Disc Golf",
    THE: "The Abba Zabba",
    RYA: "Ryan Morgan",
    SPY: "SPY Dyes",
    EDD: "Eddie Speidel",
    WEB: "Webb Webb Webb",
    HUG: "Hughes Moves",
    MAT: "Matthew Lopez",

    FA: "Free Agent",
    FREE: "Free Agent",
    "FREE AGENT": "Free Agent",
    "FREE AGENTS": "Free Agent",
  };

  return shorthand[key] || "";
}

function jsonResponse_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(
    ContentService.MimeType.JSON
  );
}

function mustGetSheet_(ss, name) {
  const sh = ss.getSheetByName(name);
  if (!sh) throw new Error(`Missing required sheet tab: ${name}`);
  return sh;
}

function mustGetOrCreateSheet_(ss, name, headers) {
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    if (headers && headers.length) sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  } else if (headers && headers.length) {
    const existing = sh
      .getRange(1, 1, 1, Math.max(headers.length, sh.getLastColumn() || 1))
      .getValues()[0];
    const joined = existing.map((x) => String(x || "").trim()).join("|");
    if (!joined.replace(/\|/g, "").trim()) {
      sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
  }
  return sh;
}

function getHeaderIndexMap_(sh) {
  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  const map = {};
  headers.forEach((h, idx) => {
    const key = String(h || "").trim();
    if (key) map[key] = idx; // 0-based
  });
  return map;
}

function appendRows_(sh, rows) {
  if (!rows || !rows.length) return;
  const startRow = sh.getLastRow() + 1;
  sh.getRange(startRow, 1, rows.length, rows[0].length).setValues(rows);
}

function findColumnIndexByHeader_(headers, candidates, fallback1Based) {
  for (const c of candidates) {
    const idx0 = headers.indexOf(c);
    if (idx0 >= 0) return idx0 + 1;
  }
  return fallback1Based;
}

function sanitizeDgsName_(rawName, pdga) {
  let name = String(rawName || "").trim();
  const p = String(pdga || "").trim();
  if (!name) return "";

  if (p) {
    const re = new RegExp("(?:\\s*#\\s*" + p + "|\\s+" + p + ")\\s*$");
    name = name.replace(re, "").trim();
  }
  name = name.replace(/\s*#\s*\d+\s*$/, "").trim();

  return name;
}

function getScriptProp_(key) {
  return PropertiesService.getScriptProperties().getProperty(key) || "";
}

// stable hash for dedupe (SHA-256 hex)
function sha256Hex_(s) {
  const bytes = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    s,
    Utilities.Charset.UTF_8
  );
  return bytes.map((b) => ("0" + ((b & 0xff) >>> 0).toString(16)).slice(-2)).join("");
}

/**
 * TEST SET: Sends 4 SMS alerts (FreeAgents, WaiverAwards, Withdrawals, LineupReminders)
 * to the ONLY registered device in AlertSubscriptions.
 *
 * Non-destructive: does NOT touch Transactions/MPO/FPO/Rosters.
 * It only writes SmsLog + updates LastSmsAt (like real sends).
 */
function sendAlertTestSet_ToOnlyRegisteredDevice() {
  const ss = getSS_();
  const smsBudget = newSmsBudget_();

  // Load current subscriptions (must be exactly one row for this test as requested)
  const subs = loadAlertSubscriptions_(ss);
  if (!subs.length) throw new Error("No rows found in AlertSubscriptions.");
  if (subs.length !== 1) {
    throw new Error(
      `Expected exactly 1 AlertSubscriptions row for this test, found ${subs.length}. ` +
        `Disable/delete others or adjust this function to target one team explicitly.`
    );
  }

  const sub = subs[0]; // the only registered device
  const team = normalizeTeam_(sub.team);
  const phone = sub.phone;

  // Ensure the system would actually send (matches current gating logic)
  if (!sub.enabled || sub.optOut) {
    throw new Error(`Subscription is not sendable (enabled=${sub.enabled}, optOut=${sub.optOut}).`);
  }

  // Unique identifiers to avoid dedupe suppression (30-min window)
  const now = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  const cycleId =
    "TEST-" +
    now.getFullYear() +
    pad(now.getMonth() + 1) +
    pad(now.getDate()) +
    "-" +
    pad(now.getHours()) +
    pad(now.getMinutes()) +
    pad(now.getSeconds());

  const testEventName = "FDG Alert Test";
  const testEventHeader = "FDG Alert Test Event Column";

  // Make a “unique enough” PDGA string each run
  const testPdga = "9" + String(now.getTime()).slice(-8);

  const results = [];

  // 1) FreeAgents (league-wide subscribers, but you have only one subscriber row)
  if (sub.freeAgents) {
    sendFreeAgentDropAlerts_(ss, {
    droppedByTeam: "Sir Krontzalot",
    playerName: `Test Drop Player (${cycleId})`,
    playerPdga: testPdga,
    budget: smsBudget,
  });
    results.push({ alertType: "FreeAgents", to: phone, team, ok: true });
  } else {
    results.push({ alertType: "FreeAgents", to: phone, team, ok: false, reason: "FreeAgents flag is FALSE" });
  }

  // 2) WaiverAwards (only teams with awards + subscribed)
  if (sub.waiverAwards) {
    const awardsByTeam = new Map();
    awardsByTeam.set(team, [
      { pdga: testPdga, name: `Test Award Player (${cycleId})`, rank: 1 },
    ]);

    sendWaiverAwardAlerts_(ss, {
    cycleId,
    eventName: testEventName,
    awardsByTeam,
    budget: smsBudget,
  });

    results.push({ alertType: "WaiverAwards", to: phone, team, ok: true });
  } else {
    results.push({ alertType: "WaiverAwards", to: phone, team, ok: false, reason: "WaiverAwards flag is FALSE" });
  }

  // 3) Withdrawals (owned player dropped from event)
  if (sub.withdrawals) {
    sendWithdrawalAlert_(ss, {
    team,
    toPhone: phone,
    playerName: `Test Withdrawal Player (${cycleId})`,
    playerPdga: testPdga,
    eventHeader: testEventHeader,
    budget: smsBudget,
  });

    results.push({ alertType: "Withdrawals", to: phone, team, ok: true });
  } else {
    results.push({ alertType: "Withdrawals", to: phone, team, ok: false, reason: "Withdrawals flag is FALSE" });
  }

  // 4) LineupReminders (tomorrow reminder)
  if (sub.lineupReminders) {
    sendLineupReminderAlerts_(ss, {
    cycleId,
    eventName: testEventName,
    budget: smsBudget,
  });

    results.push({ alertType: "LineupReminders", to: phone, team, ok: true });
  } else {
    results.push({ alertType: "LineupReminders", to: phone, team, ok: false, reason: "LineupReminders flag is FALSE" });
  }

  Logger.log("Alert Test Set complete: " + JSON.stringify(results));
  return { ok: true, cycleId, team, phone, results };
}

/***********************
 * 2) ALERT SUBSCRIPTIONS + SMS (Twilio)
 ***********************/

function loadAlertSubscriptions_(ss) {
  const sh = mustGetOrCreateSheet_(ss, ALERTS_SUBS_SHEET, ALERTS_SUBS_HEADERS);
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];

  const width = sh.getLastColumn();
  const vals = sh.getRange(2, 1, lastRow - 1, width).getValues();

  // Expect exact columns per schema:
  // 0 TeamName, 1 PhoneE164, 2 Enabled, 3 FreeAgents, 4 WaiverAwards, 5 Withdrawals, 6 LineupReminders,
  // 7 CreatedAt, 8 UpdatedAt, 9 LastSmsAt, 10 OptOut
  return vals
    .map((r) => {
      const team = normalizeTeam_(String(r[0] || "").trim());
      const phone = String(r[1] || "").trim().replace(/^'+/, "");
      return {
        team,
        phone,
        enabled: coerceBool_(r[2]),
        freeAgents: coerceBool_(r[3]),
        waiverAwards: coerceBool_(r[4]),
        withdrawals: coerceBool_(r[5]),
        lineupReminders: coerceBool_(r[6]),
        optOut: coerceBool_(r[10]),
      };
    })
    .filter((x) => x.team && x.phone);
}

function upsertAlertSubscription_(ss, { team, phoneE164, enabled, freeAgents, waiverAwards, withdrawals, lineupReminders }) {
  const sh = mustGetOrCreateSheet_(ss, ALERTS_SUBS_SHEET, ALERTS_SUBS_HEADERS);

  const t = normalizeTeam_(team);
  let phone = String(phoneE164 || "").trim().replace(/^'+/, "");

  // normalize: if user/bot sends digits only, prepend +
  if (phone && !phone.startsWith("+")) phone = "+" + phone.replace(/[^\d]/g, "");

  // validate E.164
  if (!/^\+\d{10,15}$/.test(phone)) throw new Error("Phone must be E.164 like +12345678900");

  const lastRow = sh.getLastRow();
  const now = new Date();

  // Find existing row by TeamName (column A)
  let rowIndex = -1;
  if (lastRow >= 2) {
    const teams = sh
      .getRange(2, 1, lastRow - 1, 1)
      .getValues()
      .map((r) => normalizeTeam_(String(r[0] || "").trim()));
    rowIndex = teams.indexOf(t);
    if (rowIndex >= 0) rowIndex = rowIndex + 2; // sheet row number
  }

  // Schema (11 cols):
  // A TeamName
  // B PhoneE164
  // C Enabled
  // D FreeAgents
  // E WaiverAwards
  // F Withdrawals
  // G LineupReminders
  // H CreatedAt
  // I UpdatedAt
  // J LastSmsAt
  // K OptOut

  if (rowIndex === -1) {
    // New row
    const row = [
      t,
      "'" + phone, // keep as text
      enabled ? "TRUE" : "FALSE",
      freeAgents ? "TRUE" : "FALSE",
      waiverAwards ? "TRUE" : "FALSE",
      withdrawals ? "TRUE" : "FALSE",
      lineupReminders ? "TRUE" : "FALSE",
      now, // CreatedAt
      now, // UpdatedAt
      "",  // LastSmsAt
      "FALSE", // OptOut
    ];
    sh.appendRow(row);
    return { ok: true, created: true };
  }

  // Existing row: preserve CreatedAt / LastSmsAt / OptOut
  const createdAt = sh.getRange(rowIndex, 8).getValue();
  const lastSmsAt = sh.getRange(rowIndex, 10).getValue();
  const optOutRaw = sh.getRange(rowIndex, 11).getValue();
  const optOut = coerceBool_(optOutRaw);

  // ✅ Compliance: if OptOut=TRUE, force Enabled=FALSE (ignore requested enabled=true)
  const effectiveEnabled = optOut ? false : !!enabled;

  const row = [
    t,
    "'" + phone, // keep as text
    effectiveEnabled ? "TRUE" : "FALSE",
    freeAgents ? "TRUE" : "FALSE",
    waiverAwards ? "TRUE" : "FALSE",
    withdrawals ? "TRUE" : "FALSE",
    lineupReminders ? "TRUE" : "FALSE",
    createdAt || now, // CreatedAt (preserve)
    now,              // UpdatedAt
    lastSmsAt || "",  // LastSmsAt (preserve)
    optOut ? "TRUE" : "FALSE", // OptOut (preserve, normalize)
  ];

  sh.getRange(rowIndex, 1, 1, row.length).setValues([row]);
  return { ok: true, created: false, optedOut: optOut, enabledApplied: effectiveEnabled };
}

function getSmsLogSheet_(ss) {
  return mustGetOrCreateSheet_(ss, SMS_LOG_SHEET, SMS_LOG_HEADERS);
}

function appendSmsLog_(ss, { team, toPhone, alertType, message, status, sid, error }) {
  const sh = getSmsLogSheet_(ss);
  sh.appendRow([
    new Date(),
    team || "",
    toPhone || "",
    alertType || "",
    message || "",
    status || "ERROR",
    sid || "",
    error || "",
  ]);
}

/**
 * Update LastSmsAt for a given team on successful sends.
 */
function touchLastSmsAt_(ss, team) {
  const sh = mustGetOrCreateSheet_(ss, ALERTS_SUBS_SHEET, ALERTS_SUBS_HEADERS);
  const t = normalizeTeam_(team);
  if (!t) return;

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return;

  const teams = sh.getRange(2, 1, lastRow - 1, 1).getValues().map(r => normalizeTeam_(String(r[0] || "").trim()));
  const idx = teams.indexOf(t);
  if (idx < 0) return;

  const rowIndex = idx + 2;
  sh.getRange(rowIndex, 10).setValue(new Date()); // LastSmsAt col J (10)
}

/**
 * Sends a single SMS via Twilio. NO RETRIES.
 * Returns {ok:true, sid:"..."} or {ok:false, error:"..."}.
 */
function twilioSendSmsOnce_(toE164, body) {
  toE164 = String(toE164 || "").trim().replace(/^'+/, "");

  const sid = getScriptProp_("TWILIO_ACCOUNT_SID");
  const token = getScriptProp_("TWILIO_AUTH_TOKEN");
  const from = getScriptProp_("TWILIO_FROM_NUMBER");
  const mg = getScriptProp_("TWILIO_MESSAGING_SERVICE_SID"); // NEW

  if (!sid || !token) {
    return { ok: false, error: "Missing TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN" };
  }
  if (!mg && !from) {
    return { ok: false, error: "Missing TWILIO_MESSAGING_SERVICE_SID or TWILIO_FROM_NUMBER" };
  }

  const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(sid)}/Messages.json`;

  const payload = {
    To: toE164,
    Body: body,
  };

  if (mg) {
    payload.MessagingServiceSid = mg; // preferred for A2P
  } else {
    payload.From = from; // fallback
  }

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    muteHttpExceptions: true,
    payload,
    headers: { Authorization: "Basic " + Utilities.base64Encode(sid + ":" + token) },
  });

  const code = resp.getResponseCode();
  const text = resp.getContentText() || "";

  if (code >= 200 && code < 300) {
    try {
      const json = JSON.parse(text);
      return { ok: true, sid: json.sid || "", status: json.status || "accepted" };
    } catch (e) {
      return { ok: true, sid: "", status: "accepted" };
    }
  }

  return { ok: false, error: `Twilio error (${code}): ${text}` };
}

/**
 * Cache-based dedupe (prevents spam without requiring an AlertId column in sheet).
 */
function smsDedupeHit_(dedupeKey, minutes) {
  const cache = CacheService.getScriptCache();
  const key = "sms:" + sha256Hex_(dedupeKey).slice(0, 40);
  const hit = cache.get(key);
  if (hit) return true;
  cache.put(key, "1", Math.max(60, minutes * 60));
  return false;
}

/**
 * Core dispatcher with dedupe + SmsLog.
 * Returns {ok:true, sent:true/false, reason?}
 */
function sendSmsAlert_(ss, { team, toPhone, alertType, message, dedupeKey, budget }) {
  // 0) Kill switch (Script Property)
  if (String(getScriptProp_("SMS_ENABLED")).toUpperCase() !== "TRUE") {
    appendSmsLog_(ss, {
      team,
      toPhone,
      alertType,
      message,
      status: "SKIPPED",
      sid: "",
      error: "SMS_DISABLED",
    });
    return { ok: true, sent: false, reason: "disabled" };
  }

  // 1) Per-invocation cap (true "this run" limit)
  if (budget && budget.sent >= budget.max) {
    appendSmsLog_(ss, {
      team,
      toPhone,
      alertType,
      message,
      status: "ERROR",
      sid: "",
      error: `INVOCATION_CAP_REACHED_${budget.max}`,
    });
    return { ok: false, sent: false, reason: "invocation_cap" };
  }

  // 2) Dedupe (prevents repeats for 30 minutes)
  if (dedupeKey && smsDedupeHit_(dedupeKey, SMS_DEDUPE_MINUTES)) {
    appendSmsLog_(ss, {
      team,
      toPhone,
      alertType,
      message,
      status: "SKIPPED",
      sid: "",
      error: "DUPLICATE_SUPPRESSED",
    });
    return { ok: true, sent: false, reason: "duplicate" };
  }

  // 3) Hour/day caps across all invocations
  const runCache = CacheService.getScriptCache();
  const tz = Session.getScriptTimeZone();

  const hourBucket = Utilities.formatDate(new Date(), tz, "yyyyMMddHH");
  const hourKey = `sms_hour_count_${hourBucket}`;
  const hourCur = Number(runCache.get(hourKey) || "0");
  if (hourCur >= SMS_MAX_SENDS_PER_HOUR) {
    appendSmsLog_(ss, {
      team,
      toPhone,
      alertType,
      message,
      status: "ERROR",
      sid: "",
      error: `HOUR_CAP_REACHED_${SMS_MAX_SENDS_PER_HOUR}`,
    });
    return { ok: false, sent: false, reason: "hour_cap" };
  }

  const dayBucket = Utilities.formatDate(new Date(), tz, "yyyyMMdd");
  const dayKey = `sms_day_count_${dayBucket}`;
  const dayCur = Number(runCache.get(dayKey) || "0");
  if (dayCur >= SMS_MAX_SENDS_PER_DAY) {
    appendSmsLog_(ss, {
      team,
      toPhone,
      alertType,
      message,
      status: "ERROR",
      sid: "",
      error: `DAY_CAP_REACHED_${SMS_MAX_SENDS_PER_DAY}`,
    });
    return { ok: false, sent: false, reason: "day_cap" };
  }

  // 4) Send once via Twilio
  const res = twilioSendSmsOnce_(toPhone, message);

  if (res.ok) {
    appendSmsLog_(ss, {
      team,
      toPhone,
      alertType,
      message,
      status: "SENT",
      sid: res.sid || "",
      error: "",
    });

    // increment invocation budget
    if (budget) budget.sent++;

    // increment hour/day counters (cache TTL just needs to be "long enough")
    runCache.put(hourKey, String(hourCur + 1), 60 * 60);       // 1 hour
    runCache.put(dayKey, String(dayCur + 1), 60 * 60 * 24);    // 24 hours

    // update team last-sent timestamp
    touchLastSmsAt_(ss, team);

    return { ok: true, sent: true, sid: res.sid || "" };
  }

  appendSmsLog_(ss, {
    team,
    toPhone,
    alertType,
    message,
    status: "ERROR",
    sid: "",
    error: res.error || "UNKNOWN_ERROR",
  });
  return { ok: false, sent: false, error: res.error || "UNKNOWN_ERROR" };
}

/***********************
 * 2.5) SMS TRIGGER HELPERS (the 4 alert types)
 ***********************/

function sendFreeAgentDropAlerts_(ss, { droppedByTeam, playerName, playerPdga, budget }) {
  const subs = loadAlertSubscriptions_(ss);
  const droppingTeam = normalizeTeam_(droppedByTeam);

  for (const s of subs) {
    if (!s.enabled || s.optOut || !s.freeAgents) continue;

    const msg = `FDG: ${droppingTeam} has dropped ${playerName} to Free Agents.`;
    const dedupeKey = `FREEAGENT|${s.team}|${playerPdga}|${droppingTeam}|${msg}`;

    sendSmsAlert_(ss, {
      alertType: "FreeAgents",
      team: s.team,
      toPhone: s.phone,
      message: msg,
      dedupeKey,
      budget,
    });
  }
}

function sendWithdrawalAlert_(ss, { team, toPhone, playerName, playerPdga, eventHeader, budget }) {
  const t = normalizeTeam_(team);
  const msg = `FDG: ${playerName} has dropped from ${eventHeader}.`;
  const dedupeKey = `WITHDRAWAL|${t}|${playerPdga}|${eventHeader}`;

  return sendSmsAlert_(ss, {
    alertType: "Withdrawals",
    team: t,
    toPhone,
    message: msg,
    dedupeKey,
    budget,
  });
}

function parseFormBody_(raw) {
  const out = {};
  const pairs = String(raw || "").split("&");
  for (const p of pairs) {
    if (!p) continue;
    const [k, v] = p.split("=");
    const key = decodeURIComponent(k || "").trim();
    const val = decodeURIComponent((v || "").replace(/\+/g, " ")).trim();
    if (key) out[key] = val;
  }
  return out;
}

function setOptOutByPhone_(ss, phoneE164, optOutBool) {
  const sh = mustGetOrCreateSheet_(ss, ALERTS_SUBS_SHEET, ALERTS_SUBS_HEADERS);
  const phone = String(phoneE164 || "").trim();

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return { ok: false, error: "No subscriptions" };

  const vals = sh.getRange(2, 1, lastRow - 1, 2).getValues(); // TeamName, PhoneE164
  for (let i = 0; i < vals.length; i++) {
    const rowPhone = String(vals[i][1] || "").trim().replace(/^'+/, "");
    if (rowPhone === phone) {
      const rowIndex = i + 2;
      sh.getRange(rowIndex, 11).setValue(optOutBool ? "TRUE" : "FALSE"); // OptOut col K
      sh.getRange(rowIndex, 9).setValue(new Date()); // UpdatedAt col I
      if (optOutBool) sh.getRange(rowIndex, 3).setValue("FALSE"); // Enabled col C (optional but recommended)
      return { ok: true, matchedTeam: String(vals[i][0] || "") };
    }
  }
  return { ok: false, error: "Phone not found" };
}

function twimlResponse_(message) {
  const xml =
    `<?xml version="1.0" encoding="UTF-8"?>` +
    `<Response>` +
    (message ? `<Message>${message.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}</Message>` : "") +
    `</Response>`;
  return ContentService.createTextOutput(xml).setMimeType(ContentService.MimeType.XML);
}

function sendWaiverAwardAlerts_(ss, { cycleId, eventName, awardsByTeam, budget }) {
  const subs = loadAlertSubscriptions_(ss);

  for (const s of subs) {
    if (!s.enabled || s.optOut || !s.waiverAwards) continue;

    const awards = awardsByTeam.get(s.team) || [];
    if (!awards.length) continue;

    const list = awards.slice(0, 6).map((a) => a.name).join(", ");
    const more = awards.length > 6 ? ` (+${awards.length - 6} more)` : "";

    const msg = `FDG Waivers: You have been awarded player(s) in the Waiver channel for ${eventName}: ${list}${more}.`;
    const dedupeKey = `WAIVER|${s.team}|${cycleId}|${eventName}|${awards.map(a => a.pdga).join(",")}`;

    sendSmsAlert_(ss, {
      alertType: "WaiverAwards",
      team: s.team,
      toPhone: s.phone,
      message: msg,
      dedupeKey,
      budget,
    });
  }
}

function sendLineupReminderAlerts_(ss, { cycleId, eventName, budget }) {
  const subs = loadAlertSubscriptions_(ss);

  for (const s of subs) {
    if (!s.enabled || s.optOut || !s.lineupReminders) continue;

    const msg = `FDG: ${eventName} starts tomorrow. Don't forget to set your lineup in Hyzerbase!`;
    const dedupeKey = `LINEUP|${s.team}|${cycleId}|${eventName}`;

    sendSmsAlert_(ss, {
      alertType: "LineupReminders",
      team: s.team,
      toPhone: s.phone,
      message: msg,
      dedupeKey,
      budget,
    });
  }
}

/***********************
 * 3) DGS / PDGA FETCH + PARSE
 ***********************/

/**
 * Fetches DiscGolfScene registration page and returns a Map:
 *   Map<pdgaString, "REGISTERED" | "WL #X">
 */
function fetchDgsRegistrationStatusByPdga_(url) {
  const resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: FETCH_HEADERS,
  });

  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error("Fetch failed (" + code + ") for URL: " + url);
  }

  const html = resp.getContentText();
  const chunks = html.split(/<tr\b/i);

  const out = new Map();
  const pdgaRe = /pdga\.com\/player\/(\d+)/i;

  const wlSpanRe =
    /<span\b[^>]*class=(["'])[^"']*\bwaitlist\b[^"']*\1[^>]*>\s*([^<]*?)\s*<\/span>/i;
  const wlTextRe = /\bWL\s*#\s*(\d+)\b/i;

  for (const rowHtml of chunks) {
    if (rowHtml.indexOf("pdga.com/player/") === -1) continue;

    const m = rowHtml.match(pdgaRe);
    if (!m) continue;

    const pdga = String(m[1] || "").trim();
    if (!pdga) continue;

    let status = "REGISTERED";

    const spanMatch = rowHtml.match(wlSpanRe);
    if (spanMatch && spanMatch[2]) {
      const raw = String(spanMatch[2])
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ")
        .trim();
      const wlMatch = raw.match(wlTextRe);
      if (wlMatch && wlMatch[1]) status = `WL #${String(wlMatch[1]).trim()}`;
    } else {
      const wlMatch2 = rowHtml.match(wlTextRe);
      if (wlMatch2 && wlMatch2[1]) status = `WL #${String(wlMatch2[1]).trim()}`;
    }

    const existing = out.get(pdga);
    if (!existing) out.set(pdga, status);
    else {
      const existingIsWl = /^WL\s*#\s*\d+$/i.test(existing);
      const newIsWl = /^WL\s*#\s*\d+$/i.test(status);
      if (!existingIsWl && newIsWl) out.set(pdga, status);
    }
  }

  return out;
}

/**
 * PDGA name lookup with CacheService.
 */
function fetchPdgaNameFromProfile_(pdga) {
  const p = String(pdga || "").trim();
  if (!p) return "";

  const cache = CacheService.getScriptCache();
  const cacheKey = `PDGA_NAME_${p}`;
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  const url = "https://www.pdga.com/player/" + encodeURIComponent(p);
  const resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: FETCH_HEADERS,
  });

  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) return "";

  const html = resp.getContentText();

  const title = html.match(/<title>\s*([^<]+?)\s*\|/i);
  if (title && title[1]) {
    const name = String(title[1]).trim();
    if (name && !/^\d+$/.test(name)) {
      cache.put(cacheKey, name, DGS_PDGA_PROFILE_NAME_CACHE_HOURS * 3600);
      return name;
    }
  }

  const og = html.match(/property=["']og:title["']\s+content=["']([^"']+)["']/i);
  if (og && og[1]) {
    const name = String(og[1]).trim();
    if (name && !/^\d+$/.test(name)) {
      cache.put(cacheKey, name, DGS_PDGA_PROFILE_NAME_CACHE_HOURS * 3600);
      return name;
    }
  }

  return "";
}

function guessDivisionFromPdgaProfile_(pdga) {
  const url = "https://www.pdga.com/player/" + encodeURIComponent(String(pdga || "").trim());
  const resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: FETCH_HEADERS,
  });

  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) return "";

  const html = resp.getContentText();
  const m = html.match(/\b(MPO|FPO)\b/i);
  return m ? String(m[1]).toUpperCase() : "";
}

/**
 * Returns Map<pdga, { pdga, name, division }>
 */
function fetchDgsRegisteredPlayers_(url) {
  const resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: FETCH_HEADERS,
  });

  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error("Fetch failed (" + code + ") for URL: " + url);
  }

  const html = resp.getContentText();
  const chunks = html.split(/<tr\b/i);

  const out = new Map();

  const aRe =
    /<a\b[^>]*href=(["'])([^"']*pdga\.com\/player\/(\d+)[^"']*)\1[^>]*>([\s\S]*?)<\/a>/i;

  let currentSectionDiv = "";

  function sniffSectionDiv_(chunk) {
    const headerish = chunk.match(
      /<(h1|h2|h3|h4|h5|h6|th|strong)\b[^>]*>[\s\S]*?(MPO|FPO)[\s\S]*?<\/\1>/i
    );
    if (headerish) return String(headerish[2]).toUpperCase();

    const divLabel = chunk.match(/\bDivision\b[\s\S]{0,80}\b(MPO|FPO)\b/i);
    if (divLabel) return String(divLabel[1]).toUpperCase();

    return "";
  }

  for (const rowHtml of chunks) {
    const sectionDiv = sniffSectionDiv_(rowHtml);
    if (sectionDiv) currentSectionDiv = sectionDiv;

    if (rowHtml.indexOf("pdga.com/player/") === -1) continue;

    const m = rowHtml.match(aRe);
    if (!m) continue;

    const pdga = String(m[3] || "").trim();
    let name = String(m[4] || "").trim();

    name = name
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/\s+/g, " ")
      .trim();

    if (name && /^\d+$/.test(name)) name = "";
    name = sanitizeDgsName_(name, pdga);

    const rowDivMatch = rowHtml.match(/\b(MPO|FPO)\b/i);
    const division = rowDivMatch ? String(rowDivMatch[1]).toUpperCase() : currentSectionDiv || "";

    if (pdga && !out.has(pdga)) {
      out.set(pdga, { pdga, name: name || "", division: division || "" });
    }
  }

  for (const [pdga, obj] of out.entries()) {
    if (obj.division) continue;
    try {
      const guess = guessDivisionFromPdgaProfile_(pdga);
      if (guess) out.set(pdga, { ...obj, division: guess });
    } catch (e) {}
  }

  let backfillCount = 0;
  for (const [pdga, obj] of out.entries()) {
    if (obj.name) continue;
    if (backfillCount >= DGS_NAME_BACKFILL_LIMIT) break;

    try {
      const realName = fetchPdgaNameFromProfile_(pdga);
      if (realName) {
        out.set(pdga, { ...obj, name: realName });
        backfillCount++;
      }
    } catch (e) {}
  }

  return out;
}

/***********************
 * 4) DGS -> MPO/FPO SYNC + CHECKMARK UPDATES (+ Withdrawal SMS)
 ***********************/

function syncNewPlayersFromRegistrationUrl_(ss, registrationUrl) {
  const mpoSh = mustGetSheet_(ss, "MPO");
  const fpoSh = mustGetSheet_(ss, "FPO");

  function ownershipIsArrayFormula_(sh) {
    try {
      const f = sh.getRange("B2").getFormula();
      return !!(f && f.toUpperCase().indexOf("ARRAYFORMULA") >= 0);
    } catch (e) {
      return false;
    }
  }
  const mpoBIsArray = ownershipIsArrayFormula_(mpoSh);
  const fpoBIsArray = ownershipIsArrayFormula_(fpoSh);

  function buildPdgaSet_(sh) {
    const lastRow = sh.getLastRow();
    if (lastRow < 2) return new Set();

    const headers = sh
      .getRange(1, 1, 1, sh.getLastColumn())
      .getValues()[0]
      .map((h) => String(h).trim());
    const pdgaColIndex = findColumnIndexByHeader_(headers, ["PDGA #", "Player PDGA #", "PDGA", "PDGA#"], 3);

    const pdgas = sh
      .getRange(2, pdgaColIndex, lastRow - 1, 1)
      .getValues()
      .map((r) => String(r[0]).trim());
    const set = new Set();
    for (const p of pdgas) if (p) set.add(p);
    return set;
  }

  const mpoSet = buildPdgaSet_(mpoSh);
  const fpoSet = buildPdgaSet_(fpoSh);

  const players = fetchDgsRegisteredPlayers_(registrationUrl);

  const toAppendMpo = [];
  const toAppendFpo = [];

  for (const { pdga, name, division } of players.values()) {
    const p = String(pdga || "").trim();
    if (!p) continue;

    if (mpoSet.has(p) || fpoSet.has(p)) continue;

    let finalName = String(name || "").trim();
    if (!finalName) {
      try {
        finalName = fetchPdgaNameFromProfile_(p);
      } catch (e) {}
    }
    if (!finalName) continue;

    const div = String(division || "").toUpperCase();
    const targetIsFpo = div === "FPO";

    const sh = targetIsFpo ? fpoSh : mpoSh;
    const width = sh.getLastColumn();
    const row = new Array(width).fill("");

    row[0] = sanitizeDgsName_(finalName, p);

    const shouldWriteOwnership = targetIsFpo ? !fpoBIsArray : !mpoBIsArray;
    row[1] = shouldWriteOwnership ? FREE_AGENT : "";

    row[2] = p;

    if (targetIsFpo) {
      toAppendFpo.push(row);
      fpoSet.add(p);
    } else {
      toAppendMpo.push(row);
      mpoSet.add(p);
    }
  }

  if (toAppendMpo.length) appendRows_(mpoSh, toAppendMpo);
  if (toAppendFpo.length) appendRows_(fpoSh, toAppendFpo);
}

/**
 * Updates an event column and sends withdrawal SMS if someone flips from ✅/WL -> —
 */
function updateRegistrationColumnByPdgaFromStatusMap_(ss, sheetName, headerName, statusByPdga) {
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) throw new Error("Sheet not found: " + sheetName);

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;

  const lastCol = sheet.getLastColumn();
  const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map((h) => String(h).trim());

  const eventColIndex = headers.indexOf(String(headerName).trim()) + 1;
  if (eventColIndex <= 0) throw new Error(`Header "${headerName}" not found in ${sheetName}`);

  const pdgaColIndex = findColumnIndexByHeader_(headers, ["PDGA #", "Player PDGA #", "PDGA", "PDGA#"], 3);

  const pdgaIds = sheet
    .getRange(2, pdgaColIndex, lastRow - 1, 1)
    .getValues()
    .map((r) => String(r[0]).trim());

  const before = sheet
    .getRange(2, eventColIndex, lastRow - 1, 1)
    .getValues()
    .map((r) => String(r[0] || "").trim());

  const after = pdgaIds.map((id) => {
    if (!id) return "—";
    const status = statusByPdga.get(id);
    if (!status) return "—";
    if (status === "REGISTERED") return "✅";
    return String(status); // "WL #X"
  });

  sheet.getRange(2, eventColIndex, after.length, 1).setValues(after.map((x) => [x]));

  // Withdrawal detection
  const { ownerByPdga } = getOwnershipStateFromRosters_(ss);
  const subs = loadAlertSubscriptions_(ss);
  const subByTeam = new Map(subs.map((s) => [normalizeTeam_(s.team), s]));
  const smsBudget = newSmsBudget_();
  
  for (let i = 0; i < pdgaIds.length; i++) {
    const pdga = pdgaIds[i];
    if (!pdga) continue;

    const was = before[i];
    const now = after[i];

    const wasRegistered = was === "✅" || /^WL\s*#\s*\d+$/i.test(was);
    const nowNotRegistered = now === "—";

    if (!wasRegistered || !nowNotRegistered) continue;

    const ownerTeam = normalizeTeam_(ownerByPdga.get(pdga) || "");
    if (!ownerTeam || ownerTeam === FREE_AGENT) continue;

    const sub = subByTeam.get(ownerTeam);
    if (!sub || !sub.enabled || sub.optOut || !sub.withdrawals) continue;

    const nameCell = sheet.getRange(2 + i, 1).getValue(); // assume col A is name
    const playerName = String(nameCell || "").trim() || `PDGA ${pdga}`;

    sendWithdrawalAlert_(ss, {
      team: ownerTeam,
      toPhone: sub.phone,
      playerName,
      playerPdga: pdga,
      eventHeader: headerName,
      budget: smsBudget,
    });
  }
}

function cleanupTrailingPdgaInMpoFpoNames_(ss) {
  ss = ss || getSS_();

  function cleanSheet_(sheetName) {
    const sh = mustGetSheet_(ss, sheetName);
    const lastRow = sh.getLastRow();
    if (lastRow < 2) return;

    const lastCol = sh.getLastColumn();
    const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map((h) => String(h).trim());

    const nameCol = 1; // A
    const pdgaCol = findColumnIndexByHeader_(headers, ["PDGA #", "Player PDGA #", "PDGA", "PDGA#"], 3);

    const names = sh.getRange(2, nameCol, lastRow - 1, 1).getValues();
    const pdgas = sh.getRange(2, pdgaCol, lastRow - 1, 1).getValues();

    let changed = false;
    for (let i = 0; i < names.length; i++) {
      const rawName = String(names[i][0] || "").trim();
      const pdga = String(pdgas[i][0] || "").trim();
      if (!rawName || !pdga) continue;

      const cleaned = sanitizeDgsName_(rawName, pdga);
      if (cleaned && cleaned !== rawName) {
        names[i][0] = cleaned;
        changed = true;
      }
    }

    if (changed) sh.getRange(2, nameCol, names.length, 1).setValues(names);
  }

  cleanSheet_("MPO");
  cleanSheet_("FPO");
}

/**
 * Updates ALL events listed in Config tab for BOTH MPO and FPO sheets.
 * Config format: A=Event Header, B=Registration URL
 */
function updateAllEvents() {
  const ss = getSS_();
  const cfg = ss.getSheetByName("Config");
  if (!cfg) throw new Error('Config sheet not found. Create a sheet named "Config".');

  const values = cfg.getDataRange().getValues();
  if (values.length < 2) return;

  const results = [];

  for (let i = 1; i < values.length; i++) {
    const header = String(values[i][0] || "").trim();
    const url = String(values[i][1] || "").trim();
    if (!header || !url) continue;

    try {
      syncNewPlayersFromRegistrationUrl_(ss, url);
      const statusByPdga = fetchDgsRegistrationStatusByPdga_(url);

      updateRegistrationColumnByPdgaFromStatusMap_(ss, "MPO", header, statusByPdga);
      updateRegistrationColumnByPdgaFromStatusMap_(ss, "FPO", header, statusByPdga);

      results.push([new Date(), header, "OK"]);
    } catch (e) {
      results.push([new Date(), header, "FAILED: " + (e && e.message ? e.message : e)]);
    }
  }

  cleanupTrailingPdgaInMpoFpoNames_(ss);

  try {
    updateAllStatMandoRanks();
  } catch (e) {
    results.push([new Date(), "StatMando Ranks", "FAILED: " + (e && e.message ? e.message : e)]);
  }

  try {
    sortMpoFpoByRanks();
  } catch (e) {
    results.push([new Date(), "Sort MPO/FPO", "FAILED: " + (e && e.message ? e.message : e)]);
  }

  cfg.getRange(1, 4).setValue("Last Run Time");
  cfg.getRange(1, 5).setValue("Event Header");
  cfg.getRange(1, 6).setValue("Result");

  if (results.length) {
    cfg.getRange(2, 4, results.length, 3).clearContent();
    cfg.getRange(2, 4, results.length, 3).setValues(results);
  }
}

/***********************
 * 5) StatMando
 ***********************/

function updateAllStatMandoRanks() {
  updateStatMandoRanksForDivision_("MPO", "https://statmando.com/rankings/official/mpo");
  updateStatMandoRanksForDivision_("FPO", "https://statmando.com/rankings/official/fpo");
}

function updateStatMandoRanksForDivision_(sheetName, baseUrl) {
  const ss = getSS_();
  const sh = ss.getSheetByName(sheetName);
  if (!sh) throw new Error(`Sheet not found: ${sheetName}`);

  const nameToRank = fetchStatMandoNameToRankMap_(baseUrl);

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return;

  const names = sh.getRange(2, 1, lastRow - 1, 1).getValues().map((r) => (r[0] || "").toString().trim());
  const currentRanks = sh.getRange(2, 4, lastRow - 1, 1).getValues().map((r) => (r[0] || "").toString().trim());

  let changes = 0;
  const newRanks = names.map((nm, i) => {
    if (!nm) return [currentRanks[i] || ""];
    const rank = nameToRank.get(normalizePlayerName_(nm));
    if (!rank) return [currentRanks[i] || ""];
    const rankNum = Number(rank);
    if (!isFinite(rankNum)) return [currentRanks[i] || ""];
    if (String(rankNum) !== (currentRanks[i] || "")) changes++;
    return [rankNum];
  });

  if (changes > 0) sh.getRange(2, 4, newRanks.length, 1).setValues(newRanks);
}

function fetchStatMandoNameToRankMap_(baseUrl) {
  const map = new Map();
  const MAX_PAGES = 30;

  for (let page = 1; page <= MAX_PAGES; page++) {
    const url = page === 1 ? baseUrl : `${baseUrl}?page=${page}`;
    const html = fetchHtml_(url);

    const rows = parseStatMandoRows_(html);
    if (rows.length === 0) break;

    let addedThisPage = 0;
    for (const r of rows) {
      const key = normalizePlayerName_(r.name);
      if (!map.has(key)) {
        map.set(key, r.rank);
        addedThisPage++;
      }
    }

    if (addedThisPage === 0) break;
  }

  return map;
}

function parseStatMandoRows_(html) {
  const out = [];
  const trRe = /<tr[\s\S]*?<\/tr>/gi;
  const tdRe = /<td[\s\S]*?<\/td>/gi;

  const trMatches = html.match(trRe) || [];
  for (const tr of trMatches) {
    const tds = tr.match(tdRe) || [];
    if (tds.length < 3) continue;

    const rankText = stripHtml_(tds[0]).trim();
    if (!/^\d+$/.test(rankText)) continue;

    const name = extractAnchorText_(tds[2]).trim();
    if (!name) continue;

    out.push({ rank: rankText, name });
  }

  return out;
}

function fetchHtml_(url) {
  const resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: FETCH_HEADERS,
  });

  const code = resp.getResponseCode();
  if (code < 200 || code >= 300) {
    throw new Error(`StatMando fetch failed (${code}) for URL: ${url}`);
  }
  return resp.getContentText();
}

function sortMpoFpoByRanks() {
  const ss = getSS_();
  sortOneDivision_("MPO");
  sortOneDivision_("FPO");

  function sortOneDivision_(sheetName) {
    const sh = ss.getSheetByName(sheetName);
    if (!sh) throw new Error(`Sheet not found: ${sheetName}`);

    const lastRow = sh.getLastRow();
    const lastCol = sh.getLastColumn();
    if (lastRow < 3) return;

    const range = sh.getRange(2, 1, lastRow - 1, lastCol);
    range.sort([
      { column: 4, ascending: true },
      { column: 5, ascending: true },
      { column: 1, ascending: true },
    ]);
  }
}

function extractAnchorText_(tdHtml) {
  const aRe = /<a\b[^>]*>([\s\S]*?)<\/a>/i;
  const m = tdHtml.match(aRe);
  if (m && m[1]) return stripHtml_(m[1]);
  return stripHtml_(tdHtml);
}

function stripHtml_(s) {
  if (!s) return "";
  let t = s.replace(/<[^>]*>/g, " ");
  t = t.replace(/\s+/g, " ").trim();
  t = t
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
  return t;
}

function normalizePlayerName_(name) {
  return (name || "")
    .toString()
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/***********************
 * 6) ROSTERS REBUILD
 ***********************/

function rebuildRosters() {
  const ss = getSS_();

  const draft = ss.getSheetByName(SHEET_DRAFTBOARD);
  const tx = ss.getSheetByName(SHEET_TRANSACTIONS);
  let rosterSheet = ss.getSheetByName(SHEET_ROSTERS);

  if (!draft) throw new Error("Missing sheet: DraftBoard");
  if (!tx) throw new Error("Missing sheet: Transactions");

  if (!rosterSheet) rosterSheet = ss.insertSheet(SHEET_ROSTERS);
  rosterSheet.clearContents();

  const mpo = ss.getSheetByName("MPO");
  const fpo = ss.getSheetByName("FPO");
  if (!mpo || !fpo) throw new Error("Missing MPO or FPO sheet");

  function readNamePdgaMap_(sh) {
    const vals = sh.getDataRange().getValues();
    const hdr = (vals[0] || []).map((h) => String(h || "").trim());

    const nameCol0 = hdr.indexOf("Player Name") >= 0 ? hdr.indexOf("Player Name") : 0;
    const pdgaCol0 = (() => {
      const candidates = ["PDGA #", "Player PDGA #", "PDGA", "PDGA#"];
      for (const c of candidates) {
        const idx = hdr.indexOf(c);
        if (idx >= 0) return idx;
      }
      return 2;
    })();

    const mapNameToPdga = new Map();
    const mapPdgaToName = new Map();

    for (let i = 1; i < vals.length; i++) {
      const name = String(vals[i][nameCol0] || "").trim();
      const pdga = String(vals[i][pdgaCol0] || "").trim();
      if (!name || !pdga) continue;

      mapNameToPdga.set(name, pdga);
      if (!mapPdgaToName.has(pdga)) mapPdgaToName.set(pdga, name);
    }

    return { mapNameToPdga, mapPdgaToName };
  }

  const mpoMaps = readNamePdgaMap_(mpo);
  const fpoMaps = readNamePdgaMap_(fpo);

  function pdgaFromName_(name) {
    return mpoMaps.mapNameToPdga.get(name) || fpoMaps.mapNameToPdga.get(name) || "";
  }
  function nameFromPdga_(pdga) {
    return mpoMaps.mapPdgaToName.get(pdga) || fpoMaps.mapPdgaToName.get(pdga) || "";
  }

  const dVals = draft.getDataRange().getValues();
  const dHdr = dVals[0].map((h) => String(h).trim());

  const dTeam = dHdr.indexOf("Team");
  const dDiv = dHdr.indexOf("Division");
  const dName = dHdr.indexOf("Player Name");
  const dPdga = dHdr.indexOf("Player PDGA #");

  if (dTeam < 0 || dDiv < 0 || dName < 0) {
    throw new Error("DraftBoard must include headers: Team, Division, Player Name (Player PDGA # optional)");
  }

  const current = new Map();

  for (let i = 1; i < dVals.length; i++) {
    const teamRaw = String(dVals[i][dTeam] || "").trim();
    const team = normalizeTeam_(teamRaw);
    const div = String(dVals[i][dDiv] || "").trim();
    const name = String(dVals[i][dName] || "").trim();
    if (!team || !name) continue;

    let pdga = dPdga >= 0 ? String(dVals[i][dPdga] || "").trim() : "";
    if (!pdga) pdga = pdgaFromName_(name);
    if (!pdga) continue;

    current.set(pdga, { team, name, div, source: "Draft" });
  }

  const tVals = tx.getDataRange().getValues();
  if (tVals.length >= 2) {
    const tHdr = tVals[0].map((h) => String(h).trim());

    const tType = tHdr.indexOf("Type");
    const tTeam = tHdr.indexOf("Team");
    const tPdga = tHdr.indexOf("Player PDGA #");
    const tName = tHdr.indexOf("Player Name");
    const tFrom = tHdr.indexOf("From Team");
    const tTo = tHdr.indexOf("To Team");

    if (tType < 0 || tTeam < 0 || tPdga < 0 || tTo < 0) {
      throw new Error("Transactions must include headers: Type, Team, Player PDGA #, To Team (Player Name recommended)");
    }

    for (let i = 1; i < tVals.length; i++) {
      const type = String(tVals[i][tType] || "").trim().toUpperCase();
      const team = normalizeTeam_(String(tVals[i][tTeam] || "").trim());
      let pdga = String(tVals[i][tPdga] || "").trim();
      let name = tName >= 0 ? String(tVals[i][tName] || "").trim() : "";
      const from = tFrom >= 0 ? normalizeTeam_(String(tVals[i][tFrom] || "").trim()) : "";
      const to = tTo >= 0 ? normalizeTeam_(String(tVals[i][tTo] || "").trim()) : "";

      if (!type) continue;

      if (!pdga && name) pdga = pdgaFromName_(name);
      if (!pdga) continue;

      if (!name) name = nameFromPdga_(pdga);
      const cur = current.get(pdga);

      if (type === "ADD") {
        const target = to || team;
        if (!target) continue;
        current.set(pdga, {
          team: target,
          name: cur && cur.name ? cur.name : name,
          div: cur && cur.div ? cur.div : "",
          source: "ADD",
        });
      } else if (type === "DROP") {
        const expected = from || team;
        if (!expected) continue;

        if (cur && normalizeTeam_(cur.team) === normalizeTeam_(expected)) {
          current.set(pdga, {
            team: FREE_AGENT,
            name: cur.name || name,
            div: cur.div || "",
            source: "DROP",
          });
        } else if (!cur) {
          current.set(pdga, { team: FREE_AGENT, name, div: "", source: "DROP" });
        }
      } else if (type === "TRADE") {
        const target = to;
        if (!target) continue;

        current.set(pdga, {
          team: target,
          name: cur && cur.name ? cur.name : name,
          div: cur && cur.div ? cur.div : "",
          source: "TRADE",
        });
      }
    }
  }

  const out = [["Team", "Player PDGA #", "Player Name", "Division", "Source", "Last Updated"]];
  const now = new Date();

  for (const [pdga, info] of current.entries()) {
    out.push([normalizeTeam_(info.team), pdga, info.name, info.div, info.source, now]);
  }

  out.splice(
    1,
    out.length - 1,
    ...out.slice(1).sort((a, b) => (a[0] + " " + a[2]).localeCompare(b[0] + " " + b[2]))
  );

  rosterSheet.getRange(1, 1, out.length, out[0].length).setValues(out);
}

/***********************
 * 7) OWNERSHIP STATE (from Rosters)
 ***********************/

function getOwnershipStateFromRosters_(ss) {
  const roster = ss.getSheetByName(SHEET_ROSTERS);
  const ownerByPdga = new Map();
  const countByTeam = new Map();

  if (!roster) return { ownerByPdga, countByTeam };

  const vals = roster.getDataRange().getValues();
  if (vals.length < 2) return { ownerByPdga, countByTeam };

  const hdr = vals[0].map((h) => String(h).trim());
  const iTeam = hdr.indexOf("Team");
  const iPdga = hdr.indexOf("Player PDGA #");

  if (iTeam < 0 || iPdga < 0) return { ownerByPdga, countByTeam };

  for (let r = 1; r < vals.length; r++) {
    const team = normalizeTeam_(String(vals[r][iTeam] || "").trim());
    const pdga = String(vals[r][iPdga] || "").trim();
    if (!team || !pdga) continue;

    ownerByPdga.set(pdga, team);
    countByTeam.set(team, (countByTeam.get(team) || 0) + 1);
  }

  return { ownerByPdga, countByTeam };
}

/***********************
 * 8) TRANSACTION VALIDATION
 ***********************/

function validateTransaction_(ss, data) {
  const errors = [];

  const type = String(data.type || "").trim().toUpperCase();
  const team = normalizeTeam_(data.team);

  const dropPdga = String(data.dropPdga || "").trim();
  const dropName = String(data.dropName || "").trim();
  const addPdga = String(data.addPdga || "").trim();
  const addName = String(data.addName || "").trim();

  const pdga = String(data.pdga || "").trim();
  const name = String(data.name || "").trim();

  const fromTeam = normalizeTeam_(data.fromTeam);
  const toTeam = normalizeTeam_(data.toTeam);

  if (!type) errors.push("Missing field: type");
  if (!team) errors.push("Missing field: team");

  const allowed = new Set(["ADD", "DROP", "TRADE", "SWAP"]);
  if (type && !allowed.has(type)) {
    errors.push(`Unknown transaction type: ${type}. Allowed: ADD, DROP, TRADE, SWAP.`);
    return { ok: false, errors, details: { type, team, fromTeam, toTeam } };
  }

  const { ownerByPdga, countByTeam } = getOwnershipStateFromRosters_(ss);

  if (type === "SWAP") {
    if (!dropPdga) errors.push("SWAP requires dropPdga.");
    if (!dropName) errors.push("SWAP requires dropName.");
    if (!addPdga) errors.push("SWAP requires addPdga.");
    if (!addName) errors.push("SWAP requires addName.");

    if (dropPdga && addPdga && dropPdga === addPdga) {
      errors.push("SWAP rejected: drop player and add player cannot be the same PDGA.");
    }

    const rosterCount = countByTeam.get(team) || 0;
    if (rosterCount > MAX_ROSTER) {
      errors.push(`SWAP rejected: ${team} is over roster cap (${rosterCount}/${MAX_ROSTER}). Fix roster first.`);
    }

    const dropOwner = ownerByPdga.get(dropPdga) || "";
    if (!dropOwner) {
      errors.push(`SWAP rejected: drop player ${dropName} (${dropPdga}) is not currently owned by any team.`);
    } else if (normalizeTeam_(dropOwner) !== normalizeTeam_(team)) {
      errors.push(`SWAP rejected: ${team} does not own ${dropName} (${dropPdga}). Current owner: ${dropOwner}.`);
    }

    const addOwner = ownerByPdga.get(addPdga) || "";
    const addIsFreeAgent = !addOwner || normalizeTeam_(addOwner) === FREE_AGENT;
    if (!addIsFreeAgent) {
      errors.push(`SWAP rejected: add player ${addName} (${addPdga}) is not a Free Agent. Current owner: ${addOwner}.`);
    }

    return {
      ok: errors.length === 0,
      errors,
      details: {
        type,
        team,
        dropPdga,
        dropName,
        addPdga,
        addName,
        dropCurrentOwner: dropOwner || null,
        addCurrentOwner: addOwner || null,
        rosterCountTeam: rosterCount,
      },
    };
  }

  if (!pdga) errors.push("Missing field: pdga");
  if (!name) errors.push("Missing field: name");
  if (errors.length) return { ok: false, errors, details: { type, team, fromTeam, toTeam, pdga, name } };

  const currentOwner = ownerByPdga.get(pdga) || "";
  const isFreeAgent = !currentOwner || normalizeTeam_(currentOwner) === FREE_AGENT;

  if (type === "TRADE") {
    if (!toTeam) errors.push("TRADE requires toTeam.");
    if (toTeam && team && normalizeTeam_(toTeam) === normalizeTeam_(team)) {
      errors.push("Invalid TRADE: fromTeam and toTeam cannot be the same team.");
    }
  }

  if (type === "ADD") {
    const target = toTeam || team;

    if (!target) errors.push("ADD requires team (or toTeam).");
    if (!isFreeAgent) {
      errors.push(`ADD rejected: player ${name} (${pdga}) is not a Free Agent. Current owner: ${currentOwner || "UNKNOWN"}.`);
    }
    const count = countByTeam.get(target) || 0;
    if (count >= MAX_ROSTER) {
      errors.push(`ADD rejected: ${target} already has ${count}/${MAX_ROSTER} players.`);
    }
  }

  if (type === "DROP") {
    const expectedOwner = fromTeam || team;
    if (!expectedOwner) errors.push("DROP requires team (or fromTeam).");

    if (!currentOwner) {
      errors.push(`DROP rejected: player ${name} (${pdga}) is not currently owned by any team.`);
    } else if (normalizeTeam_(currentOwner) !== normalizeTeam_(expectedOwner)) {
      errors.push(`DROP rejected: ${expectedOwner} does not own ${name} (${pdga}). Current owner: ${currentOwner}.`);
    }
  }

  if (type === "TRADE") {
    const expectedOwner = fromTeam || team;
    if (toTeam) {
      if (!currentOwner) {
        errors.push(`TRADE rejected: player ${name} (${pdga}) is not currently owned by any team.`);
      } else if (normalizeTeam_(currentOwner) !== normalizeTeam_(expectedOwner)) {
        errors.push(`TRADE rejected: ${expectedOwner} does not own ${name} (${pdga}). Current owner: ${currentOwner}.`);
      }
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    details: {
      type,
      team,
      fromTeam,
      toTeam,
      pdga,
      name,
      currentOwner: currentOwner || null,
      isFreeAgent,
      rosterCountTeam: countByTeam.get(toTeam || team) || 0,
    },
  };
}

/***********************
 * 9) WAIVERS (kept from your version; SMS wired inside handleWaiverRun_)
 ***********************/

// (No changes below this point other than SmsLog/AlertSubscriptions alignment and message templates)
// Your waiver and lineup logic continues as-is.

function loadPlayerPoolPdgaSet_(playerPoolSh) {
  const lastRow = playerPoolSh.getLastRow();
  if (lastRow < 2) return new Set();

  const idx = getHeaderIndexMap_(playerPoolSh);
  const candidates = ["PDGA #", "Player PDGA #", "PDGA", "PDGA#"];

  let pdgaCol = null;
  for (const c of candidates) {
    if (idx[c] != null) {
      pdgaCol = idx[c];
      break;
    }
  }
  if (pdgaCol == null) pdgaCol = 1;

  const width = playerPoolSh.getLastColumn();
  const rows = playerPoolSh.getRange(2, 1, lastRow - 1, width).getValues();

  const set = new Set();
  for (const r of rows) {
    const pdga = String(r[pdgaCol] || "").trim();
    if (pdga) set.add(pdga);
  }
  return set;
}

// ---- WAIVER_RUN ----
function handleWaiverRun_(data) {
  const ss = getSS_();
  const cycleId = String(data.cycleId || "").trim();
  const eventName = String(data.eventName || "").trim();

  if (!cycleId) return { ok: false, error: "Missing cycleId" };
  if (!eventName) return { ok: false, error: "Missing eventName" };

  const awardsSh = mustGetSheet_(ss, "WaiverAwardsLog");
  const requestsSh = mustGetSheet_(ss, "WaiverRequests");
  const standingsSh = mustGetSheet_(ss, "Standings");

  if (cycleAlreadyAwarded_(awardsSh, cycleId)) {
    return { ok: true, alreadyPosted: true };
  }

  const standings = loadStandings_(standingsSh);
  if (!standings.length) return { ok: false, error: "Standings tab has no rows" };

  const hasRank = standings.some((s) => s.rank != null);

  const ordered = standings.slice().sort((a, b) => {
    if (hasRank) {
      return Number(b.rank || 0) - Number(a.rank || 0) || a.team.localeCompare(b.team);
    }
    return a.points - b.points || a.team.localeCompare(b.team);
  });

  const teamCount = ordered.length;
  const requestsByTeam = loadActiveWaiverRequestsByTeam_(requestsSh, cycleId);
  const { ownerByPdga } = getOwnershipStateFromRosters_(ss);

  function isEligible_(pdga) {
    const owner = normalizeTeam_(ownerByPdga.get(String(pdga)) || "");
    return !owner || owner === FREE_AGENT;
  }

  const awardedThisRun = new Set();
  const cursorByTeam = new Map();
  for (const s of ordered) cursorByTeam.set(s.team, 0);

  const nowIso = new Date().toISOString();
  const awardRowsToAppend = [];
  const lines = [];

  const awardsByTeam = new Map();

  let round = 1;
  const MAX_ROUNDS = 50;

  while (round <= MAX_ROUNDS) {
    const beforeRoundLineCount = lines.length;
    lines.push(`— Round ${round} —`);

    let awardsThisRound = 0;

    for (let i = 0; i < ordered.length; i++) {
      const team = ordered[i].team;
      const waiverPriorityLabel = String(teamCount - i);

      const wishlist = requestsByTeam.get(team) || [];
      let cursor = cursorByTeam.get(team) || 0;

      let awarded = null;

      while (cursor < wishlist.length) {
        const pick = wishlist[cursor];
        cursor++;

        const pdga = String(pick.pdga || "").trim();
        if (!pdga) continue;

        if (awardedThisRun.has(pdga)) continue;
        if (!isEligible_(pdga)) continue;

        awarded = { pdga, name: pick.name, rank: pick.rank };
        break;
      }

      cursorByTeam.set(team, cursor);

      if (awarded) {
        awardsThisRound++;
        awardedThisRun.add(awarded.pdga);

        awardRowsToAppend.push([
          cycleId,
          nowIso,
          team,
          waiverPriorityLabel,
          awarded.pdga,
          awarded.name,
          "AWARDED",
          "",
          "OPEN",
          "",
        ]);

        lines.push(`${waiverPriorityLabel}) ${team}: ${awarded.name} (${awarded.pdga})`);

        if (!awardsByTeam.has(team)) awardsByTeam.set(team, []);
        awardsByTeam.get(team).push(awarded);
      } else {
        if (round === 1) {
          awardRowsToAppend.push([
            cycleId,
            nowIso,
            team,
            waiverPriorityLabel,
            "",
            "",
            "NO_VALID_PICK",
            "",
            "",
            "",
          ]);
        }
      }
    }

    if (awardsThisRound === 0) {
      if (round > 1) lines.length = beforeRoundLineCount;
      break;
    }

    round++;
  }

  if (awardRowsToAppend.length) appendRows_(awardsSh, awardRowsToAppend);
  rollRequestsForCycle_(requestsSh, cycleId);

  // SMS: only to teams with awards and WaiverAwards=true
  try {
    const smsBudget = newSmsBudget_();
    sendWaiverAwardAlerts_(ss, { cycleId, eventName, awardsByTeam, budget: smsBudget });
  } catch (e) {
    Logger.log("WaiverAward SMS error: " + (e && e.message ? e.message : e));
  }

  return {
    ok: true,
    alreadyPosted: false,
    title: `Waiver Awards — Cycle ${cycleId}`,
    eventName: eventName,
    lines: lines,
    footer: "Awards do not auto-add players. Use /transaction to claim.",
  };
}

// ---- WAIVER_SUBMIT ----
function handleWaiverSubmit_(data) {
  const ss = getSS_();

  const cycleId = String(data.cycleId || "").trim();
  const team = normalizeTeam_(String(data.team || "").trim());
  const submittedBy = String(data.submittedBy || "").trim();
  const picks = Array.isArray(data.picks) ? data.picks : [];

  if (!cycleId) return { ok: false, error: "Missing cycleId" };
  if (!team) return { ok: false, error: "Missing team" };
  if (!picks.length) return { ok: false, error: "No picks submitted" };

  const requestsSh = mustGetSheet_(ss, "WaiverRequests");
  const playerPoolSh = mustGetSheet_(ss, "PlayerPool");
  const playerPoolPdgas = loadPlayerPoolPdgaSet_(playerPoolSh);

  const { ownerByPdga } = getOwnershipStateFromRosters_(ss);
  function currentOwner_(pdga) {
    return normalizeTeam_(ownerByPdga.get(String(pdga)) || "");
  }

  const cleaned = [];
  const seenRanks = new Set();
  const seenPdga = new Set();

  for (const p of picks) {
    const rank = Number(p.rank);
    const pdga = String(p.pdga || "").trim();
    const name = String(p.name || "").trim();

    if (!rank || isNaN(rank)) continue;
    if (rank < 1 || rank > 10) return { ok: false, error: `Invalid rank: ${rank} (must be 1-10)` };
    if (!pdga || !name) continue;

    if (seenRanks.has(rank)) return { ok: false, error: `Duplicate rank submitted: ${rank}` };
    if (seenPdga.has(pdga)) return { ok: false, error: `Duplicate player submitted: ${name} (${pdga})` };

    if (!playerPoolPdgas.has(pdga)) return { ok: false, error: `Not found in PlayerPool: ${name} (${pdga})` };

    const owner = currentOwner_(pdga);
    if (owner && owner !== FREE_AGENT && owner !== team) {
      return { ok: false, error: `Already owned: ${name} (${pdga}) — Current owner: ${owner}` };
    }

    seenRanks.add(rank);
    seenPdga.add(pdga);
    cleaned.push({ rank, pdga, name });
  }

  if (!cleaned.length) return { ok: false, error: "No valid picks (need at least 1 ranked pick)" };

  cleaned.sort((a, b) => a.rank - b.rank);

  voidActiveRequestsForTeamCycle_(requestsSh, cycleId, team);

  const nowIso = new Date().toISOString();
  const rows = cleaned.map((x) => [
    cycleId,
    nowIso,
    team,
    submittedBy,
    x.rank,
    x.pdga,
    x.name,
    "ACTIVE",
  ]);

  appendRows_(requestsSh, rows);

  return { ok: true, cycleId, team, submittedCount: rows.length, picks: cleaned };
}

function voidActiveRequestsForTeamCycle_(requestsSh, cycleId, team) {
  const idx = getHeaderIndexMap_(requestsSh);

  const cCycle = idx["CycleId"];
  const cTeam = idx["TeamName"];
  const cStatus = idx["Status"];

  if (cCycle == null) throw new Error('WaiverRequests missing header "CycleId"');
  if (cTeam == null) throw new Error('WaiverRequests missing header "TeamName"');
  if (cStatus == null) throw new Error('WaiverRequests missing header "Status"');

  const lastRow = requestsSh.getLastRow();
  if (lastRow < 2) return;

  const width = requestsSh.getLastColumn();
  const range = requestsSh.getRange(2, 1, lastRow - 1, width);
  const rows = range.getValues();

  let changed = false;
  for (let i = 0; i < rows.length; i++) {
    const rowCycle = String(rows[i][cCycle] || "").trim();
    const rowTeam = normalizeTeam_(String(rows[i][cTeam] || "").trim());
    const status = String(rows[i][cStatus] || "").trim();

    if (rowCycle === cycleId && rowTeam === team && status === "ACTIVE") {
      rows[i][cStatus] = "VOID";
      changed = true;
    }
  }

  if (changed) range.setValues(rows);
}

/***********************
 * 10) WAIVER SUPPORT LOADERS
 ***********************/

function loadStandings_(standingsSh) {
  const idx = getHeaderIndexMap_(standingsSh);

  const rankCol = idx["Standings"];
  const teamCol = idx["Team Name"];
  const pointsCol = idx["Points"];

  if (teamCol == null) throw new Error('Standings missing header "Team Name"');

  const lastRow = standingsSh.getLastRow();
  if (lastRow < 2) return [];

  const width = standingsSh.getLastColumn();
  const rows = standingsSh.getRange(2, 1, lastRow - 1, width).getValues();

  const out = [];
  for (const r of rows) {
    const team = normalizeTeam_(String(r[teamCol] || "").trim());
    if (!team) continue;

    let rank = null;
    if (rankCol != null) {
      const rawRank = Number(r[rankCol]);
      if (!isNaN(rawRank) && rawRank > 0) rank = rawRank;
    }

    let points = 0;
    if (pointsCol != null) {
      const rawPoints = Number(r[pointsCol]);
      points = isNaN(rawPoints) ? 0 : rawPoints;
    }

    out.push({ team, rank, points });
  }

  return out;
}

function loadActiveWaiverRequestsByTeam_(requestsSh, cycleId) {
  const idx = getHeaderIndexMap_(requestsSh);

  const cCycle = idx["CycleId"];
  const cTeam = idx["TeamName"];
  const cRank = idx["Rank"];
  const cPdga = idx["PlayerPDGA"];
  const cName = idx["PlayerName"];
  const cStatus = idx["Status"];

  const required = [
    ["CycleId", cCycle],
    ["TeamName", cTeam],
    ["Rank", cRank],
    ["PlayerPDGA", cPdga],
    ["PlayerName", cName],
    ["Status", cStatus],
  ];
  for (const [h, v] of required) {
    if (v == null) throw new Error(`WaiverRequests missing header "${h}"`);
  }

  const lastRow = requestsSh.getLastRow();
  if (lastRow < 2) return new Map();

  const width = requestsSh.getLastColumn();
  const rows = requestsSh.getRange(2, 1, lastRow - 1, width).getValues();

  const map = new Map();
  for (const r of rows) {
    const rowCycle = String(r[cCycle] || "").trim();
    if (rowCycle !== cycleId) continue;

    const status = String(r[cStatus] || "").trim();
    if (status !== "ACTIVE") continue;

    const team = normalizeTeam_(String(r[cTeam] || "").trim());
    if (!team) continue;

    const rank = Number(r[cRank]);
    const pdga = String(r[cPdga] || "").trim();
    const name = String(r[cName] || "").trim();

    if (!pdga || !name || isNaN(rank)) continue;

    if (!map.has(team)) map.set(team, []);
    map.get(team).push({ rank, pdga, name });
  }

  for (const [team, list] of map.entries()) {
    list.sort((a, b) => a.rank - b.rank);
  }

  return map;
}

function cycleAlreadyAwarded_(awardsSh, cycleId) {
  const idx = getHeaderIndexMap_(awardsSh);
  const cCycle = idx["CycleId"];
  if (cCycle == null) throw new Error('WaiverAwardsLog missing header "CycleId"');

  const lastRow = awardsSh.getLastRow();
  if (lastRow < 2) return false;

  const width = awardsSh.getLastColumn();
  const rows = awardsSh.getRange(2, 1, lastRow - 1, width).getValues();

  for (const r of rows) {
    const rowCycle = String(r[cCycle] || "").trim();
    if (rowCycle === cycleId) return true;
  }
  return false;
}

function rollRequestsForCycle_(requestsSh, cycleId) {
  const idx = getHeaderIndexMap_(requestsSh);
  const cCycle = idx["CycleId"];
  const cStatus = idx["Status"];

  if (cCycle == null) throw new Error('WaiverRequests missing header "CycleId"');
  if (cStatus == null) throw new Error('WaiverRequests missing header "Status"');

  const lastRow = requestsSh.getLastRow();
  if (lastRow < 2) return;

  const width = requestsSh.getLastColumn();
  const range = requestsSh.getRange(2, 1, lastRow - 1, width);
  const rows = range.getValues();

  let changed = false;
  for (let i = 0; i < rows.length; i++) {
    const rowCycle = String(rows[i][cCycle] || "").trim();
    const status = String(rows[i][cStatus] || "").trim();

    if (rowCycle === cycleId && status === "ACTIVE") {
      rows[i][cStatus] = "ROLLED";
      changed = true;
    }
  }

  if (changed) range.setValues(rows);
}

/***********************
 * 11) LINEUP REMINDERS (dedupe + log + SMS)
 ***********************/

function handleLineupReminderRun_(ss, data) {
  const cycleId = String(data.cycleId || "").trim();
  const eventName = String(data.eventName || "").trim();
  const runAt = data.runAt ? String(data.runAt).trim() : "";

  if (!cycleId) return { ok: false, error: "Missing cycleId" };
  if (!eventName) return { ok: false, error: "Missing eventName" };

  const sh = mustGetOrCreateSheet_(ss, LINEUP_LOG_SHEET, LINEUP_LOG_HEADERS);

  const lastRow = sh.getLastRow();
  if (lastRow >= 2) {
    const vals = sh.getRange(2, 1, lastRow - 1, 2).getValues();
    for (const r of vals) {
      const c = String(r[0] || "").trim();
      const e = String(r[1] || "").trim();
      if (c === cycleId && e === eventName) {
        return { ok: true, alreadyPosted: true };
      }
    }
  }

  const now = new Date();
  const meta = { cycleId, eventName, runAt: runAt || now.toISOString() };

  sh.appendRow([cycleId, eventName, runAt || now.toISOString(), now, "LOGGED", JSON.stringify(meta)]);

  try {
  const smsBudget = newSmsBudget_();
  sendLineupReminderAlerts_(ss, { cycleId, eventName, budget: smsBudget });
} catch (e) {
  Logger.log("LineupReminder SMS error: " + (e && e.message ? e.message : e));
}

return { ok: true, alreadyPosted: false }; // (or whatever you want)
}

/***********************
 * 12) WEBAPP ENDPOINT (doPost) — patched for compliance
 ***********************/

function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(25000);

  const ss = getSS_();
  const log = ss.getSheetByName(SHEET_WEBHOOKLOG) || ss.insertSheet(SHEET_WEBHOOKLOG);

  try {
    const raw = e && e.postData && e.postData.contents ? e.postData.contents : "";
    const headers = (e && e.parameter) ? e.parameter : {}; // not super useful; Apps Script doesn't expose all headers reliably

    /*****************************************************************
     * TWILIO INBOUND (STOP/HELP/START)
     * ✅ Minimal hardening: require *some* Twilio signature signal.
     *
     * NOTE: Apps Script doesn't reliably expose HTTP headers.
     * If you can’t read X-Twilio-Signature here, you can’t truly verify.
     * We will:
     *  - attempt verification if header is available via e.headers (if your deployment provides it)
     *  - otherwise, block inbound unless the request "looks like Twilio" AND includes AccountSid
     *****************************************************************/
    const looksFormEncoded =
      raw && raw.trim().charAt(0) !== "{" && raw.indexOf("From=") >= 0 && raw.indexOf("Body=") >= 0;

    if (looksFormEncoded) {
      const form = parseFormBody_(raw);
      const from = String(form.From || "").trim().replace(/^'+/, "");
      const bodyRaw = String(form.Body || "").trim();
      const body = bodyRaw.toUpperCase();
      const accountSid = String(form.AccountSid || "").trim();

      // ✅ Hard block if not plausibly Twilio (AccountSid is normally present on inbound SMS webhooks)
      if (!accountSid) {
        log.appendRow([new Date(), "REJECTED", "TWILIO_INBOUND_MISSING_ACCOUNTSID", "", "", "", from, "", bodyRaw]);
        return twimlResponse_("");
      }

      const allowedSid = getScriptProp_("TWILIO_ACCOUNT_SID");
      if (allowedSid && accountSid !== allowedSid) {
        log.appendRow([new Date(), "REJECTED", "TWILIO_INBOUND_WRONG_ACCOUNTSID", "", "", "", from, "", bodyRaw]);
        return twimlResponse_("");
      }

      // ✅ Try Twilio signature verification IF available.
      // If your environment provides e.headers['X-Twilio-Signature'] and full URL, verify.
      // Otherwise, we already did a minimal block above.
      try {
        const sig =
          (e && e.headers && (e.headers["X-Twilio-Signature"] || e.headers["x-twilio-signature"])) || "";
        if (sig) {
          const ok = verifyTwilioRequest_(e, sig, form);
          if (!ok) {
            log.appendRow([new Date(), "REJECTED", "TWILIO_BAD_SIGNATURE", "", "", "", from, "", bodyRaw]);
            return twimlResponse_("");
          }
        } else {
          // No signature available -> minimal block already applied.
          // (If your deployment can expose headers, you should rely on signature verification.)
        }
      } catch (sigErr) {
        log.appendRow([new Date(), "ERROR", "TWILIO_VERIFY_ERROR", "", "", "", from, "", String(sigErr)]);
        // safer to reject if verification throws
        return twimlResponse_("");
      }

      // Log inbound
      log.appendRow([new Date(), "RECEIVED", "TWILIO_INBOUND", "", "", "", from, "", bodyRaw]);

      // STOP keywords
      if (["STOP", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"].includes(body)) {
        try {
          setOptOutByPhone_(ss, from, true);
        } catch (e2) {
          log.appendRow([new Date(), "ERROR", "TWILIO_STOP_OPT_OUT_FAIL", "", "", "", from, "", String(e2)]);
        }
        return twimlResponse_(
          "You have been unsubscribed from FDG alerts. Reply START to re-subscribe (or re-enable in the dashboard)."
        );
      }

      // HELP keyword
      if (body === "HELP") {
       const support = getScriptProp_("FDG_SUPPORT_EMAIL") || "support@example.com";
       return twimlResponse_(
        `FDG Alerts: transactional league notifications. Msg&data rates may apply. Reply STOP to opt out. Support: ${support}`
       );
}

      // START keywords (optional re-subscribe)
      if (["START", "YES"].includes(body)) {
        try {
          setOptOutByPhone_(ss, from, false);
        } catch (e3) {
          log.appendRow([new Date(), "ERROR", "TWILIO_START_OPT_IN_FAIL", "", "", "", from, "", String(e3)]);
        }
        return twimlResponse_("You are re-subscribed to FDG alerts. Reply STOP to opt out.");
      }

      return twimlResponse_("");
    }

    /*****************************************************************
     * JSON BOT WEBHOOK PATH
     * ✅ Compliance: move TX_SECRET to Script Properties
     *****************************************************************/
    const data = safeJson_(raw || "{}", {});
    const expectedSecret = getScriptProp_("TX_SECRET"); // <-- moved out of code
    if (!expectedSecret) throw new Error("Server misconfig: missing Script Property TX_SECRET");
    if (data.secret !== expectedSecret) throw new Error("Unauthorized (bad secret)");

    // WAIVERS
    if (data && data.action === "WAIVER_RUN") {
      log.appendRow([new Date(), "RECEIVED", "WAIVER_RUN", "", "", "", "", "", `cycleId=${data.cycleId || ""}`]);
      return jsonResponse_(handleWaiverRun_(data));
    }

    if (data && data.action === "WAIVER_SUBMIT") {
      log.appendRow([
        new Date(),
        "RECEIVED",
        "WAIVER_SUBMIT",
        normalizeTeam_(data.team),
        "",
        "",
        "",
        "",
        `cycleId=${data.cycleId || ""}`,
      ]);
      return jsonResponse_(handleWaiverSubmit_(data));
    }

    // LINEUP REMINDERS
    if (data && data.action === "LINEUP_REMINDER_RUN") {
      log.appendRow([
        new Date(),
        "RECEIVED",
        "LINEUP_REMINDER_RUN",
        "",
        "",
        "",
        "",
        "",
        `cycleId=${data.cycleId || ""} event=${data.eventName || ""}`,
      ]);
      const res = handleLineupReminderRun_(ss, data);
      return jsonResponse_(res);
    }

    // ALERT SUBSCRIPTIONS
    if (data && data.action === "ALERTS_SET") {
      log.appendRow([new Date(), "RECEIVED", "ALERTS_SET", normalizeTeam_(data.team), "", "", "", "", ""]);

      const res = upsertAlertSubscription_(ss, {
        team: data.team,
        phoneE164: data.phoneE164,
        enabled: coerceBool_(data.enabled),
        freeAgents: coerceBool_(data.freeAgents),
        waiverAwards: coerceBool_(data.waiverAwards),
        withdrawals: coerceBool_(data.withdrawals),
        lineupReminders: coerceBool_(data.lineupReminders),
      });

      return jsonResponse_({ ok: true, ...res });
    }

    // TRANSACTIONS
    const sh = ss.getSheetByName(SHEET_TRANSACTIONS);
    if (!sh) throw new Error('Sheet "Transactions" not found');

    const mode = String(data.mode || "").trim().toLowerCase(); // "validate" or "" (commit)
    const date = data.date ? new Date(data.date) : new Date();
    const type = String(data.type || "").trim().toUpperCase();
    const notes = String(data.notes || "").trim();

    const team = normalizeTeam_(data.team);
    const fromTeam = normalizeTeam_(data.fromTeam);
    const toTeam = normalizeTeam_(data.toTeam);

    const dropPdga = String(data.dropPdga || "").trim();
    const dropName = String(data.dropName || "").trim();
    const addPdga = String(data.addPdga || "").trim();
    const addName = String(data.addName || "").trim();

    const pdga = String(data.pdga || "").trim();
    const name = String(data.name || "").trim();

    if (!type || !team) throw new Error(`Missing required fields. type=${type} team=${team}`);

    // Log received
    if (type === "SWAP") {
      log.appendRow([
        new Date(),
        "RECEIVED",
        type,
        team,
        dropPdga,
        dropName,
        team,
        FREE_AGENT,
        `add=${addName} (${addPdga})${mode ? " mode=" + mode : ""}`,
      ]);
    } else {
      log.appendRow([
        new Date(),
        "RECEIVED",
        type,
        team,
        pdga,
        name,
        fromTeam,
        toTeam,
        mode ? `mode=${mode}` : "",
      ]);
    }

    // Validate
    const verdict = validateTransaction_(
      ss,
      type === "SWAP"
        ? { type, team, dropPdga, dropName, addPdga, addName }
        : { type, team, pdga, name, fromTeam, toTeam }
    );

    if (!verdict.ok) {
      log.appendRow([
        new Date(),
        "VALIDATION_FAIL",
        type,
        team,
        pdga || dropPdga,
        name || dropName,
        fromTeam,
        toTeam,
        verdict.errors.join(" | "),
      ]);
      return jsonResponse_({ ok: false, errors: verdict.errors, details: verdict.details });
    }

    if (mode === "validate") {
      log.appendRow([new Date(), "VALIDATION_OK", type, team, pdga || dropPdga, name || dropName, fromTeam, toTeam, ""]);
      return jsonResponse_({ ok: true, details: verdict.details });
    }

    // Commit
    if (type === "SWAP") {
      sh.appendRow([date, "DROP", team, dropPdga, dropName, team, FREE_AGENT, notes]);
      sh.appendRow([date, "ADD", team, addPdga, addName, FREE_AGENT, team, notes]);
      SpreadsheetApp.flush();

      try {
        const smsBudget = newSmsBudget_();
        sendFreeAgentDropAlerts_(ss, {
          droppedByTeam: team,
          playerName: dropName,
          playerPdga: dropPdga,
          budget: smsBudget,
        });
      } catch (e4) {
        Logger.log("FreeAgentDrop SMS error: " + (e4 && e4.message ? e4.message : e4));
      }

      log.appendRow([new Date(), "BEFORE_REBUILD"]);
      rebuildRosters();
      SpreadsheetApp.flush();
      log.appendRow([new Date(), "AFTER_REBUILD"]);

      return jsonResponse_({ ok: true });
    }

    sh.appendRow([date, type, team, pdga, name, fromTeam, toTeam, notes]);
    SpreadsheetApp.flush();

    if (type === "DROP") {
      try {
        const smsBudget = newSmsBudget_();
        sendFreeAgentDropAlerts_(ss, {
          droppedByTeam: team,
          playerName: name,
          playerPdga: pdga,
          budget: smsBudget,
        });
      } catch (e5) {
        Logger.log("FreeAgentDrop SMS error: " + (e5 && e5.message ? e5.message : e5));
      }
    }

    log.appendRow([new Date(), "BEFORE_REBUILD"]);
    rebuildRosters();
    SpreadsheetApp.flush();
    log.appendRow([new Date(), "AFTER_REBUILD"]);

    return jsonResponse_({ ok: true });
  } catch (err) {
    log.appendRow([new Date(), "ERROR", String(err && err.message ? err.message : err)]);
    return jsonResponse_({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

/**
 * OPTIONAL (recommended): Twilio signature verification.
 * Will only run if doPost() was able to read X-Twilio-Signature (not always possible in Apps Script).
 *
 * Requirements:
 * - Script Property: TWILIO_AUTH_TOKEN
 * - Your Web App URL known at runtime: ScriptApp.getService().getUrl()
 */
function verifyTwilioRequest_(e, signature, formParamsObj) {
  const token = getScriptProp_("TWILIO_AUTH_TOKEN");
  if (!token) throw new Error("Missing Script Property TWILIO_AUTH_TOKEN (needed for signature verify)");

  // Twilio signs the exact URL it requested (your web app URL) + sorted params.
  // Apps Script URL should match what Twilio is configured to hit.
  const url = getScriptProp_("TWILIO_WEBHOOK_URL") || ScriptApp.getService().getUrl();

  // Build string = url + (params sorted by key, concatenated key+value)
  const keys = Object.keys(formParamsObj || {}).sort();
  let data = url;
  for (const k of keys) data += k + String(formParamsObj[k] ?? "");

  const mac = Utilities.computeHmacSha1Signature(data, token);
  const expected = Utilities.base64Encode(mac);

  // Timing-safe compare (best-effort)
  const a = String(expected || "");
  const b = String(signature || "");
  if (a.length !== b.length) return false;

  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

/***********************
 * 13) DEBUG + MENU
 ***********************/

function debugRegistrationParse() {
  const url = "PASTE_ONE_REGISTRATION_URL_HERE";
  const ss = getSS_();

  const status = fetchDgsRegistrationStatusByPdga_(url);
  Logger.log("Parsed status rows: " + status.size);

  let i = 0;
  for (const [pdga, st] of status.entries()) {
    Logger.log(`${pdga} => ${st}`);
    if (++i >= 10) break;
  }

  const players = fetchDgsRegisteredPlayers_(url);
  Logger.log("Parsed players (pdga+name): " + players.size);
}

/***********************
 * 13B) PDGA LIVE SCORING (Hyzerbase-style)
 * - Writes scores into PlayerPool columns:
 *     SFO_R1, BEO_R2, etc.
 * - Reads events from ScoringConfig
 * - Logs runs to ScoringLog
 *
 * REQUIRED SHEETS:
 *  - PlayerPool (headers: Player Name | PDGA #)
 *  - ScoringConfig (headers: EventCode | TournID | StartDate | EndDate | MPO_Rounds | FPO_Rounds | Enabled)
 *  - ScoringLog (headers: Timestamp | EventCode | Division | Round | TournID | Status | Message | PlayersWritten | PlayersSkipped | MetaJson)
 ***********************/

const SHEET_PLAYERPOOL = "PlayerPool";
const SHEET_SCORING_CONFIG = "ScoringConfig";
const SHEET_SCORING_LOG = "ScoringLog";

const SCORING_CONFIG_HEADERS = [
  "EventCode",
  "TournID",
  "StartDate",
  "EndDate",
  "MPO_Rounds",
  "FPO_Rounds",
  "Enabled",
  "EventHeader",
];

const SCORING_LOG_HEADERS = [
  "Timestamp",
  "EventCode",
  "Division",
  "Round",
  "TournID",
  "Status",
  "Message",
  "PlayersWritten",
  "PlayersSkipped",
  "MetaJson",
];

// Sentinel / invalid hole score values
const HOLE_SENTINELS = new Set(["", " ", "0", "00", "888", "999", "DNF", "DQ", "WD"]);

/***********************
 * 14C) NIGHTLY EVENT ORCHESTRATION (auto J15, lock, finalize, standings sort)
 ***********************/

// where the Team tab "current event" lives
const TEAMTAB_EVENTCODE_CELL_A1 = "J15";

// script-property guards
function finalizedGuardKey_(eventCode) {
  return `EVENT_FINALIZED_${String(eventCode || "").trim().toUpperCase()}`;
}
function wasEventFinalized_(eventCode) {
  return PropertiesService.getScriptProperties().getProperty(finalizedGuardKey_(eventCode)) === "1";
}
function markEventFinalized_(eventCode) {
  PropertiesService.getScriptProperties().setProperty(finalizedGuardKey_(eventCode), "1");
}

/**
 * Load enabled scoring config rows sorted by StartDate asc.
 * Uses your existing loadScoringConfig_ helper.
 */
function loadEnabledEventsSorted_(ss) {
  const cfgSh = mustGetOrCreateSheet_(ss, SHEET_SCORING_CONFIG, SCORING_CONFIG_HEADERS);
  const cfg = loadScoringConfig_(cfgSh) || [];
  cfg.sort((a, b) => (a.startDate.getTime() - b.startDate.getTime()) || String(a.eventCode).localeCompare(String(b.eventCode)));
  return cfg;
}

/**
 * Determine:
 * - active events today (can be 1, but supports multiple)
 * - next upcoming event (first with startDate > today)
 */
function getActiveAndNextEvents_(eventsSorted, todayDateOnly) {
  const active = [];
  let next = null;

  for (const ev of eventsSorted) {
    if (dateInRange_(todayDateOnly, ev.startDate, ev.endDate)) active.push(ev);
    if (!next && ev.startDate && ev.startDate.getTime() > todayDateOnly.getTime()) next = ev;
  }

  // If none active, "current" becomes next upcoming
  return { active, next };
}

/**
 * Write J15 (EventCode cell) on all Team tabs.
 */
function syncTeamTabsEventCodeCell_(ss, eventCode) {
  const code = String(eventCode || "").trim().toUpperCase();
  if (!code) return;

  const teams = canonTeamsNoFA_();
  for (const team of teams) {
    const sh = getTeamSheetForCanon_(ss, team);
    if (!sh) continue;
    sh.getRange(TEAMTAB_EVENTCODE_CELL_A1).setValue(code);
  }
}

/**
 * Safe auto-lock: upserts LOCKED lineups for teams that have a complete valid lineup.
 * - Never throws because one team is missing/invalid
 * - Skips teams that are incomplete
 * - Validates lineup PDGAs are on roster PDGA range
 */
function autoLockLineupsForEventCodeSafe_(ss, eventCode) {
  ensureLineupSheets_();

  const code = String(eventCode || "").trim().toUpperCase();
  if (!code) return { ok: false, error: "Missing EventCode" };

  const poolSh = mustGetSheet_(ss, SHEET_PLAYERPOOL);
  const lineupsSh = mustGetSheet_(ss, SHEET_LINEUPS);
  const eventHeader = getEventHeaderForCode_(code);
  const now = new Date();
  const teams = canonTeamsNoFA_();

  let lockedCount = 0;
  const skipped = [];

  for (const team of teams) {
    const teamSh = getTeamSheetForCanon_(ss, team);
    if (!teamSh) {
      skipped.push({ team, reason: "Missing team tab" });
      continue;
    }

    // Optional: if J15 is filled and doesn't match, skip
    const tabCode = String(teamSh.getRange(TEAMTAB_EVENTCODE_CELL_A1).getValue() || "").trim().toUpperCase();
    if (tabCode && tabCode !== code) {
      skipped.push({ team, reason: `Team tab EventCode=${tabCode} != ${code}` });
      continue;
    }

    // Read lineup PDGAs (L17:L22)
    const lineupPdgas = teamSh.getRange(TEAMTAB_LINEUP_PDGA_RANGE_A1).getValues().flat()
      .map(v => String(v || "").trim())
      .filter(v => v);

    if (lineupPdgas.length !== 6) {
      skipped.push({ team, reason: `Incomplete lineup (${lineupPdgas.length}/6)` });
      continue;
    }

    // Validate they’re on roster PDGA list (C3:C12)
    const rosterPdgas = new Set(
      teamSh.getRange(TEAMTAB_ROSTER_PDGA_RANGE_A1).getValues().flat()
        .map(v => String(v || "").trim())
        .filter(v => v)
    );

    let bad = false;
    for (const pdga of lineupPdgas) {
      if (!rosterPdgas.has(pdga)) {
        skipped.push({ team, reason: `Lineup PDGA ${pdga} not on roster` });
        bad = true;
        break;
      }
    }
    if (bad) continue;

    const names = lineupPdgas.map(pdga => getPlayerPoolNameByPdga_(poolSh, pdga));

    upsertLineupsRow_(lineupsSh, {
      EventCode: code,
      EventHeader: eventHeader,
      Team: team,
      Status: "LOCKED",
      SubmittedAt: now,
      LockedAt: now,
      FinalizedAt: "",
      Slot1_Name: names[0], Slot1_PDGA: lineupPdgas[0],
      Slot2_Name: names[1], Slot2_PDGA: lineupPdgas[1],
      Slot3_Name: names[2], Slot3_PDGA: lineupPdgas[2],
      Slot4_Name: names[3], Slot4_PDGA: lineupPdgas[3],
      Slot5_Name: names[4], Slot5_PDGA: lineupPdgas[4],
      Slot6_Name: names[5], Slot6_PDGA: lineupPdgas[5],
      MetaJson: JSON.stringify({ source: "AutoLockNightly", eventCodeCell: TEAMTAB_EVENTCODE_CELL_A1, lineupRange: TEAMTAB_LINEUP_PDGA_RANGE_A1 })
    });

    lockedCount++;
  }

  return { ok: true, eventCode: code, lockedCount, skipped };
}

/**
 * Sort Standings by Season Total desc (or fallback to Points desc if Season Total not present).
 * Also optionally rewrites "Standings" rank column if it exists.
 */
function sortStandingsByScoreDesc_() {
  const ss = getSS_();
  const SHEET_STANDINGS = "Standings";
  const sh = mustGetSheet_(ss, SHEET_STANDINGS);

  const headers = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(h => String(h || "").trim());
  const idxStandings0 = headers.indexOf("Standings");
  const idxTeam0 = headers.indexOf("Team Name");

  // Prefer Season Total, else Points
  let sortCol0 = headers.indexOf("Season Total");
  if (sortCol0 < 0) sortCol0 = headers.indexOf("Points");
  if (sortCol0 < 0) throw new Error('Standings missing "Season Total" (preferred) or "Points".');

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 3) return;

  // Sort the data rows (row 2..)
  sh.getRange(2, 1, lastRow - 1, lastCol).sort([
    { column: sortCol0 + 1, ascending: false },
    ...(idxTeam0 >= 0 ? [{ column: idxTeam0 + 1, ascending: true }] : [])
  ]);

  // Re-number Standings column if present
  if (idxStandings0 >= 0) {
    const rows = lastRow - 1;
    const ranks = Array.from({ length: rows }, (_, i) => [i + 1]);
    sh.getRange(2, idxStandings0 + 1, rows, 1).setValues(ranks);
  }
}

/**
 * EndDate-based auto finalize.
 * Runs only once per event (script property guard), and only if there are LOCKED lineups.
 */
function autoFinalizeIfEndDateTonight_(ss, ev, todayDateOnly) {
  if (!ev || !ev.eventCode || !ev.endDate) return { ok: false, reason: "missing_ev" };

  const code = String(ev.eventCode).trim().toUpperCase();

  // Only finalize on the EndDate (date-only compare)
  if (ev.endDate.getTime() !== todayDateOnly.getTime()) return { ok: true, finalized: false, reason: "not_enddate" };

  // Guard: never finalize twice
  if (wasEventFinalized_(code)) return { ok: true, finalized: false, reason: "already_finalized_guard" };

  // Try finalize (your function throws if nothing LOCKED)
  finalizeLineupsForEventCode(code);

  // Sort standings after finalize
  sortStandingsByScoreDesc_();

  // Mark as finalized
  markEventFinalized_(code);

  return { ok: true, finalized: true, eventCode: code };
}

/**
 * Nightly runner (trigger calls this)
 * - Finds enabled events active today (ET / script TZ)
 * - Scores MPO + FPO for today's round number
 */
function scoreNightlyPdgaLive() {
  const ss = getSS_();
  const tz = Session.getScriptTimeZone();

  const now = new Date();
  const todayDateOnly = new Date(Utilities.formatDate(now, tz, "yyyy/MM/dd") + " 00:00:00");

  const poolSh = mustGetSheet_(ss, SHEET_PLAYERPOOL);
  const logSh = mustGetOrCreateSheet_(ss, SHEET_SCORING_LOG, SCORING_LOG_HEADERS);

  const eventsSorted = loadEnabledEventsSorted_(ss);
  if (!eventsSorted.length) {
    logScoring_(logSh, {
      eventCode: "",
      division: "",
      round: "",
      tournId: "",
      status: "SKIP",
      message: "No enabled rows found in ScoringConfig.",
      written: 0,
      skipped: 0,
      meta: {}
    });
    return;
  }

  const { active, next } = getActiveAndNextEvents_(eventsSorted, todayDateOnly);

  // If we have an active event, show it on all Team tabs (J15)
  if (active.length) {
    // Usually only one is active; if multiple, we set J15 to the first one by StartDate
    syncTeamTabsEventCodeCell_(ss, active[0].eventCode);
  } else if (next) {
    // No active event today → pre-stage J15 to next upcoming
    syncTeamTabsEventCodeCell_(ss, next.eventCode);
  }

  // --- Run scoring for each active event today ---
  for (const ev of active) {
    // Still lock Team event columns on StartDate (your existing behavior)
    if (ev.startDate && todayDateOnly.getTime() === ev.startDate.getTime()) {
      try {
        lockTeamEventColumnToScoring_(ss, ev.eventCode, ev.eventHeader);
      } catch (e) {
        Logger.log(`TeamTab lock-to-scoring failed for ${ev.eventCode}: ${String(e && e.message ? e.message : e)}`);
      }
    }

    // Only score if we have tournId
    if (!ev.tournId) {
      logScoring_(logSh, {
        eventCode: ev.eventCode,
        division: "ALL",
        round: "",
        tournId: "",
        status: "SKIP",
        message: "Missing TournID in ScoringConfig (skipping scoring fetch).",
        written: 0,
        skipped: 0,
        meta: { eventHeader: ev.eventHeader }
      });
      continue;
    }

    // This calls your existing scorer (writes SFO_R#, recomputes SFO_T/Season_T, etc.)
    runScoringForEvent_(poolSh, logSh, ev, todayDateOnly);
  }

  // --- Auto-lock lineups nightly for each active event (safe, no throws) ---
  for (const ev of active) {
    try {
      const res = autoLockLineupsForEventCodeSafe_(ss, ev.eventCode);
      Logger.log(`AutoLock ${ev.eventCode}: locked=${res.lockedCount} skipped=${(res.skipped || []).length}`);
    } catch (e) {
      Logger.log(`AutoLock error for ${ev.eventCode}: ${String(e && e.message ? e.message : e)}`);
    }
  }

  // --- Auto-finalize on EndDate (after scoring + after autolock) ---
  // If multiple events were active (rare), finalize any whose EndDate is today.
  let finalizedAny = false;
  for (const ev of active) {
    try {
      const fin = autoFinalizeIfEndDateTonight_(ss, ev, todayDateOnly);
      if (fin && fin.finalized) finalizedAny = true;
    } catch (e) {
      Logger.log(`Finalize error for ${ev.eventCode}: ${String(e && e.message ? e.message : e)}`);
    }
  }

  // --- After finalize, advance J15 to the next upcoming EventCode ---
  if (finalizedAny) {
    const tomorrow = new Date(todayDateOnly.getTime() + 24 * 60 * 60 * 1000);
    const { next: nextAfter } = getActiveAndNextEvents_(eventsSorted, tomorrow);
    if (nextAfter && nextAfter.eventCode) {
      syncTeamTabsEventCodeCell_(ss, nextAfter.eventCode);
    }
  }
}

function runScoringForEvent_(poolSh, logSh, ev, todayDateOnly) {
  const roundNum = dayIndexRound_(ev.startDate, todayDateOnly);

  const maxRounds = Math.max(ev.mpoRounds || 0, ev.fpoRounds || 0);
  if (roundNum < 1 || roundNum > maxRounds) {
    logScoring_(logSh, {
      eventCode: ev.eventCode,
      division: "ALL",
      round: roundNum,
      tournId: ev.tournId,
      status: "SKIP",
      message: `No round for today (computed round=${roundNum}, maxRounds=${maxRounds}).`,
      written: 0,
      skipped: 0,
      meta: {},
    });
    return;
  }

  const colHeader = `${ev.eventCode}_R${roundNum}`;

  try {
    const merged = new Map();
    let skippedTotal = 0;

    // MPO only if this round exists for MPO
    if (roundNum <= (ev.mpoRounds || 0)) {
      try {
        const mpoJson = fetchPdgaRound_(ev.tournId, "MPO", roundNum);
        const mpoParsed = parsePdgaRound_(mpoJson);
        const mpoRes = computeDivisionScores_(mpoParsed);
        skippedTotal += mpoRes.skipped;
        for (const [pdga, pts] of mpoRes.pdgaToScore.entries()) merged.set(pdga, pts);
      } catch (e) {
        logScoring_(logSh, {
          eventCode: ev.eventCode,
          division: "MPO",
          round: roundNum,
          tournId: ev.tournId,
          status: "ERROR",
          message: `MPO fetch/parse failed: ${String(e && e.message ? e.message : e)}`,
          written: 0,
          skipped: 0,
          meta: { colHeader },
        });
      }
    } else {
      logScoring_(logSh, {
        eventCode: ev.eventCode,
        division: "MPO",
        round: roundNum,
        tournId: ev.tournId,
        status: "SKIP",
        message: `MPO round ${roundNum} > MPO_Rounds (${ev.mpoRounds || 0}).`,
        written: 0,
        skipped: 0,
        meta: { colHeader },
      });
    }

    // FPO only if this round exists for FPO
    if (roundNum <= (ev.fpoRounds || 0)) {
      try {
        const fpoJson = fetchPdgaRound_(ev.tournId, "FPO", roundNum);
        const fpoParsed = parsePdgaRound_(fpoJson);
        const fpoRes = computeDivisionScores_(fpoParsed);
        skippedTotal += fpoRes.skipped;
        for (const [pdga, pts] of fpoRes.pdgaToScore.entries()) merged.set(pdga, pts);
      } catch (e) {
        logScoring_(logSh, {
          eventCode: ev.eventCode,
          division: "FPO",
          round: roundNum,
          tournId: ev.tournId,
          status: "ERROR",
          message: `FPO fetch/parse failed: ${String(e && e.message ? e.message : e)}`,
          written: 0,
          skipped: 0,
          meta: { colHeader },
        });
      }
    } else {
      logScoring_(logSh, {
        eventCode: ev.eventCode,
        division: "FPO",
        round: roundNum,
        tournId: ev.tournId,
        status: "SKIP",
        message: `FPO round ${roundNum} > FPO_Rounds (${ev.fpoRounds || 0}).`,
        written: 0,
        skipped: 0,
        meta: { colHeader },
      });
    }

    if (merged.size === 0) {
      logScoring_(logSh, {
        eventCode: ev.eventCode,
        division: "ALL",
        round: roundNum,
        tournId: ev.tournId,
        status: "SKIP",
        message: "No valid scores parsed (merged map empty).",
        written: 0,
        skipped: skippedTotal,
        meta: { colHeader },
      });
      return;
    }

    // ✅ Option A: auto-create scoring column if missing
    ensureOrCreatePlayerPoolColumn_(poolSh, colHeader);

    const written = writeScoresToPlayerPool_(poolSh, colHeader, merged);
    recomputeTotalsAndSortPlayerPool_(poolSh);

    logScoring_(logSh, {
      eventCode: ev.eventCode,
      division: "ALL",
      round: roundNum,
      tournId: ev.tournId,
      status: "OK",
      message: `Wrote ${written} merged scores to PlayerPool column ${colHeader}.`,
      written,
      skipped: skippedTotal,
      meta: { colHeader, mergedCount: merged.size },
    });
  } catch (err) {
    logScoring_(logSh, {
      eventCode: ev.eventCode,
      division: "ALL",
      round: roundNum,
      tournId: ev.tournId,
      status: "ERROR",
      message: String(err && err.message ? err.message : err),
      written: 0,
      skipped: 0,
      meta: { colHeader },
    });
  }
}

/**
 * Manual test runner (menu calls this)
 */
function testScoringPrompt() {
  const ui = SpreadsheetApp.getUi();
  const ss = getSS_();

  const eventCode = ui
    .prompt("Test PDGA Scoring", "EventCode (ex: SFO)", ui.ButtonSet.OK_CANCEL)
    .getResponseText()
    .trim()
    .toUpperCase();
  if (!eventCode) return;

  const roundStr = ui
    .prompt("Test PDGA Scoring", "Round number (ex: 1)", ui.ButtonSet.OK_CANCEL)
    .getResponseText()
    .trim();
  const roundNum = Number(roundStr);
  if (!isFinite(roundNum) || roundNum < 1) return;

  const cfgSh = mustGetSheet_(ss, SHEET_SCORING_CONFIG);
  const poolSh = mustGetSheet_(ss, SHEET_PLAYERPOOL);
  const logSh = mustGetOrCreateSheet_(ss, SHEET_SCORING_LOG, SCORING_LOG_HEADERS);

  const cfg = loadScoringConfig_(cfgSh);
  const ev = cfg.find((x) => x.eventCode === eventCode);
  if (!ev) throw new Error(`EventCode not found/enabled in ScoringConfig: ${eventCode}`);

  const colHeader = `${ev.eventCode}_R${roundNum}`;

  const merged = new Map();
  let skippedTotal = 0;

  // MPO
  try {
    const mpoJson = fetchPdgaRound_(ev.tournId, "MPO", roundNum);
    const mpoParsed = parsePdgaRound_(mpoJson);
    const mpoRes = computeDivisionScores_(mpoParsed);
    skippedTotal += mpoRes.skipped;
    for (const [pdga, pts] of mpoRes.pdgaToScore.entries()) merged.set(pdga, pts);
  } catch (e) {}

  // FPO
  try {
    const fpoJson = fetchPdgaRound_(ev.tournId, "FPO", roundNum);
    const fpoParsed = parsePdgaRound_(fpoJson);
    const fpoRes = computeDivisionScores_(fpoParsed);
    skippedTotal += fpoRes.skipped;
    for (const [pdga, pts] of fpoRes.pdgaToScore.entries()) merged.set(pdga, pts);
  } catch (e) {}

  ensureOrCreatePlayerPoolColumn_(poolSh, colHeader);
  const written = writeScoresToPlayerPool_(poolSh, colHeader, merged);
  recomputeTotalsAndSortPlayerPool_(poolSh);

  logScoring_(logSh, {
    eventCode,
    division: "ALL",
    round: roundNum,
    tournId: ev.tournId,
    status: "OK",
    message: `TEST wrote ${written} merged scores into ${colHeader}`,
    written,
    skipped: skippedTotal,
    meta: { test: true },
  });

  ui.alert(`Test complete.\nWrote: ${written}\nSkipped: ${skippedTotal}\nColumn: ${colHeader}`);
}

/** -------------------------
 * Config + helpers
 * ------------------------*/

function loadScoringConfig_(cfgSh) {
  const lastRow = cfgSh.getLastRow();
  if (lastRow < 2) return [];

  const headers = cfgSh
    .getRange(1, 1, 1, cfgSh.getLastColumn())
    .getValues()[0]
    .map((h) => String(h || "").trim());

  const idx = {};
  headers.forEach((h, i) => {
    if (h) idx[h] = i;
  });

  // Require these columns (TournID can be blank for some events)
  const required = ["EventCode", "StartDate", "EndDate", "MPO_Rounds", "FPO_Rounds", "Enabled", "EventHeader"];
  for (const h of required) {
    if (idx[h] == null) throw new Error(`ScoringConfig missing header: ${h}`);
  }

  const rows = cfgSh.getRange(2, 1, lastRow - 1, cfgSh.getLastColumn()).getValues();
  const out = [];

  for (const r of rows) {
    const enabled = coerceBool_(r[idx["Enabled"]]);
    if (!enabled) continue;

    const eventCode = String(r[idx["EventCode"]] || "").trim().toUpperCase();
    const tournId = String(r[idx["TournID"]] || "").trim(); // can be blank
    const startDate = asDateOnly_(r[idx["StartDate"]]);
    const endDate = asDateOnly_(r[idx["EndDate"]]);

    const mpoRounds = Number(r[idx["MPO_Rounds"]]);
    const fpoRounds = Number(r[idx["FPO_Rounds"]]);

    const eventHeader = String(r[idx["EventHeader"]] || "").trim();

    // ✅ Allow blank TournID (still needed for scoring, but locking Team tabs can still work)
    if (!eventCode || !startDate || !endDate || !eventHeader) continue;

    out.push({
      eventCode,
      tournId,
      startDate,
      endDate,
      mpoRounds: isFinite(mpoRounds) ? mpoRounds : 0,
      fpoRounds: isFinite(fpoRounds) ? fpoRounds : 0,
      eventHeader, // NEW
    });
  }

  return out;
}

function asDateOnly_(v) {
  if (!v) return null;
  const d = v instanceof Date ? v : new Date(v);
  if (!isFinite(d.getTime())) return null;
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function dateInRange_(d, start, end) {
  return d && start && end && d.getTime() >= start.getTime() && d.getTime() <= end.getTime();
}

function dayIndexRound_(startDateOnly, todayDateOnly) {
  const msPerDay = 24 * 60 * 60 * 1000;
  const diffDays = Math.floor((todayDateOnly.getTime() - startDateOnly.getTime()) / msPerDay);
  return diffDays + 1;
}

/**
 * Returns true if PlayerPool has spill/array formulas in A2 or B2.
 * Sorting a sheet with spill formulas will "separate" computed columns from value columns.
 */
function playerPoolHasSpillFormulas_(playerPoolSh) {
  const a2 = playerPoolSh.getRange(2, 1).getFormula() || "";
  const b2 = playerPoolSh.getRange(2, 2).getFormula() || "";
  const hasFormula = (f) => String(f || "").trim().length > 0;
  const looksSpill = (f) => /ARRAYFORMULA|SORT|FILTER|QUERY|UNIQUE|IMPORTRANGE/i.test(String(f || ""));
  return (hasFormula(a2) && looksSpill(a2)) || (hasFormula(b2) && looksSpill(b2));
}

/**
 * Creates/refreshes a sorted view of PlayerPool without mutating PlayerPool itself.
 * This keeps PlayerPool safe even if it contains ARRAYFORMULA columns.
 */
function refreshPlayerPoolLeaderboardView_() {
  const ss = getSS_();
  const src = mustGetSheet_(ss, SHEET_PLAYERPOOL);

  const viewName = "Players Leaderboard"; // rename if you want
  let view = ss.getSheetByName(viewName);
  if (!view) view = ss.insertSheet(viewName);

  // Keep Row 1 as "group labels" and Row 2 as headers+sorted output
  // Clear only the old output area (row 2+ OR row 3+ depending on where you place formula)
  const lastRow = Math.max(view.getLastRow(), 1);
  const lastCol = Math.max(view.getLastColumn(), 1);

  // If you want Row 1 AND Row 2 to be decorative, put formula in A3 and clear from row 3.
  // For the simplest setup: Row 1 decorative, Row 2 is the live header row from PlayerPool.
  // So we place formula in A2 and clear from row 2 down.
  if (lastRow >= 2) {
    view.getRange(2, 1, lastRow - 1, lastCol).clearContent();
  }

  // Sort entire PlayerPool by Season_T desc, but start output at row 2
  const formula =
    `=LET(` +
    `hdr, ${SHEET_PLAYERPOOL}!1:1, ` +
    `col, MATCH("Season_T", hdr, 0), ` +
    `SORT(${SHEET_PLAYERPOOL}!A:ZZ, col, FALSE)` +
    `)`;

  view.getRange(2, 1).setFormula(formula);

  // Freeze the group-label band row
  view.setFrozenRows(1);
}

/** -------------------------
 * PDGA fetch + parse
 * ------------------------*/

function fetchPdgaRound_(tournId, division, roundNum) {
  const url =
    "https://www.pdga.com/apps/tournament/live-api/live_results_fetch_round" +
    `?TournID=${encodeURIComponent(String(tournId))}` +
    `&Division=${encodeURIComponent(String(division))}` +
    `&Round=${encodeURIComponent(String(roundNum))}`;

  const resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: { Accept: "application/json" },
  });

  const code = resp.getResponseCode();
  const text = resp.getContentText() || "";

  if (code < 200 || code >= 300) {
    throw new Error(`PDGA fetch failed (${code}) for ${division} R${roundNum}: ${text.slice(0, 300)}`);
  }

  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`PDGA response not JSON for ${division} R${roundNum}: ${text.slice(0, 300)}`);
  }
}

function parsePdgaRound_(json) {
  const data = json && json.data ? json.data : null;
  if (!data) return { pars: [], scores: [] };

  const holes = Array.isArray(data.holes) ? data.holes : [];
  const scores = Array.isArray(data.scores) ? data.scores : [];

  const pars = holes.map((h) => Number(h && (h.Par ?? h.par)));
  return { pars, scores };
}

/** -------------------------
 * Hyzerbase scoring
 * ------------------------*/

function computeDivisionScores_({ pars, scores }) {
  const pdgaToScore = new Map();
  let skipped = 0;

  for (const s of scores) {
    const pdga = String(s && (s.PDGANum ?? s.pdga ?? s.PDGA ?? "")).trim();
    const holeScores = Array.isArray(s && (s.HoleScores ?? s.holeScores ?? []))
      ? (s.HoleScores ?? s.holeScores)
      : [];

    if (!pdga) continue;

    const res = computeHyzerbasePoints_(pars, holeScores);
    if (!res.ok) {
      skipped++;
      continue;
    }

    pdgaToScore.set(pdga, res.points);
  }

  return { pdgaToScore, skipped };
}

function computeHyzerbasePoints_(pars, holeScores) {
  if (!pars || !pars.length) return { ok: false, points: 0, reason: "no_pars" };
  const n = Math.min(pars.length, holeScores.length);
  if (n === 0) return { ok: false, points: 0, reason: "no_scores" };

  let total = 0;

  for (let i = 0; i < n; i++) {
    const par = Number(pars[i]);
    const raw = holeScores[i];

    const rawStr = String(raw ?? "").trim();
    if (HOLE_SENTINELS.has(rawStr)) return { ok: false, points: 0, reason: `sentinel_${rawStr}` };

    const strokes = Number(raw);
    if (!isFinite(par) || !isFinite(strokes) || strokes <= 0) return { ok: false, points: 0, reason: "invalid" };

    if (strokes === 1) {
      total += 12;
      continue;
    }

    const diff = strokes - par;
    if (diff === -3) total += 9;
    else if (diff === -2) total += 7;
    else if (diff === -1) total += 3;
    else if (diff === 0) total += 1;
    else total += -diff; // bogey+ => negative strokes over par
  }

  return { ok: true, points: total };
}

/** -------------------------
 * PlayerPool writing
 * ------------------------*/

function ensureOrCreatePlayerPoolColumn_(playerPoolSh, headerName) {
  const header = String(headerName || "").trim();
  if (!header) throw new Error("ensureOrCreatePlayerPoolColumn_: headerName required");

  const lastCol = Math.max(1, playerPoolSh.getLastColumn());
  const headers = playerPoolSh
    .getRange(1, 1, 1, lastCol)
    .getValues()[0]
    .map((h) => String(h || "").trim());

  const existingIdx0 = headers.indexOf(header);
  if (existingIdx0 >= 0) return existingIdx0 + 1; // 1-based col

  // Append new header at far right
  const newColIdx1 = headers.length + 1;
  playerPoolSh.getRange(1, newColIdx1).setValue(header);

  return newColIdx1;
}

/**
 * Recomputes:
 *  - Each EVENT_T column = sum(EVENT_R1..EVENT_Rn) for that event code
 *  - Season_T = sum(all *_T columns excluding Season_T)
 *
 * Rules:
 *  - Only sums numeric values
 *  - Blank/non-numeric round cells count as 0
 *  - Writes numbers (not formulas)
 */
function recomputePlayerPoolTotals_(playerPoolSh) {
  const lastRow = playerPoolSh.getLastRow();
  const lastCol = playerPoolSh.getLastColumn();
  if (lastRow < 2 || lastCol < 2) return;

  const headers = playerPoolSh
    .getRange(1, 1, 1, lastCol)
    .getValues()[0]
    .map((h) => String(h || "").trim());

  // Required columns
  const seasonCol1 = headers.indexOf("Season_T") + 1;
  if (seasonCol1 <= 0) throw new Error('PlayerPool missing header "Season_T"');

  // Build: eventCode -> { tCol1, rCols1[] }
  const eventMap = new Map();

  // EVENT_T
  const tRe = /^([A-Z0-9]+)_T$/i;
  // EVENT_R#
  const rRe = /^([A-Z0-9]+)_R(\d+)$/i;

  for (let c1 = 1; c1 <= headers.length; c1++) {
    const h = headers[c1 - 1];
    if (!h) continue;

    const tm = h.match(tRe);
    if (tm) {
      const code = String(tm[1]).toUpperCase();
      if (!eventMap.has(code)) eventMap.set(code, { tCol1: 0, rCols1: [] });
      eventMap.get(code).tCol1 = c1;
      continue;
    }

    const rm = h.match(rRe);
    if (rm) {
      const code = String(rm[1]).toUpperCase();
      if (!eventMap.has(code)) eventMap.set(code, { tCol1: 0, rCols1: [] });
      eventMap.get(code).rCols1.push(c1);
      continue;
    }
  }

  // Sort round columns in numeric order for each event (R1, R2, ...)
  for (const [code, obj] of eventMap.entries()) {
    obj.rCols1.sort((a, b) => {
      const ha = headers[a - 1];
      const hb = headers[b - 1];
      const ma = ha.match(rRe);
      const mb = hb.match(rRe);
      const ra = ma ? Number(ma[2]) : 0;
      const rb = mb ? Number(mb[2]) : 0;
      return ra - rb;
    });
  }

  // Read full body once
  const rowCount = lastRow - 1;
  const bodyRange = playerPoolSh.getRange(2, 1, rowCount, lastCol);
  const body = bodyRange.getValues(); // 2D

  // Prepare writes
  // We'll edit body[][] in memory, then write back only affected columns to minimize churn.
  const colsToWrite = new Set();

  // 1) EVENT_T recompute
  for (const [code, obj] of eventMap.entries()) {
    const tCol1 = obj.tCol1;
    const rCols1 = obj.rCols1;

    // Only compute if we have a T column AND at least one round column
    if (!tCol1 || !rCols1.length) continue;

    const tIdx0 = tCol1 - 1;
    const rIdx0s = rCols1.map((c1) => c1 - 1);

    for (let r0 = 0; r0 < body.length; r0++) {
      let sum = 0;
      for (const ci0 of rIdx0s) {
        const v = body[r0][ci0];
        const n = typeof v === "number" ? v : Number(String(v || "").trim());
        if (isFinite(n)) sum += n;
      }
      body[r0][tIdx0] = sum;
    }

    colsToWrite.add(tCol1);
  }

  // 2) Season_T recompute as sum of all *_T (excluding Season_T)
  const tCols1 = [];
  for (let c1 = 1; c1 <= headers.length; c1++) {
    const h = headers[c1 - 1];
    if (!h) continue;
    if (h === "Season_T") continue;
    if (tRe.test(h)) tCols1.push(c1);
  }

  const seasonIdx0 = seasonCol1 - 1;
  const tIdx0s = tCols1.map((c1) => c1 - 1);

  for (let r0 = 0; r0 < body.length; r0++) {
    let sum = 0;
    for (const ci0 of tIdx0s) {
      const v = body[r0][ci0];
      const n = typeof v === "number" ? v : Number(String(v || "").trim());
      if (isFinite(n)) sum += n;
    }
    body[r0][seasonIdx0] = sum;
  }
  colsToWrite.add(seasonCol1);

  // Write back only the columns we changed
  // (We’ll batch contiguous columns into fewer setValues calls)
  const sortedCols = Array.from(colsToWrite).sort((a, b) => a - b);

  // helper to write a column block [startCol..endCol]
  function writeBlock_(startCol1, endCol1) {
    const width = endCol1 - startCol1 + 1;
    const out = body.map((row) => row.slice(startCol1 - 1, endCol1));
    playerPoolSh.getRange(2, startCol1, rowCount, width).setValues(out);
  }

  // merge contiguous runs
  let runStart = null;
  let prev = null;
  for (const c1 of sortedCols) {
    if (runStart == null) {
      runStart = c1;
      prev = c1;
      continue;
    }
    if (c1 === prev + 1) {
      prev = c1;
      continue;
    }
    writeBlock_(runStart, prev);
    runStart = c1;
    prev = c1;
  }
  if (runStart != null) writeBlock_(runStart, prev);
}

/**
 * Convenience wrapper: recompute totals then (optionally) sort.
 * If PlayerPool contains spill formulas (ARRAYFORMULA/etc), DO NOT sort it.
 * Instead, refresh a separate Leaderboard view.
 */
function recomputeTotalsAndSortPlayerPool_(playerPoolSh) {
  recomputePlayerPoolTotals_(playerPoolSh);

  if (playerPoolHasSpillFormulas_(playerPoolSh)) {
    // DO NOT sort PlayerPool — it will detach spill columns from value columns.
    // Keep PlayerPool stable and refresh a sorted view instead.
    try {
      refreshPlayerPoolLeaderboardView_();
    } catch (e) {
      // non-fatal
      Logger.log("Leaderboard refresh failed: " + (e && e.message ? e.message : e));
    }
    return;
  }

  // Safe to sort if A/B are normal values (not spill formulas)
  sortPlayerPoolBySeasonTotal_(playerPoolSh);
}

/**
 * Sort PlayerPool:
 *  1) Season_T (desc)
 *  2) PDGA # (asc)
 */
function sortPlayerPoolBySeasonTotal_(playerPoolSh) {
  const lastRow = playerPoolSh.getLastRow();
  const lastCol = playerPoolSh.getLastColumn();
  if (lastRow < 3 || lastCol < 2) return;

  const headers = playerPoolSh
    .getRange(1, 1, 1, lastCol)
    .getValues()[0]
    .map((h) => String(h || "").trim());

  const seasonCol1 = headers.indexOf("Season_T") + 1;
  if (seasonCol1 <= 0) throw new Error('PlayerPool missing header "Season_T"');

  const pdgaCol1 = findPlayerPoolPdgaCol1_(headers);
  if (pdgaCol1 <= 0) throw new Error('PlayerPool missing PDGA header (expected "PDGA #")');

  // Sort rows 2..lastRow across all columns
  const range = playerPoolSh.getRange(2, 1, lastRow - 1, lastCol);
  range.sort([
    { column: seasonCol1, ascending: false },
    { column: pdgaCol1, ascending: true },
  ]);
}

/**
 * Robustly locate the PlayerPool PDGA column (1-based), allowing minor header variations.
 */
function findPlayerPoolPdgaCol1_(headers) {
  const candidates = ["PDGA #", "Player PDGA #", "PDGA", "PDGA#", "PDGANum", "PDGA Num"];
  for (const c of candidates) {
    const idx0 = headers.indexOf(c);
    if (idx0 >= 0) return idx0 + 1;
  }
  return 0;
}

function writeScoresToPlayerPool_(playerPoolSh, headerName, pdgaToScore) {
  const lastRow = playerPoolSh.getLastRow();
  if (lastRow < 2) return 0;

  // Re-read headers AFTER potential column creation
  const lastCol = Math.max(1, playerPoolSh.getLastColumn());
  const headers = playerPoolSh
    .getRange(1, 1, 1, lastCol)
    .getValues()[0]
    .map((h) => String(h || "").trim());

  const pdgaColIdx1 = findPlayerPoolPdgaCol1_(headers);
  if (pdgaColIdx1 <= 0) throw new Error('PlayerPool missing PDGA header (expected "PDGA #")');

  const targetColIdx1 = headers.indexOf(String(headerName || "").trim()) + 1;
  if (targetColIdx1 <= 0) throw new Error(`Could not find scoring column "${headerName}" after creation`);

  const rowCount = lastRow - 1;

  const pdgas = playerPoolSh
    .getRange(2, pdgaColIdx1, rowCount, 1)
    .getValues()
    .map((r) => String(r[0] || "").trim());

  // Read existing values so we only update rows that have parsed scores
  const existing = playerPoolSh.getRange(2, targetColIdx1, rowCount, 1).getValues();

  let written = 0;
  for (let i = 0; i < pdgas.length; i++) {
    const pdga = pdgas[i];
    if (!pdga) continue;

    if (!pdgaToScore.has(pdga)) continue; // ✅ do not overwrite if PDGA missing/invalid
    existing[i][0] = pdgaToScore.get(pdga);
    written++;
  }

  playerPoolSh.getRange(2, targetColIdx1, rowCount, 1).setValues(existing);
  return written;
}

/** -------------------------
 * Logging
 * ------------------------*/

function logScoring_(logSh, { eventCode, division, round, tournId, status, message, written, skipped, meta }) {
  // Ensure headers exist (first time / if someone deleted them)
  const a1 = String(logSh.getRange(1, 1).getValue() || "").trim();
  if (a1 !== "Timestamp") {
    logSh.getRange(1, 1, 1, SCORING_LOG_HEADERS.length).setValues([SCORING_LOG_HEADERS]);
  }

  const row = [
    new Date(),
    eventCode || "",
    division || "",
    String(round ?? ""),
    tournId || "",
    status || "",
    message || "",
    Number(written || 0),
    Number(skipped || 0),
    meta ? JSON.stringify(meta) : "",
  ];
  logSh.appendRow(row);
}

function lockTeamEventColumnToScoring_(ss, eventCode, eventHeader) {
  const code = String(eventCode || "").trim().toUpperCase();
  const header = String(eventHeader || "").trim();
  if (!code) throw new Error("Missing eventCode");
  if (!header) throw new Error(`Missing EventHeader for ${code}`);

  // One-time guard per event
  const props = PropertiesService.getScriptProperties();
  const guardKey = `TEAMCOL_LOCKED_${code}`;
  if (props.getProperty(guardKey) === "1") return;

  const SKIP = new Set([
    "MPO","FPO","DraftBoard","Transactions","Rosters","Config","PlayerPool","ScoringConfig","ScoringLog",
    "WebhookLog","WaiverRequests","WaiverAwardsLog","Standings","AlertSubscriptions","SmsLog","LineupRemindersLog",
    "Players Leaderboard"
  ]);

  const headerRow = 2;
  const dataStartRow = 3;
  const dataEndRow = 12;
  const rowCount = dataEndRow - dataStartRow + 1;

  const tColName = `${code}_T`; // ex: SFO_T

  for (const sh of ss.getSheets()) {
    if (SKIP.has(sh.getName())) continue;

    const lastCol = sh.getLastColumn();
    if (lastCol < 4) continue;

    const headers = sh
      .getRange(headerRow, 1, 1, lastCol)
      .getValues()[0]
      .map((h) => String(h || "").trim());

    const eventCol1 = headers.indexOf(header) + 1;
    if (eventCol1 <= 0) continue;

    const formulas = [];
    for (let r = dataStartRow; r <= dataEndRow; r++) {
      const f =
        `=IF($C${r}="","",IFERROR(` +
        `INDEX(PlayerPool!$A:$ZZ,` +
        `MATCH($C${r},PlayerPool!$B:$B,0),` +
        `MATCH("${tColName}",PlayerPool!$1:$1,0)` +
        `),` +
        `""` +
        `))`;
      formulas.push([f]);
    }

    sh.getRange(dataStartRow, eventCol1, rowCount, 1).setFormulas(formulas);
  }

  props.setProperty(guardKey, "1");
}

/**
 * Resets the one-time "Team Tab Event Column lock" guard for a given event code.
 * Run once if you change EventHeader text, add new team tabs, or want to re-apply formulas.
 */
function resetTeamLockForEventCode(eventCode) {
  const code = String(eventCode || "").trim().toUpperCase();
  if (!code) throw new Error("resetTeamLockForEventCode: eventCode required");
  PropertiesService.getScriptProperties().deleteProperty(`TEAMCOL_LOCKED_${code}`);
}

/***********************
 * 14) TRIGGERS (AUTO REFRESH + SCORING)
 ***********************/

function createAutoRefreshTriggers() {
  deleteFdgTriggers_();

  const hours = [0, 6, 12, 18];
  for (const h of hours) {
    ScriptApp.newTrigger("updateAllEvents")
      .timeBased()
      .everyDays(1)
      .atHour(h)
      .nearMinute(0)
      .create();
  }
  Logger.log("Created triggers for updateAllEvents at hours: " + hours.join(", "));
}

function deleteFdgTriggers_() {
  // Only deletes the registration refresh triggers.
  // (Scoring triggers are managed separately below.)
  const handlersToRemove = new Set(["updateAllEvents"]);
  const triggers = ScriptApp.getProjectTriggers();

  let removed = 0;
  for (const t of triggers) {
    const fn = t.getHandlerFunction();
    if (handlersToRemove.has(fn)) {
      ScriptApp.deleteTrigger(t);
      removed++;
    }
  }

  Logger.log("Removed " + removed + " existing FDG triggers.");
}

/***********************
 * 14B) SCORING TRIGGERS
 ***********************/

function createScoringTrigger() {
  // Remove any existing scoring triggers first (avoid duplicates)
  deleteScoringTrigger();

  // Runs daily at ~11:05 PM (script timezone must be America/New_York)
  ScriptApp.newTrigger("scoreNightlyPdgaLive")
    .timeBased()
    .everyDays(1)
    .atHour(23)
    .nearMinute(5)
    .create();

  Logger.log("Created nightly scoring trigger for scoreNightlyPdgaLive at ~11:05 PM.");
}

function deleteScoringTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  let removed = 0;

  for (const t of triggers) {
    if (t.getHandlerFunction() === "scoreNightlyPdgaLive") {
      ScriptApp.deleteTrigger(t);
      removed++;
    }
  }

  Logger.log("Removed " + removed + " scoring trigger(s).");
}

function runScoringNow() {
  // Convenience wrapper so the menu label is clear
  scoreNightlyPdgaLive();
}

/***********************
 * 14.9) LINEUPS (Manual Hyzerbase lineup capture)
 *
 * Requires sheets:
 * - Lineups
 * - LineupHistory
 * - PlayerPool (headers include SFO_T, BEO_T, etc.)
 * - Standings (Team Name, Season Total, and event header columns like "Supreme Flight Open")
 ***********************/

const SHEET_LINEUPS = "Lineups";
const SHEET_LINEUP_HISTORY = "LineupHistory";
// NOTE: SHEET_PLAYERPOOL already defined above
// NOTE: SHEET_STANDINGS already defined above

// ---- TEAM TAB UI RANGES (Lineup block starts at J15) ----
const TEAMTAB_NEXT_EVENTCODE_A1 = "J15";        // EventCode value cell
const TEAMTAB_LINEUP_SLOT_RANGE_A1 = "J17:J22"; // slots 1..6
const TEAMTAB_LINEUP_NAME_RANGE_A1 = "K17:K22"; // dropdown: names
const TEAMTAB_LINEUP_PDGA_RANGE_A1 = "L17:L22"; // auto-filled PDGA
const TEAMTAB_ROSTER_NAME_RANGE_A1 = "B3:B12";  // roster names
const TEAMTAB_ROSTER_PDGA_RANGE_A1 = "C3:C12";  // roster pdgas

function setupTeamTabs_LineupBlock_J15() {
  const ss = getSS_();
  const teams = canonTeamsNoFA_();

  for (const team of teams) {
    const teamSh = getTeamSheetForCanon_(ss, team);
    if (!teamSh) continue;

    // Clear the block area (now includes Points col M)
    teamSh.getRange("J15:M22").clearContent().clearDataValidations();

    /***********************
     * NEW: STYLING (baked-in)
     ***********************/
    const HEADER_GREEN = "#63d297";
    const LIGHT_GREEN = "#e7f9ef";

    // K15 says "Lineup"
    teamSh.getRange("K15").setValue("Lineup").setFontWeight("bold");

    // J15:L15 background = header green
    teamSh.getRange("J15:L15").setBackground(HEADER_GREEN);

    // M15, J17:M17, J19:M19, J21:M21 = light green
    teamSh.getRange("M15").setBackground(LIGHT_GREEN);
    teamSh.getRangeList(["J17:M17", "J19:M19", "J21:M21"]).setBackground(LIGHT_GREEN);

    // J18:M18, J20:M20, J22:M22 = white
    teamSh.getRangeList(["J18:M18", "J20:M20", "J22:M22"]).setBackground("#ffffff");

    /***********************
     * EXISTING CONTENT BUILD
     ***********************/

    // Event code cell (J15) - keep as your eventCode value cell
    // (If you want J15 blank, leave it. If you want it bold, keep it.)
    teamSh.getRange("J15").setValue("").setFontWeight("bold");

    // Total label + total formula (L15 and M15)
    teamSh.getRange("L15").setValue("Total:").setFontWeight("bold");
    teamSh.getRange("M15")
      .setFormula(`=IF(COUNTA($M$17:$M$22)=0,"",SUM($M$17:$M$22))`)
      .setFontWeight("bold");

    // Headers (Row 16)
    teamSh.getRange("J16").setValue("Lineup Slot").setFontWeight("bold");
    teamSh.getRange("K16").setValue("Player Name").setFontWeight("bold");
    teamSh.getRange("L16").setValue("PDGA #").setFontWeight("bold");
    teamSh.getRange("M16").setValue("Points").setFontWeight("bold");

    // Slot numbers 1..6
    teamSh.getRange(TEAMTAB_LINEUP_SLOT_RANGE_A1)
      .setValues([[1],[2],[3],[4],[5],[6]])
      .setFontWeight("bold");

    // Dropdown on Player Name cells (K17:K22) from roster names (B3:B12)
    const dvNames = SpreadsheetApp.newDataValidation()
      .requireValueInRange(teamSh.getRange(TEAMTAB_ROSTER_NAME_RANGE_A1), true)
      .setAllowInvalid(false)
      .build();
    teamSh.getRange(TEAMTAB_LINEUP_NAME_RANGE_A1).setDataValidation(dvNames);

    // PDGA auto-fill formulas in L17:L22
    const pdgaFormulas = [];
    for (let r = 17; r <= 22; r++) {
      pdgaFormulas.push([
        `=IF($K${r}="","",IFERROR(INDEX($C$3:$C$12, MATCH($K${r},$B$3:$B$12,0)),""))`
      ]);
    }
    teamSh.getRange(TEAMTAB_LINEUP_PDGA_RANGE_A1).setFormulas(pdgaFormulas);

    // Points formulas in M17:M22 (pulls EVENT_T from PlayerPool using J15 event code)
    const pointsFormulas = [];
    for (let r = 17; r <= 22; r++) {
      pointsFormulas.push([
        `=IF($L${r}="","",IFERROR(` +
          `INDEX(PlayerPool!$A:$ZZ,` +
          `MATCH($L${r},PlayerPool!$B:$B,0),` +
          `MATCH($J$15&"_T",PlayerPool!$1:$1,0)` +
          `),` +
          `""` +
        `))`
      ]);
    }
    teamSh.getRange("M17:M22").setFormulas(pointsFormulas);
  }
}

/**
 * Helper: convert 1-based column index -> A1 letter (e.g., 1->A, 27->AA)
 */
function colLetter_(col1) {
  let n = Number(col1);
  let s = "";
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

// Your Lineups sheet headers (exact)
const LINEUPS_HEADERS = [
  "EventCode","EventHeader","Team","Status",
  "SubmittedAt","LockedAt","FinalizedAt",
  "Slot1_Name","Slot1_PDGA",
  "Slot2_Name","Slot2_PDGA",
  "Slot3_Name","Slot3_PDGA",
  "Slot4_Name","Slot4_PDGA",
  "Slot5_Name","Slot5_PDGA",
  "Slot6_Name","Slot6_PDGA",
  "MetaJson"
];

// Your LineupHistory sheet headers (exact)
const LINEUP_HISTORY_HEADERS = [
  "EventCode","Team","FinalizedAt",
  "Slot1_Name","Slot1_PDGA",
  "Slot2_Name","Slot2_PDGA",
  "Slot3_Name","Slot3_PDGA",
  "Slot4_Name","Slot4_PDGA",
  "Slot5_Name","Slot5_PDGA",
  "Slot6_Name","Slot6_PDGA",
  "TeamEventTotal","MetaJson"
];

// Reverse map: Canonical team name -> team tab code
const TEAM_TABCODE_BY_CANON = {
  "Sir Krontzalot": "SIR",
  "Exalted Evil": "EXA",
  "Tree Ninja Disc Golf": "TRE",
  "The Abba Zabba": "THE",
  "Ryan Morgan": "RYA",
  "SPY Dyes": "SPY",
  "Eddie Speidel": "EDD",
  "Webb Webb Webb": "WEB",
  "Hughes Moves": "HUG",
  "Matthew Lopez": "MAT",
};

function ensureLineupSheets_() {
  const ss = getSS_();
  mustGetOrCreateSheet_(ss, SHEET_LINEUPS, LINEUPS_HEADERS);
  mustGetOrCreateSheet_(ss, SHEET_LINEUP_HISTORY, LINEUP_HISTORY_HEADERS);
  mustGetSheet_(ss, SHEET_PLAYERPOOL);
  mustGetSheet_(ss, SHEET_STANDINGS);
}

function canonTeamsNoFA_() {
  return CANON_TEAMS.filter((t) => t && String(t).trim() && t !== FREE_AGENT);
}

function getTeamSheetForCanon_(ss, canonTeam) {
  // Try full-name tab first (if you ever rename tabs later)
  let sh = ss.getSheetByName(canonTeam);
  if (sh) return sh;

  // Then try your actual code tabs: SIR/EXA/...
  const code = TEAM_TABCODE_BY_CANON[canonTeam];
  if (code) {
    sh = ss.getSheetByName(code);
    if (sh) return sh;
  }

  // Last resort: normalize + try again
  const norm = normalizeTeam_(canonTeam);
  if (norm && norm !== canonTeam) {
    sh = ss.getSheetByName(norm) || ss.getSheetByName(TEAM_TABCODE_BY_CANON[norm] || "");
    if (sh) return sh;
  }

  return null;
}

function getEventHeaderForCode_(eventCode) {
  const ss = getSS_();
  const code = String(eventCode || "").trim().toUpperCase();
  if (!code) return "";

  try {
    const cfgSh = ss.getSheetByName("ScoringConfig");
    if (cfgSh) {
      const cfg = loadScoringConfig_(cfgSh); // existing helper in your backend
      const ev = cfg.find((x) => x.eventCode === code);
      if (ev && ev.eventHeader) return String(ev.eventHeader).trim();
    }
  } catch (e) {}

  return code;
}

function getPlayerPoolNameByPdga_(poolSh, pdgaStr) {
  if (!pdgaStr) return "";
  const map = getHeaderIndexMap_(poolSh);

  const pdgaCol = map["PDGA #"] ?? map["PDGA"];
  const nameCol = map["Player Name"] ?? map["Name"];

  if (pdgaCol == null || nameCol == null) return "";

  const last = poolSh.getLastRow();
  if (last < 2) return "";

  const pdgaVals = poolSh.getRange(2, pdgaCol + 1, last - 1, 1).getValues().flat();
  const idx = pdgaVals.findIndex((v) => String(v || "").trim() === String(pdgaStr).trim());
  if (idx < 0) return "";

  const name = poolSh.getRange(2 + idx, nameCol + 1).getValue();
  return String(name || "").trim();
}

function getPlayerPoolEventPtsByPdga_(poolSh, pdgaStr, eventCode) {
  if (!pdgaStr || !eventCode) return 0;

  const map = getHeaderIndexMap_(poolSh);
  const ptsHeader = String(eventCode).trim().toUpperCase() + "_T";
  const ptsCol = map[ptsHeader];
  if (ptsCol == null) return 0;

  const pdgaCol = map["PDGA #"] ?? map["PDGA"];
  if (pdgaCol == null) return 0;

  const last = poolSh.getLastRow();
  if (last < 2) return 0;

  const pdgaVals = poolSh.getRange(2, pdgaCol + 1, last - 1, 1).getValues().flat();
  const idx = pdgaVals.findIndex((v) => String(v || "").trim() === String(pdgaStr).trim());
  if (idx < 0) return 0;

  const v = poolSh.getRange(2 + idx, ptsCol + 1).getValue();
  const n = Number(v);
  return isFinite(n) ? n : 0;
}

function findLineupsRow_(lineupsSh, eventCode, teamName) {
  const last = lineupsSh.getLastRow();
  if (last < 2) return -1;

  const map = getHeaderIndexMap_(lineupsSh);
  const ecCol = map["EventCode"];
  const teamCol = map["Team"];
  if (ecCol == null || teamCol == null) throw new Error("Lineups missing EventCode or Team headers");

  const rows = lineupsSh.getRange(2, 1, last - 1, lineupsSh.getLastColumn()).getValues();
  for (let i = 0; i < rows.length; i++) {
    const ec = String(rows[i][ecCol] || "").trim().toUpperCase();
    const tm = String(rows[i][teamCol] || "").trim();
    if (ec === String(eventCode).trim().toUpperCase() && tm === String(teamName).trim()) {
      return 2 + i;
    }
  }
  return -1;
}

function upsertLineupsRow_(lineupsSh, rowObj) {
  const map = getHeaderIndexMap_(lineupsSh);
  const cols = lineupsSh.getLastColumn();

  const eventCode = String(rowObj.EventCode || "").trim().toUpperCase();
  const team = String(rowObj.Team || "").trim();
  if (!eventCode || !team) throw new Error("upsertLineupsRow_: EventCode and Team required");

  const existingRow = findLineupsRow_(lineupsSh, eventCode, team);

  const row = new Array(cols).fill("");
  for (const [k, v] of Object.entries(rowObj)) {
    const c = map[k];
    if (c != null) row[c] = v;
  }

  if (existingRow > 0) {
    lineupsSh.getRange(existingRow, 1, 1, cols).setValues([row]);
  } else {
    lineupsSh.appendRow(row);
  }
}

function updateStandingsForEvent_(eventHeader, teamTotalsMap) {
  const ss = getSS_();
  const sh = mustGetSheet_(ss, SHEET_STANDINGS);
  const map = getHeaderIndexMap_(sh);

  const teamCol = map["Team Name"];
  const seasonCol = map["Season Total"];
  const eventCol = map[eventHeader];

  if (teamCol == null) throw new Error("Standings missing 'Team Name' header.");
  if (seasonCol == null) throw new Error("Standings missing 'Season Total' header.");
  if (eventCol == null) throw new Error(`Standings missing event column header: '${eventHeader}'`);

  const lastRow = sh.getLastRow();
  if (lastRow < 2) throw new Error("Standings has no team rows.");

  const headerRow = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0].map(h => String(h||"").trim());
  const nonEventHeaders = new Set(["Standings", "Team Name", "Season Total"]);
  const eventCols = [];
  for (let c = 0; c < headerRow.length; c++) {
    if (!headerRow[c]) continue;
    if (nonEventHeaders.has(headerRow[c])) continue;
    eventCols.push(c);
  }

  const data = sh.getRange(2, 1, lastRow - 1, sh.getLastColumn()).getValues();

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const team = String(row[teamCol] || "").trim();
    if (!team) continue;

    if (teamTotalsMap.has(team)) {
      row[eventCol] = teamTotalsMap.get(team);

      let season = 0;
      for (const c of eventCols) {
        const n = Number(row[c]);
        if (isFinite(n)) season += n;
      }
      row[seasonCol] = season;
    }
  }

  sh.getRange(2, 1, data.length, sh.getLastColumn()).setValues(data);
}

/**
 * LOCK lineups for EventCode:
 * - reads K15 + K18:K23 from each team tab
 * - validates K18:K23 are all on roster C3:C12
 * - upserts into Lineups as LOCKED
 */
function lockLineupsForEventCode(eventCode) {
  ensureLineupSheets_();
  const ss = getSS_();
  const poolSh = mustGetSheet_(ss, SHEET_PLAYERPOOL);
  const lineupsSh = mustGetSheet_(ss, SHEET_LINEUPS);

  const code = String(eventCode || "").trim().toUpperCase();
  if (!code) throw new Error("lockLineupsForEventCode: eventCode required");

  const eventHeader = getEventHeaderForCode_(code);
  const now = new Date();
  const teams = canonTeamsNoFA_();

  const errors = [];

  for (const team of teams) {
    const teamSh = getTeamSheetForCanon_(ss, team);
    if (!teamSh) {
      errors.push(`Missing Team tab for: ${team} (expected ${TEAM_TABCODE_BY_CANON[team] || team})`);
      continue;
    }

    const tabCode = String(teamSh.getRange(TEAMTAB_NEXT_EVENTCODE_A1).getValue() || "").trim().toUpperCase();
    if (tabCode && tabCode !== code) {
      errors.push(`${team}: Team tab shows EventCode=${tabCode} in ${TEAMTAB_NEXT_EVENTCODE_A1} but you tried to lock ${code}`);
      continue;
    }

    const lineupPdgas = teamSh.getRange(TEAMTAB_LINEUP_PDGA_RANGE_A1).getValues().flat()
      .map((v) => String(v || "").trim())
      .filter((v) => v);

    if (lineupPdgas.length !== 6) {
      errors.push(`${team}: lineup must have exactly 6 PDGA selections (found ${lineupPdgas.length})`);
      continue;
    }

    const rosterPdgas = new Set(
      teamSh.getRange(TEAMTAB_ROSTER_PDGA_RANGE_A1).getValues().flat()
        .map((v) => String(v || "").trim())
        .filter((v) => v)
    );

    for (const pdga of lineupPdgas) {
      if (!rosterPdgas.has(pdga)) {
        errors.push(`${team}: lineup PDGA ${pdga} not found on roster range ${TEAMTAB_ROSTER_PDGA_RANGE_A1}`);
      }
    }
    if (errors.some((x) => x.startsWith(team + ":"))) continue;

    const names = lineupPdgas.map((pdga) => getPlayerPoolNameByPdga_(poolSh, pdga));

    upsertLineupsRow_(lineupsSh, {
      EventCode: code,
      EventHeader: eventHeader,
      Team: team,
      Status: "LOCKED",
      SubmittedAt: now,
      LockedAt: now,
      FinalizedAt: "",
      Slot1_Name: names[0], Slot1_PDGA: lineupPdgas[0],
      Slot2_Name: names[1], Slot2_PDGA: lineupPdgas[1],
      Slot3_Name: names[2], Slot3_PDGA: lineupPdgas[2],
      Slot4_Name: names[3], Slot4_PDGA: lineupPdgas[3],
      Slot5_Name: names[4], Slot5_PDGA: lineupPdgas[4],
      Slot6_Name: names[5], Slot6_PDGA: lineupPdgas[5],
      MetaJson: JSON.stringify({ source: "TeamTab", eventCodeCell: TEAMTAB_NEXT_EVENTCODE_A1, lineupRange: TEAMTAB_LINEUP_PDGA_RANGE_A1 })
    });
  }

  if (errors.length) {
    throw new Error("Lineup lock errors:\n- " + errors.join("\n- "));
  }
}

/**
 * FINALIZE event:
 * - reads LOCKED Lineups rows for EventCode
 * - pulls each player’s EventCode_T from PlayerPool
 * - totals, appends to LineupHistory
 * - marks Lineups FINALIZED
 * - writes Standings[EventHeader] + recomputes Season Total
 */
function finalizeLineupsForEventCode(eventCode) {
  ensureLineupSheets_();
  const ss = getSS_();
  const poolSh = mustGetSheet_(ss, SHEET_PLAYERPOOL);
  const lineupsSh = mustGetSheet_(ss, SHEET_LINEUPS);
  const histSh = mustGetSheet_(ss, SHEET_LINEUP_HISTORY);

  const code = String(eventCode || "").trim().toUpperCase();
  if (!code) throw new Error("finalizeLineupsForEventCode: eventCode required");

  const eventHeader = getEventHeaderForCode_(code);

  const map = getHeaderIndexMap_(lineupsSh);
  const last = lineupsSh.getLastRow();
  if (last < 2) throw new Error("No Lineups rows found.");

  const rows = lineupsSh.getRange(2, 1, last - 1, lineupsSh.getLastColumn()).getValues();
  const now = new Date();

  let finalizedCount = 0;
  const teamTotals = new Map();

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const ec = String(r[map["EventCode"]] || "").trim().toUpperCase();
    const status = String(r[map["Status"]] || "").trim().toUpperCase();
    if (ec !== code) continue;
    if (status !== "LOCKED") continue;

    const team = String(r[map["Team"]] || "").trim();
    if (!team) continue;

    const slots = [
      { name: r[map["Slot1_Name"]], pdga: r[map["Slot1_PDGA"]] },
      { name: r[map["Slot2_Name"]], pdga: r[map["Slot2_PDGA"]] },
      { name: r[map["Slot3_Name"]], pdga: r[map["Slot3_PDGA"]] },
      { name: r[map["Slot4_Name"]], pdga: r[map["Slot4_PDGA"]] },
      { name: r[map["Slot5_Name"]], pdga: r[map["Slot5_PDGA"]] },
      { name: r[map["Slot6_Name"]], pdga: r[map["Slot6_PDGA"]] },
    ].map((s) => ({ name: String(s.name || "").trim(), pdga: String(s.pdga || "").trim() }));

    for (const s of slots) {
      if (!s.name && s.pdga) s.name = getPlayerPoolNameByPdga_(poolSh, s.pdga);
    }

    const pts = slots.map((s) => getPlayerPoolEventPtsByPdga_(poolSh, s.pdga, code));
    const total = pts.reduce((a, b) => a + b, 0);

    histSh.appendRow([
      code, team, now,
      slots[0].name, slots[0].pdga,
      slots[1].name, slots[1].pdga,
      slots[2].name, slots[2].pdga,
      slots[3].name, slots[3].pdga,
      slots[4].name, slots[4].pdga,
      slots[5].name, slots[5].pdga,
      total,
      JSON.stringify({ points: pts, ptsHeader: code + "_T" })
    ]);

    const sheetRow = 2 + i;
    lineupsSh.getRange(sheetRow, map["Status"] + 1).setValue("FINALIZED");
    lineupsSh.getRange(sheetRow, map["FinalizedAt"] + 1).setValue(now);

    teamTotals.set(team, total);
    finalizedCount++;
  }

  if (!finalizedCount) {
    throw new Error(`No LOCKED Lineups found for EventCode=${code}. Nothing finalized.`);
  }

  updateStandingsForEvent_(eventHeader, teamTotals);
}

// UI prompts (these get called by your menu)
function uiLockLineupsPrompt() {
  const ui = SpreadsheetApp.getUi();
  const res = ui.prompt("Lock Lineups", "EventCode (ex: SFO)", ui.ButtonSet.OK_CANCEL);
  if (res.getSelectedButton() !== ui.Button.OK) return;
  lockLineupsForEventCode(String(res.getResponseText() || "").trim());
  ui.alert("Locked lineups for " + String(res.getResponseText() || "").trim().toUpperCase());
}

function uiFinalizeLineupsPrompt() {
  const ui = SpreadsheetApp.getUi();
  const res = ui.prompt("Finalize Lineups", "EventCode (ex: SFO)", ui.ButtonSet.OK_CANCEL);
  if (res.getSelectedButton() !== ui.Button.OK) return;
  finalizeLineupsForEventCode(String(res.getResponseText() || "").trim());
  ui.alert("Finalized lineups for " + String(res.getResponseText() || "").trim().toUpperCase());
}

/***********************
 * 15) MENU
 ***********************/

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("League Tools")
    .addItem("Refresh Registrations (Config)", "updateAllEvents")
    .addItem("Refresh StatMando Ranks (MPO/FPO)", "updateAllStatMandoRanks")
    .addItem("Sort MPO/FPO by Rank (D, E, A)", "sortMpoFpoByRanks")
    .addItem("Debug: Registration Parse (set URL in code)", "debugRegistrationParse")
    .addSeparator()
    .addItem("Rebuild Rosters (Draft + Transactions)", "rebuildRosters")
    .addSeparator()
    // --- SCORING MENU ITEMS ---
    .addItem("Run PDGA Scoring Now (Hyzerbase)", "runScoringNow")
    .addItem("Test PDGA Scoring (Prompt)", "testScoringPrompt")
    .addItem("Create Nightly Scoring Trigger (11:05 PM ET)", "createScoringTrigger")
    .addItem("Delete Nightly Scoring Trigger", "deleteScoringTrigger")
    .addSeparator()
    // --- EXISTING AUTO REFRESH TRIGGERS ---
    .addItem("Create Auto Refresh Triggers (6/12/6/12)", "createAutoRefreshTriggers")
    .addSeparator()
    .addItem("Lineups: Lock (Prompt EventCode)", "uiLockLineupsPrompt")
    .addItem("Lineups: Finalize (Prompt EventCode)", "uiFinalizeLineupsPrompt")
    .addToUi();
}
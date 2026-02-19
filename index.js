require("dotenv").config();

const fetch =
  global.fetch ||
  ((...args) => import("node-fetch").then(({ default: f }) => f(...args)));

const { Client, GatewayIntentBits, Events } = require("discord.js");
const cron = require("node-cron");
const { DateTime } = require("luxon");

// =====================================================
// Global crash guards (do NOT reference `client` here)
// =====================================================
process.on("unhandledRejection", (err) => console.error("UnhandledRejection:", err));
process.on("uncaughtException", (err) => console.error("UncaughtException:", err));

// =====================================================
// Config / Constants
// =====================================================
const TEAM_NAMES = new Set([
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
]);

// Match what your Sheets ownership formula outputs (singular)
const FREE = "Free Agent";

// Waiver Awards Scheduler (12:00 PM ET on specific dates)
const WAIVER_AWARD_DATES = [
  { event: "Supreme Flight Open", date: "2026-03-03" },
  { event: "Big Easy Open", date: "2026-03-17" },
  { event: "Queen City Classic", date: "2026-03-31" },
  { event: "PDGA Champions Cup", date: "2026-04-14" },
  { event: "Jonesboro Open", date: "2026-04-21" },
  { event: "Kansas City Wide Open", date: "2026-04-28" },
  { event: "Waco Annual Charity Open", date: "2026-05-05" },
  { event: "The Open at Austin", date: "2026-05-12" },
  { event: "OTB Open", date: "2026-05-26" },
  { event: "Cascade Challenge", date: "2026-06-02" },
  { event: "Northwest Championship", date: "2026-06-09" },
  { event: "European Open", date: "2026-06-23" },
  { event: "Swedish Open", date: "2026-06-30" },
  { event: "Ale Open", date: "2026-07-07" },
  { event: "Heinola Open", date: "2026-07-14" },
  { event: "U.S. Women’s Disc Golf Championship", date: "2026-07-21" },
  { event: "Champions Landing Open", date: "2026-07-28" },
  { event: "Ledgestone Open", date: "2026-08-04" },
  { event: "Discmania Challenge", date: "2026-08-11" },
  { event: "Preserve Championship", date: "2026-08-18" },
  { event: "PDGA Pro World Championships", date: "2026-09-01" },
  { event: "Idlewild Open", date: "2026-09-08" },
  { event: "Green Mountain Championship", date: "2026-09-22" },
  { event: "MVP Open", date: "2026-09-29" },
  {
    event: "United States and Throw Pink Women's Disc Golf Championship",
    date: "2026-10-13",
  },
];

// =====================================================
// Lineup Reminder Schedule (12:00 PM ET day-before Round 1)
// =====================================================
const LINEUP_REMINDER_DATES = [
  { event: "Supreme Flight Open", date: "2026-02-26" },
  { event: "Big Easy Open", date: "2026-03-12" },
  { event: "Queen City Classic", date: "2026-03-26" },
  { event: "PDGA Champions Cup", date: "2026-04-08" },
  { event: "Jonesboro Open", date: "2026-04-16" },
  { event: "Kansas City Wide Open", date: "2026-04-23" },
  { event: "Waco Annual Charity Open", date: "2026-04-30" },
  { event: "Open at Austin", date: "2026-05-06" },
  { event: "OTB Open", date: "2026-05-20" },
  { event: "Cascade Challenge", date: "2026-05-28" },
  { event: "Northwest Championship", date: "2026-06-03" },
  { event: "European Open", date: "2026-06-17" },
  { event: "Swedish Open", date: "2026-06-25" },
  { event: "Ale Open", date: "2026-07-02" },
  { event: "Heinola Open", date: "2026-07-09" },
  { event: "US Women’s Disc Golf Championship", date: "2026-07-15" },
  { event: "Champions Landing Open", date: "2026-07-23" },
  { event: "Ledgestone Open", date: "2026-07-29" },
  { event: "Discmania Challenge", date: "2026-08-06" },
  { event: "DGPT Doubles at the Preserve", date: "2026-08-13" },
  { event: "PDGA Pro World Championships", date: "2026-08-25" },
  { event: "Idlewild Open", date: "2026-09-03" },
  { event: "Green Mountain Championship", date: "2026-09-16" },
  { event: "MVP Open x OTB", date: "2026-09-23" },
  { event: "United States Disc Golf Championship", date: "2026-10-07" },
];

// =====================================================
// Env checks
// =====================================================
function requireEnv(name) {
  if (!process.env[name]) throw new Error(`Missing ${name} in .env`);
  return process.env[name];
}

function envOptional(name, fallback = "") {
  const v = String(process.env[name] ?? "").trim();
  return v ? v : fallback;
}

requireEnv("DISCORD_TOKEN");
requireEnv("APPS_SCRIPT_URL");
requireEnv("TX_SECRET");
requireEnv("PLAYERPOOL_CSV_URL");
requireEnv("WAIVER_CHANNEL_ID");

// Optional: separate channel for lineup reminders (defaults to WAIVER_CHANNEL_ID)
const REMINDER_CHANNEL_ID = envOptional("REMINDER_CHANNEL_ID", process.env.WAIVER_CHANNEL_ID);

function envBool(name, fallback = false) {
  const v = String(process.env[name] ?? "").trim().toLowerCase();
  if (!v) return fallback;
  return v === "true" || v === "1" || v === "yes" || v === "on";
}

const ENABLE_WAIVER_RUN = envBool("ENABLE_WAIVER_RUN", false);

// =====================================================
// Discord client
// =====================================================
const client = new Client({ intents: [GatewayIntentBits.Guilds] });
client.on("error", (err) => console.error("Client error:", err));

// =====================================================
// PlayerPool cache (used for ALL autocomplete + PDGA resolution)
// =====================================================
let players = []; // [{ name, pdga }]
let nameToPdga = new Map(); // exact name -> pdga
let playerPoolLoaded = false;

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

async function refreshPlayerPool() {
  const res = await fetch(process.env.PLAYERPOOL_CSV_URL);
  if (!res.ok) throw new Error(`PlayerPool CSV fetch failed: ${res.status}`);

  const text = await res.text();
  const lines = text.split(/\r?\n/).filter(Boolean);

  const data = [];
  const map = new Map();

  for (let i = 1; i < lines.length; i++) {
    const [nameRaw, pdgaRaw] = parseCsvLine(lines[i]);
    const name = (nameRaw || "").replace(/^"|"$/g, "").trim();
    const pdga = (pdgaRaw || "").replace(/^"|"$/g, "").trim();
    if (!name || !pdga) continue;

    data.push({ name, pdga });
    if (!map.has(name)) map.set(name, pdga);
  }

  players = data;
  nameToPdga = map;
  playerPoolLoaded = true;

  console.log(`✅ PlayerPool refreshed: ${players.length} players`);
}

function searchPlayers(query, limit = 25) {
  const q = (query || "").toLowerCase().trim();
  if (!q) return players.slice(0, limit);

  const matches = [];
  for (const p of players) {
    if (p.name.toLowerCase().includes(q)) matches.push(p);
    if (matches.length >= limit) break;
  }
  return matches;
}

// =====================================================
// Time / schedule helpers
// =====================================================
function todayET() {
  return DateTime.now().setZone("America/New_York").toFormat("yyyy-LL-dd");
}

function waiverEventsForToday() {
  const t = todayET();
  return WAIVER_AWARD_DATES.filter((x) => x.date === t);
}

function nextWaiverCycleET() {
  const now = DateTime.now().setZone("America/New_York").startOf("day");
  const future = WAIVER_AWARD_DATES
    .map((x) => ({ ...x, dt: DateTime.fromISO(x.date, { zone: "America/New_York" }) }))
    .filter((x) => x.dt >= now)
    .sort((a, b) => a.dt.toMillis() - b.dt.toMillis());

  return future.length ? future[0] : null;
}

function lineupReminderEventsForToday() {
  const t = todayET();
  return LINEUP_REMINDER_DATES.filter((x) => x.date === t);
}

// =====================================================
// Apps Script webhook calls
// =====================================================
async function postJson_(payload) {
  const res = await fetch(process.env.APPS_SCRIPT_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const json = await res.json().catch(() => ({
    ok: false,
    error: "Bad JSON response from Apps Script",
  }));

  if (!json.ok) {
    const errs =
      Array.isArray(json.errors) && json.errors.length
        ? json.errors
        : [json.error || "Apps Script returned ok:false"];
    throw new Error(errs.join("\n"));
  }
  return json;
}

function postTransactionRow({ type, team, pdga, name, fromTeam, toTeam, notes, mode }) {
  return postJson_({
    secret: process.env.TX_SECRET,
    date: new Date().toISOString(),
    type,
    team,
    pdga,
    name,
    fromTeam,
    toTeam,
    notes: notes || "",
    ...(mode ? { mode } : {}),
  });
}

function postSwap({ team, dropPdga, dropName, addPdga, addName, notes, mode }) {
  return postJson_({
    secret: process.env.TX_SECRET,
    date: new Date().toISOString(),
    type: "SWAP",
    team,
    dropPdga,
    dropName,
    addPdga,
    addName,
    notes: notes || "",
    ...(mode ? { mode } : {}),
  });
}

function postWaiverRun({ cycleId, eventName, runAtIso }) {
  return postJson_({
    secret: process.env.TX_SECRET,
    action: "WAIVER_RUN",
    cycleId,
    eventName,
    runAt: runAtIso || new Date().toISOString(),
  });
}

function postWaiverSubmit({ cycleId, team, submittedBy, picks }) {
  return postJson_({
    secret: process.env.TX_SECRET,
    action: "WAIVER_SUBMIT",
    cycleId,
    team,
    submittedBy,
    picks,
  });
}

function postLineupReminderRun({ cycleId, eventName, runAtIso }) {
  return postJson_({
    secret: process.env.TX_SECRET,
    action: "LINEUP_REMINDER_RUN",
    cycleId, // e.g. "2026-02-26"
    eventName,
    runAt: runAtIso || new Date().toISOString(),
  });
}

// ✅ Alerts webhook call (saves to AlertSubscriptions tab)
// NOTE: We no longer expose "enabled" in /alerts. We always set enabled=true on save.
function postAlertsSet({ team, phoneE164, freeAgents, waiverAwards, withdrawals, lineupReminders }) {
  return postJson_({
    secret: process.env.TX_SECRET,
    action: "ALERTS_SET",
    team,
    phoneE164,
    enabled: true, // keep backend compatibility while removing the user-facing toggle
    freeAgents: !!freeAgents,
    waiverAwards: !!waiverAwards,
    withdrawals: !!withdrawals,
    lineupReminders: !!lineupReminders,
  });
}

// =====================================================
// Waiver awards runner
// =====================================================
async function runWaiverAwardsForEvent(eventName, dateString) {
  console.log(`🧾 Waiver run triggered for ${eventName} (${dateString})`);

  const cycleId = dateString;

  const result = await postWaiverRun({
    cycleId,
    eventName,
    runAtIso: new Date().toISOString(),
  });

  if (result.alreadyPosted) {
    console.log(`ℹ️ Waiver run already posted for cycle ${cycleId}`);
    return { ok: true, alreadyPosted: true };
  }

  const channel = await client.channels.fetch(process.env.WAIVER_CHANNEL_ID);
  if (!channel || !channel.isTextBased()) {
    throw new Error("WAIVER_CHANNEL_ID is not a text channel the bot can access.");
  }

  const header =
    `🧾 **${result.title}**\n` +
    `🏟️ Event: **${eventName}**\n` +
    `📅 Date: **${dateString}**\n\n`;

  const body = Array.isArray(result.lines) ? result.lines.join("\n") : "_No awards returned._";
  const footer = result.footer ? `\n\n_${result.footer}_` : "";

  await channel.send(header + body + footer);
  console.log(`✅ Waiver awards posted to channel ${process.env.WAIVER_CHANNEL_ID}`);

  return { ok: true, alreadyPosted: false };
}

// =====================================================
// Ready: schedule + refresh
// =====================================================
client.once(Events.ClientReady, async () => {
  console.log(`🤖 Logged in as ${client.user.tag}`);

  cron.schedule(
    "0 12 * * *",
    async () => {
      try {
        // -----------------------------
        // Waiver Awards
        // -----------------------------
        const todaysWaiverEvents = waiverEventsForToday();
        if (todaysWaiverEvents.length) {
          for (const ev of todaysWaiverEvents) {
            await runWaiverAwardsForEvent(ev.event, ev.date);
          }
        }

        // -----------------------------
        // Lineup Reminders (Discord post)
        // -----------------------------
        const todaysLineupReminders = lineupReminderEventsForToday();
        if (todaysLineupReminders.length) {
          const channel = await client.channels.fetch(REMINDER_CHANNEL_ID);
          if (!channel || !channel.isTextBased()) {
            throw new Error(
              "REMINDER_CHANNEL_ID / WAIVER_CHANNEL_ID is not a text channel the bot can access."
            );
          }

          for (const ev of todaysLineupReminders) {
            // Log in Apps Script first (dedupe lives there)
            let result = null;
            try {
              result = await postLineupReminderRun({
                cycleId: ev.date,
                eventName: ev.event,
                runAtIso: new Date().toISOString(),
              });
            } catch (e) {
              console.error("❌ LineupReminder log call failed (Apps Script):", e);
            }

            if (result && result.alreadyPosted) {
              console.log(`ℹ️ Lineup reminder already posted for cycle ${ev.date} (${ev.event})`);
              continue;
            }

            await channel.send(
              `⏰ **Lineup Reminder**\n` +
                `🏟️ Event: **${ev.event}**\n` +
                `📅 Round 1 starts tomorrow.\n\n` +
                `✅ Please double-check:\n` +
                `• Your registered players\n` +
                `• Your lineup / starters\n` +
                `• Any last-minute swaps\n\n` +
                `_SMS reminders are controlled via /alerts → lineupreminders (when backend is wired)._`
            );

            console.log(`✅ Lineup reminder posted for ${ev.event} (${ev.date})`);
          }
        }
      } catch (err) {
        console.error("Noon ET cron job error:", err);
      }
    },
    { timezone: "America/New_York" }
  );

  // Initial PlayerPool load
  try {
    await refreshPlayerPool();
  } catch (e) {
    console.error("❌ Initial PlayerPool refresh failed:", e);
  }

  // Refresh PlayerPool every 6 hours
  setInterval(async () => {
    try {
      await refreshPlayerPool();
    } catch (e) {
      console.error("❌ PlayerPool refresh failed:", e);
    }
  }, 6 * 60 * 60 * 1000);
});

// =====================================================
// Interaction handler
// =====================================================
client.on(Events.InteractionCreate, async (interaction) => {
  try {
    // ---- Autocomplete ----
    if (interaction.isAutocomplete()) {
      try {
        if (!playerPoolLoaded) return interaction.respond([]);

        const focused = interaction.options.getFocused(true);
        const query = String(focused?.value ?? "");

        const source = searchPlayers(query, 25);
        const matches = source.map((p) => ({
          name: `${p.name} (${p.pdga})`.slice(0, 100),
          value: p.name.slice(0, 100),
        }));

        return interaction.respond(matches);
      } catch (e) {
        console.error("Autocomplete error:", e);
        try {
          return interaction.respond([]);
        } catch (_) {
          return;
        }
      }
    }

    if (!interaction.isChatInputCommand()) return;

    // =========================
    // /alerts
    // =========================
    if (interaction.commandName === "alerts") {
      const team = interaction.options.getString("team", true);
      const phone = interaction.options.getString("phone", true);

      // removed: enabled
      const freeAgents = interaction.options.getBoolean("freeagents", true);
      const waiverAwards = interaction.options.getBoolean("waiverawards", true);
      const withdrawals = interaction.options.getBoolean("withdrawals", true);
      const lineupReminders = interaction.options.getBoolean("lineupreminders", true);

      if (!TEAM_NAMES.has(team)) {
        return interaction.reply({ content: `❌ Invalid team: ${team}`, ephemeral: true });
      }

      // Basic E.164 validation
      if (!/^\+\d{10,15}$/.test(phone)) {
        return interaction.reply({
          content: `❌ Phone must be E.164 like **+12345678900** (you sent: ${phone})`,
          ephemeral: true,
        });
      }

      await interaction.deferReply({ ephemeral: true });

      try {
        const res = await postAlertsSet({
          team,
          phoneE164: phone,
          freeAgents,
          waiverAwards,
          withdrawals,
          lineupReminders,
        });

        const created = !!res.created;

        return interaction.editReply(
          `✅ **SMS Alerts Saved**\n` +
            `🏷️ Team: **${team}**\n` +
            `📱 Phone: **${phone}**\n\n` +
            `• Free Agent Drops: **${freeAgents ? "Yes" : "No"}**\n` +
            `• Waiver Awards (only if you win): **${waiverAwards ? "Yes" : "No"}**\n` +
            `• Withdrawals: **${withdrawals ? "Yes" : "No"}**\n` +
            `• Lineup Reminders (day before): **${lineupReminders ? "Yes" : "No"}**\n\n` +
            `${created ? "_New subscription created._" : "_Subscription updated._"}`
        );
      } catch (err) {
        return interaction.editReply(`❌ ${String(err?.message || err)}`);
      }
    }

    // ==============================
    // /waiver_run_now
    // ==============================
    if (interaction.commandName === "waiver_run_now") {
      await interaction.deferReply({ ephemeral: true });

      try {
        if (!ENABLE_WAIVER_RUN) {
          return interaction.editReply("🛑 Manual waiver runs are currently disabled.");
        }

        if (!interaction.inGuild()) {
          return interaction.editReply("❌ This command can only be used in a server.");
        }

        const next = nextWaiverCycleET();
        if (!next) {
          return interaction.editReply("❌ No upcoming waiver cycle found in schedule.");
        }

        await runWaiverAwardsForEvent(next.event, next.date);

        return interaction.editReply(
          `✅ Waiver awards triggered.\n📅 Cycle: **${next.date}** (${next.event})`
        );
      } catch (err) {
        console.error("waiver_run_now error:", err);
        return interaction.editReply(`❌ ${String(err?.message || err)}`);
      }
    }

    // =========================
    // /waivers
    // =========================
    if (interaction.commandName === "waivers") {
      if (!playerPoolLoaded) {
        return interaction.reply({
          content: "❌ PlayerPool is still loading. Try again in ~10 seconds.",
          ephemeral: true,
        });
      }

      const team = interaction.options.getString("team", true);

      if (!TEAM_NAMES.has(team)) {
        return interaction.reply({ content: `❌ Invalid team: ${team}`, ephemeral: true });
      }

      const next = nextWaiverCycleET();
      if (!next) {
        return interaction.reply({
          content: "❌ No upcoming waiver cycle found in schedule.",
          ephemeral: true,
        });
      }

      await interaction.deferReply({ ephemeral: true });

      const cycleId = next.date;

      const picks = [];
      for (let r = 1; r <= 10; r++) {
        const opt = interaction.options.getString(`pick${r}`, false);
        if (!opt) continue;

        const pdga = nameToPdga.get(opt);
        if (!pdga) {
          return interaction.editReply(`❌ Not found in PlayerPool: ${opt}`);
        }

        picks.push({ rank: r, pdga, name: opt });
      }

      if (!picks.length) {
        return interaction.editReply("❌ Submit at least 1 pick (pick1..pick10).");
      }

      const seen = new Set();
      for (const p of picks) {
        if (seen.has(p.pdga)) {
          return interaction.editReply(`❌ Duplicate player selected: ${p.name} (${p.pdga})`);
        }
        seen.add(p.pdga);
      }

      let result;
      try {
        result = await postWaiverSubmit({
          cycleId,
          team,
          submittedBy: String(interaction.user.id),
          picks,
        });
      } catch (err) {
        return interaction.editReply(`❌ ${String(err?.message || err)}`);
      }

      const returned = Array.isArray(result?.picks) ? result.picks : picks;
      const lines = returned.map((p) => `${p.rank}) ${p.name} (${p.pdga})`).join("\n");

      return interaction.editReply(
        `✅ **Waiver request submitted**\n` +
          `📅 Cycle: **${cycleId}** (${next.event})\n` +
          `🏷️ Team: **${team}**\n\n` +
          `${lines}\n\n` +
          `_Resubmitting /waivers replaces your previous request for this cycle._`
      );
    }

    // =========================
    // /transaction
    // =========================
    if (interaction.commandName === "transaction") {
      if (!playerPoolLoaded) {
        return interaction.reply({
          content: "❌ PlayerPool is still loading. Try again in ~10 seconds.",
          ephemeral: true,
        });
      }

      const team = interaction.options.getString("team", true);
      const addName = interaction.options.getString("add_player", false);
      const dropName = interaction.options.getString("drop_player", false);
      const notes = interaction.options.getString("notes", false) || "";

      if (!TEAM_NAMES.has(team)) {
        return interaction.reply({ content: `❌ Invalid team: ${team}`, ephemeral: true });
      }
      if (!addName && !dropName) {
        return interaction.reply({
          content: "❌ You must provide add_player and/or drop_player.",
          ephemeral: true,
        });
      }

      await interaction.deferReply({ ephemeral: false });

      if (dropName && addName) {
        const dropPdga = nameToPdga.get(dropName);
        const addPdga = nameToPdga.get(addName);

        if (!dropPdga) throw new Error(`Could not resolve PDGA for drop_player: ${dropName}`);
        if (!addPdga) throw new Error(`Could not resolve PDGA for add_player: ${addName}`);

        await postSwap({ team, dropPdga, dropName, addPdga, addName, notes });

        const noteLine = notes ? `\n📝 Notes: ${notes}` : "";
        const who = `\n👤 Submitted by: <@${interaction.user.id}>`;

        return interaction.editReply(
          `✅ **${team} SWAP Logged**\n` +
            `⬇️ **DROP**: ${dropName} → **${FREE}**\n` +
            `⬆️ **ADD**: ${addName} ← **${FREE}**` +
            noteLine +
            who
        );
      }

      const receiptLines = [];

      if (dropName) {
        const pdga = nameToPdga.get(dropName);
        if (!pdga) throw new Error(`Could not resolve PDGA for drop_player: ${dropName}`);

        await postTransactionRow({
          type: "DROP",
          team,
          pdga,
          name: dropName,
          fromTeam: team,
          toTeam: FREE,
          notes,
        });
        receiptLines.push(`⬇️ **DROP**: ${dropName} → **${FREE}**`);
      }

      if (addName) {
        const pdga = nameToPdga.get(addName);
        if (!pdga) throw new Error(`Could not resolve PDGA for add_player: ${addName}`);

        await postTransactionRow({
          type: "ADD",
          team,
          pdga,
          name: addName,
          fromTeam: FREE,
          toTeam: team,
          notes,
        });
        receiptLines.push(`⬆️ **ADD**: ${addName} ← **${FREE}**`);
      }

      const noteLine = notes ? `\n📝 Notes: ${notes}` : "";
      const who = `\n👤 Submitted by: <@${interaction.user.id}>`;

      return interaction.editReply(
        `✅ **${team} Transaction Logged**\n` + receiptLines.join("\n") + noteLine + who
      );
    }

    // =========================
    // /trade
    // =========================
    if (interaction.commandName === "trade") {
      if (!playerPoolLoaded) {
        return interaction.reply({
          content: "❌ PlayerPool is still loading. Try again in ~10 seconds.",
          ephemeral: true,
        });
      }

      const teamA = interaction.options.getString("team_a", true);
      const teamB = interaction.options.getString("team_b", true);
      const playerA = interaction.options.getString("player_a", true);
      const playerB = interaction.options.getString("player_b", true);
      const notes = interaction.options.getString("notes", false) || "";

      if (!TEAM_NAMES.has(teamA) || !TEAM_NAMES.has(teamB)) {
        return interaction.reply({ content: "❌ Invalid team code(s).", ephemeral: true });
      }
      if (teamA === teamB) {
        return interaction.reply({
          content: "❌ team_a and team_b must be different.",
          ephemeral: true,
        });
      }

      const pdgaA = nameToPdga.get(playerA);
      const pdgaB = nameToPdga.get(playerB);
      if (!pdgaA) throw new Error(`Could not resolve PDGA for player_a: ${playerA}`);
      if (!pdgaB) throw new Error(`Could not resolve PDGA for player_b: ${playerB}`);

      await interaction.deferReply({ ephemeral: false });

      await postTransactionRow({
        type: "TRADE",
        team: teamA,
        pdga: pdgaA,
        name: playerA,
        fromTeam: teamA,
        toTeam: teamB,
        notes,
      });

      await postTransactionRow({
        type: "TRADE",
        team: teamB,
        pdga: pdgaB,
        name: playerB,
        fromTeam: teamB,
        toTeam: teamA,
        notes,
      });

      const noteLine = notes ? `\n📝 Notes: ${notes}` : "";
      const who = `\n👤 Submitted by: <@${interaction.user.id}>`;

      return interaction.editReply(
        `🤝 **Trade Logged**\n` +
          `➡️ **${teamA}** sent: ${playerA}\n` +
          `⬅️ **${teamB}** sent: ${playerB}` +
          noteLine +
          who
      );
    }
  } catch (err) {
    console.error("❌ Error:", err);

    const msg = `❌ ${String(err?.message || err)}`;
    try {
      if (interaction.deferred || interaction.replied) {
        return interaction.followUp({ content: msg, ephemeral: true });
      }
      return interaction.reply({ content: msg, ephemeral: true });
    } catch (e) {
      console.error("Failed to respond to interaction:", e);
      return;
    }
  }
});

client.login(process.env.DISCORD_TOKEN);
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
// Env checks
// =====================================================
function requireEnv(name) {
  if (!process.env[name]) throw new Error(`Missing ${name} in .env`);
  return process.env[name];
}

requireEnv("DISCORD_TOKEN");
requireEnv("APPS_SCRIPT_URL");
requireEnv("TX_SECRET");
requireEnv("PLAYERPOOL_CSV_URL");
requireEnv("WAIVER_CHANNEL_ID");

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

// Client-level error logging (safe now)
client.on("error", (err) => console.error("Client error:", err));

// =====================================================
// PlayerPool cache (used for ALL autocomplete + PDGA resolution)
// =====================================================
let players = []; // [{ name, pdga }]
let nameToPdga = new Map(); // exact name -> pdga

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
// Time / Waiver schedule helpers
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
        const todaysEvents = waiverEventsForToday();
        if (todaysEvents.length === 0) return;

        for (const ev of todaysEvents) {
          await runWaiverAwardsForEvent(ev.event, ev.date);
        }
      } catch (err) {
        console.error("Waiver cron job error:", err);
      }
    },
    { timezone: "America/New_York" }
  );

  try {
    await refreshPlayerPool();
  } catch (e) {
    console.error("❌ Initial PlayerPool refresh failed:", e);
  }

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

      // ACK immediately
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

    // Always try to respond somehow, but don’t crash if interaction expired
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

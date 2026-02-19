require("dotenv").config();
const { REST, Routes, SlashCommandBuilder } = require("discord.js");

// =======================
// Env checks
// =======================
function requireEnv(name) {
  if (!process.env[name]) throw new Error(`Missing ${name} in .env`);
  return process.env[name];
}

const DISCORD_TOKEN = requireEnv("DISCORD_TOKEN");
const GUILD_ID = requireEnv("GUILD_ID");
const CLIENT_ID = requireEnv("CLIENT_ID");

function envBool(name, fallback = false) {
  const v = String(process.env[name] ?? "").trim().toLowerCase();
  if (!v) return fallback;
  return v === "true" || v === "1" || v === "yes" || v === "on";
}

const ENABLE_WAIVER_RUN = envBool("ENABLE_WAIVER_RUN", false);

// =======================
// Constants / Helpers
// =======================
const TEAM_CHOICES = [
  { name: "Sir Krontzalot", value: "Sir Krontzalot" },
  { name: "Exalted Evil", value: "Exalted Evil" },
  { name: "Tree Ninja Disc Golf", value: "Tree Ninja Disc Golf" },
  { name: "The Abba Zabba", value: "The Abba Zabba" },
  { name: "Ryan Morgan", value: "Ryan Morgan" },
  { name: "SPY Dyes", value: "SPY Dyes" },
  { name: "Eddie Speidel", value: "Eddie Speidel" },
  { name: "Webb Webb Webb", value: "Webb Webb Webb" },
  { name: "Hughes Moves", value: "Hughes Moves" },
  { name: "Matthew Lopez", value: "Matthew Lopez" },
];

function addTeamOption(cmd, optionName = "team", description = "Team") {
  return cmd.addStringOption((opt) =>
    opt
      .setName(optionName)
      .setDescription(description)
      .setRequired(true)
      .addChoices(...TEAM_CHOICES)
  );
}

function addNotesOption(cmd) {
  return cmd.addStringOption((opt) =>
    opt.setName("notes").setDescription("Notes (optional)").setRequired(false)
  );
}

function addAutocompletePlayerOption(cmd, optionName, description, required) {
  return cmd.addStringOption((opt) =>
    opt
      .setName(optionName)
      .setDescription(description)
      .setAutocomplete(true)
      .setRequired(required)
  );
}

function addWaiverPickOptions(cmd) {
  for (let i = 1; i <= 10; i++) {
    cmd.addStringOption((opt) =>
      opt
        .setName(`pick${i}`)
        .setDescription(`Wishlist pick #${i} (autocomplete)`)
        .setAutocomplete(true)
        .setRequired(i === 1)
    );
  }
  return cmd;
}

// =======================
// Command Definitions
// =======================
const transactionCmd = new SlashCommandBuilder()
  .setName("transaction")
  .setDescription("Log an add, drop, or swap (add and drop together = swap).");

addTeamOption(transactionCmd, "team", "Team making the move");
addAutocompletePlayerOption(transactionCmd, "add_player", "Player to add (autocomplete)", false);
addAutocompletePlayerOption(transactionCmd, "drop_player", "Player to drop (autocomplete)", false);
addNotesOption(transactionCmd);

const tradeCmd = new SlashCommandBuilder()
  .setName("trade")
  .setDescription("Log a trade (writes two rows).");

addTeamOption(tradeCmd, "team_a", "Team A");
addAutocompletePlayerOption(tradeCmd, "player_a", "Player from Team A (autocomplete)", true);
addTeamOption(tradeCmd, "team_b", "Team B");
addAutocompletePlayerOption(tradeCmd, "player_b", "Player from Team B (autocomplete)", true);
addNotesOption(tradeCmd);

const waiversCmd = new SlashCommandBuilder()
  .setName("waivers")
  .setDescription("Submit your ranked waiver wishlist (resubmitting replaces your previous request).");

addTeamOption(waiversCmd, "team", "Team submitting the wishlist");
addWaiverPickOptions(waiversCmd);

// ✅ Alerts command (Twilio preferences)
const alertsCmd = new SlashCommandBuilder()
  .setName("alerts")
  .setDescription("Configure SMS text alerts for your team.");

addTeamOption(alertsCmd, "team", "Team to receive alerts for");
alertsCmd
  .addStringOption((opt) =>
    opt
      .setName("phone")
      .setDescription("Phone number in E.164 format (ex: +12345678900)")
      .setRequired(true)
  )
  .addBooleanOption((opt) =>
    opt
      .setName("enabled")
      .setDescription("Enable/disable all SMS alerts")
      .setRequired(true)
  )
  .addBooleanOption((opt) =>
    opt
      .setName("freeagents")
      .setDescription("Alert on new Free Agents (drops)")
      .setRequired(true)
  )
  .addBooleanOption((opt) =>
    opt
      .setName("waiverawards")
      .setDescription("Alert only if you win a waiver award")
      .setRequired(true)
  )
  .addBooleanOption((opt) =>
    opt
      .setName("withdrawals")
      .setDescription("Alert if your player withdraws from an event")
      .setRequired(true)
  );

const waiverRunNowCmd = new SlashCommandBuilder()
  .setName("waiver_run_now")
  .setDescription("ADMIN: Run waiver awards immediately for the next waiver cycle.");

const commands = [
  transactionCmd,
  tradeCmd,
  waiversCmd,
  alertsCmd,
  ...(ENABLE_WAIVER_RUN ? [waiverRunNowCmd] : []),
].map((c) => c.toJSON());

// =======================
// Deploy
// =======================
async function main() {
  const rest = new REST({ version: "10" }).setToken(DISCORD_TOKEN);

  await rest.put(Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID), {
    body: commands,
  });

  console.log("✅ Slash commands deployed to your server.");
}

main().catch((err) => {
  console.error("❌ Failed to deploy commands:", err);
  process.exit(1);
});

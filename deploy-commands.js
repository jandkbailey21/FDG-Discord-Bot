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
  // Require pick1; allow pick2..pick10 optional
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
addAutocompletePlayerOption(
  transactionCmd,
  "add_player",
  "Player to add (autocomplete)",
  false
);
addAutocompletePlayerOption(
  transactionCmd,
  "drop_player",
  "Player to drop (autocomplete)",
  false
);
addNotesOption(transactionCmd);

const tradeCmd = new SlashCommandBuilder()
  .setName("trade")
  .setDescription("Log a trade (writes two rows).");

addTeamOption(tradeCmd, "team_a", "Team A");
addAutocompletePlayerOption(
  tradeCmd,
  "player_a",
  "Player from Team A (autocomplete)",
  true
);
addTeamOption(tradeCmd, "team_b", "Team B");
addAutocompletePlayerOption(
  tradeCmd,
  "player_b",
  "Player from Team B (autocomplete)",
  true
);
addNotesOption(tradeCmd);

const waiversCmd = new SlashCommandBuilder()
  .setName("waivers")
  .setDescription(
    "Submit your ranked waiver wishlist (resubmitting replaces your previous request)."
  );

addTeamOption(waiversCmd, "team", "Team submitting the wishlist");
addWaiverPickOptions(waiversCmd);

// Build JSON payload for Discord API

const waiverRunNowCmd = new SlashCommandBuilder()
  .setName("waiver_run_now")
  .setDescription("ADMIN: Run waiver awards immediately for the next waiver cycle.");

const commands = [
  transactionCmd,
  tradeCmd,
  waiversCmd,
  waiverRunNowCmd   // 👈 ADD THIS
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

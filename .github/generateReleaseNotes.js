const fs = require("fs");
const { execSync } = require("child_process");

const OUTPUT_FILE = "RELEASE_NOTES.md";

const GITMOJI_CATEGORIES = {
  "Breaking changes": ["💥", ":boom:"],
  "Features": ["✨", ":sparkles:", "⚗️", ":alembic:", "🚩", ":triangular_flag_on_post:", "🧑‍💻", ":technologist:"],
  "Bug Fixes": ["🐛", ":bug:", "🚑️", ":ambulance:", "🩹", ":adhesive_bandage:"],
  "Documentation": ["📝", ":books:", "📄", ":page_facing_up:", "💡", ":bulb:"],
  "Genesis": ["🌱", ":seedling:"],
  "Configuration": ["🔧", ":wrench:"],
  "Tests": ["✅", ":white_check_mark:", "🧪", ":test_tube:"],
  "Performance": ["⚡", ":zap:", "🧵", ":thread:"],
  "Refactoring": ["♻", ":recycle:", "🏗️", ":building_construction:", "🚚", ":truck:", "🧱", ":bricks:"],
  "Logging": ["🔊", ":loud_sound:", "🔇", ":mute:"],
  "Deployments": ["🚀", ":rocket:"],
  "Work in Progress": ["🚧", ":construction:"],
  "Security": ["🔒", ":lock:"],
  "Localization": ["🌍", ":earth_africa:"],
  "Devtools / CI": ["💚", ":green_heart:", "👷", ":construction_worker:", "🔨", ":hammer:"],
  "Removed": ["🔥", ":fire:", "⚰️", ":coffin:"],
  "Dependencies": ["📌", ":pushpin:", "➕", ":heavy_plus_sign:", "➖", ":heavy_minus_sign:", "⬆️", ":arrow_up:", "⬇️", ":arrow_down:"],
};

function generateReleaseNotes() {
  const commits = execSync(
    "git log --oneline $(git describe --tags --abbrev=0 HEAD^)..HEAD --pretty=format:%s"
  )
    .toString()
    .split("\n");

  const categoryCommits = {};
  Object.keys(GITMOJI_CATEGORIES).forEach((category) => {
    categoryCommits[category] = [];
  });
  let uncategorized = [];

  commits.forEach((commit) => {
    let categorized = false;
    for (const [category, emojis] of Object.entries(GITMOJI_CATEGORIES)) {
      if (emojis.some((emoji) => commit.startsWith(emoji))) {
        categoryCommits[category].push(commit.trim());
        categorized = true;
        break;
      }
    }
    if (!categorized) {
      uncategorized.push(commit);
    }
  });

  let releaseNotes = "";

  for (const [category, commits] of Object.entries(categoryCommits)) {
    if (commits.length > 0) {
      releaseNotes += `## ${category}\n\n`;
      commits.forEach((commit) => {
        releaseNotes += `- ${commit}\n`;
      });
      releaseNotes += "\n";
    }
  }

  if (uncategorized.length > 0) {
    releaseNotes += "## Uncategorized\n\n";
    uncategorized.forEach((commit) => {
      releaseNotes += `- ${commit}\n`;
    });
    releaseNotes += "\n";
  }

  fs.writeFileSync(OUTPUT_FILE, releaseNotes);
  console.log(`Release notes generated in ${OUTPUT_FILE}`);
}

generateReleaseNotes();

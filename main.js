// run_simple_visual_numbered_test_snake.js (ESM)
import path from "path";
import { fileURLToPath } from "url";

import { determine_os_path } from "./utilities/directory_tools/determine_os_path.js";
import { create_directory } from "./utilities/directory_tools/create_directory.js";
import { color_text } from "./utilities/console_logs/console_colors.js";

// === imports for each step ===
import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import { step_1_run_alpha_wrestler_list } from "./src/step_1_get_wrestler_list.js";

import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";
import { step_4_create_wrestler_match_history_data } from "./src/step_4_create_wrestler_match_history_metrics.js";

import { step_5_create_team_division_data } from "./src/step_5_create_team_division_data.js";
import { step_6_append_team_division_updates } from "./src/step_6_append_team_division_data.js"; // ad hoc updates for missing records
import { step_7_append_team_division_to_match_metrics } from "./src/step_7_append_team_division_to_match_metrics.js";
import { step_8_append_team_division_to_wrestler_list } from "./src/step_8_append_team_division_to_wreslter_list.js";

import { step_9_create_state_qualfier_reference } from "./src/step_9_create_state_qualfier_reference.js";
import { step_11_append_state_qualifier_to_match_metrics } from "./src/step_11_append_state_qualifier_to_match_metrics.js";
import { step_12_append_state_qualifier_to_wrestler_list } from "./src/step_12_append_state_qualifier_to_wrestler_list.js";

import { execute_load_data_to_bigquery } from "./utilities/google_cloud/load_process/step_0_load_main_job.js"; // step 14 load google cloud & bigquery

import { step_15_close_chrome_dev } from "./src/step_15_close_chrome_developer.js";

import { close_pools } from "./utilities/mysql/mysql_pool.js"; // Step 20

// ====================================================
// 🧩 STEP TOGGLES todo:
// ====================================================
const step_flags = {
  step_0: false,  // 🚀 launch chrome

  // MATCH LIST
  step_1: false,  // 📄 get wrestler list

  // OLD url list source; now pulled from step 3
  // step_2: false, // 🔗 optional URL array; normally false; step 3 uses step 1 output

  // MATCH HISTORY
  step_3: false,  // 🏟️ get match history
  step_4: false, // 📄 create match history metrics

  // CREATE TEAM REGION / DIVISION
  step_5: false, // create team division
  step_6: false, // append team division to table (ad hoc updates for teams that don't have division/regoin data)
  step_7: false, // append team division to match history metrics
  step_8: false, // append team division to wrestler list

  // CREATE 2024-25 STATE QUALIFIER LIST
  step_9: false, // create 2024-25 state qualifier list
  // step_10: true, // append team division to table (ad hoc updates for teams that don't have division/regoin data)

  step_11: false, // append state qualifier to match history metrics
  step_12: false, // append state qualifier to wrestler list

  // LOAD GOOGLE CLOUD / BIGQUERY
  step_14: false, // load data into Google cloud / bigquery

  step_15: false,  // 🧹 close browser
};

// 🧪 each step can run test or full //todo:
const test_flags = {
  step_1_is_test: false, // run small sample for wrestler list
  step_3_is_test: true, // run small sample for match history
  step_4_is_test: false, // run small sample for match history metrics
};

// ====================================================
// ⚙️ GLOBAL CONFIG — all tunable numbers here
// ====================================================
const config = {
  governing_body: "Colorado High School Activities Association",

  wrestling_season: "2024-25", // todo:
  // wrestling_season: "2025-26",

  // HIGH SCHOOL BOYS = set category & gender
  track_wrestling_category: "High School Boys",
  gender: "M",

  // HIGH SCHOOL GIRLS = set category & gender
  // track_wrestling_category: "High School Girls",
  // gender: "F",

  url_home_page: "https://www.trackwrestling.com/",
  url_login_page: "https://www.trackwrestling.com/seasons/index.jsp",

  // Step #1 config
  alpha_list_limit_test: 1,
  alpha_list_limit_full: 30,

  // Step #3 config
  matches_page_limit_test: 5,
  matches_page_limit_full: 10000,
  step_3_loop_start: 0, // 🌀 starting index for Step #3 loop
};

// ====================================================
// 🎨 HELPERS
// ====================================================
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const adjusted_season = config.wrestling_season.replace("-", "_");
const adjusted_gender = config.track_wrestling_category
  .toLowerCase()           // make lowercase
  .replace(/\s+/g, "_")
  ;   // replace all spaces with underscores

const step_icons = {
  0:"0️⃣",1:"1️⃣",2:"2️⃣",3:"3️⃣",4:"4️⃣",
  5:"5️⃣",6:"6️⃣",7:"7️⃣",8:"8️⃣",9:"9️⃣",
  10:"🔟",11:"1️⃣1️⃣",12:"1️⃣2️⃣",13:"1️⃣3️⃣",14:"1️⃣4️⃣",
  15:"1️⃣5️⃣",16:"1️⃣6️⃣",17:"1️⃣7️⃣",18:"1️⃣8️⃣",19:"1️⃣9️⃣"
};

// convert milliseconds → h:mm:ss
function format_duration(ms) {
  const total_seconds = Math.floor(ms / 1000);
  const hours = Math.floor(total_seconds / 3600);
  const minutes = Math.floor((total_seconds % 3600) / 60);
  const seconds = total_seconds % 60;
  return `${hours}:${minutes.toString().padStart(2, "0")}:${seconds
    .toString()
    .padStart(2, "0")}`;
}
function log_step_start(n, msg) {
  console.log(color_text(`${step_icons[n] || "🔹"}  Step #${n}: ${msg}`, "cyan"));
}
function log_step_skip(n, msg) {
  console.log(color_text(`${step_icons[n] || "⏭️"}  Step #${n}: ⏭️  Skipped (${msg})`, "yellow"));
}
function log_step_success(n, msg, duration = null) {
  const time = duration ? ` ⏱️  (${format_duration(duration)})` : "";
  console.log(color_text(`${step_icons[n] || "✅"}  Step #${n}: ✅  ${msg}${time}`, "green"));
}
function log_error(msg) {
  console.error(color_text(`💥 ${msg}`, "red"));
}

// ====================================================
// 🚀 MAIN ORCHESTRATOR
// ====================================================
async function main() {
  const program_start = Date.now();
  console.log(color_text(`\n🏁 ➕ Starting main program for ${config.wrestling_season}`, "red"));

  const directory = determine_os_path();
  const input_dir = await create_directory("input", directory);
  const output_dir = await create_directory("output", directory);

  const ctx = {
    config,
    paths: {
      input_dir,
      output_dir,
      wrestler_list_csv: path.join(input_dir, `wrestler_list_scrape_data_${adjusted_season}_${adjusted_gender}.csv`),
      url_array_js: path.join(input_dir, `wrestler_match_urls_${adjusted_season}_${adjusted_gender}.js`),
      match_csv: path.join(output_dir, `wrestler_match_history_scrape_data_${adjusted_season}_${adjusted_gender}.csv`),
    },
    browser: null,
    page: null,
    context: null,
  };

  try {
    // === STEP 0 LAUNCH GOOGLE CHROME DEVTOOLS ===
    if (step_flags.step_0) {
      const start = Date.now();
      log_step_start(0, "Launching Chrome DevTools 🚀");
      const { browser, page, context } = await step_0_launch_chrome_developer(config.url_home_page);
      ctx.browser = browser;
      ctx.page = page;
      ctx.context = context;
      log_step_success(0, "Chrome launched successfully", Date.now() - start);
    } else log_step_skip(0, "chrome launch");

    // === STEP 1 SCRAPE WRESTLER LIST ===
    if (step_flags.step_1) {
      const start = Date.now();
      const is_test = test_flags.step_1_is_test;
      const limit = is_test ? config.alpha_list_limit_test : config.alpha_list_limit_full;
      log_step_start(1, `Fetching ${limit} wrestlers (${is_test ? "🧪 TEST MODE" : "FULL"}) 📄`);

      await step_1_run_alpha_wrestler_list(
        config.url_login_page,
        limit,
        config.wrestling_season,
        config.track_wrestling_category,
        ctx.page,
        ctx.browser,
        ctx.paths.wrestler_list_csv,
        is_test
      );

      log_step_success(1, `Wrestler list saved → ${ctx.paths.wrestler_list_csv}`, Date.now() - start);
    } else log_step_skip(1, "wrestler list generation");

    // === STEP 3 SCRAPE MATCH HISTORY METRICS ===
    if (step_flags.step_3) {
      const start = Date.now();
      const is_test = test_flags.step_3_is_test;
      const limit = is_test ? config.matches_page_limit_test : config.matches_page_limit_full;
      const loop_start = config.step_3_loop_start;

      log_step_start(
        3,
        `Scraping match history (limit=${limit}, step_3_loop_start=${loop_start}) ${is_test ? "🧪 TEST MODE" : "🏟️ FULL"}`
      );

      await step_3_get_wrestler_match_history(
        config.url_home_page,
        config.url_login_page,
        limit,
        loop_start,
        config.wrestling_season,
        config.track_wrestling_category,
        config.gender,
        ctx.page,
        ctx.browser,
        ctx.context,
        ctx.paths.match_csv,
        ctx.paths.wrestler_list_csv,
        is_test
      );

      log_step_success(3, `Match history saved → ${ctx.paths.match_csv}`, Date.now() - start);
    } else log_step_skip(3, "match history");

    // === STEP 4  CREATE MATCH HISTORY METRICS ===
    if (step_flags.step_4) {
      const start = Date.now();
      const is_test = test_flags.step_4_is_test;
      const limit = is_test ? config.matches_page_limit_test : config.matches_page_limit_full;
      const loop_start = config.step_3_loop_start;

      log_step_start(4, `Create match history metrics (limit=${limit}, step_3_loop_start=${loop_start}) ${is_test ? "🧪 TEST MODE" : "🏟️ FULL"}`);

      await step_4_create_wrestler_match_history_data(config);

      log_step_success(4, `Match history metrics created → ${ctx.paths.match_csv}`, Date.now() - start);
    } else log_step_skip(4, "create match history metrics");

    // === STEP 5 CREATE TEAM DIVISION ===
    if (step_flags.step_5) {
      const start = Date.now();

      log_step_start(5,  "Start creating team division 🔗");

      await step_5_create_team_division_data();

      log_step_success(5, `Create team division`, Date.now() - start);
    } else log_step_skip(5, "Create team division");

    // === STEP 6 APPEND TEAM DIVISION UPDATES ===
    if (step_flags.step_6) {
      const start = Date.now();

      log_step_start(6,  "Start append team division updates 🔗");

      await step_6_append_team_division_updates();

      log_step_success(6, `Append team division`, Date.now() - start);
    } else log_step_skip(6, "Append team division");

    // === STEP 7 APPEND TEAM DIVISION TO MATCH HISTORY ===
    if (step_flags.step_7) {
      const start = Date.now();

      log_step_start(7,  "Start append team division updates to match history 🔗");

      await step_7_append_team_division_to_match_metrics();

      log_step_success(7, `Append team division updates to match history`, Date.now() - start);
    } else log_step_skip(7, "Append team division updates to match history");

    // === STEP 8 APPEND TEAM DIVISION TO WRESTLER LIST ===
    if (step_flags.step_8) {
      const start = Date.now();

      log_step_start(8,  "Start append team division updates to wrestler list 🔗");

      await step_8_append_team_division_to_wrestler_list();

      log_step_success(8, `Append team division updates to wrestler list`, Date.now() - start);
    } else log_step_skip(8, "Append team division updates to wrestler list");

    // === STEP 9 CREATE STATE QUALIFIER PLACE REFERENCE ===
    if (step_flags.step_9) {
      const start = Date.now();

      log_step_start(9,  "Start creating state qualifier place reference 🔗");

      await step_9_create_state_qualfier_reference();

      log_step_success(9, `Create state qualifier place reference`, Date.now() - start);
    } else log_step_skip(9, "Create state qualifier place reference");
    
    // === STEP 11 APPEND STATE QUALIFIER & PLACE TO MATCH HISTORY ===
    if (step_flags.step_11) {
      const start = Date.now();

      log_step_start(11,  "Start append state qualifier & place updates to match history 🔗");

      await step_11_append_state_qualifier_to_match_metrics();

      log_step_success(11, `Append state qualifier & place updates to match history`, Date.now() - start);
    } else log_step_skip(11, "Append state qualifier & place updates to match history");
    
    // === STEP 12 APPEND STATE QUALIFIER & PLACE TO WRESTLER LIST ===
    if (step_flags.step_12) {
      const start = Date.now();

      log_step_start(12,  "Start append state qualifier & place updates to wrestler list 🔗");

      await step_12_append_state_qualifier_to_wrestler_list();

      log_step_success(12, `Append state qualifier & place updates to wrestler list`, Date.now() - start);
    } else log_step_skip(12, "Append state qualifier & place updates to wrestler list");

    // === STEP 14 LOAD DATA TO GOOGLE CLOULD / BIGQUERY ===
    if (step_flags.step_14) {
      const start = Date.now();

      log_step_start(14, "Start Loading Data to Google Cloud & Bigquery 🔗");

      await execute_load_data_to_bigquery("wrestler");

      log_step_success(14, "Data loaded to Google Cloud & Bigquery", Date.now() - start);
    } else log_step_skip(14, "Load Data to Google Cloud & Bigquery 🔗");

    // === STEP 15 CLOSE BROWSER ===
    if (step_flags.step_15) {
      const start = Date.now();
      log_step_start(15, "Closing Chrome DevTools 🧹");

      await step_15_close_chrome_dev(ctx.browser, ctx.context);

      log_step_success(15, "Browser closed successfully", Date.now() - start);
    } else log_step_skip(15, "close browser");

    const total_ms = Date.now() - program_start;
    console.log(color_text(`\n⏲️  Total duration: ${format_duration(total_ms)}`, "cyan"));
    console.log(color_text("\n🏆 🎉 All steps completed successfully!\n", "green"));

  } catch (err) {
    console.error(err);
  } finally {
    await close_pools(); // Step 20: Close once, at the very end
  }

}



// ====================================================
main().catch(e => {
  log_error(e?.stack || e);
  // process.exit(1);
});

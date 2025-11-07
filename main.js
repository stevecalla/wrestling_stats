// run_simple_visual_numbered_test_snake.js (ESM)
import path from "path";
import { fileURLToPath } from "url";

import { determine_os_path } from "./utilities/directory_tools/determine_os_path.js";
import { create_directory } from "./utilities/directory_tools/create_directory.js";
import { color_text } from "./utilities/console_logs/console_colors.js";

// === imports for each step ===
import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import { step_1_run_alpha_wrestler_list } from "./src/step_1_get_wrestler_list.js";
import { step_2_write_wrestler_match_url_array } from "./src/step_2_create_wrestler_match_url_array.js";
import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";
import { execute_load_data_to_bigquery } from "./utilities/google_cloud/load_process/step_0_load_main_job.js"; // step 7 load google cloud & bigquery


import { step_9_close_chrome_dev } from "./src/step_9_close_chrome_developer.js";

// ====================================================
// 🧩 STEP TOGGLES todo:
// ====================================================
const step_flags = {
  step_0: false,  // 🚀 launch chrome
  step_1: false,  // 📄 get wrestler list
  step_2: false, // 🔗 optional URL array; normally false; step 3 uses step 1 output
  step_3: false,  // 🏟️ get match history
  step_4: false, // todo: reserved for get team list &/or team results (but should be able to use step 3)

  step_7: false, // load data into Google cloud / bigquery

  step_9: false,  // 🧹 close browser
};

// 🧪 each step can run test or full //todo:
const test_flags = {
  step_1_is_test: true, // run small sample for wrestler list
  step_3_is_test: true, // run small sample for match history
};

// ====================================================
// ⚙️ GLOBAL CONFIG — all tunable numbers here
// ====================================================
const config = {
  wrestling_season: "2024-25", // todo:
  // wrestling_season: "2025-26",

  url_home_page: "https://www.trackwrestling.com/",
  url_login_page: "https://www.trackwrestling.com/seasons/index.jsp",

  // Step #1 config
  alpha_list_limit_test: 1,
  alpha_list_limit_full: 30,

  // Step #3 config
  matches_page_limit_test: 5,
  matches_page_limit_full: 2000,
  step_3_loop_start: 0, // 🌀 starting index for Step #3 loop
};

// ====================================================
// 🎨 HELPERS
// ====================================================
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const adjusted_season = config.wrestling_season.replace("-", "_");

const step_icons = {
  0: "0️⃣", 1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣",
  5: "5️⃣", 6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣",
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
      wrestler_list_csv: path.join(input_dir,  `wrestlers_alpha_${adjusted_season}.csv`),
      url_array_js:      path.join(input_dir,  `wrestler_match_urls_${adjusted_season}.js`),
      match_csv:         path.join(output_dir, `tw_matches_full_${adjusted_season}.csv`),
    },
    browser: null,
    page: null,
    context: null,
  };

  // === STEP 0 ===
  if (step_flags.step_0) {
    const start = Date.now();
    log_step_start(0, "Launching Chrome DevTools 🚀");
    const { browser, page, context } = await step_0_launch_chrome_developer(config.url_home_page);
    ctx.browser = browser;
    ctx.page = page;
    ctx.context = context;
    log_step_success(0, "Chrome launched successfully", Date.now() - start);
  } else log_step_skip(0, "chrome launch");

  // === STEP 1 ===
  if (step_flags.step_1) {
    const start = Date.now();
    const is_test = test_flags.step_1_is_test;
    const limit = is_test ? config.alpha_list_limit_test : config.alpha_list_limit_full;
    log_step_start(1, `Fetching ${limit} wrestlers (${is_test ? "🧪 TEST MODE" : "FULL"}) 📄`);

    await step_1_run_alpha_wrestler_list(
      config.url_login_page,
      limit,
      config.wrestling_season,
      ctx.page,
      ctx.browser,
      ctx.paths.wrestler_list_csv,
      is_test
    );

    log_step_success(1, `Wrestler list saved → ${ctx.paths.wrestler_list_csv}`, Date.now() - start);
  } else log_step_skip(1, "wrestler list generation");

  // === STEP 2 ===
  if (step_flags.step_2) {
    const start = Date.now();
    log_step_start(2, "Building match URL array 🔗");

    await step_2_write_wrestler_match_url_array(
      ctx.paths.wrestler_list_csv,
      ctx.paths.url_array_js,
      path.basename(ctx.paths.url_array_js)
    );
    log_step_success(2, `URL array written → ${ctx.paths.url_array_js}`, Date.now() - start);
  } else log_step_skip(2, "URL array generation");

  // === STEP 3 ===
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
      ctx.page,
      ctx.browser,
      ctx.context,
      ctx.paths.match_csv,
      ctx.paths.wrestler_list_csv,
      is_test
    );

    log_step_success(3, `Match history saved → ${ctx.paths.match_csv}`, Date.now() - start);
  } else log_step_skip(3, "match history");
  
    // === STEP 7 ===
  if (step_flags.step_7) {
    const start = Date.now();
    log_step_start(7, "Start Loading Data to Google Cloud & Bigquery 🔗");

    await execute_load_data_to_bigquery();
    
    log_step_success(7, "Data loaded to Google Cloud & Bigquery", Date.now() - start);
  } else log_step_skip(7, "Load Data to Google Cloud & Bigquery 🔗");

  // === STEP 9 ===
  if (step_flags.step_9) {
    const start = Date.now();
    log_step_start(9, "Closing Chrome DevTools 🧹");

    await step_9_close_chrome_dev(ctx.browser, ctx.context);

    log_step_success(9, "Browser closed successfully", Date.now() - start);
  } else log_step_skip(9, "close browser");

  const total_ms = Date.now() - program_start;
  console.log(color_text(`\n⏲️  Total duration: ${format_duration(total_ms)}`, "cyan"));
  console.log(color_text("\n🏆 🎉 All steps completed successfully!\n", "green"));
}

// ====================================================
main().catch(e => {
  log_error(e?.stack || e);
  // process.exit(1);
});

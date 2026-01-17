// run_simple_visual_numbered_test_snake.js (ESM)
import path from "path";
import { fileURLToPath } from "url";

import { determine_os_path } from "./utilities/directory_tools/determine_os_path.js";
import { create_directory } from "./utilities/directory_tools/create_directory.js";
import { color_text } from "./utilities/console_logs/console_colors.js";

// === imports for each step ===
import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import { step_0_launch_chrome_developer_v3 } from "./src/step_3_get_wrestler_match_history_parallel_scrape_v4/step_0_launch_chrome_developer_v3.js";

import { step_1_run_alpha_wrestler_list } from "./src/step_1_get_wrestler_list.js";

import { step_2_get_team_schedule } from "./src/step_2_get_team_schedule.js";

import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";
import { step_3_get_wrestler_match_history_v2 } from "./src/step_3_get_wrestler_match_history_v2.js";
import { step_3_get_wrestler_match_history_v3 } from "./src/step_3_get_wrestler_match_history_v3.js";
import { step_3_get_wrestler_match_history_v4 } from "./src/step_3_get_wrestler_match_history_v4.js";

import { step_4_create_wrestler_match_history_data } from "./src/step_4_create_wrestler_match_history_metrics.js";

import { step_5_create_team_division_data } from "./src/step_5_create_team_division_data.js";

import { step_6_append_team_division_updates } from "./src/step_6_append_team_division_data.js"; // ad hoc updates for missing records

import { step_7_append_team_division_to_match_metrics } from "./src/step_7_append_team_division_to_match_metrics.js";

import { step_8_append_team_division_to_wrestler_list } from "./src/step_8_append_team_division_to_wreslter_list.js";

import { step_9_create_state_qualfier_reference } from "./src/step_9_create_state_qualfier_reference.js";

import { step_10_append_ad_hoc_wrestler_to_state_qualifier_list } from "./src/step_10_append_ad_hoc_wrestler_to_state_qualifier_list.js";

import { step_11_append_state_qualifier_to_match_metrics } from "./src/step_11_append_state_qualifier_to_match_metrics.js";

import { step_12_append_state_qualifier_to_wrestler_list } from "./src/step_12_append_state_qualifier_to_wrestler_list.js";

import { step_13_apply_2025_state_qualifier_team_division_to_2026 } from "./src/step_13_apply_2025_state_qualifier_team_division_to_2026.js";

import { step_14_append_onthemat_rankings_to_wrestler_list } from "./src/step_14_append_onthemat_rankings_to_wrestler_list.js";

import { step_15_append_onthemat_rankings_to_match_history } from "./src/step_15_append_onthemat_rankings_to_match_history.js";

import { execute_load_data_to_bigquery } from "./utilities/google_cloud/load_process/step_0_load_main_job.js"; // step_17 load google cloud & bigquery

import { step_18_transfer_tables_between_windows_and_mac } from "./utilities/transfer_tables_between_windows_and_mac/sync_wrestling_tables_full_refresh.js";

import { step_19_close_chrome_dev } from "./src/step_19_close_chrome_developer.js";

import { close_pools } from "./utilities/mysql/mysql_pool.js"; // Step 20
import { step_2a_append_team_id_to_team_schedule_data } from "./src/step_2a_append_team_id_to_team_schedule_data.js";
import { preflight_cdp_ports } from "./utilities/chrome_dev_tools/preflight_cdp_ports.js";

// ====================================================
// 🧩 STEP TOGGLES
// ====================================================
const step_flags = {

  // LAUNCH CHROME
  step_0: true,  // 🚀 launch chrome

  // GET WRESTLER LIST
  step_1: true,  // 📄 get wrestler list

  // GET TEAM SCHEDULE
  step_2: true, // get team schedule
  // step_2a: false, // happens inside step2; append team id to team schedule scrape data 

  // GET MATCH HISTORY
  step_3: false,  // 🏟️ get match history
  step_3_v2: false,  // 🏟️ get match history - parallel version that doesn't work
  step_3_v3: false,  // 🏟️ get match history - parallel version test, precessor to v4
  step_3_v4: true,  // 🏟️ get match history

  step_4: true, // 📄 create match history metrics

  // CREATE TEAM REGION / DIVISION
  step_5: true, // create team division
  step_6: true, // append team division to table (ad hoc updates for teams that don't have division/regoin data)
  step_7: true, // append team division to match history metrics
  step_8: true, // append team division to wrestler list

  // CREATE 2024-25 STATE QUALIFIER LIST
  step_9: true, // create 2024-25 state qualifier list
  step_10: true, // append team division to table (ad hoc updates for teams that don't have division/regoin data)
  step_11: true, // append state qualifier to match history metrics
  step_12: true, // append state qualifier to wrestler list

  // APPLY 2025 STATE QUALIFIER & TEAM DIVISION TO 2026 WRESTLER LIST
  step_13: true, // append 2025 state qualifier & team division to 2026 wrestler list

  // APPEND ONTHEMAT RANKINGS TO 2026 WRESTLER LIST
  step_14: true, // append ONTHEMAT rankings to 2026 wrestler list
  step_15: true, // append ONTHEMAT rankings to to match history metrics

  // LOAD GOOGLE CLOUD / BIGQUERY
  step_17: true, // load data into Google cloud / bigquery

  // TRANSFER TABLES BETWEEN WINDOWS & MAC
  step_18: true,  // 🧹 transfer tables between windos & mac

  step_19: false,  // 🧹 close browser
};

// 🧪 each step can run test or full
const test_flags = {
  step_1_is_test: false, // run small sample for wrestler list
  step_3_is_test: false, // run small sample for match history
  step_4_is_test: false, // run small sample for match history metrics
};

// ====================================================
// ⚙️ GLOBAL CONFIG — all tunable numbers here
// ====================================================
async function load_config(custom = {}) {
  const defaults = {
    governing_body: "Colorado High School Activities Association",

    // HIGH SCHOOL BOYS = set category, season & gender
    track_wrestling_category: "High School Boys",
    wrestling_season: "2024-25",
    // wrestling_season: "2025-26",
    gender: "M",

    // HIGH SCHOOL GIRLS = set category, season & gender
    // track_wrestling_category: "High School Girls",
    // wrestling_season: "2024-25",
    // wrestling_season: "2025-26",
    // gender: "F",

    // SQL WHERE STATEMENT
    sql_where_filter_state_qualifier: "",
    sql_where_filter_onthemat_ranking_list: "",
    sql_team_id_list: "",
    sql_wrestler_id_list: "",

    // URL
    url_home_page: "https://www.trackwrestling.com/",
    url_login_page: "https://www.trackwrestling.com/seasons/index.jsp",

    // STEP #1 CONFIG FOR TESTING
    alpha_list_limit_test: 1,   // only 26 letters in alpha; loops by alpha, by grade (1 = A for each grade)
    alpha_list_limit_full: 30,  // only 26 letters in alpha; loops by alpha, by grade

    // STEP #3 CONFIG FOR TESTING
    matches_page_limit_test: 2,
    matches_page_limit_full: 10000,
    step_3_loop_start: 0,
  };

  return { ...defaults, ...custom };
}

const hs_boys_2026 = {
  // HIGH SCHOOL BOYS = set category, season & gender
  track_wrestling_category: "High School Boys",
  wrestling_season: "2025-26",
  gender: "M",
  use_scheduled_events_iterator_query: true,
  use_wrestler_list_iterator_query: false,
  // sql_where_filter_state_qualifier: "AND wrestler_is_state_tournament_qualifier IS NOT NULL",
  // sql_team_id_list: "AND team_id IN (764192150, 839403150)",
  // sql_wrestler_id_list: "AND wrestler_id IN (35527236132, 35671717132)",
};

const hs_girls_2026 = {
  // HIGH SCHOOL BOYS = set category, season & gender
  track_wrestling_category: "High School Girls",
  wrestling_season: "2025-26",
  gender: "F",
  use_scheduled_events_iterator_query: false,
  use_wrestler_list_iterator_query: true,
  // sql_where_filter_state_qualifier: "AND wrestler_is_state_tournament_qualifier IS NOT NULL",
  // sql_team_id_list: "AND team_id IN (764192150, 839403150)",
  // sql_wrestler_id_list: "AND wrestler_id IN (35527236132, 35671717132)",
};

// ====================================================
// 🎨 HELPERS
// ====================================================
const step_icons = {
  0: "0️⃣", 1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 3_2: "3️⃣_2️⃣", 3_3: "3️⃣3️⃣", 3_4: "3️⃣4️⃣", 4: "4️⃣",
  5: "5️⃣", 6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣",
  10: "1️⃣0️⃣", 11: "1️⃣1️⃣", 12: "1️⃣2️⃣", 13: "1️⃣3️⃣", 14: "1️⃣4️⃣",
  15: "1️⃣5️⃣", 16: "1️⃣6️⃣", 17: "1️⃣7️⃣", 18: "1️⃣8️⃣", 19: "1️⃣9️⃣"
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
async function main(config) {
  const program_start = Date.now();

  // This line creates a complete configuration using defaults, but overrides any default with a user-provided value when available.
  config = await load_config(config);

  console.log(color_text(`\n🏁 ➕ Starting main program for ${config.wrestling_season}`, "red"));
  console.log(
    color_text(
      `\n🔧 Final Config Loaded for Season ${config.wrestling_season}\n` +
      `----------------------------------------------\n` +
      ` Governing Body         → ${config.governing_body}\n` +
      ` Category               → ${config.track_wrestling_category}\n` +
      ` Season                 → ${config.wrestling_season}\n` +
      ` Gender                 → ${config.gender}\n` +
      ` SQL State Qual Filter  → ${config.sql_where_filter_state_qualifier}\n` +
      ` SQL OnTheMat Filter    → ${config.sql_where_filter_onthemat_ranking_list}\n` +
      ` SQL Team Id List       → ${config.sql_team_id_list}\n` +
      ` SQL Wrestler Id List   → ${config.sql_wrestler_id_list}\n` +
      ` Home Page              → ${config.url_home_page}\n` +
      ` Login Page             → ${config.url_login_page}\n` +
      ` Alpha List (test)      → ${config.alpha_list_limit_test}\n` +
      ` Alpha List (full)      → ${config.alpha_list_limit_full}\n` +
      ` Matches (test)         → ${config.matches_page_limit_test}\n` +
      ` Matches (full)         → ${config.matches_page_limit_full}\n` +
      ` Step 3 Loop Start      → ${config.step_3_loop_start}\n` +
      `----------------------------------------------`,
      "cyan"
    )
  );

  const directory = determine_os_path();
  const input_dir = await create_directory("input", directory);
  const output_dir = await create_directory("output", directory);

  const adjusted_season = config.wrestling_season.replace("-", "_");
  const adjusted_gender = config.track_wrestling_category
    .toLowerCase()           // make lowercase
    .replace(/\s+/g, "_")
    ;   // replace all spaces with underscores

  const ctx = {
    config,
    paths: {
      input_dir,
      output_dir,
      wrestler_list_csv: path.join(input_dir, `wrestler_list_scrape_data_${adjusted_season}_${adjusted_gender}.csv`),
      team_schedule_csv: path.join(input_dir, `team_schedule_scrape_data_${adjusted_season}_${adjusted_gender}.csv`),
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

      // Kill any stale CDP listeners before launching / connecting
      await preflight_cdp_ports([9222, 9223, 9224, 9225, 9226]);

      const { browser, page, context } = await step_0_launch_chrome_developer(config.url_home_page);

      ctx.browser = browser;
      ctx.page = page;
      ctx.context = context;

      log_step_success(0, "Chrome launched successfully", Date.now() - start);
    } else log_step_skip(0, "Chrome launch");

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
    } else log_step_skip(1, "Wrestler list generation");

    // === STEP 2 SCRAPE TEAM SCHEDULE ===
    if (step_flags.step_2) {
      const start = Date.now();
      const is_test = test_flags.step_1_is_test;
      const limit = is_test ? config.matches_page_limit_test : config.matches_page_limit_full;
      const loop_start = config.step_3_loop_start;

      log_step_start(
        2,
        `Scraping team schedule (limit=${limit}, step_3_loop_start=${loop_start}) ${is_test ? "🧪 TEST MODE" : "🏟️ FULL"}`
      );

      await step_2_get_team_schedule(
        config.url_home_page,
        config.url_login_page,
        limit,
        loop_start,
        config.wrestling_season,
        config.track_wrestling_category,
        config.gender,
        config.sql_team_id_list,
        config.sql_wrestler_id_list,
        config.sql_where_filter_state_qualifier,
        ctx.page,
        ctx.browser,
        ctx.context,
        ctx.paths.team_schedule_csv,
        is_test
      );

      await step_2a_append_team_id_to_team_schedule_data();

      log_step_success(2, `Team schedule saved → ${ctx.paths.team_schedule_csv}`, Date.now() - start);
    } else log_step_skip(2, "Team schedule generation");

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
        config.sql_where_filter_state_qualifier,
        config.sql_where_filter_onthemat_ranking_list,
        config.sql_team_id_list,
        config.sql_wrestler_id_list,
        ctx.page,
        ctx.browser,
        ctx.context,
        ctx.paths.match_csv,
        config.use_scheduled_events_iterator_query,
        config.use_wrestler_list_iterator_query,
      );

      log_step_success(3, `Match history saved → ${ctx.paths.match_csv}`, Date.now() - start);
    } else log_step_skip(3, "Match history");

    // step_3_v2
    if (step_flags.step_3_v2) {
      const start = Date.now();

      const is_test = test_flags.step_3_is_test;
      const limit = is_test ? config.matches_page_limit_test : config.matches_page_limit_full;
      const loop_start = config.step_3_loop_start;

      log_step_start(
        3_2,
        `Scraping match history (limit=${limit}, step_3_v2_loop_start=${loop_start}) ${is_test ? "🧪 TEST MODE" : "🏟️ FULL"}`
      );

      await step_3_get_wrestler_match_history_v2(
        config.url_home_page,
        config.url_login_page,
        limit,
        loop_start,

        config.wrestling_season,
        config.track_wrestling_category,
        config.gender,

        config.sql_where_filter_state_qualifier,
        config.sql_where_filter_onthemat_ranking_list,
        config.sql_team_id_list,
        config.sql_wrestler_id_list,

        ctx.paths.match_csv,

        config.use_scheduled_events_iterator_query,
        config.use_wrestler_list_iterator_query,
      );

      log_step_success(3_2, `Match history saved → ${ctx.paths.match_csv}`, Date.now() - start);
    } else log_step_skip(3_2, "Match history");

    // parallel; no tracking ports 4 seems to work well, 5 okay, 6 fine
    // todo: not setup to run on linux or mac... need to adjust launch v3 and parallel v3
    if (step_flags.step_3_v3) {

      const port_list = [9223, 9224, 9225, 9226, 9227, 9228];

      const PORTS_TO_LAUNCH = 4;
      const ports_to_use = port_list.slice(0, PORTS_TO_LAUNCH);

      const LAUNCH_DELAY_MS = 750;
      const limit = 5;

      // =============================
      // TOTAL WALL TIME (START)
      // =============================
      const wall_start = Date.now();

      const results = await Promise.allSettled(
        ports_to_use.map((port, idx) => (async () => {

          // stagger launches
          if (idx > 0) {
            await new Promise(res => setTimeout(res, LAUNCH_DELAY_MS));
          }

          // =============================
          // PER-PORT TIME (START)
          // =============================
          const port_start = Date.now();

          // auto-advance by 5 per port
          const loop_start = idx * limit;

          log_step_start(
            3_3,
            `Scraping match history (port=${port}, loop_start=${loop_start}, limit=${limit})`
          );

          log_step_start(0, `Launching Chrome DevTools 🚀 (port=${port})`);

          const { browser, page, context } =
            await step_0_launch_chrome_developer_v3(config.url_home_page, port);

          log_step_success(
            0,
            `Chrome launched successfully (port=${port})`,
            Date.now() - port_start
          );

          const match_csv_path = ctx.paths.match_csv.replace(
            /\.csv$/i,
            `_${port}.csv`
          );

          await step_3_get_wrestler_match_history_v3(
            config.url_home_page,
            config.url_login_page,
            limit,
            loop_start,

            config.wrestling_season,
            config.track_wrestling_category,
            config.gender,

            config.sql_where_filter_state_qualifier,
            config.sql_where_filter_onthemat_ranking_list,
            config.sql_team_id_list,
            config.sql_wrestler_id_list,

            page,
            browser,
            context,
            port,

            match_csv_path,
            config.use_scheduled_events_iterator_query,
            config.use_wrestler_list_iterator_query
          );

          // =============================
          // PER-PORT TIME (END)
          // =============================
          const port_ms = Date.now() - port_start;

          log_step_success(
            3_3,
            `Match history saved → ${match_csv_path} (port=${port})`,
            port_ms
          );

          // Return timing so we can summarize after allSettled
          return { port, loop_start, limit, match_csv_path, port_ms };

        })())
      );

      // =============================
      // TOTAL WALL TIME (END)
      // =============================
      const wall_ms = Date.now() - wall_start;

      console.log("========================================");
      console.log(`⏱️  TOTAL WALL TIME: ${wall_ms} ms (${(wall_ms / 1000).toFixed(2)}s)`);
      console.log("========================================");

      // -----------------------------
      // INSPECT RESULTS + PORT TIMINGS
      // -----------------------------
      results.forEach((result, idx) => {
        const port = ports_to_use[idx];

        if (result.status === "rejected") {
          console.error(`❌ Port ${port} failed`, result.reason);
        } else {
          const { port_ms, loop_start, limit } = result.value;
          console.log(
            `✅ Port ${port} completed — port_time=${port_ms} ms (${(port_ms / 1000).toFixed(2)}s) ` +
            `slice=[${loop_start}..${loop_start + limit - 1}]`
          );
        }
      });

    } else log_step_skip(3_3, "Match history");

    // parallel with db tracking
    if (step_flags.step_3_v4) {

      const start = Date.now();

      const is_test = test_flags.step_3_is_test;
      const limit = is_test ? config.matches_page_limit_test : config.matches_page_limit_full;
      const loop_start = config.step_3_loop_start;

      // log_step_start(
      //   3_4,
      //   `Scraping match history (limit=${limit}, step_3_v2_loop_start=${loop_start}) ${is_test ? "🧪 TEST MODE" : "🏟️ FULL"}`
      // );

      await step_3_get_wrestler_match_history_v4(
        config.url_home_page,
        config.url_login_page,

        limit,
        loop_start,

        config.wrestling_season,
        config.track_wrestling_category,
        config.gender,

        config.sql_where_filter_state_qualifier,
        config.sql_where_filter_onthemat_ranking_list,
        config.sql_team_id_list,
        config.sql_wrestler_id_list,

        ctx.paths.match_csv,

        config.use_scheduled_events_iterator_query,
        config.use_wrestler_list_iterator_query,
      );

      log_step_success(3_4, `Match history saved → ${ctx.paths.match_csv}`, Date.now() - start);

    } else log_step_skip(3_4, "Match history");

    // === STEP 4  CREATE MATCH HISTORY METRICS ===
    if (step_flags.step_4) {
      const start = Date.now();
      const is_test = test_flags.step_4_is_test;
      const limit = is_test ? config.matches_page_limit_test : config.matches_page_limit_full;
      const loop_start = config.step_3_loop_start;

      log_step_start(4, `Create match history metrics (limit=${limit}, step_3_loop_start=${loop_start}) ${is_test ? "🧪 TEST MODE" : "🏟️ FULL"}`);

      await step_4_create_wrestler_match_history_data(config);

      log_step_success(4, `Match history metrics created → ${ctx.paths.match_csv}`, Date.now() - start);
    } else log_step_skip(4, "Create match history metrics");

    // === STEP 5 CREATE TEAM DIVISION ===
    if (step_flags.step_5) {
      const start = Date.now();

      log_step_start(5, "Start creating team division 🔗");

      await step_5_create_team_division_data();

      log_step_success(5, `Create team division`, Date.now() - start);
    } else log_step_skip(5, "Create team division");

    // === STEP 6 APPEND TEAM DIVISION UPDATES / MISSING REGION / DIVISION ===
    if (step_flags.step_6) {
      const start = Date.now();

      log_step_start(6, "Start append team division updates 🔗");

      await step_6_append_team_division_updates();

      log_step_success(6, `Append team division`, Date.now() - start);
    } else log_step_skip(6, "Append team division");

    // === STEP 7 APPEND TEAM DIVISION TO MATCH HISTORY ===
    if (step_flags.step_7) {
      const start = Date.now();

      log_step_start(7, "Start append team division updates to match history 🔗");

      await step_7_append_team_division_to_match_metrics();

      log_step_success(7, `Append team division updates to match history`, Date.now() - start);
    } else log_step_skip(7, "Append team division updates to match history");

    // === STEP 8 APPEND TEAM DIVISION TO WRESTLER LIST ===
    if (step_flags.step_8) {
      const start = Date.now();

      log_step_start(8, "Start append team division updates to wrestler list 🔗");

      await step_8_append_team_division_to_wrestler_list();

      log_step_success(8, `Append team division updates to wrestler list`, Date.now() - start);
    } else log_step_skip(8, "Append team division updates to wrestler list");

    // === STEP 9 CREATE STATE QUALIFIER PLACE REFERENCE ===
    if (step_flags.step_9) {
      const start = Date.now();

      log_step_start(9, "Start creating state qualifier place reference 🔗");

      await step_9_create_state_qualfier_reference();

      log_step_success(9, `Create state qualifier place reference`, Date.now() - start);
    } else log_step_skip(9, "Create state qualifier place reference");

    // === STEP 10 APPEND TEAM DIVISION UPDATES ===
    if (step_flags.step_10) {
      const start = Date.now();

      log_step_start(10, "Start ad hoc wrestler to state qualifier list 🔗");

      await step_10_append_ad_hoc_wrestler_to_state_qualifier_list();

      log_step_success(10, `Append ad hoc wrestler to state qualifier list`, Date.now() - start);
    } else log_step_skip(10, "Append ad hoc wrestler to state qualifier list");

    // === STEP 11 APPEND STATE QUALIFIER & PLACE TO MATCH HISTORY ===
    if (step_flags.step_11) {
      const start = Date.now();

      log_step_start(11, "Start append state qualifier & place updates to match history 🔗");

      await step_11_append_state_qualifier_to_match_metrics();

      log_step_success(11, `Append state qualifier & place updates to match history`, Date.now() - start);
    } else log_step_skip(11, "Append state qualifier & place updates to match history");

    // === STEP 12 APPEND STATE QUALIFIER & PLACE TO WRESTLER LIST ===
    if (step_flags.step_12) {
      const start = Date.now();

      log_step_start(12, "Start append state qualifier & place updates to wrestler list 🔗");

      await step_12_append_state_qualifier_to_wrestler_list();

      log_step_success(12, `Append state qualifier & place updates to wrestler list`, Date.now() - start);
    } else log_step_skip(12, "Append state qualifier & place updates to wrestler list");

    // === STEP 13 APPEND 2025 STATE QUALIFIER TEAM DIVISION TO 2026 ===
    if (step_flags.step_13) {
      const start = Date.now();

      log_step_start(13, "Start append 2025 state qualifier & place to 2026 🔗");

      await step_13_apply_2025_state_qualifier_team_division_to_2026();

      log_step_success(13, `Append 2025 state qualifier & place to 2026`, Date.now() - start);
    } else log_step_skip(13, "Append 2025 state qualifier & place to 2026");

    // === STEP 14 APPEND ONTHEMAT RANKINGS TO 2026 ===
    if (step_flags.step_14) {
      const start = Date.now();

      log_step_start(14, "Start append ONTHEMAT rankings to 2026 wrestler list 🔗");

      await step_14_append_onthemat_rankings_to_wrestler_list();

      log_step_success(14, `Append append ONTHEMAT rankings to 2026 wrestler list`, Date.now() - start);
    } else log_step_skip(14, "Append append ONTHEMAT rankings to 2026 wrestler list");

    // === STEP 15 APPEND ONTHEMAT RANKINGS TO 2026 ===
    if (step_flags.step_15) {
      const start = Date.now();

      log_step_start(15, "Start append ONTHEMAT rankings to 2026 match history metrics🔗");

      await step_15_append_onthemat_rankings_to_match_history();

      log_step_success(15, `Append append ONTHEMAT rankings to 2026 match history metrics`, Date.now() - start);
    } else log_step_skip(15, "Append append ONTHEMAT rankings to 2026 match history metrics");

    // === step_17 LOAD DATA TO GOOGLE CLOULD / BIGQUERY ===
    if (step_flags.step_17) {
      const start = Date.now();

      log_step_start(17, "Start Loading Data to Google Cloud & Bigquery 🔗");

      await execute_load_data_to_bigquery("wrestler");

      log_step_success(17, "Data loaded to Google Cloud & Bigquery", Date.now() - start);
    } else log_step_skip(17, "Load Data to Google Cloud & Bigquery 🔗");

    // === STEP 18 CLOSE BROWSER ===
    if (step_flags.step_18) {
      const start = Date.now();
      log_step_start(18, "Transfer tables between windows & mac 🧹");

      await step_18_transfer_tables_between_windows_and_mac();

      log_step_success(18, "Transfer tables between windows & mac successfully", Date.now() - start);
    } else log_step_skip(18, "Transfer tables between windows & mac");

    // === STEP 19 CLOSE BROWSER ===
    if (step_flags.step_19) {
      const start = Date.now();
      log_step_start(19, "Closing Chrome DevTools 🧹");

      await step_19_close_chrome_dev(ctx.browser, ctx.context);

      log_step_success(19, "Browser closed successfully", Date.now() - start);
    } else log_step_skip(19, "Close browser");

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
// main(hs_girls_2026).catch(e => {
//   log_error(e?.stack || e);
//   // process.exit(1);
// });

export { main as execute_scrape_track_wrestling }

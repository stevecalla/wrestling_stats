// src/step_3_get_wrestler_match_history_v2.js
//
// Orchestrates:
//   STEP 1 → create tasks table
//   STEP 2 → seed tasks
//   STEP 4 → run parallel workers (which call STEP 3 workers internally)

import { step_1_create_scrape_tasks_table } from "./step_3_get_wrestler_match_history_parallel_scrape/step_1_create_scrape_tasks_table.js";

import { step_2_seed_tasks } from "./step_3_get_wrestler_match_history_parallel_scrape/step_2_insert_seed_tasks.js";

// import { step_4_run_workers } from "./step_3_get_wrestler_match_history_parallel_scrape/step_4_run_workers.js";

import { color_text } from "../utilities/console_logs/console_colors.js";

/* -------------------------------------------------
    GET MTN TIME
--------------------------------------------------*/
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

function get_now_mtn() {
    const now_utc = new Date();
    const offset_hours = get_mountain_time_offset_hours(now_utc);
    return new Date(now_utc.getTime() + offset_hours * 60 * 60 * 1000);
}

// function format_ymd(date) {
//     return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
// }

function format_ymd(date) {
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    const dd = String(date.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
}

function format_ymd_hour(date) {
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    const dd = String(date.getDate()).padStart(2, "0");
    const hh = String(date.getHours()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}T${hh}`;
}

/* -------------------------------------------------
   STEP 3 ORCHESTRATOR
--------------------------------------------------*/

async function run_step_3() {
    // -----------------------------------------------
    // STEP 1: ensure scrape task table exists
    // -----------------------------------------------
    await step_1_create_scrape_tasks_table();

    // -----------------------------------------------
    // STEP 2: seed tasks
    // -----------------------------------------------
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

    const now_mtn = get_now_mtn();

    const { task_set_id } = await step_2_seed_tasks({
        wrestling_season: hs_boys_2026.wrestling_season,
        track_wrestling_category: hs_boys_2026.track_wrestling_category,
        gender: hs_boys_2026.gender,

        sql_where_filter_state_qualifier: "",
        sql_team_id_list: "",
        sql_wrestler_id_list: "",

        job_type: "hs_boys_2026",

        seed_limit: 10,          // 👈 only seed 10 tasks; set to 0 to eliminate limit

        reset_pending: true,   // if true, sets DONE/FAILED back to PENDING

        time_bucket: format_ymd(now_mtn),           // daily MTN bucket
        // time_bucket: format_ymd_hour(now_mtn),   // hourly MTN bucket

        prune_keep_last_n: 3,
    });


    console.log(
        color_text(
            `\n📌 Step_3 task_set_id = ${task_set_id}`,
            "cyan"
        )
    );

    // -----------------------------------------------
    // STEP 4: run parallel workers (STEP 3 workers)
    // -----------------------------------------------
    // const t0 = Date.now(); // ⏱ start timer

    // const { processed, done, failed, results } = await step_4_run_workers({
    //     // required
    //     task_set_id,

    //     // optional parallelism
    //     workers: 2,
    //     batch_size: 1,

    //     // safety / retry behavior
    //     max_attempts: 3,
    //     lock_ttl_minutes: 30,

    //     // worker behavior
    //     idle_sleep_ms: 1500,
    //     log_every_batches: 5,

    //     // scraper behavior (passed to step_3_run_worker)
    //     url_home_page: "https://www.trackwrestling.com/",
    //     url_login_page: "https://www.trackwrestling.com/seasons/index.jsp",

    //     headless: false,
    //     slow_mo_ms: 50,
    //     navigation_timeout_ms: 30000,

    //     // optional behavior
    //     quiet: false, // if true: skip logs (except errors)
    // });

    // const elapsed_ms = Date.now() - t0;
    // const total_seconds = Math.floor(elapsed_ms / 1000);

    // const hh = String(Math.floor(total_seconds / 3600)).padStart(2, "0");
    // const mm = String(Math.floor((total_seconds % 3600) / 60)).padStart(2, "0");
    // const ss = String(total_seconds % 60).padStart(2, "0");

    // const elapsed_hms = `${hh}:${mm}:${ss} HH:MM:SS`;

    // console.log(
    //     color_text(
    //         `\n✅ Step_3 complete\n` +
    //         `   processed=${processed}\n` +
    //         `   done=${done}\n` +
    //         `   failed=${failed}\n` +
    //         `   results=${results}\n` +
    //         `   elapsed=${elapsed_hms}\n`,
    //         failed ? "yellow" : "green"
    //     )
    // );

}

/* -------------------------------------------------
   invoke orchestrator
--------------------------------------------------*/

await run_step_3();




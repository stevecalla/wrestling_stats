// src/step_3_get_wrestler_match_history_v2.js
//
// Orchestrates:
//   STEP 1 → create tasks table
//   STEP 2 → seed tasks
//   STEP 4 → run parallel workers (which call STEP 3 workers internally)
//
// ✅ Update in this version:
// - Stop launching a single Chrome/page here
// - Call step_5_run_workers(...) and pass port_list
// - Keep your existing task seeding + task_set_id logging
// - Keep HH:MM:SS timer output

import { step_1_create_scrape_tasks_table } from "./step_3_get_wrestler_match_history_parallel_scrape/step_1_create_scrape_tasks_table.js";

import { step_2_seed_tasks } from "./step_3_get_wrestler_match_history_parallel_scrape/step_2_insert_seed_tasks.js";

import { step_5_run_workers } from "./step_3_get_wrestler_match_history_parallel_scrape/step_5_run_workers.js";

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

function format_ymd(date) {
    // takes in: 2025-12-22T01:13:16.162Z
    const formatted_date = date.toISOString().slice(0, 10);
    return formatted_date; // "2025-12-22"
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
export async function main(
    url_home_page,
    url_login_page,
    matches_page_limit = 5,
    loop_start = 0,

    wrestling_season = "2024-25",
    track_wrestling_category = "High School Boys",
    gender,

    sql_where_filter_state_qualifier,
    sql_where_filter_onthemat_ranking_list,
    sql_team_id_list,
    sql_wrestler_id_list,

    file_path,

    use_scheduled_events_iterator_query = false,
    use_wrestler_list_iterator_query = true
) {
    console.log("********** 2", file_path);

    // -----------------------------------------------
    // STEP 1: ensure scrape task table exists
    // -----------------------------------------------
    await step_1_create_scrape_tasks_table();

    // -----------------------------------------------
    // STEP 2: seed tasks
    // -----------------------------------------------
    const now_mtn = get_now_mtn();

    const { task_set_id } = await step_2_seed_tasks({
        wrestling_season,
        track_wrestling_category,
        gender,

        // optional list filters
        sql_where_filter_state_qualifier,
        sql_where_filter_onthemat_ranking_list,
        sql_team_id_list,
        sql_wrestler_id_list,

        use_scheduled_events_iterator_query,
        use_wrestler_list_iterator_query,

        job_type: `${wrestling_season} ${track_wrestling_category} ${sql_where_filter_state_qualifier} ${sql_where_filter_onthemat_ranking_list} ${sql_team_id_list} ${sql_wrestler_id_list}`,

        seed_limit: 0, // 👈 only seed 10 tasks; set to 0 to eliminate limit
        reset_pending: true, // if true, sets DONE/FAILED back to PENDING
        time_bucket: format_ymd(now_mtn), // daily MTN bucket
        // time_bucket: format_ymd_hour(now_mtn), // hourly MTN bucket
        prune_keep_last_n: 3,
    });

    console.log(color_text(`\n📌 Step_3 task_set_id = ${task_set_id}`, "cyan"));

    // -----------------------------------------------
    // STEP 4: run parallel workers (each worker uses a port from port_list)
    // -----------------------------------------------
    const port_list = [9223, 9224, 9225, 9226];

    const t0 = Date.now(); // ⏱ start timer

    const { processed, done, failed, results } = await step_5_run_workers({
        // required
        task_set_id,

        // V1
        // batch_size: 5,
        // idle_sleep_ms: 1500,
        // slow_mo_ms: 50,

        // V2
        // batch_size: 1, // note: test
        // idle_sleep_ms: 200, // note: test
        // slow_mo_ms: 10, // note: test

        // optional parallelism
        workers: Math.min(1, port_list.length), // 👈 bump to 4 when ready
        batch_size: 5,

        // safety / retry behavior
        max_attempts: 3,
        lock_ttl_minutes: 30,

        // worker behavior
        idle_sleep_ms: 1500,
        log_every_batches: 5,

        // scraper behavior (passed through to workers/sessions)
        url_home_page,
        url_login_page,

        matches_page_limit,
        loop_start,

        wrestling_season,
        track_wrestling_category,
        gender,

        file_path,

        slow_mo_ms: 50,
        navigation_timeout_ms: 30000,

        // ✅ NEW: ports for per-worker chrome sessions
        port_list,

        // optional behavior
        quiet: false, // if true: skip logs (except errors)
    });

    const elapsed_ms = Date.now() - t0;
    const total_seconds = Math.floor(elapsed_ms / 1000);

    const hh = String(Math.floor(total_seconds / 3600)).padStart(2, "0");
    const mm = String(Math.floor((total_seconds % 3600) / 60)).padStart(2, "0");
    const ss = String(total_seconds % 60).padStart(2, "0");

    const elapsed_hms = `${hh}:${mm}:${ss}`;

    console.log(
        color_text(
            `\n✅ Step_3 complete\n` +
            `   processed=${processed}\n` +
            `   done=${done}\n` +
            `   failed=${failed}\n` +
            `   results=${JSON.stringify(results)}\n` +
            `   elapsed=${elapsed_hms} (HH:MM:SS)\n`,
            failed ? "yellow" : "green"
        )
    );
}

/* -------------------------------------------------
   invoke orchestrator
--------------------------------------------------*/

export { main as step_3_get_wrestler_match_history_v2 };

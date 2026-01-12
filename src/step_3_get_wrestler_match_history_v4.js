// src\step_3_get_wrestler_match_history_v4.js
import os from "os";

import { step_0_launch_chrome_developer_v3 } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_0_launch_chrome_developer_v3.js";
import { step_1_create_scrape_tasks_table, step_1_truncate_scrape_tasks_table } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_1_create_scrape_tasks_table.js";
import { step_2_seed_tasks } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_2_insert_seed_tasks.js";
import { step_3_get_match_history_worker_v4 } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_3_get_match_history_worker_v4.js";

import { color_text } from "../utilities/console_logs/console_colors.js";

// import { build_worker_id } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_4_scrape_tasks_repo.js";

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

/* -------------------------------------------------
   STEP 3 ORCHESTRATOR
--------------------------------------------------*/
export async function main(
  url_home_page,
  url_login_page,

  limit,
  loop_start,

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
  // -----------------------------------------------
  // STEP 1: ensure scrape task table exists
  // -----------------------------------------------
  await step_1_truncate_scrape_tasks_table();
  await step_1_create_scrape_tasks_table();

  // -----------------------------------------------
  // STEP 2: seed tasks (returns task_set_id)
  // -----------------------------------------------
  const now_mtn = get_now_mtn();

  const { task_set_id } = await step_2_seed_tasks({
    wrestling_season,
    track_wrestling_category,
    gender,

    sql_where_filter_state_qualifier,
    sql_where_filter_onthemat_ranking_list,
    sql_team_id_list,
    sql_wrestler_id_list,

    use_scheduled_events_iterator_query,
    use_wrestler_list_iterator_query,

    job_type: `${wrestling_season} ${track_wrestling_category} ${sql_where_filter_state_qualifier} ${sql_where_filter_onthemat_ranking_list} ${sql_team_id_list} ${sql_wrestler_id_list}`,

    seed_limit: 0, // 👈 only seed n tasks; set to 0 to eliminate limit
    reset_pending: true, // if true, sets DONE/FAILED back to PENDING

    time_bucket: format_ymd(now_mtn), // daily MTN bucket
    // time_bucket: format_ymd_hour(now_mtn), // hourly MTN bucket
    prune_keep_last_n: 3,
  });

  console.log(color_text(`\n📌 task_set_id = ${task_set_id}`, "cyan"));

  // -----------------------------------------------
  // STEP 4: run parallel workers
  // -----------------------------------------------
  const port_list = [9223, 9224, 9225, 9226, 9227, 9228];

  const PORTS_TO_LAUNCH = 4;
  const ports_to_use = port_list.slice(0, PORTS_TO_LAUNCH);

  const LAUNCH_DELAY_MS = 750;

  // how many tasks to claim per DB transaction
  const claim_batch_size = 15;

  const wall_start = Date.now();

  async function build_worker_id(port) {
    return `${os.hostname()}|pid=${process.pid}|port=${port}`;
  }

  const results = await Promise.allSettled(
    ports_to_use.map((port, idx) => (async () => {
      if (idx > 0) await new Promise((res) => setTimeout(res, LAUNCH_DELAY_MS));

      const port_start = Date.now();
      const worker_id = await build_worker_id(port);

      console.log(color_text(`\n🚀 Worker starting: ${worker_id}`, "cyan"));

      console.log(color_text(`0️⃣ Step #0: Launching Chrome DevTools (port=${port})`, "cyan"));
      const { browser, page, context } = await step_0_launch_chrome_developer_v3(url_home_page, port);

      const match_csv_path = file_path.replace(/\.csv$/i, `_${port}.csv`);

      // ✅ worker runs until it can't claim any more tasks
      const worker_summary = await step_3_get_match_history_worker_v4({
        url_home_page,
        url_login_page,

        limit,
        loop_start,

        wrestling_season,
        track_wrestling_category,
        gender,

        sql_where_filter_state_qualifier,
        sql_where_filter_onthemat_ranking_list,
        sql_team_id_list,
        sql_wrestler_id_list,

        page,
        browser,
        context,
        port,

        file_path: match_csv_path,

        use_scheduled_events_iterator_query,
        use_wrestler_list_iterator_query,

        /* =========================================================
         ✅ REQUIRED CHANGE ONLY:
         new params to support tasks table mode
        ========================================================= */
        task_set_id,
        worker_id,
        claim_batch_size
      });

      const port_ms = Date.now() - port_start;

      return { port, port_ms, match_csv_path };
    })())
  );

  const wall_ms = Date.now() - wall_start;

  console.log("========================================");
  console.log(`⏱️  TOTAL WALL TIME: ${wall_ms} ms (${(wall_ms / 1000).toFixed(2)}s)`);
  console.log("========================================");

  results.forEach((r, idx) => {
    const port = ports_to_use[idx];

    if (r.status === "rejected") {
      console.error(`❌ Port ${port} failed`, r.reason);
      return;
    }

    const { port_ms } = r.value;
    console.log(
      `✅ Port ${port} done — port_time=${port_ms} ms (${(port_ms / 1000).toFixed(2)}s) `
    );
  });
}

export { main as step_3_get_wrestler_match_history_v4 };

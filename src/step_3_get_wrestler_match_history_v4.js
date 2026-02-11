// src\step_3_get_wrestler_match_history_v4.js
import os from "os";

import { step_0_launch_chrome_developer_v3 } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_0_launch_chrome_developer_v3.js";
import { step_1_create_scrape_tasks_table, step_1_truncate_scrape_tasks_table } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_1_create_scrape_tasks_table.js";
import { step_2_seed_tasks, requeue_locked_failed_for_task_set } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_2_insert_seed_tasks.js";
import { step_3_get_match_history_worker_v4 } from "./step_3_get_wrestler_match_history_parallel_scrape_v4/step_3_get_match_history_worker_v4.js";

import { color_text } from "../utilities/console_logs/console_colors.js";

import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

function get_now_mtn() {
  const now_utc = new Date();
  const offset_hours = get_mountain_time_offset_hours(now_utc);
  return new Date(now_utc.getTime() + offset_hours * 60 * 60 * 1000);
}

function format_ymd(date) {
  return date.toISOString().slice(0, 10);
}

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
  use_wrestler_list_iterator_query = true,

  reset_tasks_table = false, // true = truncate/reset; false = resume

  TASK_SET_ID_OVERRIDE = null // ✅ pass PASS-1 task_set_id into PASS-2
) {
  // -----------------------------------------------
  // STEP 1: ensure scrape task table exists
  // -----------------------------------------------
  await step_1_create_scrape_tasks_table();

  let task_set_id = TASK_SET_ID_OVERRIDE || null;

  if (reset_tasks_table) {
    await step_1_truncate_scrape_tasks_table();

    // -----------------------------------------------
    // STEP 2: seed tasks (returns task_set_id)
    // -----------------------------------------------
    const now_mtn = get_now_mtn();

    const seeded = await step_2_seed_tasks({
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

      seed_limit: 0,
      reset_pending: false, // if NOT truncating, requeue LOCKED/FAILED for same task_set_id

      time_bucket: format_ymd(now_mtn),
      prune_keep_last_n: 3,

      task_set_id: TASK_SET_ID_OVERRIDE, // ✅ NEW: force reuse when provided
    });

    task_set_id = seeded.task_set_id;// ✅ assign to outer-scope var

    console.log(color_text(`\n📌 task_set_id = ${task_set_id}`, "cyan"));
  }

  // -----------------------------------------------
  // STEP 3: resume behavior (requeue LOCKED/FAILED)
  // -----------------------------------------------
  if (!task_set_id) {
    throw new Error(
      "No task_set_id available. If reset_tasks_table=false, pass TASK_SET_ID_OVERRIDE. If reset_tasks_table=true, seeding must return task_set_id."
    );
  }

  if (!reset_tasks_table) {
    //  ✅ compute your updated_at timestamps the same way you do in seeder
    const now_utc = new Date();
    const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
    const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

    const { requeued_count } = await requeue_locked_failed_for_task_set({
      task_set_id,
      updated_at_mtn: now_mtn,
      updated_at_utc: now_utc,
    });

    console.log(color_text(`♻️ Requeued LOCKED/FAILED: ${requeued_count}`, "yellow"));
  }


  // -----------------------------------------------
  // STEP 4: run parallel workers
  // -----------------------------------------------
  const port_list = [9223, 9224, 9225, 9226, 9227, 9228];
  const PORTS_TO_LAUNCH = 3;
  const ports_to_use = port_list.slice(0, PORTS_TO_LAUNCH);
  const LAUNCH_DELAY_MS = 2500;
  const claim_batch_size = 15;

  const wall_start = Date.now();

  async function build_worker_id(port) {
    return `${os.hostname()}|pid=${process.pid}|port=${port}`;
  }

  const results = await Promise.allSettled(
    ports_to_use.map((port, idx) => (async () => {
      if (idx > 0) await new Promise(res => setTimeout(res, idx * LAUNCH_DELAY_MS));

      const port_start = Date.now();
      const worker_id = await build_worker_id(port);

      console.log(color_text(`\n🚀 Worker starting: ${worker_id}`, "cyan"));

      console.log(color_text(`0️⃣ Step #0: Launching Chrome DevTools (port=${port})`, "cyan"));
      const { browser, page, context } = await step_0_launch_chrome_developer_v3(url_home_page, port);

      const match_csv_path = file_path.replace(/\.csv$/i, `_${port}.csv`);

      await step_3_get_match_history_worker_v4({
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
    console.log(`✅ Port ${port} done — port_time=${port_ms} ms (${(port_ms / 1000).toFixed(2)}s)`);
  });

  return { task_set_id }; // ✅ NEW: so wrapper can pass into PASS 2
}

export { main as step_3_get_wrestler_match_history_v4 };
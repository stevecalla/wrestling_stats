// src/step_3_get_wrestler_match_history_parallel_scrape/step_2_insert_seed_tasks.js
// Seeds tasks from wrestler_list_scrape_data into wrestler_match_history_scrape_tasks
//
// ✅ v2: uses task_set_id so overlapping scheduled jobs do not collide.
// ✅ upsert: inserts new tasks; updates name_link for existing tasks in same task_set_id.
// ✅ optional reset: only resets tasks for the task_set_id (not global).
// ✅ optional prune: delete old task sets to prevent table growth.
//
// 🔧 UPDATE: task_set_id now includes a readable date postfix (YYYY-MM-DD)
//            so it's easy to tell which run/day created the task set.
//
// 🔧 UPDATE: writes BOTH MTN + UTC timestamps (created/updated) to match house pattern:
//            created_at_utc / created_at_mtn / updated_at_utc / updated_at_mtn

import crypto from "crypto";

import { get_pool } from "../../utilities/mysql/mysql_pool.js";
import { color_text } from "../../utilities/console_logs/console_colors.js";

import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";
import { step_1_create_scrape_tasks_table } from "./step_1_create_scrape_tasks_table.js";

/* -------------------------------------------------
   helpers
--------------------------------------------------*/

// ✅ produce YYYY-MM-DD from:
// - time_bucket if it's already a date-like postfix
// - otherwise "today" (UTC) for stable labeling
// NOTE: we still keep this fallback as UTC because time_bucket is expected to be passed
// from the orchestrator in MTN if you want MTN alignment.
function derive_date_postfix(time_bucket) {
  const tb = String(time_bucket || "").trim();

  // If caller passes something like "2025-12-21" or "2025-12-21T18"
  const m = tb.match(/^(\d{4}-\d{2}-\d{2})/);
  if (m && m[1]) return m[1];

  // Default: today's UTC date
  return new Date().toISOString().slice(0, 10);
}

// stable-ish id: lets two jobs run at different times with different scopes
// You can pass your own task_set_id if desired.
function build_task_set_id({
  wrestling_season,
  track_wrestling_category,
  gender,
  job_type,
  job_name = "step_3",
  // include a time bucket to avoid collisions if you want uniqueness per run
  // e.g. "2025-12-21T18" (hourly bucket) or a fixed string for daily schedules
  time_bucket = null,
} = {}) {
  const base = [
    job_name,
    wrestling_season,
    track_wrestling_category,
    gender,
    job_type,
    time_bucket || "static",
  ].join("|");

  // shorter friendly hash
  const hash = crypto.createHash("sha1").update(base).digest("hex").slice(0, 12);

  // 🔧 readable date postfix first
  const date_postfix = derive_date_postfix(time_bucket);

  // example:
  // step_3_hs_boys_2026_e10482704f76_2025-12-21
  return `${job_name}_${job_type}_${hash}_${date_postfix}`;
}

// optional: hard prune by keeping last N distinct task_set_id for this scope
async function prune_old_task_sets({
  pool,
  wrestling_season,
  track_wrestling_category,
  gender,
  job_type,
  keep_last_n = 3,
} = {}) {
  // Find old task_set_ids by most recent updated_at_utc
  const [rows] = await pool.query(
    `
      SELECT task_set_id, MAX(updated_at_utc) AS last_seen
      FROM wrestler_match_history_scrape_tasks
      WHERE wrestling_season=? AND track_wrestling_category=? AND gender=? AND job_type=?
      GROUP BY task_set_id
      ORDER BY last_seen DESC
    `,
    [wrestling_season, track_wrestling_category, gender, job_type]
  );

  const to_delete = rows.slice(keep_last_n).map((r) => r.task_set_id);
  if (!to_delete.length) return { deleted_sets: 0, deleted_rows: 0 };

  const [del] = await pool.query(
    `
      DELETE FROM wrestler_match_history_scrape_tasks
      WHERE task_set_id IN (${to_delete.map(() => "?").join(",")})
    `,
    to_delete
  );

  return { deleted_sets: to_delete.length, deleted_rows: del?.affectedRows ?? 0 };
}

/* -------------------------------------------------
   main seeder
--------------------------------------------------*/

async function main({
  task_set_id = null, // optional override
  job_type = "list", // "list" | "events" (or other future job_types)
  job_name = "step_3",

  // scope
  wrestling_season,
  track_wrestling_category,
  gender,

  // optional list filters
  sql_where_filter_state_qualifier = "",
  sql_team_id_list = "",
  sql_wrestler_id_list = "",

  // behavior
  reset_pending = false, // if true: resets tasks only for this task_set_id
  prune_keep_last_n = 0, // if >0: prunes old task sets for this scope/job_type (keeps last N)
  // if you want a unique task_set_id per run, pass something like:
  // time_bucket: new Date().toISOString().slice(0, 13)  // hourly
  time_bucket = null,
  seed_limit = 0, // if >0, LIMIT N rows from wrestler_list_scrape_data
} = {}) {
  if (!wrestling_season || !track_wrestling_category || !gender) {
    throw new Error(
      "seed_tasks missing required args: wrestling_season, track_wrestling_category, gender"
    );
  }

  const pool = await get_pool();

  // ✅ ensure schema exists (this MUST add created_at_utc/mtn and updated_at_utc/mtn columns)
  // await step_1_create_scrape_tasks_table();

  // ✅ Batch timestamps (UTC → MTN) — house pattern
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  // For inserts:
  const created_at_mtn = now_mtn;
  const created_at_utc = now_utc;

  // For updates (and also initial insert's updated_*):
  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  const resolved_task_set_id =
    task_set_id ||
    build_task_set_id({
      wrestling_season,
      track_wrestling_category,
      gender,
      job_type,
      job_name,
      time_bucket,
    });

  console.log(
    color_text(
      `\n🌱 Seeding step_3 tasks\n` +
        `   task_set_id=${resolved_task_set_id}\n` +
        `   job_type=${job_type}\n` +
        `   scope=${wrestling_season} / ${track_wrestling_category} / ${gender}\n` +
        `   date_postfix=${derive_date_postfix(time_bucket)}\n` +
        `   now_mtn=${created_at_mtn.toISOString()}\n`,
        `   now_utc=${created_at_utc.toISOString()}\n` +
      "cyan"
    )
  );

  // ✅ Upsert tasks for this task_set_id only.
  // Note: status is always inserted as PENDING for new rows.
  // Existing rows keep their status unless you reset_pending=true.
  const [insert_result] = await pool.query(
    `
      INSERT INTO wrestler_match_history_scrape_tasks
        (
          task_set_id,
          job_type,
          wrestling_season,
          track_wrestling_category,
          gender,
          wrestler_id,
          name_link,
          status,
          attempt_count,
          created_at_mtn,
          updated_at_mtn,
          created_at_utc,
          updated_at_utc
        )
      SELECT
        ? AS task_set_id,
        ? AS job_type,
        w.wrestling_season,
        w.track_wrestling_category,
        w.gender,
        w.wrestler_id,
        w.name_link,
        'PENDING' AS status,
        0 AS attempt_count,
        ? AS created_at_mtn,
        ? AS updated_at_mtn,
        ? AS created_at_utc,
        ? AS updated_at_utc
      FROM wrestler_list_scrape_data w
      WHERE 1=1
        AND w.name_link IS NOT NULL AND w.name_link <> ''
        AND w.wrestling_season = ?
        AND w.track_wrestling_category = ?
        AND w.gender = ?
        ${sql_where_filter_state_qualifier}
        ${sql_team_id_list}
        ${sql_wrestler_id_list}
      ${seed_limit && seed_limit > 0 ? `LIMIT ${Number(seed_limit)}` : ``} -- LIMIT
      ON DUPLICATE KEY UPDATE
        name_link = VALUES(name_link),
        updated_at_mtn = VALUES(updated_at_mtn),
        updated_at_utc = VALUES(updated_at_utc)
    `,
    [
      resolved_task_set_id,
      job_type,

      created_at_mtn,
      updated_at_mtn,
      created_at_utc,
      updated_at_utc,

      wrestling_season,
      track_wrestling_category,
      gender,
    ]
  );

  console.log(
    color_text(
      `✅ Seed insert/upsert affected rows: ${insert_result?.affectedRows ?? "?"}`,
      "green"
    )
  );

  // Optional: reset only this task_set_id
  if (reset_pending) {
    const [reset_result] = await pool.query(
      `
        UPDATE wrestler_match_history_scrape_tasks
        SET status='PENDING',
            locked_by=NULL,
            locked_at_utc=NULL,
            last_error=NULL,
            attempt_count=0,
            updated_at_mtn=?,
            updated_at_utc=?
        WHERE task_set_id=?
      `,
      [updated_at_mtn, updated_at_utc, resolved_task_set_id]
    );

    console.log(
      color_text(
        `♻️ Reset tasks to PENDING (task_set_id only): ${reset_result?.affectedRows ?? "?"}`,
        "yellow"
      )
    );
  }

  // Optional: prune older task_set_id groups for this scope/job_type
  if (prune_keep_last_n && prune_keep_last_n > 0) {
    const { deleted_sets, deleted_rows } = await prune_old_task_sets({
      pool,
      wrestling_season,
      track_wrestling_category,
      gender,
      job_type,
      keep_last_n: prune_keep_last_n,
    });

    console.log(
      color_text(
        `🧹 Prune old task sets — deleted_sets=${deleted_sets}, deleted_rows=${deleted_rows} (kept last ${prune_keep_last_n})`,
        "yellow"
      )
    );
  }

  // Quick counts for this task_set_id
  const [counts] = await pool.query(
    `
      SELECT status, COUNT(*) AS cnt
      FROM wrestler_match_history_scrape_tasks
      WHERE task_set_id=?
      GROUP BY status
      ORDER BY status
    `,
    [resolved_task_set_id]
  );

  console.log(color_text("\n📊 Task counts (this task_set_id):", "cyan"));
  for (const r of counts) console.log(`- ${r.status}: ${r.cnt}`);

  return { task_set_id: resolved_task_set_id };
}

export { main as step_2_seed_tasks };

// utilities/mysql/iter_name_links_from_db.js
import { get_pool } from "./mysql_pool.js";

// NOTE: THE QUERIERS BELOW ARE FOR WRESTLER PAGES BASED ON A VARIETY OF CRITERIA BUT MOSTLY A DIRECT PULL OF WRESTLERS BASED ON CATEGORY (High School Boys, High School Girls), seasson, gender, state qualifier, team id, wrestler id
/** Fast COUNT(*) with a simple filter to avoid NULL/blank links */
export async function count_rows_in_db_wrestler_links(
  wrestling_season,
  track_wrestling_category,
  gender,
  sql_where_filter_state_qualifier,
  sql_where_filter_onthemat_ranking_list,
  sql_team_id_list,
  sql_wrestler_id_list,
) {
  const pool = await get_pool();
  const [rows] = await pool.query(
    `
      SELECT 
        COUNT(*) AS cnt 
      FROM wrestler_list_scrape_data 
      WHERE 1 = 1
        AND name_link IS NOT NULL AND name_link <> ''
        AND track_wrestling_category = "${track_wrestling_category}"
        AND wrestling_season = "${wrestling_season}"
        AND gender = "${gender}"
        ${sql_where_filter_state_qualifier}
        ${sql_where_filter_onthemat_ranking_list}
        ${sql_team_id_list}
        ${sql_wrestler_id_list}
    `
  );
  return Number(rows?.[0]?.cnt || 0);
}

/**
 * Async generator: yields { i, url } without loading everything into memory.
 * Uses LIMIT/OFFSET in small batches for stable, low-memory iteration.
 */

export async function* iter_name_links_from_db({
  start_at = 0,
  limit = Infinity,           // same semantic as CSV version
  batch_size = 500,           // tune as desired
  wrestling_season,
  track_wrestling_category,
  gender,
  sql_where_filter_state_qualifier,
  sql_where_filter_onthemat_ranking_list,
  sql_team_id_list,
  sql_wrestler_id_list,
} = {}) {
  const pool = await get_pool();

  let yielded = 0;
  let offset = start_at;

  const link_query = `
      SELECT 
        id, 
        name_link
      FROM wrestler_list_scrape_data
      WHERE 1 = 1
        AND name_link IS NOT NULL AND name_link <> ''
        AND track_wrestling_category = "${track_wrestling_category}"
        AND wrestling_season = "${wrestling_season}"
        AND gender = "${gender}"
        ${sql_where_filter_state_qualifier}
        ${sql_where_filter_onthemat_ranking_list}
        ${sql_team_id_list}
        ${sql_wrestler_id_list}
      ORDER BY id
      LIMIT ? OFFSET ?
  `;

  console.log('log inside iter_name_links_from_db', link_query);

  // console.log("sql where filter", sql_where_filter_state_qualifier);

  while (yielded < limit) {
    const to_fetch = Math.min(batch_size, limit - yielded);
    const [rows] = await pool.query(link_query, [to_fetch, offset]);

    if (!rows.length) break;

    for (const row of rows) {
      const url = row.name_link;
      const i = yielded + 1; // 1-based like your CSV logs
      yield { i, url };
      yielded += 1;
      if (yielded >= limit) break;
    }

    offset += rows.length;
  }
}

// NOTE: THE QUERIES BELOW ARE FOR WRESTLER PAGE LINKS BASED ON THE EVENTS HAPPENING TODAY & YESTERDAY
// SOURCE: utilities\raw_sql\discovery_get_team_schedule_and_wrestler_list.sql
/** Fast COUNT(*) with a simple filter to avoid NULL/blank links */
export async function count_name_links_based_on_event_schedule(
  wrestling_season,
  track_wrestling_category,
  // gender, 
  // sql_where_filter_state_qualifier,
  // sql_team_id_list,
  // sql_wrestler_id_list,
) {
  const pool = await get_pool();
  const [rows] = await pool.query(
    `
      -- retrieves events from yesterday & today
      WITH recent_events AS (
        SELECT 
            ts.start_date,
            ts.team_name_raw,
            ts.team_id,
            ts.event_name,
            ts.wrestling_season,
            ts.track_wrestling_category
        FROM team_schedule_scrape_data ts
        WHERE ts.wrestling_season = "${wrestling_season}"
          AND ts.track_wrestling_category = "${track_wrestling_category}"
          AND team_name_raw LIKE "%, CO%"
          AND ts.start_date IN (
                -- CURDATE()                                -- today
                DATE_SUB(CURDATE(), INTERVAL 1 DAY)       -- yesterday
                -- "2025-12-02"
                -- "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05", "2025-12-06"
              )
      )
      SELECT 
  
        COUNT(DISTINCT w.id) as cnt

      FROM recent_events re

      LEFT JOIN wrestler_list_scrape_data w
        ON w.wrestling_season             = re.wrestling_season
          AND w.track_wrestling_category  = re.track_wrestling_category
          AND (
                -- 1) primary: match on team_id when present
                (re.team_id IS NOT NULL AND w.team_id = re.team_id)
                -- 2) fallback: match on team name when event.team_id is NULL
                OR (re.team_id IS NULL AND w.team = re.team_name_raw)
              )
      WHERE 1 = 1
        AND w.name_link IS NOT NULL AND w.name_link <> ''
      -- GROUP BY 1, 2
      -- ORDER BY 1, 2
      ;

    `
  );
  return Number(rows?.[0]?.cnt || 0);
}

export async function* iter_name_links_based_on_event_schedule({
  start_at = 0,
  limit = Infinity,           // same semantic as CSV version
  batch_size = 500,           // tune as desired
  wrestling_season,
  track_wrestling_category,
  // gender,
  // sql_where_filter_state_qualifier,
  // sql_team_id_list,
  // sql_wrestler_id_list,
} = {}) {
  const pool = await get_pool();

  let yielded = 0;
  let offset = start_at;

  const link_query = `
      -- retrieves events from yesterday & today
      WITH recent_events AS (
        SELECT 
            ts.start_date,
            ts.team_name_raw,
            ts.team_id,
            ts.event_name,
            ts.wrestling_season,
            ts.track_wrestling_category
        FROM team_schedule_scrape_data ts
        WHERE ts.wrestling_season = "${wrestling_season}"
          AND ts.track_wrestling_category = "${track_wrestling_category}"
          AND team_name_raw LIKE "%, CO%"
          AND ts.start_date IN (
                -- CURDATE()                               -- today
                DATE_SUB(CURDATE(), INTERVAL 1 DAY)       -- yesterday
                -- "2025-12-02"
                -- "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05", "2025-12-06"
              )
      )
      SELECT DISTINCT

        w.id,
        w.name_link

      FROM recent_events re

      LEFT JOIN wrestler_list_scrape_data w ON w.wrestling_season = re.wrestling_season
          AND w.track_wrestling_category  = re.track_wrestling_category
          AND (
                -- 1) primary: match on team_id when present
                (re.team_id IS NOT NULL AND w.team_id = re.team_id)
                -- 2) fallback: match on team name when event.team_id is NULL
                OR (re.team_id IS NULL AND w.team = re.team_name_raw)
              )
      WHERE 1 = 1
        AND w.name_link IS NOT NULL AND w.name_link <> ''
      GROUP BY 1, 2
      ORDER BY 1, 2
      LIMIT ? OFFSET ?
      ;
  `;

  console.log('log inside iter_name_links_from_db', link_query);

  while (yielded < limit) {
    const to_fetch = Math.min(batch_size, limit - yielded);
    const [rows] = await pool.query(link_query, [to_fetch, offset]);

    if (!rows.length) break;

    for (const row of rows) {
      const url = row.name_link;
      const i = yielded + 1; // 1-based like your CSV logs
      yield { i, url };
      yielded += 1;
      if (yielded >= limit) break;
    }

    offset += rows.length;
  }
}



export async function count_rows_in_db_scrape_task(
  task_set_id
) {
  const pool = await get_pool();
  const [rows] = await pool.query(
    `
      SELECT 
        COUNT(*) AS cnt 
      FROM wrestler_match_history_scrape_tasks
      WHERE 1 = 1
        AND task_set_id = "${task_set_id}"
      ;
    `
  );
  return Number(rows?.[0]?.cnt || 0);
}

export async function* iter_name_links_based_on_scrape_task({
  start_at = 0,
  limit = Infinity,
  batch_size = 500,

  task_set_id,

  // Filters (optional):
  //
  // status:
  //   - Used to restrict which task rows we iterate.
  //   - In the parallel-worker model, the common use is:
  //       status = "LOCKED"
  //     so the iterator reads only tasks that have been claimed by a worker.
  //
  // locked_by:
  //   - Used WITH status="LOCKED" to ensure a worker only processes the rows
  //     it has claimed, e.g. locked_by="w1".
  //   - This prevents workers from re-reading each other’s tasks.
  //
  // task_id_list:
  //   - Optional explicit allow-list of task ids.
  //   - Useful for debugging / replaying known tasks.
  //   - If provided, the iterator returns only rows whose id is in this list
  //     (and still within the same task_set_id).
  //
  // Expected usage patterns:
  //   A) Parallel worker “owned tasks” iteration (most common):
  //        iter_name_links_based_on_scrape_task({
  //          task_set_id,
  //          status: "LOCKED",
  //          locked_by: worker_id,
  //          limit: 1,              // often 1 per invocation
  //        })
  //
  //   B) Non-worker / sequential “entire task set” iteration:
  //        iter_name_links_based_on_scrape_task({ task_set_id })
  //
  status = null,          // e.g. "LOCKED"
  locked_by = null,       // e.g. "worker_1"
  task_id_list = null,    // optional: array of ids
} = {}) {
  if (!task_set_id) {
    throw new Error("iter_name_links_based_on_scrape_task requires task_set_id");
  }

  const pool = await get_pool();

  let yielded = 0;
  let offset = start_at;

  // We always scope by task_set_id so we never bleed into other runs.
  // Everything else is optional and further narrows the set.
  const where_parts = [`task_set_id = "${task_set_id}"`];

  // If status is set, we only return tasks in that status (PENDING / LOCKED / DONE / FAILED).
  // In worker mode, you generally want status="LOCKED".
  if (status) where_parts.push(`status = "${status}"`);

  // If locked_by is set, we only return tasks currently locked by that worker.
  // In worker mode, use locked_by=worker_id to get “my tasks only”.
  if (locked_by) where_parts.push(`locked_by = "${locked_by}"`);

  // Optional: restrict to an explicit set of task ids.
  // Commonly used for debugging / reruns.
  if (Array.isArray(task_id_list) && task_id_list.length > 0) {
    const ids = task_id_list
      .map((v) => Number(v))
      .filter((n) => Number.isFinite(n));

    if (ids.length > 0) where_parts.push(`id IN (${ids.join(",")})`);
  }

  const where_sql = where_parts.length
    ? `WHERE ${where_parts.join(" AND ")}`
    : "";

  // IMPORTANT:
  // - ORDER BY id makes pagination deterministic (LIMIT/OFFSET behaves consistently)
  // - LIMIT/OFFSET allows batch iteration without loading everything into memory
  const link_query = `
      SELECT 
        id, wrestler_id, name_link, wrestling_season, track_wrestling_category, gender
      FROM wrestler_match_history_scrape_tasks
      ${where_sql}
      ORDER BY id
      LIMIT ? OFFSET ?
  `;

  while (yielded < limit) {
    const to_fetch = Math.min(batch_size, limit - yielded);
    const [rows] = await pool.query(link_query, [to_fetch, offset]);

    if (!rows.length) break;

    for (const row of rows) {
      const url = row.name_link;

      // i is 1-based within THIS iterator invocation (not global DB id).
      // If you need global progress like "7 of 10", pass that separately from the worker
      // (since each worker may run limit=1 per call).
      const i = yielded + 1;

      yield { i, url };

      yielded += 1;
      if (yielded >= limit) break;
    }

    offset += rows.length;
  }
}

/**
 * Progress snapshot for a task_set_id
 * - done/locked/failed/pending counts
 * - min/max updated_at_mtn
 * - duration_hh_mm_ss between min/max
 */
export async function get_task_set_progress(task_set_id) {
  if (!task_set_id) throw new Error("get_task_set_progress requires task_set_id");

  const pool = await get_pool();

  const [rows] = await pool.query(
    `
    SELECT
      task_set_id,
      SUM(status='DONE')    AS done_count,
      SUM(status='LOCKED')  AS locked_count,
      SUM(status='FAILED')  AS failed_count,
      SUM(status='PENDING') AS pending_count,
      COUNT(*) AS total_count,
      MIN(updated_at_mtn) AS min_updated_at_mtn,
      MAX(updated_at_mtn) AS max_updated_at_mtn,
      SEC_TO_TIME(
        TIMESTAMPDIFF(
          SECOND,
          MIN(updated_at_mtn),
          MAX(updated_at_mtn)
        )
      ) AS duration_hh_mm_ss
    FROM wrestler_match_history_scrape_tasks
    WHERE task_set_id = ?
    GROUP BY task_set_id
    `,
    [task_set_id]
  );

  const r = rows?.[0] || null;

  if (!r) {
    return {
      task_set_id,
      done_count: 0,
      locked_count: 0,
      failed_count: 0,
      pending_count: 0,
      total_count: 0,
      min_updated_at_mtn: null,
      max_updated_at_mtn: null,
      duration_hh_mm_ss: "00:00:00",
    };
  }

  return {
    task_set_id: r.task_set_id,
    done_count: Number(r.done_count || 0),
    locked_count: Number(r.locked_count || 0),
    failed_count: Number(r.failed_count || 0),
    pending_count: Number(r.pending_count || 0),
    total_count: Number(r.total_count || 0),
    min_updated_at_mtn: r.min_updated_at_mtn,
    max_updated_at_mtn: r.max_updated_at_mtn,
    duration_hh_mm_ss: String(r.duration_hh_mm_ss || "00:00:00"),
  };
}



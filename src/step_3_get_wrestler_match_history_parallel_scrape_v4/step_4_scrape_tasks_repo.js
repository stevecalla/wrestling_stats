/// src\step_3_get_wrestler_match_history_parallel_scrape_v4\step_4_scrape_tasks_repo.js

import os from "os";
import { get_pool } from "../../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

function get_now_utc() {
  return new Date();
}

function get_now_mtn() {
  const now_utc = new Date();
  const offset_hours = get_mountain_time_offset_hours(now_utc);
  return new Date(now_utc.getTime() + offset_hours * 60 * 60 * 1000);
}

function fmt_mysql_dt(d) {
  // "YYYY-MM-DD HH:MM:SS"
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

export function build_worker_id(port) {
  return `${os.hostname()}|pid=${process.pid}|port=${port}`;
}

/**
 * Claim up to N PENDING tasks for a given task_set_id.
 * Uses SELECT ... FOR UPDATE SKIP LOCKED (MySQL 8+).
 */
export async function claim_next_tasks({
  task_set_id,
  claim_batch_size = 25,
  worker_id,
}) {
  const pool = await get_pool();
  const conn = await pool.getConnection();

  try {
    await conn.beginTransaction();

    // 1) select tasks to claim (skip locked)
    const [rows] = await conn.query(
      `
      SELECT id, wrestler_id, name_link
      FROM wrestler_match_history_scrape_tasks
      WHERE task_set_id = ?
        AND status = 'PENDING'
      ORDER BY id
      LIMIT ?
      FOR UPDATE SKIP LOCKED
      `,
      [task_set_id, claim_batch_size]
    );

    if (!rows.length) {
      await conn.commit();
      return [];
    }

    const ids = rows.map((r) => r.id);

    const now_utc = get_now_utc();
    const now_mtn = get_now_mtn();

    // 2) mark them LOCKED
    await conn.query(
      `
      UPDATE wrestler_match_history_scrape_tasks
      SET
        status = 'LOCKED',
        locked_by = ?,
        locked_at_utc = ?,
        attempt_count = attempt_count + 1,
        updated_at_utc = ?,
        updated_at_mtn = ?
      WHERE id IN (${ids.map(() => "?").join(",")})
      `,
      [
        worker_id,
        fmt_mysql_dt(now_utc),
        fmt_mysql_dt(now_utc),
        fmt_mysql_dt(now_mtn),
        ...ids,
      ]
    );

    await conn.commit();
    return rows;
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

export async function mark_task_done({ task_id }) {
  const pool = await get_pool();
  const now_utc = get_now_utc();
  const now_mtn = get_now_mtn();

  await pool.query(
    `
    UPDATE wrestler_match_history_scrape_tasks
    SET
      status = 'DONE',
      last_error = NULL,
      updated_at_utc = ?,
      updated_at_mtn = ?
    WHERE id = ?
    `,
    [fmt_mysql_dt(now_utc), fmt_mysql_dt(now_mtn), task_id]
  );
}

export async function mark_task_failed({ task_id, error }) {
  const pool = await get_pool();
  const now_utc = get_now_utc();
  const now_mtn = get_now_mtn();

  await pool.query(
    `
    UPDATE wrestler_match_history_scrape_tasks
    SET
      status = 'FAILED',
      last_error = ?,
      updated_at_utc = ?,
      updated_at_mtn = ?
    WHERE id = ?
    `,
    [String(error), fmt_mysql_dt(now_utc), fmt_mysql_dt(now_mtn), task_id]
  );
}

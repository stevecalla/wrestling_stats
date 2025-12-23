// src/step_3_get_wrestler_match_history_parallel_scrape/step_4_parallel_scrape_worker.js
//
// Step 3: Worker that claims tasks (SKIP LOCKED) and processes them safely in parallel.
//
// ✅ supports multiple workers
// ✅ lock TTL + reclaim stale locks
// ✅ attempt_count increment on claim
// ✅ DONE / FAILED with last_error
// ✅ writes BOTH MTN + UTC updated timestamps

import { get_pool } from "../../utilities/mysql/mysql_pool.js";
import { color_text } from "../../utilities/console_logs/console_colors.js";
import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

import { step_0_launch_chrome_developer_parallel_scrape } from "./step_0_launch_chrome_developer_parallel_scrape.js";
import { step_3_get_wrestler_match_history_scrape } from "./step_3_get_wrestler_match_history_scrape.js";

// ✅ NEW: get total count for nicer progress logs
import { count_rows_in_db_scrape_task } from "../../utilities/mysql/iter_name_links_from_db.js";

/* -------------------------------------------------
helpers
--------------------------------------------------*/

function build_batch_timestamps() {
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  return {
    updated_at_utc: now_utc,
    updated_at_mtn: now_mtn,
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function now_iso() {
  return new Date().toISOString();
}

function stringify_err(err) {
  if (!err) return "unknown_error";
  if (typeof err === "string") return err;
  return err?.stack || err?.message || JSON.stringify(err);
}

function is_abort_error(err) {
  const msg = String(err?.message || "");
  return (
    err?.name === "AbortError" ||
    msg.includes("aborted") ||
    msg.includes("AbortError") ||
    msg.includes("The operation was aborted")
  );
}

function is_target_closed_error(err) {
  const msg = String(err?.message || "");
  return (
    err?.code === "E_TARGET_CLOSED" ||
    msg.includes("Target closed") ||
    msg.includes("Target page, context or browser has been closed") ||
    msg.includes("CDP connection closed") ||
    msg.includes("WebSocket is not open") ||
    msg.includes("Execution context was destroyed") ||
    msg.includes("Frame was detached")
  );
}

/* -------------------------------------------------
DB task operations
--------------------------------------------------*/

async function reclaim_stale_locks({
  pool,
  task_set_id,
  lock_ttl_minutes = 30,
  worker_id = "worker",
} = {}) {
  const { updated_at_utc, updated_at_mtn } = build_batch_timestamps();

  const [res] = await pool.query(
    `
      UPDATE wrestler_match_history_scrape_tasks
      SET status='PENDING',
          locked_by=NULL,
          locked_at_utc=NULL,
          last_error=CONCAT('reclaimed by ', ?, ' at ', ?),
          updated_at_mtn=?,
          updated_at_utc=?
      WHERE task_set_id=?
        AND status='LOCKED'
        AND locked_at_utc < (UTC_TIMESTAMP() - INTERVAL ? MINUTE)
    `,
    [worker_id, now_iso(), updated_at_mtn, updated_at_utc, task_set_id, lock_ttl_minutes]
  );

  return res?.affectedRows ?? 0;
}

async function claim_tasks({
  pool,
  task_set_id,
  worker_id,
  batch_size = 5,
  max_attempts = 3,
} = {}) {
  const { updated_at_utc, updated_at_mtn } = build_batch_timestamps();

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    const [rows] = await conn.query(
      `
        SELECT id, wrestler_id, name_link, wrestling_season, track_wrestling_category, gender
        FROM wrestler_match_history_scrape_tasks
        WHERE task_set_id=?
          AND status='PENDING'
          AND attempt_count < ?
        ORDER BY id
        LIMIT ?
        FOR UPDATE SKIP LOCKED
      `,
      [task_set_id, max_attempts, batch_size]
    );

    if (!rows.length) {
      await conn.commit();
      return [];
    }

    const ids = rows.map((r) => r.id);

    await conn.query(
      `
        UPDATE wrestler_match_history_scrape_tasks
        SET status='LOCKED',
            locked_by=?,
            locked_at_utc=UTC_TIMESTAMP(),
            attempt_count=attempt_count+1,
            updated_at_mtn=?,
            updated_at_utc=?
        WHERE id IN (${ids.map(() => "?").join(",")})
      `,
      [worker_id, updated_at_mtn, updated_at_utc, ...ids]
    );

    await conn.commit();
    return rows;
  } catch (err) {
    try {
      await conn.rollback();
    } catch {}
    throw err;
  } finally {
    conn.release();
  }
}

async function mark_done({ pool, id } = {}) {
  const { updated_at_utc, updated_at_mtn } = build_batch_timestamps();

  const [res] = await pool.query(
    `
      UPDATE wrestler_match_history_scrape_tasks
      SET status='DONE',
          last_error=NULL,
          updated_at_utc=?,
          updated_at_mtn=?
      WHERE id=?
    `,
    [updated_at_utc, updated_at_mtn, id]
  );
  return res?.affectedRows ?? 0;
}

async function mark_failed({ pool, id, err_msg } = {}) {
  const { updated_at_utc, updated_at_mtn } = build_batch_timestamps();

  const [res] = await pool.query(
    `
      UPDATE wrestler_match_history_scrape_tasks
      SET status='FAILED',
          last_error=?,
          updated_at_utc=?,
          updated_at_mtn=?
      WHERE id=?
    `,
    [String(err_msg || "failed"), updated_at_utc, updated_at_mtn, id]
  );
  return res?.affectedRows ?? 0;
}

async function get_task_counts({ pool, task_set_id } = {}) {
  const [rows] = await pool.query(
    `
      SELECT status, COUNT(*) AS cnt
      FROM wrestler_match_history_scrape_tasks
      WHERE task_set_id=?
      GROUP BY status
      ORDER BY status
    `,
    [task_set_id]
  );
  const out = {};
  for (const r of rows) out[r.status] = Number(r.cnt || 0);
  return out;
}

async function release_my_locks({ pool, task_set_id, worker_id } = {}) {
  const { updated_at_utc, updated_at_mtn } = build_batch_timestamps();

  const [res] = await pool.query(
    `
      UPDATE wrestler_match_history_scrape_tasks
      SET status='PENDING',
          locked_by=NULL,
          locked_at_utc=NULL,
          last_error=CONCAT('released by ', ?, ' at ', ?),
          updated_at_utc=?,
          updated_at_mtn=?
      WHERE task_set_id=?
        AND status='LOCKED'
        AND locked_by=?
    `,
    [worker_id, now_iso(), updated_at_utc, updated_at_mtn, task_set_id, worker_id]
  );

  return res?.affectedRows ?? 0;
}

/* -------------------------------------------------
main worker loop
--------------------------------------------------*/

async function main({
  task_set_id,
  worker_id = "worker_1",

  batch_size = 5,
  max_attempts = 3,

  lock_ttl_minutes = 30,
  idle_sleep_ms = 1500,
  log_every_batches = 5,

  // scraper behavior
  url_home_page = "https://www.trackwrestling.com",
  url_login_page = "https://www.trackwrestling.com/seasons/index.jsp",

  slow_mo_ms = 0,
  navigation_timeout_ms = 30000,

  // ✅ per-worker port (required in your newer mode)
  port = null,

  // passthrough
  file_path = null,

  // graceful stop
  signal = null,
  release_locks_on_shutdown = true,
} = {}) {
  if (!task_set_id) throw new Error("run_worker requires task_set_id");
  if (!port) throw new Error("run_worker requires port");

  const pool = await get_pool();

  // ✅ NEW: compute total once for better “X of Y” logs
  const total_in_task_set = await count_rows_in_db_scrape_task(task_set_id);

  console.log(
    color_text(
      `\n🏃 Step_3 worker starting\n` +
        `   worker_id=${worker_id}\n` +
        `   task_set_id=${task_set_id}\n` +
        `   port=${port}\n` +
        `   batch_size=${batch_size}\n` +
        `   max_attempts=${max_attempts}\n` +
        `   lock_ttl_minutes=${lock_ttl_minutes}\n` +
        `   total_in_task_set=${total_in_task_set}\n`,
      "cyan"
    )
  );

  let batch_i = 0;
  let processed = 0;
  let done = 0;
  let failed = 0;

  let stop_requested = false;

  if (signal) {
    if (signal.aborted) stop_requested = true;
    else {
      signal.addEventListener(
        "abort",
        () => {
          stop_requested = true;
          console.log(color_text(`🛑 [${worker_id}] stop signal received`, "yellow"));
        },
        { once: true }
      );
    }
  }

  // per-worker browser session (reuse across tasks)
  let browser = null;
  let page = null;
  let context = null;

  async function ensure_session() {
    if (browser && page && context && browser.isConnected?.() && !page.isClosed?.()) {
      return { browser, page, context };
    }

    console.log(color_text(`🧩 [${worker_id}] launching/attaching chrome on port=${port}`, "cyan"));

    ({ browser, page, context } = await step_0_launch_chrome_developer_parallel_scrape(
      url_home_page,
      port,
      { force_relaunch: false }
    ));

    try {
      page.setDefaultTimeout?.(navigation_timeout_ms);
      page.setDefaultNavigationTimeout?.(navigation_timeout_ms);
    } catch {}

    return { browser, page, context };
  }

  async function cleanup(reason = "cleanup") {
    stop_requested = true;

    if (browser) {
      console.log(color_text(`🧹 [${worker_id}] closing browser session (${reason})`, "yellow"));
      try {
        await browser.close(); // closes CDP connection
      } catch {}
      browser = null;
      page = null;
      context = null;
    }

    if (release_locks_on_shutdown) {
      try {
        const released = await release_my_locks({ pool, task_set_id, worker_id });
        if (released > 0) {
          console.log(color_text(`🔓 [${worker_id}] released locks: ${released}`, "yellow"));
        }
      } catch (e) {
        console.warn(
          color_text(`⚠️ [${worker_id}] release locks failed: ${e?.message || e}`, "yellow")
        );
      }
    }
  }

  try {
    while (true) {
      if (stop_requested) {
        await cleanup("stop_requested");
        console.log(
          color_text(
            `\n⏹️  Worker ${worker_id} stopped early\n` +
              `   processed=${processed}, done=${done}, failed=${failed}\n`,
            "yellow"
          )
        );
        return { processed, done, failed, stopped: true };
      }

      if (batch_i % 10 === 0) {
        const reclaimed = await reclaim_stale_locks({
          pool,
          task_set_id,
          lock_ttl_minutes,
          worker_id,
        });
        if (reclaimed > 0) {
          console.log(color_text(`♻️ Reclaimed stale locks: ${reclaimed}`, "yellow"));
        }
      }

      const tasks = await claim_tasks({
        pool,
        task_set_id,
        worker_id,
        batch_size,
        max_attempts,
      });

      if (!tasks.length) {
        const counts = await get_task_counts({ pool, task_set_id });
        const pending = counts.PENDING || 0;
        const locked = counts.LOCKED || 0;

        if (pending === 0 && locked === 0) {
          console.log(
            color_text(
              `\n✅ Worker ${worker_id} done — no PENDING/LOCKED remaining\n` +
                `   processed=${processed}, done=${done}, failed=${failed}\n`,
              "green"
            )
          );
          return { processed, done, failed, stopped: false };
        }

        const chunk = 250;
        let slept = 0;
        while (slept < idle_sleep_ms) {
          if (stop_requested) break;
          await sleep(Math.min(chunk, idle_sleep_ms - slept));
          slept += chunk;
        }
        continue;
      }

      batch_i += 1;

      for (const t of tasks) {
        processed += 1;
        if (stop_requested) break;

        try {
          const s = await ensure_session();

          await step_3_get_wrestler_match_history_scrape(
            url_home_page,
            url_login_page,

            1, // matches_page_limit: single task
            0, // loop_start

            t.wrestling_season,
            t.track_wrestling_category,
            t.gender,

            s.page,
            s.browser,
            s.context,

            file_path,

            task_set_id,
            port,
            worker_id
          );

          await mark_done({ pool, id: t.id });
          done += 1;
        } catch (err) {
          if (stop_requested && (is_abort_error(err) || is_target_closed_error(err))) {
            console.log(color_text(`🛑 [${worker_id}] abort during task; exiting loop`, "yellow"));
            break;
          }

          const msg = stringify_err(err);

          if (is_target_closed_error(err)) {
            console.warn(color_text(`♻️ [${worker_id}] scraper session died; restarting session`, "yellow"));
            try {
              await browser?.close?.();
            } catch {}
            browser = null;
            page = null;
            context = null;
          }

          await mark_failed({ pool, id: t.id, err_msg: msg });
          failed += 1;

          console.log(
            color_text(
              `❌ FAILED task id=${t.id} wrestler_id=${t.wrestler_id} — ${msg}`,
              "red"
            )
          );
        }
      }

      if (batch_i % log_every_batches === 0) {
        const counts = await get_task_counts({ pool, task_set_id });
        console.log(
          color_text(
            `📦 worker=${worker_id} batches=${batch_i} processed=${processed} done=${done} failed=${failed} ` +
              `| counts=${JSON.stringify(counts)}`,
            "cyan"
          )
        );
      }
    }
  } finally {
    await cleanup("finally");
  }
}

export { main as step_4_run_worker };

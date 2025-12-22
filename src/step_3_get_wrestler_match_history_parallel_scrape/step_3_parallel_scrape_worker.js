// src/step_3_get_wrestler_match_history_parallel_scrape/step_3_parallel_scrape_worker.js
//
// Step 3: Worker that claims tasks (SKIP LOCKED) and processes them safely in parallel.
//
// ✅ supports multiple workers
// ✅ lock TTL + reclaim stale locks
// ✅ attempt_count increment on claim
// ✅ DONE / FAILED with last_error
// ✅ writes BOTH MTN + UTC updated timestamps
//
// 🔧 UPDATE: integrates real scraping logic via per-worker Playwright session (Option A)
// 🔧 UPDATE: graceful shutdown on Ctrl+C via AbortSignal:
//    - stop claiming new tasks
//    - close Playwright session
//    - optionally release locks for THIS worker (keeps TTL safety)

import { get_pool } from "../../utilities/mysql/mysql_pool.js";
import { color_text } from "../../utilities/console_logs/console_colors.js";
import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

import {
  create_scraper_session,
  is_target_closed_error,
} from "./step_3_scrape_match_history_task.js";

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

// ✅ optional: release locks held by THIS worker on shutdown
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

  // scraper behavior (passed to step_3_run_worker)
  url_home_page = "https://www.trackwrestling.com",
  url_login_page = "https://www.trackwrestling.com/seasons/index.jsp",

  headless = false,
  slow_mo_ms = 0,
  navigation_timeout_ms = 30000,

  // ✅ NEW: graceful stop signal
  signal = null,

  // ✅ NEW: when stopping, release locks held by this worker (safe in single runner)
  release_locks_on_shutdown = true,
} = {}) {
  if (!task_set_id) throw new Error("run_worker requires task_set_id");

  const pool = await get_pool();

  console.log(
    color_text(
      `\n🏃 Step_3 worker starting\n` +
        `   worker_id=${worker_id}\n` +
        `   task_set_id=${task_set_id}\n` +
        `   batch_size=${batch_size}\n` +
        `   max_attempts=${max_attempts}\n` +
        `   lock_ttl_minutes=${lock_ttl_minutes}\n` +
        `   headless=${headless}\n`,
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

  // per-worker scraper session (lazy init on first task)
  let session = null;
  let session_scope_key = null;

  async function ensure_session_for_task(t) {
    const scope_key = `${t.wrestling_season}|${t.track_wrestling_category}|${t.gender}`;

    if (session && session_scope_key === scope_key) return session;

    if (session) {
      console.log(color_text(`♻️ [${worker_id}] scope changed → restarting browser`, "yellow"));
      await session.close().catch(() => {});
      session = null;
      session_scope_key = null;
    }

    session = await create_scraper_session({
      worker_id,
      url_home_page,
      url_login_page,
      wrestling_season: t.wrestling_season,
      track_wrestling_category: t.track_wrestling_category,
      gender: t.gender,
      headless,
      slow_mo_ms,
      navigation_timeout_ms,
    });

    session_scope_key = scope_key;
    return session;
  }

  async function cleanup(reason = "cleanup") {
    // Stop future work
    stop_requested = true;

    // close browser/session
    if (session) {
      console.log(color_text(`🧹 [${worker_id}] closing browser session (${reason})`, "yellow"));
      await session.close().catch(() => {});
      session = null;
      session_scope_key = null;
    }

    // optional: release locks this worker holds so tasks can be re-claimed
    if (release_locks_on_shutdown) {
      try {
        const released = await release_my_locks({ pool, task_set_id, worker_id });
        if (released > 0) {
          console.log(color_text(`🔓 [${worker_id}] released locks: ${released}`, "yellow"));
        }
      } catch (e) {
        console.warn(color_text(`⚠️ [${worker_id}] release locks failed: ${e?.message || e}`, "yellow"));
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

        // sleep in small chunks so Ctrl+C abort is responsive
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
          const s = await ensure_session_for_task(t);

          // 1) scrape
          const rows = await s.scrape_one({ name_link: t.name_link });

          // 2) persist (delete snapshot + upsert)
          await s.persist_one({
            rows,
            wrestling_season: t.wrestling_season,
            track_wrestling_category: t.track_wrestling_category,
            gender: t.gender,
          });

          // 3) mark done
          await mark_done({ pool, id: t.id });
          done += 1;
        } catch (err) {
          // If we're stopping, don't convert mid-shutdown errors into FAILED if they are abort-ish
          if (stop_requested && (is_abort_error(err) || is_target_closed_error(err))) {
            console.log(color_text(`🛑 [${worker_id}] abort during task; exiting loop`, "yellow"));
            break;
          }

          const msg = stringify_err(err);

          if (is_target_closed_error(err)) {
            console.warn(color_text(`♻️ [${worker_id}] scraper died; restarting session`, "yellow"));
            if (session) {
              await session.close().catch(() => {});
              session = null;
              session_scope_key = null;
            }
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

export { main as step_3_run_worker };

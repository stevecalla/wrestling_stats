// src/step_3_get_wrestler_match_history_parallel_scrape/step_4_run_workers.js
//
// Step 4 runner: spins up N workers in a single Node process.
//
// ✅ import + await:
//    import { step_4_run_workers } from "./step_4_run_workers.js";
//    const totals = await step_4_run_workers({ task_set_id, workers: 4, batch_size: 3 });
//
// ✅ each worker uses SKIP LOCKED so they don't collide
//
// 🔧 UPDATE: align params with step_3_run_worker (scraper/runtime options pass-through)
// 🔧 UPDATE: graceful shutdown on Ctrl+C (SIGINT) / SIGTERM via AbortController

import { step_3_run_worker } from "./step_3_parallel_scrape_worker.js";
import { color_text } from "../../utilities/console_logs/console_colors.js";
import { chromium } from "playwright";

async function assert_playwright_browsers_installed() {
  try {
    const b = await chromium.launch({ headless: true });
    await b.close();
  } catch (e) {
    console.error("❌ Playwright browsers not installed.");
    console.error("Run: npx playwright install chromium");
    throw e;
  }
}

function to_int(v, def) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function to_bool(v, def = false) {
  if (v === undefined || v === null) return def;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "no", "n", "off"].includes(s)) return false;
  return def;
}

async function main({
  // required
  task_set_id,

  // optional parallelism
  workers = 3,
  batch_size = 5,

  // safety / retry behavior
  max_attempts = 3,
  lock_ttl_minutes = 30,

  // worker behavior
  idle_sleep_ms = 1500,
  log_every_batches = 5,

  // scraper behavior (passed to step_3_run_worker)
  url_home_page = "https://www.trackwrestling.com",
  url_login_page = "https://www.trackwrestling.com/seasons/index.jsp",

  headless = false,
  slow_mo_ms = 0,
  navigation_timeout_ms = 30000,

  // optional behavior
  quiet = false, // if true: skip logs (except errors)

  // ✅ NEW: allow external abort signal (orchestrator can pass one)
  signal = null,

  // ✅ NEW: hard timeout for shutdown
  shutdown_timeout_ms = 20000,
} = {}) {
  await assert_playwright_browsers_installed();

  if (!task_set_id) {
    throw new Error("step_4_run_workers missing required arg: task_set_id");
  }

  const workers_n = Math.max(1, to_int(workers, 3));
  const batch_size_n = Math.max(1, to_int(batch_size, 5));

  const max_attempts_n = Math.max(1, to_int(max_attempts, 3));
  const lock_ttl_minutes_n = Math.max(5, to_int(lock_ttl_minutes, 30));

  const idle_sleep_ms_n = Math.max(250, to_int(idle_sleep_ms, 1500));
  const log_every_batches_n = Math.max(1, to_int(log_every_batches, 5));

  const headless_b = to_bool(headless, false);
  const slow_mo_ms_n = Math.max(0, to_int(slow_mo_ms, 0));
  const navigation_timeout_ms_n = Math.max(5000, to_int(navigation_timeout_ms, 30000));

  // -------------------------------------------------
  // Graceful shutdown wiring (Ctrl+C / SIGTERM)
  // -------------------------------------------------
  const internal_ac = new AbortController();
  const internal_signal = internal_ac.signal;

  let stop_requested = false;
  let shutting_down = false;

  function request_stop(reason = "abort") {
    if (stop_requested) return;
    stop_requested = true;
    if (!quiet) {
      console.log(color_text(`\n🛑 Step_4 stop requested (${reason})`, "yellow"));
    }
    try {
      internal_ac.abort();
    } catch {}
  }

  // If an external signal is provided, mirror it into our internal controller
  if (signal) {
    if (signal.aborted) request_stop("external signal already aborted");
    else {
      signal.addEventListener(
        "abort",
        () => request_stop("external abort signal"),
        { once: true }
      );
    }
  }

  async function graceful_shutdown(reason) {
    if (shutting_down) return;
    shutting_down = true;

    request_stop(reason);

    // don't hang forever (Promise.all can hang if something is stuck)
    const t = setTimeout(() => {
      console.log(color_text("⛔ Step_4 shutdown timeout reached; forcing exit.", "red"));
      process.exit(1);
    }, shutdown_timeout_ms);

    try {
      // just let workers unwind; they'll see abort and close their sessions
      await Promise.race([
        workers_promise, // defined below
        new Promise((_, rej) =>
          setTimeout(() => rej(new Error("shutdown timeout")), shutdown_timeout_ms)
        ),
      ]);
    } catch (e) {
      if (!quiet) {
        console.log(color_text(`⚠️ Step_4 shutdown race ended: ${e?.message || e}`, "yellow"));
      }
    } finally {
      clearTimeout(t);
    }
  }

  // Attach SIG handlers at Step_4 scope (safe even if orchestrator also does this)
  const on_sigint = () => graceful_shutdown("SIGINT/Ctrl+C");
  const on_sigterm = () => graceful_shutdown("SIGTERM");

  process.on("SIGINT", on_sigint);
  process.on("SIGTERM", on_sigterm);

  if (!quiet) {
    console.log(
      color_text(
        `\n🚀 Step_4 run_workers\n` +
          `   task_set_id=${task_set_id}\n` +
          `   workers=${workers_n}\n` +
          `   batch_size=${batch_size_n}\n` +
          `   max_attempts=${max_attempts_n}\n` +
          `   lock_ttl_minutes=${lock_ttl_minutes_n}\n` +
          `   idle_sleep_ms=${idle_sleep_ms_n}\n` +
          `   log_every_batches=${log_every_batches_n}\n` +
          `   headless=${headless_b}\n` +
          `   slow_mo_ms=${slow_mo_ms_n}\n` +
          `   navigation_timeout_ms=${navigation_timeout_ms_n}\n`,
        "cyan"
      )
    );
  }

  // Create worker promises
  const promises = [];
  for (let i = 1; i <= workers_n; i++) {
    const worker_id = `w${i}`;
    promises.push(
      step_3_run_worker({
        task_set_id,
        worker_id,

        // worker controls
        batch_size: batch_size_n,
        max_attempts: max_attempts_n,
        lock_ttl_minutes: lock_ttl_minutes_n,
        idle_sleep_ms: idle_sleep_ms_n,
        log_every_batches: log_every_batches_n,

        // scraper controls
        url_home_page,
        url_login_page,
        headless: headless_b,
        slow_mo_ms: slow_mo_ms_n,
        navigation_timeout_ms: navigation_timeout_ms_n,

        // ✅ NEW: tell workers when to stop
        signal: internal_signal,
      })
    );
  }

  // Run workers
  const workers_promise = Promise.allSettled(promises);

  let settled;
  try {
    settled = await workers_promise;
  } finally {
    // detach signal handlers so repeated calls don't pile up listeners
    process.off("SIGINT", on_sigint);
    process.off("SIGTERM", on_sigterm);
  }

  // Convert settled results into your existing totals structure
  const results = settled.map((r) => {
    if (r.status === "fulfilled") return r.value;
    return { processed: 0, done: 0, failed: 0, error: String(r.reason?.message || r.reason) };
  });

  const totals = results.reduce(
    (acc, r) => {
      acc.processed += r?.processed || 0;
      acc.done += r?.done || 0;
      acc.failed += r?.failed || 0;
      return acc;
    },
    { processed: 0, done: 0, failed: 0 }
  );

  if (!quiet) {
    const suffix = stop_requested ? " (stopped early)" : "";
    console.log(
      color_text(
        `\n🏁 Step_4 complete${suffix}\n` +
          `   processed=${totals.processed}\n` +
          `   done=${totals.done}\n` +
          `   failed=${totals.failed}\n`,
        totals.failed ? "yellow" : "green"
      )
    );
  }

  return { ...totals, results };
}

export { main as step_4_run_workers };

// src/step_3_get_wrestler_match_history_parallel_scrape/step_4_run_workers.js
//
// Step 4 runner: spins up N workers in a single Node process.
// ✅ assigns a unique devtools port to each worker from port_list
// ✅ aggregates totals

import { step_4_run_worker } from "./step_4_parallel_scrape_worker.js";
import { color_text } from "../../utilities/console_logs/console_colors.js";

async function main({
  task_set_id,

  workers = 2,
  batch_size = 1,

  max_attempts = 3,
  lock_ttl_minutes = 30,

  idle_sleep_ms = 1500,
  log_every_batches = 5,

  url_home_page,
  url_login_page,

  slow_mo_ms = 0,
  navigation_timeout_ms = 30000,

  file_path = null,

  // ✅ required
  port_list = [],

  // optional
  quiet = false,
} = {}) {
  if (!task_set_id) throw new Error("step_4_run_workers requires task_set_id");
  if (!Array.isArray(port_list) || port_list.length === 0) {
    throw new Error("step_4_run_workers requires port_list (non-empty)");
  }

  const n = Math.min(workers, port_list.length);
  if (n <= 0) throw new Error("workers must be >= 1");

  if (!quiet) {
    console.log(
      color_text(
        `\n🚀 Step_4 run_workers\n` +
          `   task_set_id=${task_set_id}\n` +
          `   workers=${n}\n` +
          `   batch_size=${batch_size}\n` +
          `   max_attempts=${max_attempts}\n` +
          `   lock_ttl_minutes=${lock_ttl_minutes}\n` +
          `   ports=${JSON.stringify(port_list.slice(0, n))}\n`,
        "cyan"
      )
    );
  }

  // optional: allow Ctrl+C to stop workers nicely
  const controller = new AbortController();
  const signal = controller.signal;

  process.on("SIGINT", () => {
    if (!quiet) console.log(color_text("🛑 SIGINT received — stopping workers...", "yellow"));
    controller.abort();
  });

  const results = await Promise.all(
    Array.from({ length: n }).map((_, idx) => {
      const worker_id = `w${idx + 1}`;
      const port = port_list[idx];

      return step_4_run_worker({
        task_set_id,
        worker_id,
        port,

        batch_size,
        max_attempts,
        lock_ttl_minutes,
        idle_sleep_ms,
        log_every_batches,

        url_home_page,
        url_login_page,

        slow_mo_ms,
        navigation_timeout_ms,

        file_path,

        signal,
        release_locks_on_shutdown: true,
      });
    })
  );

  const processed = results.reduce((a, r) => a + (r.processed || 0), 0);
  const done = results.reduce((a, r) => a + (r.done || 0), 0);
  const failed = results.reduce((a, r) => a + (r.failed || 0), 0);

  return { processed, done, failed, results };
}

export { main as step_5_run_workers };

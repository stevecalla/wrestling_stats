// src/step_3_get_wrestler_match_history.js (ESM, snake_case)
//
// ✅ Update in this version:
// - This module can now run in TWO modes:
//   A) "external session" mode (backward compatible): you pass in page/browser/context
//   B) "self-managed session" mode: if page/browser/context are not provided, it will
//      wait for DevTools, launch/attach via step_0_launch_chrome_developer_parallel_scrape(url_home_page, port),
//      login, scrape, and then close the CDP connection.
//
// This makes it usable from a parallel worker that only knows { port, task_set_id }.
//
//
// ✅ FIX (minimal):
// - Remove duplicate extract logic
// - Fix rows-before-declare bug
// - After switching to fresh-frame helpers, stop using stale target_frame
// - Only browser.close() if created_session_here=true

import net from "net"; // for wait_until_port_is_open function

import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// Save files to csv & msyql
import { save_to_csv_file } from "../../utilities/create_and_load_csv_files/save_to_csv_file.js";
import {
  upsert_wrestler_match_history,
  delete_wrestler_match_history_for_wrestler, // delete helper for per-wrestler snapshot
} from "../../utilities/mysql/upsert_wrestler_match_history.js";

import {
  count_rows_in_db_scrape_task,
  iter_name_links_based_on_scrape_task,
  get_task_set_progress,
} from "../../utilities/mysql/iter_name_links_from_db.js";

// import { step_0_launch_chrome_developer } from "../step_0_launch_chrome_developer.js";
import { step_0_launch_chrome_developer_parallel_scrape } from "./step_0_launch_chrome_developer_parallel_scrape.js";

import { auto_login_select_season } from "../../utilities/scraper_tasks/auto_login_select_season.js";

import { color_text } from "../../utilities/console_logs/console_colors.js";
// import { step_19_close_chrome_dev } from "./step_19_close_chrome_developer.js";
import { step_19_close_chrome_dev } from "../step_19_close_chrome_developer.js";

/* ------------------------------------------
   global handler (minimal but safer)
-------------------------------------------*/
process.on("unhandledRejection", (err) => {
  const msg = String(err?.message || "");

  if (
    msg.includes("Page.handleJavaScriptDialog") &&
    (
      msg.includes("No dialog is showing") ||
      msg.includes("session closed") ||
      msg.includes("Internal server error") ||
      msg.includes("Protocol error")
    )
  ) {
    console.warn("⚠️ Suppressed Playwright dialog error:", msg);
    return;
  }

  // ✅ swallow common recoverable Playwright crashes so the worker can recover
  if (
    msg.includes("Frame has been detached") ||
    msg.includes("Frame was detached") ||
    msg.includes("Execution context was destroyed") ||
    msg.includes("Target page, context or browser has been closed") ||
    msg.includes("CDP connection closed") ||
    msg.includes("WebSocket is not open")
  ) {
    console.warn(
      "⚠️ Suppressed Playwright recoverable crash (unhandledRejection):",
      msg
    );
    return;
  }

  throw err;
});

process.on("uncaughtException", (err) => {
  const msg = String(err?.message || "");

  // Same bucket as your recoverable CDP/frame churn
  if (
    msg.includes("Frame has been detached") ||
    msg.includes("Frame was detached") ||
    msg.includes("Execution context was destroyed") ||
    msg.includes("Target page, context or browser has been closed") ||
    msg.includes("CDP connection closed") ||
    msg.includes("WebSocket is not open") ||
    msg.includes("is interrupted by another navigation")
  ) {
    console.warn("⚠️ Suppressed Playwright recoverable crash (uncaughtException):", msg);
    return;
  }

  // Anything else should still crash loudly
  console.error("❌ Uncaught exception (fatal):", err);
  process.exit(1);
});

/* ------------------------------------------
   small helpers
-------------------------------------------*/
function get_target_frame(page) {
  return (
    page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) ||
    page.mainFrame()
  );
}

async function with_fresh_target_frame(page, fn) {
  try {
    const tf = get_target_frame(page);
    return await fn(tf);
  } catch (e) {
    const msg = String(e?.message || "");
    if (msg.includes("Frame has been detached") || msg.includes("Frame was detached")) {
      // retry once with a fresh frame
      const tf2 = get_target_frame(page);
      return await fn(tf2);
    }
    throw e;
  }
}

async function close_extra_tabs(context, keep_page) {
  try {
    const pages = context?.pages?.() || [];
    for (const p of pages) {
      if (p !== keep_page && !p.isClosed?.()) {
        console.log("🧹 closing extra tab:", p.url?.() || "<no url yet>");
        await p.close();
      }
    }
  } catch (err) {
    console.warn("⚠️ close_extra_tabs error (ignored):", err?.message || err);
  }
}

function handles_dead({ browser, context, page }) {
  return !browser?.isConnected?.() || !context || !page || page.isClosed?.();
}

// 🔧 NEW: central helper to recognize CDP/target closed style errors
function is_cdp_disconnect_error(err) {
  const msg = String(err?.message || "");
  return (
    err?.code === "E_TARGET_CLOSED" ||
    msg.includes("Execution context was destroyed") ||
    msg.includes("Target page, context or browser has been closed") ||
    msg.includes("Target closed") ||
    msg.includes("Session closed") ||
    msg.includes("has been closed") ||
    msg.includes("CDP connection closed") ||
    msg.includes("WebSocket is not open") ||
    msg.includes("Frame was detached") ||
    msg.includes("Frame has been detached")
  );
}

// 🔧 safe wrapper around auto_login_select_season
async function safe_auto_login(page, wrestling_season, track_wrestling_category) {
  try {
    await page.evaluate(auto_login_select_season, {
      wrestling_season,
      track_wrestling_category,
    });
  } catch (e) {
    const msg = String(e?.message || "");
    if (
      msg.includes("Execution context was destroyed") ||
      msg.includes("Frame was detached") ||
      msg.includes("Target page, context or browser has been closed")
    ) {
      console.warn(
        "⚠️ auto_login_select_season evaluate interrupted by navigation/context close; continuing..."
      );
      return;
    }
    throw e;
  }
}

async function relogin(
  page,
  load_timeout_ms,
  wrestling_season,
  track_wrestling_category,
  url_login_page
) {
  const login_url = url_login_page;
  await safe_goto(page, login_url, { timeout: load_timeout_ms });
  await page.waitForTimeout(1000);
  await safe_auto_login(page, wrestling_season, track_wrestling_category);
  await page.waitForTimeout(800);
}

async function safe_goto(page, url, opts = {}) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", ...opts });
  } catch (err) {
    const msg = String(err?.message || "");

    if (msg.includes("is interrupted by another navigation")) {
      console.warn("⚠️ Ignored navigation interruption, site redirected itself.");
      await page.waitForLoadState("domcontentloaded").catch(() => { });
    } else if (msg.includes("Target page, context or browser has been closed")) {
      err.code = "E_TARGET_CLOSED";
      throw err;
    } else if (err?.name === "TimeoutError" || msg.includes("Timeout")) {
      err.code = "E_GOTO_TIMEOUT";
      throw err;
    } else {
      throw err;
    }
  }
  return page.url();
}

async function wait_ms(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Wait until DevTools port is open
 */
async function wait_until_port_is_open(
  port,
  max_wait_ms = 5000,
  host = "127.0.0.1"
) {
  const start_time = Date.now();

  while (Date.now() - start_time < max_wait_ms) {
    const is_open = await new Promise((resolve) => {
      const socket = new net.Socket();

      socket
        .setTimeout(500)
        .once("connect", () => {
          socket.destroy();
          resolve(true);
        })
        .once("timeout", () => {
          socket.destroy();
          resolve(false);
        })
        .once("error", () => {
          socket.destroy();
          resolve(false);
        })
        .connect(port, host);
    });

    if (is_open) return true;

    await wait_ms(200);
  }

  console.warn(`⚠️ DevTools port ${port} not open after ${max_wait_ms}ms`);
  return false;
}

/**
 * More robust DevTools readiness check
 */
async function wait_until_devtools_ready(
  port,
  max_wait_ms = 7000,
  host = "127.0.0.1"
) {
  const start_time = Date.now();
  const ok = await wait_until_port_is_open(port, max_wait_ms, host);
  if (!ok) return false;

  const elapsed = Date.now() - start_time;
  const remaining = Math.max(max_wait_ms - elapsed, 0);
  const deadline = Date.now() + remaining;

  if (typeof fetch !== "function") return true;

  const endpoint = `http://${host}:${port}/json/version`;

  while (Date.now() < deadline) {
    try {
      const res = await fetch(endpoint);
      if (res.ok) {
        const j = await res.json().catch(() => null);
        if (j && j.Browser) return true;
        return true;
      }
    } catch { }
    await wait_ms(200);
  }

  console.warn(`⚠️ DevTools not ready at ${endpoint}`);
  return false;
}

async function safe_wait_for_selector(frame_or_page, selector, opts = {}) {
  try {
    await frame_or_page.waitForSelector(selector, { state: "visible", ...opts });
  } catch (err) {
    const msg = String(err?.message || "");

    if (
      msg.includes("Target page, context or browser has been closed") ||
      msg.includes("Frame was detached") ||
      msg.includes("Execution context was destroyed")
    ) {
      err.code = "E_TARGET_CLOSED";
    } else if (err?.name === "TimeoutError" || msg.includes("Timeout")) {
      err.code = "E_GOTO_TIMEOUT";
    }

    throw err;
  }
}

async function helper_browser_close_restart_relogin(
  port,
  browser,
  page,
  context,
  url_home_page,
  load_timeout_ms,
  wrestling_season,
  track_wrestling_category,
  url_login_page,
  cause
) {
  if (context) await close_extra_tabs(context, page);

  try {
    if (browser?.isConnected?.()) {
      await browser.close();
    }
  } catch (e) {
    console.warn("⚠️ browser.close error ignored:", e?.message || e);
  }

  if (cause) {
    console.warn(`♻️ ${cause} — reconnecting...`);
  } else {
    console.warn("♻️ reconnecting...");
  }

  await wait_until_devtools_ready(port, 8000).catch(() => false);

  ({ browser, page, context } = await step_0_launch_chrome_developer_parallel_scrape(
    url_home_page,
    port,
    { force_relaunch: true }
  ));

  browser.on?.("disconnected", () => {
    console.warn("⚠️ CDP disconnected");
  });

  await relogin(
    page,
    load_timeout_ms,
    wrestling_season,
    track_wrestling_category,
    url_login_page
  );

  return { browser, page, context };
}

function build_wrestler_matches_url(url_home_page, page, raw_url) {
  try {
    const cur = new URL(page.url(), url_home_page);
    const tim = cur.searchParams.get("TIM") || String(Date.now());
    const sid = cur.searchParams.get("twSessionId") || "";

    const stored = new URL(raw_url, url_home_page);
    const wid = stored.searchParams.get("wrestlerId");

    const base = new URL("/seasons/WrestlerMatches.jsp", url_home_page).toString();
    const params = new URLSearchParams();

    params.set("TIM", tim);
    if (sid) params.set("twSessionId", sid);
    if (wid) params.set("wrestlerId", wid);

    return `${base}?${params.toString()}`;
  } catch {
    return raw_url;
  }
}

/* ------------------------------------------
   extractor_source
-------------------------------------------*/
function extractor_source() {
  return () => {
    const norm = (s) =>
      (s || "")
        .normalize("NFKD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/\s+/g, " ")
        .trim();

    const to_date = (y, m, d) => {
      const yy = +y < 100 ? +y + 2000 : +y;
      const dt = new Date(yy, +m - 1, +d);
      return isNaN(+dt) ? null : dt;
    };

    const fmt_mdy = (d) => {
      if (!(d instanceof Date) || isNaN(+d)) return "";
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const yy = String(d.getFullYear());
      return `${mm}/${dd}/${yy}`;
    };

    const parse_date_range_text = (raw) => {
      const t = norm(raw);
      if (!t) return { start_date: "", end_date: "" };

      let m =
        t.match(
          /^(\d{1,2})[\/\-](\d{1,2})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/
        );
      if (m) {
        const [, m1, d1, m2, d2, y2] = m;
        const start_obj = to_date(y2, m1, d1);
        const end_obj = to_date(y2, m2, d2);
        return { start_date: fmt_mdy(start_obj), end_date: fmt_mdy(end_obj) };
      }

      m =
        t.match(
          /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/
        );
      if (m) {
        const [, m1, d1, y1, m2, d2, y2] = m;
        const start_obj = to_date(y1, m1, d1);
        const end_obj = to_date(y2, m2, d2);
        return { start_date: fmt_mdy(start_obj), end_date: fmt_mdy(end_obj) };
      }

      m = t.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
      if (m) {
        const [, mm, dd, yy] = m;
        const d = to_date(yy, mm, dd);
        return { start_date: fmt_mdy(d), end_date: "" };
      }

      m = t.match(/(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/);
      if (m) {
        const [token] = m;
        const [mm, dd, yy] = token.split(/[\/\-]/);
        const d = to_date(yy, mm, dd);
        return { start_date: fmt_mdy(d), end_date: "" };
      }

      return { start_date: "", end_date: "" };
    };

    const sel = document.querySelector("#wrestler");
    const sel_opt =
      sel?.selectedOptions?.[0] ||
      document.querySelector("#wrestler option[selected]");

    const wrestler_id = (sel_opt?.value || "").trim();
    const opt_text = norm(sel_opt?.textContent || "");
    const wrestler = opt_text.includes(" - ")
      ? opt_text.split(" - ").slice(1).join(" - ").trim()
      : opt_text;

    const rows = [];
    let match_order = 1;

    for (const tr of document.querySelectorAll("tr.dataGridRow")) {
      const tds = tr.querySelectorAll("td");
      if (tds.length < 5) continue;

      const date_raw = norm(tds[1]?.innerText);
      const { start_date, end_date } = parse_date_range_text(date_raw);

      const event_raw = norm(tds[2]?.innerText);
      const weight_raw = norm(tds[3]?.innerText);
      const details_cell = tds[4];
      const details_text_raw = norm(details_cell?.innerText);

      let opponent_id = "";
      const link_nodes = Array.from(
        details_cell.querySelectorAll('a[href*="wrestlerId="]')
      );
      for (const a of link_nodes) {
        const href = a.getAttribute("href") || "";
        const m = href.match(/wrestlerId=(\d+)/);
        if (m && m[1] && m[1] !== wrestler_id) {
          opponent_id = m[1];
          break;
        }
      }

      rows.push({
        page_url: location.href,
        wrestler_id,
        wrestler,
        start_date,
        end_date,
        event: event_raw,
        weight_category: weight_raw,
        match_order,
        opponent_id,
        raw_details: details_text_raw,
      });

      match_order += 1;
    }

    return rows;
  };
}

// 🔧 track whether we've already attached the dialog handler
let dialog_handler_attached = false;

/* ------------------------------------------
   main orchestrator
-------------------------------------------*/
async function main(
  url_home_page,
  url_login_page,
  matches_page_limit,
  loop_start,

  wrestling_season,
  track_wrestling_category,
  gender,

  // ✅ can be passed in OR omitted (self-managed session)
  page,
  browser,
  context,

  file_path,

  task_set_id,
  port,
  worker_id,

) {
  if (!task_set_id) {
    throw new Error("task_set_id is required (wrestler_match_history_scrape_tasks scope)");
  }
  if (!port) {
    throw new Error("port is required (per-worker Chrome DevTools port)");
  }

  const load_timeout_ms = 30000;
  const MAX_ATTEMPTS_PER_WRESTLER = 2;

  // ✅ allow self-managed session creation
  let created_session_here = false;

  if (!page || !browser || !context) {
    console.log(
      color_text(
        `🧩 No page/browser/context provided — launching/attaching via DevTools port ${port}`,
        "cyan"
      )
    );

    await wait_until_devtools_ready(port, 8000).catch(() => false);

    ({ browser, page, context } = await step_0_launch_chrome_developer_parallel_scrape(
      url_home_page,
      port,
      { force_relaunch: false }
    ));

    created_session_here = true;

    browser.on?.("disconnected", () => console.warn("⚠️ CDP disconnected — Chrome closed"));
  }

  // 🔧 handle JS dialogs safely (MUST NOT crash if CDP session is closing)
  if (page && !dialog_handler_attached) {
    dialog_handler_attached = true;

    page.on("dialog", (dialog) => {
      // Log immediately — no await, no throw risk
      try {
        console.log(
          color_text(
            `📣 JS dialog detected: "${dialog.message()}" → auto-accepting so navigation can continue`,
            "yellow"
          )
        );
      } catch {
        // logging must never be allowed to crash the worker
      }

      // IMPORTANT: never await; never let this reject unhandled
      dialog.accept().catch((err) => {
        const msg = String(err?.message || "");

        // Expected during shutdown / CDP disconnect — silently ignore
        if (
          msg.includes("Page.handleJavaScriptDialog") ||
          msg.includes("session closed") ||
          msg.includes("Internal server error") ||
          msg.includes("Protocol error") ||
          msg.includes("Target closed")
        ) {
          return;
        }

        console.warn(`⚠️ Failed to handle dialog: ${msg}`);
      });
    });
  }

  // DB: count + cap
  const total_rows_in_db = await count_rows_in_db_scrape_task(task_set_id);
  const no_of_urls = Math.min(matches_page_limit, total_rows_in_db);

  let headers_written = false;

  // counters
  let processed = 0;
  let csv_write_iterations = 0;
  let total_rows_written_csv = 0;
  let total_rows_inserted_db = 0;
  let total_rows_updated_db = 0;
  let auto_recover_cdp_count = 0;
  let auto_recover_timeout_count = 0;
  let hard_reset_count = 0;

  browser.on?.("disconnected", () =>
    console.warn("⚠️ CDP disconnected — Chrome closed")
  );

  // INITIAL LOGIN
  await safe_goto(page, url_login_page, { timeout: load_timeout_ms });
  await page.waitForTimeout(2000);

  console.log("step 1: on index.jsp, auto login for:", wrestling_season);
  await safe_auto_login(page, wrestling_season, track_wrestling_category);
  await page.waitForTimeout(1000);

  console.log(
    color_text(
      `📄 DB has ${total_rows_in_db} wrestler links derived from scrape tasks table`,
      "green"
    )
  );

  console.log(
    color_text(
      `\x1b[33m⚙️ Processing up to ${no_of_urls} (min of page limit vs DB size) starting at index ${loop_start}\x1b[0m\n`,
      "green"
    )
  );

  const iterator = iter_name_links_based_on_scrape_task({
    start_at: 0,                // for locked scope, always start at 0
    limit: matches_page_limit,  // typically 1 per task
    batch_size: 500,
    task_set_id,

    // ✅ only process rows this worker has claimed
    status: "LOCKED",
    locked_by: worker_id,
  });

  for await (const { i, url } of iterator) {
    const loop_number = processed + 1;

    console.log(
      color_text(
        `\n🔁 Starting loop #${loop_number} for DB index=${i}, loop_start=${loop_start}, planned_total=${no_of_urls}`,
        "cyan"
      )
    );

    if (handles_dead({ browser, context, page })) {
      ({ browser, page, context } = await helper_browser_close_restart_relogin(
        port,
        browser,
        page,
        context,
        url_home_page,
        load_timeout_ms,
        wrestling_season,
        track_wrestling_category,
        url_login_page,
        "handles_dead detected"
      ));
    }

    let attempts = 0;
    while (attempts < MAX_ATTEMPTS_PER_WRESTLER) {
      attempts += 1;

      try {
        const all_rows = [];

        const effective_url = build_wrestler_matches_url(url_home_page, page, url);

        console.log("step 2a: go to url:", effective_url);
        await safe_goto(page, effective_url, { timeout: load_timeout_ms });

        console.log("step 3: wait for redirect");
        await page
          .waitForURL(/seasons\/index\.jsp/i, { timeout: 5000 })
          .catch(() => { });

        if (/seasons\/index\.jsp/i.test(page.url())) {
          console.log(
            "step 3a: on index.jsp, starting auto login for season:",
            wrestling_season
          );

          await safe_auto_login(page, wrestling_season, track_wrestling_category);
          await page.waitForTimeout(1000);

          const effective_url_after_login = build_wrestler_matches_url(
            url_home_page,
            page,
            url
          );
          console.log(
            "step 3b: re-navigating to original URL after login:",
            effective_url_after_login
          );

          await safe_goto(page, effective_url_after_login, { timeout: load_timeout_ms });
          await page.waitForTimeout(1000);
        }

        if (/MainFrame\.jsp/i.test(page.url())) {
          const effective_url_mainframe = build_wrestler_matches_url(
            url_home_page,
            page,
            url
          );

          await safe_goto(page, effective_url_mainframe, { timeout: load_timeout_ms });
        }

        console.log("step 4: wait for dropdown");
        await with_fresh_target_frame(page, async (tf) => {
          await safe_wait_for_selector(tf, "#wrestler", { timeout: load_timeout_ms });
        });

        console.log("step 5: extract rows");
        const rows = await with_fresh_target_frame(page, async (tf) => {
          await tf.waitForLoadState?.("domcontentloaded");
          await page.waitForTimeout(1000);
          return await tf.evaluate(extractor_source());
        });

        // ✅ progress (DB-driven; global task_set_id progress)
        let prog = null;
        try {
          prog = await get_task_set_progress(task_set_id);
        } catch (e) {
          console.warn("⚠️ get_task_set_progress failed (ignored):", e?.message || e);
        }

        const total = prog?.total_count ?? total_rows_in_db;

        // "completed" = Done + Failed (Locked is in-flight)
        const completed = (prog?.done_count ?? 0) + (prog?.failed_count ?? 0);
        const locked = prog?.locked_count ?? 0;
        const done = prog?.done_count ?? 0;
        const failed = prog?.failed_count ?? 0;
        const pending = prog?.pending_count ?? 0;
        const duration = prog?.duration_hh_mm_ss ?? "00:00:00";

        console.log(
          color_text(
            `✔ ${completed} of ${total} ` +
            `(done=${done}, locked=${locked}, failed=${failed}, pending=${pending}, duration=${duration}) ` +
            `(invocation ${processed + 1} of ${no_of_urls}). rows returned: ${rows.length} from ${url}`,
            "red"
          )
        );

        all_rows.push(...rows);

        // delete existing match history for this wrestler/season/category
        const this_wrestler_id = rows?.[0]?.wrestler_id;
        if (this_wrestler_id) {
          try {
            console.log(
              color_text(
                `🧹 deleting existing match history for wrestler_id=${this_wrestler_id} (${wrestling_season}, ${track_wrestling_category})`,
                "yellow"
              )
            );
            await delete_wrestler_match_history_for_wrestler(
              { wrestling_season, track_wrestling_category },
              this_wrestler_id
            );
          } catch (e) {
            console.error(
              "⚠️ failed to delete existing match history for wrestler_id=" +
              this_wrestler_id +
              ":",
              e?.message || e
            );
          }
        }

        console.log("step 6: save to csv");
        csv_write_iterations += 1;
        const headers_written_now = await save_to_csv_file(
          all_rows,
          i,
          headers_written,
          file_path
        );
        headers_written = headers_written_now;
        total_rows_written_csv += all_rows.length;
        console.log(`\x1b[33m➕ tracking headers_written: ${headers_written}\x1b[0m\n`);

        console.log("step 7: save to sql db\n");
        try {
          const { inserted, updated } = await upsert_wrestler_match_history(rows, {
            wrestling_season,
            track_wrestling_category,
            gender,
          });
          total_rows_inserted_db += inserted;
          total_rows_updated_db += updated;
          console.log(
            color_text(`🛠️ DB upsert — inserted: ${inserted}, updated: ${updated}`, "green")
          );
        } catch (e) {
          console.error("❌ DB upsert failed:", e?.message || e);
        }

        processed += 1;

        const HARD_RESET_LIMIT = 30;
        if (processed % HARD_RESET_LIMIT === 0 && processed < no_of_urls) {
          hard_reset_count += 1;
          console.log(
            color_text(
              `=================================
HARD RESTART AT ${HARD_RESET_LIMIT}
♻️ Processed ${processed} wrestler pages — recycling browser session (hard reset at ${HARD_RESET_LIMIT}).
===================================`,
              "yellow"
            )
          );
          ({ browser, page, context } = await helper_browser_close_restart_relogin(
            port,
            browser,
            page,
            context,
            url_home_page,
            load_timeout_ms,
            wrestling_season,
            track_wrestling_category,
            url_login_page,
            `processed ${HARD_RESET_LIMIT} pages`
          ));
        }

        const resume_from_index = i + 1;
        console.log(
          color_text(
            `📊 Loop summary #${loop_number} — processed_loops=${processed}, csv_write_iterations=${csv_write_iterations}, total_rows_written_csv=${total_rows_written_csv}, total_db_inserted=${total_rows_inserted_db}, total_db_updated=${total_rows_updated_db}, auto_recover_cdp=${auto_recover_cdp_count}, auto_recover_timeouts=${auto_recover_timeout_count}, hard_resets=${hard_reset_count}, last_db_index=${i}, resume_from_index=${resume_from_index}`,
            "cyan"
          )
        );

        break; // success
      } catch (e) {
        const msg = String(e?.message || "");

        if (is_cdp_disconnect_error(e) || e?.code === "E_GOTO_TIMEOUT") {
          const is_timeout = e?.code === "E_GOTO_TIMEOUT";
          const cause = is_timeout ? "navigation timeout" : "CDP/target closed";

          if (is_timeout) auto_recover_timeout_count += 1;
          else auto_recover_cdp_count += 1;

          const recover_attempt_no =
            auto_recover_cdp_count + auto_recover_timeout_count;

          console.warn(
            color_text(
              `♻️ Auto-recover #${recover_attempt_no} triggered due to ${cause} (attempt ${attempts}/${MAX_ATTEMPTS_PER_WRESTLER})`,
              "yellow"
            )
          );

          ({ browser, page, context } = await helper_browser_close_restart_relogin(
            port,
            browser,
            page,
            context,
            url_home_page,
            load_timeout_ms,
            wrestling_season,
            track_wrestling_category,
            url_login_page,
            cause
          ));

          const effective_url_after_reconnect = build_wrestler_matches_url(
            url_home_page,
            page,
            url
          );

          await safe_goto(page, effective_url_after_reconnect, { timeout: load_timeout_ms });
          continue; // retry this wrestler
        }

        console.error("❌ Fatal error while processing wrestler link", {
          index: i,
          url,
          attempts,
          msg,
        });
        throw e;
      }
    }
  }

  // ✅ Only close the CDP connection if we created it here
  if (created_session_here) {
    try {
      await browser.close();
    } catch { }
  }

  console.log(
    color_text(
      `\n✅ done. processed ${processed} wrestler pages from DB scrape tasks. csv_write_iterations=${csv_write_iterations}, total_rows_written_csv=${total_rows_written_csv}, total_db_inserted=${total_rows_inserted_db}, total_db_updated=${total_rows_updated_db}, auto_recover_cdp=${auto_recover_cdp_count}, auto_recover_timeouts=${auto_recover_timeout_count}, hard_resets=${hard_reset_count}`,
      "green"
    )
  );

  return {
    processed,
    csv_write_iterations,
    total_rows_written_csv,
    total_rows_inserted_db,
    total_rows_updated_db,
    auto_recover_cdp_count,
    auto_recover_timeout_count,
    hard_reset_count,
    created_session_here,
  };
}

export { main as step_3_get_wrestler_match_history_scrape };

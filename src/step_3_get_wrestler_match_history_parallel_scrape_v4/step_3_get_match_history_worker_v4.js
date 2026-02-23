// src/step_3_get_wrestler_match_history_parallel_scrape_v4/step_3_get_match_history_worker_v4.js

import net from "net"; // for wait_until_port_is_open function
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// 🔧 global handler to suppress the noisy dialog error
process.on("unhandledRejection", (err) => {
  const msg = String(err?.message || "");
  if (msg.includes("Page.handleJavaScriptDialog") && msg.includes("No dialog is showing")) {
    console.warn(
      "⚠️ Suppressed Playwright dialog error: Page.handleJavaScriptDialog → No dialog is showing"
    );
    return; // swallow this specific harmless error
  }
  throw err; // rethrow everything else
});

// Save files to csv & msyql
import { save_to_csv_file } from "../../utilities/create_and_load_csv_files/save_to_csv_file.js";
import {
  upsert_wrestler_match_history,
  // delete helper for per-wrestler snapshot
  delete_wrestler_match_history_for_wrestler,
} from "../../utilities/mysql/upsert_wrestler_match_history.js";

import {
  count_rows_in_db_wrestler_links,
  iter_name_links_from_db,
  count_name_links_based_on_event_schedule,
  iter_name_links_based_on_event_schedule,
  get_task_set_progress,
} from "../../utilities/mysql/iter_name_links_from_db.js";

import { step_0_launch_chrome_developer_v3 } from "./step_0_launch_chrome_developer_v3.js";
import { auto_login_select_season } from "../../utilities/scraper_tasks/auto_login_select_season.js";

import { color_text } from "../../utilities/console_logs/console_colors.js";
import { step_19_close_chrome_dev } from "../step_19_close_chrome_developer.js";

/* =========================================================
   ✅ REQUIRED CHANGE ONLY:
   integrate tasks table claim + DONE/FAILED tracking
========================================================= */
import os from "os";
import { get_pool } from "../../utilities/mysql/mysql_pool.js";

/* ------------------------------------------
   small helpers
-------------------------------------------*/
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
    msg.includes("Page crashed") ||
    msg.includes("Target crashed") ||
    msg.includes("Execution context was destroyed") ||
    msg.includes("Target page, context or browser has been closed") ||
    msg.includes("Target closed") ||
    msg.includes("Session closed") ||
    msg.includes("has been closed") ||
    msg.includes("CDP connection closed") ||
    msg.includes("WebSocket is not open")
  );
}

// ✅ NEW: safe sleep that won’t throw if page dies mid-wait
async function safe_sleep(page, ms) {
  if (!page || page.isClosed?.()) return;
  try {
    await page.waitForTimeout(ms);
  } catch {
    // swallow (page/context may have closed)
  }
}

// 🔧 NEW: safe wrapper around auto_login_select_season
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

/**
 * ✅ UPDATED: relogin is now resilient to page closes
 * - No hard dependency on waitForTimeout
 * - Uses load states; sleeps are safe
 */
async function relogin(page, load_timeout_ms, wrestling_season, track_wrestling_category, url_login_page) {
  const login_url = url_login_page;
  await safe_goto(page, login_url, { timeout: load_timeout_ms });

  await page
    .waitForLoadState("domcontentloaded", { timeout: Math.min(load_timeout_ms, 15000) })
    .catch(() => { });

  await safe_sleep(page, 500);

  await safe_auto_login(page, wrestling_season, track_wrestling_category);

  await page
    .waitForLoadState("domcontentloaded", { timeout: Math.min(load_timeout_ms, 15000) })
    .catch(() => { });

  await safe_sleep(page, 300);
}

async function safe_goto(page, url, opts = {}) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", ...opts });
  } catch (err) {
    const msg = String(err?.message || "");

    // ✅ treat Chromium crashes as recoverable
    if (msg.includes("Page crashed") || msg.includes("Target crashed")) {
      err.code = "E_TARGET_CLOSED";
      throw err;
    }

    if (msg.includes("is interrupted by another navigation")) {
      console.warn("⚠️ Ignored navigation interruption, site redirected itself.");
      await page.waitForLoadState("domcontentloaded").catch(() => { });
      return page.url();
    }

    if (msg.includes("Target page, context or browser has been closed")) {
      err.code = "E_TARGET_CLOSED";
      throw err;
    }

    if (err?.name === "TimeoutError" || msg.includes("Timeout")) {
      err.code = "E_GOTO_TIMEOUT";
      throw err;
    }

    throw err;
  }

  return page.url();
}

async function wait_ms(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Wait until DevTools port is open
 */
async function wait_until_port_is_open(port = 9222, max_wait_ms = 5000, host = "127.0.0.1") {
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
async function wait_until_devtools_ready(port = 9222, max_wait_ms = 7000, host = "127.0.0.1") {
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

/* =========================================================
   ✅ NEW: shared recovery mutex
   - Prevent watchdog + main loop from recovering at same time
========================================================= */
async function with_recovery_mutex(state, fn) {
  // wait for ongoing recovery if present
  while (state._recovery_promise) {
    try {
      await state._recovery_promise;
    } catch {
      // ignore; next recovery attempt can proceed
    }
  }

  const p = (async () => {
    state.recovering = true;
    try {
      return await fn();
    } finally {
      state.recovering = false;
      state._recovery_promise = null;
    }
  })();

  state._recovery_promise = p;
  return await p;
}

async function helper_browser_close_restart_relogin(
  browser,
  page,
  context,
  port,
  url_home_page,
  load_timeout_ms,
  wrestling_season,
  track_wrestling_category,
  url_login_page,
  cause
) {
  // ✅ reduce fragility: only close tabs if keep_page looks healthy
  if (context && page && !page.isClosed?.()) {
    await close_extra_tabs(context, page);
  }

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

  ({ browser, page, context } = await step_0_launch_chrome_developer_v3(url_home_page, port));

  browser.on?.("disconnected", () => {
    console.warn("⚠️ CDP disconnected");
  });

  await relogin(page, load_timeout_ms, wrestling_season, track_wrestling_category, url_login_page);

  return { browser, page, context };
}

/* =========================================================
   🐶 MINIMAL WATCHDOG  (UPDATED with mutex)
========================================================= */
function start_watchdog({
  state, // { browser, page, context, recovering, _recovery_promise }
  port,
  url_home_page,
  load_timeout_ms,
  wrestling_season,
  track_wrestling_category,
  url_login_page,
  interval_ms = 5000,
}) {
  let stopped = false;
  let local_recovering = false;

  const tick = async () => {
    if (stopped || local_recovering) return;

    // ✅ if main loop is recovering, watchdog must back off
    if (state.recovering || state._recovery_promise) return;

    try {
      const port_ok = await wait_until_port_is_open(port, 1200).catch(() => false);

      const dead =
        !port_ok ||
        !state.browser?.isConnected?.() ||
        !state.context ||
        !state.page ||
        state.page.isClosed?.();

      if (!dead) return;

      local_recovering = true;

      await with_recovery_mutex(state, async () => {
        console.warn(
          `[port=${port}] 🐶 watchdog: dead session detected (port_ok=${port_ok}, connected=${state.browser?.isConnected?.()}) — recovering...`
        );

        const res = await helper_browser_close_restart_relogin(
          state.browser,
          state.page,
          state.context,
          port,
          url_home_page,
          load_timeout_ms,
          wrestling_season,
          track_wrestling_category,
          url_login_page,
          `watchdog detected dead session (port_ok=${port_ok})`
        );

        state.browser = res.browser;
        state.page = res.page;
        state.context = res.context;

        console.warn(`[port=${port}] 🐶 watchdog: recovery complete`);
      });
    } catch (e) {
      console.warn(`[port=${port}] 🐶 watchdog: recovery failed:`, e?.message || e);
    } finally {
      local_recovering = false;
    }
  };

  const timer = setInterval(() => void tick(), interval_ms);

  return {
    stop() {
      stopped = true;
      clearInterval(timer);
    },
  };
}

function build_wrestler_matches_url(url_home_page, page, raw_url) {
  try {
    const stored = new URL(raw_url, url_home_page);
    const wid = stored.searchParams.get("wrestlerId");

    const cur = new URL(page.url(), url_home_page);
    const sid = cur.searchParams.get("twSessionId") || "";

    const base = new URL("/seasons/WrestlerMatches.jsp", url_home_page);
    if (wid) base.searchParams.set("wrestlerId", wid);

    if (sid) base.searchParams.set("twSessionId", sid);

    base.searchParams.set("TIM", String(Date.now()));

    return base.toString();
  } catch {
    return raw_url;
  }
}

/* ------------------------------------------
   extractor_source  (reverted to pre-bout_index rows)
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

      // A: MM/DD - MM/DD/YYYY
      let m = t.match(
        /^(\d{1,2})[\/\-](\d{1,2})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/
      );
      if (m) {
        const [, m1, d1, m2, d2, y2] = m;
        const start_obj = to_date(y2, m1, d1);
        const end_obj = to_date(y2, m2, d2);
        return { start_date: fmt_mdy(start_obj), end_date: fmt_mdy(end_obj) };
      }

      // B: MM/DD/YYYY - MM/DD/YYYY
      m = t.match(
        /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/
      );
      if (m) {
        const [, m1, d1, y1, m2, d2, y2] = m;
        const start_obj = to_date(y1, m1, d1);
        const end_obj = to_date(y2, m2, d2);
        return { start_date: fmt_mdy(start_obj), end_date: fmt_mdy(end_obj) };
      }

      // C: MM/DD/YYYY
      m = t.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
      if (m) {
        const [, mm, dd, yy] = m;
        const d = to_date(yy, mm, dd);
        return { start_date: fmt_mdy(d), end_date: "" };
      }

      // fallback: first full date token
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
      sel?.selectedOptions?.[0] || document.querySelector("#wrestler option[selected]");

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
      const link_nodes = Array.from(details_cell.querySelectorAll('a[href*="wrestlerId="]'));
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

/* =========================================================
   ✅ REQUIRED CHANGE ONLY:
   tasks table helpers (inline, minimal)
========================================================= */
function get_now() {
  return new Date();
}

function fmt_mysql_dt_utc(d) {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function fmt_mysql_dt_mtn(d) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Denver",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    hourCycle: "h23",
  }).formatToParts(d);

  const get = (type) => parts.find((p) => p.type === type)?.value;

  let hh = get("hour");
  if (hh === "24") hh = "00";

  return `${get("year")}-${get("month")}-${get("day")} ${hh}:${get("minute")}:${get("second")}`;
}

function build_worker_id(port) {
  return `${os.hostname()}|pid=${process.pid}|port=${port}`;
}

async function claim_next_tasks({ task_set_id, claim_batch_size = 25, worker_id }) {
  const pool = await get_pool();
  const conn = await pool.getConnection();

  const now_utc = get_now();
  // NOTE: fmt_mysql_dt_mtn() already formats in America/Denver, so pass normal Date()
  const now_mtn = now_utc;

  const query_skip_lock = `
      SELECT 
        id, wrestler_id, name_link
      FROM wrestler_match_history_scrape_tasks
      WHERE task_set_id = ?
        AND status = 'PENDING'
      ORDER BY id
      LIMIT ?
      FOR UPDATE SKIP LOCKED
  `;

  async function query_claim_task(ids) {
    const query = `
        UPDATE wrestler_match_history_scrape_tasks
        SET
          status = 'LOCKED',
          locked_by = ?,
          locked_at_utc = ?,
          attempt_count = attempt_count + 1,
          updated_at_utc = ?,
          updated_at_mtn = ?
        WHERE id IN (${ids.map(() => "?").join(",")})
    `;
    return query;
  }

  try {
    await conn.beginTransaction();

    const [rows] = await conn.query(query_skip_lock, [task_set_id, claim_batch_size]);

    if (!rows.length) {
      await conn.commit();
      return [];
    }

    const ids = rows.map((r) => r.id);

    await conn.query(await query_claim_task(ids), [
      worker_id,
      fmt_mysql_dt_utc(now_utc),
      fmt_mysql_dt_utc(now_utc),
      fmt_mysql_dt_mtn(now_mtn),
      ...ids,
    ]);

    await conn.commit();
    return rows;
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

async function mark_task_done({ task_id }) {
  const pool = await get_pool();
  const now_utc = get_now();
  const now_mtn = now_utc;

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
    [fmt_mysql_dt_utc(now_utc), fmt_mysql_dt_mtn(now_mtn), task_id]
  );
}

async function mark_task_failed({ task_id, error }) {
  const pool = await get_pool();
  const now_utc = get_now();
  const now_mtn = now_utc;

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
    [String(error), fmt_mysql_dt_utc(now_utc), fmt_mysql_dt_mtn(now_mtn), task_id]
  );
}

async function requeue_tasks_for_worker({ task_set_id, worker_id, reason = "PORT_STUCK_OR_DEAD" }) {
  const pool = await get_pool();
  const now_utc = get_now();
  const now_mtn = now_utc;

  const [res] = await pool.query(
    `
    UPDATE wrestler_match_history_scrape_tasks
    SET
      status = 'PENDING',
      last_error = CONCAT(?, ' | ', COALESCE(last_error, '')),
      updated_at_utc = ?,
      updated_at_mtn = ?
    WHERE task_set_id = ?
      AND status = 'LOCKED'
      AND locked_by = ?
    `,
    [reason, fmt_mysql_dt_utc(now_utc), fmt_mysql_dt_mtn(now_mtn), task_set_id, worker_id]
  );

  console.log(
    `[requeue] task_set_id=${task_set_id} worker_id=${worker_id} requeued=${res?.affectedRows || 0}`
  );

  return res?.affectedRows || 0;
}

/* ------------------------------------------
   main orchestrator
-------------------------------------------*/
async function main({
  url_home_page,
  url_login_page,

  matches_page_limit = 5,
  loop_start = 0,

  wrestling_season = "2024-25",
  track_wrestling_category = "High School Boys",
  gender,

  sql_where_filter_state_qualifier,
  sql_where_filter_onthemat_ranking_list,
  sql_team_id_list,
  sql_wrestler_id_list,

  page,
  browser,
  context,
  port,

  file_path,

  use_scheduled_events_iterator_query = false,
  use_wrestler_list_iterator_query = true,

  task_set_id = null,
  worker_id = null,
  claim_batch_size = 25,

  is_extended_failure_retry = true, // PASS 1 default: more tolerant (longer timeout + more attempts)
}) {
  // ✅ NEW: retry tuning (kept very local / minimal)
  const load_timeout_ms = is_extended_failure_retry ? 60000 : 30000;
  const MAX_ATTEMPTS_PER_WRESTLER = is_extended_failure_retry ? 2 : 1;
  const PROG_REFRESH_MS = is_extended_failure_retry ? 5000 : 2500;

  // ✅ NEW: port-prefixed logger (minimal)
  const p = (...args) => console.log(`[port=${port}]`, ...args);

  // 🔧 handle JS dialogs safely by auto-accepting so they don't block navigation
  if (page && !dialog_handler_attached) {
    dialog_handler_attached = true;

    page.on("dialog", async (dialog) => {
      try {
        p(
          color_text(
            `📣 JS dialog detected: "${dialog.message()}" → auto-accepting so navigation can continue`,
            "yellow"
          )
        );
        await dialog.accept();
      } catch (err) {
        p(`⚠️ Failed to handle dialog: ${err.message}`);
      }
    });
  }

  const use_tasks_table = Boolean(task_set_id);
  const effective_worker_id = worker_id || build_worker_id(port);

  const mode = (() => {
    if (use_tasks_table) return "tasks";
    if (use_scheduled_events_iterator_query && !use_wrestler_list_iterator_query) {
      return "events";
    }
    if (!use_scheduled_events_iterator_query && use_wrestler_list_iterator_query) {
      return "list";
    }
    p(
      "⚠️ iterator flags ambiguous (use_scheduled_events_iterator_query=" +
      use_scheduled_events_iterator_query +
      ", use_wrestler_list_iterator_query=" +
      use_wrestler_list_iterator_query +
      "); defaulting to list-based iterator."
    );
    return "list";
  })();

  let total_rows_in_db;

  if (mode === "events") {
    total_rows_in_db = await count_name_links_based_on_event_schedule(
      wrestling_season,
      track_wrestling_category
    );
  } else if (mode === "list") {
    total_rows_in_db = await count_rows_in_db_wrestler_links(
      wrestling_season,
      track_wrestling_category,
      gender,
      sql_where_filter_state_qualifier,
      sql_where_filter_onthemat_ranking_list,
      sql_team_id_list,
      sql_wrestler_id_list
    );
  } else {
    total_rows_in_db = 0;
  }

  const no_of_urls = mode === "tasks" ? matches_page_limit : Math.min(matches_page_limit, total_rows_in_db);

  let headers_written = false;

  let processed = 0;
  let csv_write_iterations = 0;
  let total_rows_written_csv = 0;
  let total_rows_inserted_db = 0;
  let total_rows_updated_db = 0;
  let auto_recover_cdp_count = 0;
  let auto_recover_timeout_count = 0;
  let hard_reset_count = 0;

  // ✅ shared state for watchdog + loop (UPDATED)
  const state = {
    browser,
    page,
    context,
    recovering: false,
    _recovery_promise: null,
  };
  let watchdog = null;

  // ✅ NEW: task progress cache (prevents DB hammering)
  let _last_prog_fetch_ms = 0;
  let _last_prog_value = null;

  async function get_task_progress_cached(task_set_id_in) {
    if (!task_set_id_in) return null;

    const now = Date.now();
    if (_last_prog_value && now - _last_prog_fetch_ms < PROG_REFRESH_MS) {
      return _last_prog_value;
    }

    try {
      const prog = await get_task_set_progress(task_set_id_in);
      _last_prog_value = prog;
      _last_prog_fetch_ms = now;
      return prog;
    } catch (e) {
      console.warn("⚠️ get_task_set_progress failed (ignored):", e?.message || e);
      return _last_prog_value; // return stale if available
    }
  }

  try {
    browser.on?.("disconnected", () => p("⚠️ CDP disconnected — Chrome closed"));

    // INITIAL LOGIN
    await safe_goto(state.page, url_login_page, { timeout: load_timeout_ms });
    await safe_sleep(state.page, 2000);

    p("step 1: on index.jsp, auto login for:", wrestling_season);
    await safe_auto_login(state.page, wrestling_season, track_wrestling_category);
    await safe_sleep(state.page, 1000);

    // ✅ start watchdog AFTER we have a valid logged-in page
    watchdog = start_watchdog({
      state,
      port,
      url_home_page,
      load_timeout_ms,
      wrestling_season,
      track_wrestling_category,
      url_login_page,
      interval_ms: is_extended_failure_retry ? 6000 : 4000, // ✅ NEW: slightly calmer in is_extended_failure_retry
    });

    if (mode === "events") {
      p(
        color_text(
          `📄 DB has ${total_rows_in_db} wrestler links derived from scheduled events (yesterday & today)`,
          "green"
        )
      );
    } else if (mode === "list") {
      p(color_text(`📄 DB has ${total_rows_in_db} wrestler links (wrestler_list_scrape_data)`, "green"));
    } else {
      p(
        color_text(
          `📄 Tasks mode enabled (task_set_id=${task_set_id}, worker_id=${effective_worker_id}, claim_batch_size=${claim_batch_size})`,
          "green"
        )
      );
    }

    p(
      color_text(
        `\x1b[33m⚙️ Processing up to ${no_of_urls} starting at index ${loop_start}\x1b[0m\n`,
        "green"
      )
    );

    async function* iter_tasks_table() {
      while (true) {
        const port_ok = await wait_until_port_is_open(port, 1500).catch(() => false);
        if (!port_ok) {
          await requeue_tasks_for_worker({
            task_set_id,
            worker_id: effective_worker_id,
            reason: `PORT_NOT_OPEN port=${port}`,
          });

          throw new Error(`DevTools port ${port} is not open; requeued LOCKED tasks for this worker.`);
        }

        const tasks = await claim_next_tasks({
          task_set_id,
          claim_batch_size,
          worker_id: effective_worker_id,
        });

        if (!tasks.length) return;

        for (const t of tasks) {
          yield { i: t.id, url: t.name_link, task_id: t.id, wrestler_id_task: t.wrestler_id };
        }
      }
    }

    const iterator =
      mode === "tasks"
        ? iter_tasks_table()
        : mode === "events"
          ? iter_name_links_based_on_event_schedule({
            start_at: loop_start,
            limit: matches_page_limit,
            batch_size: 500,
            wrestling_season,
            track_wrestling_category,
          })
          : iter_name_links_from_db({
            start_at: loop_start,
            limit: matches_page_limit,
            batch_size: 500,
            wrestling_season,
            track_wrestling_category,
            gender,
            sql_where_filter_state_qualifier,
            sql_where_filter_onthemat_ranking_list,
            sql_team_id_list,
            sql_wrestler_id_list,
          });

    for await (const iter_item of iterator) {
      // ✅ always use the latest state refs
      browser = state.browser;
      page = state.page;
      context = state.context;

      const { i, url } = iter_item;
      const task_id = iter_item?.task_id || null;

      let task_finalized = false;
      let last_error_for_task = null;

      const loop_number = processed + 1;
      p(
        color_text(
          `\n🔁 Starting loop #${loop_number} for DB index=${i}, loop_start=${loop_start}, planned_total=${no_of_urls}`,
          "cyan"
        )
      );

      if (handles_dead({ browser, context, page })) {
        const res = await with_recovery_mutex(state, async () => {
          return await helper_browser_close_restart_relogin(
            state.browser,
            state.page,
            state.context,
            port,
            url_home_page,
            load_timeout_ms,
            wrestling_season,
            track_wrestling_category,
            url_login_page,
            "handles_dead detected"
          );
        });

        state.browser = res.browser;
        state.page = res.page;
        state.context = res.context;

        browser = state.browser;
        page = state.page;
        context = state.context;
      }

      let attempts = 0;
      while (attempts < MAX_ATTEMPTS_PER_WRESTLER) {
        attempts += 1;

        try {
          const all_rows = [];

          const effective_url = build_wrestler_matches_url(url_home_page, page, url);

          p("step 2a: go to url:", effective_url);
          await safe_goto(page, effective_url, { timeout: load_timeout_ms });

          p("step 2b: find target frame");
          let target_frame = page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();

          p("step 3: wait for redirect");
          await page.waitForURL(/seasons\/index\.jsp/i, { timeout: 5000 }).catch(() => { });

          if (/seasons\/index\.jsp/i.test(page.url())) {
            p("step 3a: on index.jsp, starting auto login for season:", wrestling_season);

            await safe_auto_login(page, wrestling_season, track_wrestling_category);
            await safe_sleep(page, 1000);

            const effective_url_after_login = build_wrestler_matches_url(url_home_page, page, url);
            p("step 3b: re-navigating to original URL after login:", effective_url_after_login);

            await safe_goto(page, effective_url_after_login, { timeout: load_timeout_ms });
            await safe_sleep(page, 1000);

            target_frame = page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
          }

          if (/MainFrame\.jsp/i.test(page.url())) {
            const effective_url_mainframe = build_wrestler_matches_url(url_home_page, page, url);

            await safe_goto(page, effective_url_mainframe, { timeout: load_timeout_ms });
            target_frame = page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
          }

          p("step 4: wait for dropdown");
          await safe_wait_for_selector(target_frame, "#wrestler", { timeout: load_timeout_ms });

          p("step 5: extract rows");
          await target_frame.waitForLoadState?.("domcontentloaded");
          await safe_sleep(page, 1000);

          let rows;
          try {
            rows = await target_frame.evaluate(extractor_source());
          } catch (e) {
            const msg = String(e?.message || "");
            if (
              msg.includes("Target page, context or browser has been closed") ||
              msg.includes("Frame was detached") ||
              msg.includes("Execution context was destroyed")
            ) {
              e.code = "E_TARGET_CLOSED";
            }

            if (e?.code === "E_TARGET_CLOSED") {
              const res = await with_recovery_mutex(state, async () => {
                return await helper_browser_close_restart_relogin(
                  state.browser,
                  state.page,
                  state.context,
                  port,
                  url_home_page,
                  load_timeout_ms,
                  wrestling_season,
                  track_wrestling_category,
                  url_login_page,
                  "frame died during evaluate"
                );
              });

              state.browser = res.browser;
              state.page = res.page;
              state.context = res.context;

              browser = state.browser;
              page = state.page;
              context = state.context;

              const effective_url_retry = build_wrestler_matches_url(url_home_page, page, url);
              await safe_goto(page, effective_url_retry, { timeout: load_timeout_ms });

              const tf = page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
              rows = await tf.evaluate(extractor_source());
            } else {
              throw e;
            }
          }

          // progress (tasks version)  ✅ cached
          const prog = await get_task_progress_cached(task_set_id);

          const total = prog?.total_count ?? total_rows_in_db;
          const completed = (prog?.done_count ?? 0) + (prog?.failed_count ?? 0);
          const locked = prog?.locked_count ?? 0;
          const done = prog?.done_count ?? 0;
          const failed = prog?.failed_count ?? 0;
          const pending = prog?.pending_count ?? 0;
          const duration = prog?.duration_hh_mm_ss ?? "00:00:00";

          p(
            color_text(
              `✔ ${completed} of ${total} ` +
              `(done=${done}, locked=${locked}, failed=${failed}, pending=${pending}, duration=${duration}) ` +
              `(invocation ${processed + 1} of ${no_of_urls}). rows returned: ${rows.length} from ${url}`,
              "red"
            )
          );

          all_rows.push(...rows);

          const this_wrestler_id = rows[0]?.wrestler_id;
          if (this_wrestler_id) {
            try {
              p(
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
              p(
                "⚠️ failed to delete existing match history for wrestler_id=" + this_wrestler_id + ":",
                e?.message || e
              );
            }
          }

          p("step 6: save to csv");
          csv_write_iterations += 1;
          const headers_written_now = await save_to_csv_file(all_rows, i, headers_written, file_path);
          headers_written = headers_written_now;
          total_rows_written_csv += all_rows.length;
          p(`\x1b[33m➕ tracking headers_written: ${headers_written}\x1b[0m\n`);

          p("step 7: save to sql db\n");
          try {
            const { inserted, updated } = await upsert_wrestler_match_history(rows, {
              wrestling_season,
              track_wrestling_category,
              gender,
            });
            total_rows_inserted_db += inserted;
            total_rows_updated_db += updated;
            p(color_text(`🛠️ DB upsert — inserted: ${inserted}, updated: ${updated}`, "green"));
          } catch (e) {
            p("❌ DB upsert failed:", e?.message || e);
          }

          processed += 1;

          const HARD_RESET_LIMIT = is_extended_failure_retry ? 30 : 20; // ✅ NEW: slightly more frequent recycling in is_extended_failure_retry false
          if (processed % HARD_RESET_LIMIT === 0 && processed < no_of_urls) {
            hard_reset_count += 1;
            p(
              color_text(
                `=================================
HARD RESTART AT ${HARD_RESET_LIMIT}
♻️ Processed ${processed} wrestler pages — recycling browser session (hard reset at ${HARD_RESET_LIMIT}).
===================================`,
                "yellow"
              )
            );

            const res = await with_recovery_mutex(state, async () => {
              return await helper_browser_close_restart_relogin(
                state.browser,
                state.page,
                state.context,
                port,
                url_home_page,
                load_timeout_ms,
                wrestling_season,
                track_wrestling_category,
                url_login_page,
                `processed ${HARD_RESET_LIMIT} pages`
              );
            });

            state.browser = res.browser;
            state.page = res.page;
            state.context = res.context;

            browser = state.browser;
            page = state.page;
            context = state.context;
          }

          const resume_from_index = i + 1;
          p(
            color_text(
              `📊 Loop summary #${loop_number} — processed_loops=${processed}, csv_write_iterations=${csv_write_iterations}, total_rows_written_csv=${total_rows_written_csv}, total_db_inserted=${total_rows_inserted_db}, total_db_updated=${total_rows_updated_db}, auto_recover_cdp=${auto_recover_cdp_count}, auto_recover_timeouts=${auto_recover_timeout_count}, hard_resets=${hard_reset_count}, last_db_index=${i}, resume_from_index=${resume_from_index}`,
              "cyan"
            )
          );

          if (mode === "tasks" && task_id) {
            try {
              await mark_task_done({ task_id });
              task_finalized = true;
              p(color_text(`✅ task DONE (task_id=${task_id})`, "green"));
            } catch (e) {
              p("⚠️ failed to mark task DONE:", e?.message || e);
            }
          }

          break; // ✅ success
        } catch (e) {
          last_error_for_task = e;

          if (is_cdp_disconnect_error(e) || e?.code === "E_GOTO_TIMEOUT") {
            const is_timeout = e?.code === "E_GOTO_TIMEOUT";
            const cause = is_timeout ? "navigation timeout" : "CDP/target closed";

            if (is_timeout) auto_recover_timeout_count += 1;
            else auto_recover_cdp_count += 1;

            const recover_attempt_no = auto_recover_cdp_count + auto_recover_timeout_count;

            p(
              color_text(
                `♻️ Auto-recover #${recover_attempt_no} triggered due to ${cause} (attempt ${attempts}/${MAX_ATTEMPTS_PER_WRESTLER})`,
                "yellow"
              )
            );

            const res = await with_recovery_mutex(state, async () => {
              return await helper_browser_close_restart_relogin(
                state.browser,
                state.page,
                state.context,
                port,
                url_home_page,
                load_timeout_ms,
                wrestling_season,
                track_wrestling_category,
                url_login_page,
                cause
              );
            });

            state.browser = res.browser;
            state.page = res.page;
            state.context = res.context;

            browser = state.browser;
            page = state.page;
            context = state.context;

            const effective_url_after_reconnect = build_wrestler_matches_url(url_home_page, page, url);
            await safe_goto(page, effective_url_after_reconnect, { timeout: load_timeout_ms });

            continue;
          }

          p("❌ Fatal error while processing wrestler link", {
            index: i,
            url,
            attempts,
            msg: String(e?.message || ""),
          });

          if (mode === "tasks" && task_id) {
            try {
              await mark_task_failed({ task_id, error: e?.message || e });
              task_finalized = true;
              p(color_text(`❌ task FAILED (task_id=${task_id})`, "red"));
            } catch (ee) {
              p("⚠️ failed to mark task FAILED:", ee?.message || ee);
            }

            break; // tasks-mode: move to next task
          }

          throw e;
        }
      }

      if (mode === "tasks" && task_id && !task_finalized) {
        try {
          const msg =
            last_error_for_task?.message ||
            String(last_error_for_task || "exhausted retries without a thrown fatal error");
          await mark_task_failed({ task_id, error: `EXHAUSTED_RETRIES: ${msg}` });
          task_finalized = true;
          p(color_text(`❌ task FAILED (exhausted retries) (task_id=${task_id})`, "red"));
        } catch (e) {
          p("⚠️ failed to mark task FAILED after exhausted retries:", e?.message || e);
        }
      }
    }

    // closes CDP connection (not the external Chrome instance)
    await state.browser.close();

    p(
      color_text(
        `\n✅ done. processed ${processed} wrestler pages from DB via ${mode} iterator (wrestler_list / scheduled_events / tasks). csv_write_iterations=${csv_write_iterations}, total_rows_written_csv=${total_rows_written_csv}, total_db_inserted=${total_rows_inserted_db}, total_db_updated=${total_rows_updated_db}, auto_recover_cdp=${auto_recover_cdp_count}, auto_recover_timeouts=${auto_recover_timeout_count}, hard_resets=${hard_reset_count}`,
        "green"
      )
    );
  } finally {
    try {
      watchdog?.stop?.();
    } catch { }
  }
}

export { main as step_3_get_match_history_worker_v4 };
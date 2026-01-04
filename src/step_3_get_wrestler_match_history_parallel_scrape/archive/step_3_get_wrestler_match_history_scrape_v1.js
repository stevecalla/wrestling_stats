  // src/step_3_get_wrestler_match_history.js (ESM, snake_case)
  //
  // ✅ Update in this version:
  // - TWO modes:
  //   A) external session mode: pass in page/browser/context
  //   B) self-managed session mode: if not provided, attach via DevTools port,
  //      login, scrape, then close CDP connection.
  //
  // ✅ Stability + correctness fixes:
  // - Fresh-frame helpers (never reuse stale target_frame)
  // - Dialog handler cannot crash worker during CDP churn
  // - Soft reset (new page in same context) before hard relaunch (Windows stability)
  // - Only attach browser disconnected log once
  // - Only browser.close() if created_session_here=true
  // - Smaller redirect wait (avoid 5s penalty on every loop)

  import net from "net";

  import path from "path";
  import { fileURLToPath } from "url";

  import dotenv from "dotenv";
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  dotenv.config({ path: path.resolve(__dirname, "../.env") });

  // Save files to csv & mysql
  import { save_to_csv_file } from "../../utilities/create_and_load_csv_files/save_to_csv_file.js";
  import {
    upsert_wrestler_match_history,
    delete_wrestler_match_history_for_wrestler,
  } from "../../utilities/mysql/upsert_wrestler_match_history.js";

  import {
    count_rows_in_db_scrape_task,
    iter_name_links_based_on_scrape_task,
    get_task_set_progress,
  } from "../../utilities/mysql/iter_name_links_from_db.js";

  import { step_0_launch_chrome_developer_parallel_scrape } from "./step_0_launch_chrome_developer_parallel_scrape.js";

  import { auto_login_select_season } from "../../utilities/scraper_tasks/auto_login_select_season.js";

  import { color_text } from "../../utilities/console_logs/console_colors.js";

  /* ------------------------------------------
    global handlers (safer)
  -------------------------------------------*/
  process.on("unhandledRejection", (err) => {
    const msg = String(err?.message || "");

    if (
      msg.includes("Page.handleJavaScriptDialog") &&
      (msg.includes("No dialog is showing") ||
        msg.includes("session closed") ||
        msg.includes("Internal server error") ||
        msg.includes("Protocol error") ||
        msg.includes("Target closed"))
    ) {
      console.warn("⚠️ Suppressed Playwright dialog error:", msg);
      return;
    }

    if (
      msg.includes("Frame has been detached") ||
      msg.includes("Frame was detached") ||
      msg.includes("Execution context was destroyed") ||
      msg.includes("Target page, context or browser has been closed") ||
      msg.includes("CDP connection closed") ||
      msg.includes("WebSocket is not open") ||
      msg.includes("session closed") ||
      msg.includes("Session closed")
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

    if (
      msg.includes("Frame has been detached") ||
      msg.includes("Frame was detached") ||
      msg.includes("Execution context was destroyed") ||
      msg.includes("Target page, context or browser has been closed") ||
      msg.includes("CDP connection closed") ||
      msg.includes("WebSocket is not open") ||
      msg.includes("is interrupted by another navigation") ||
      msg.includes("session closed") ||
      msg.includes("Session closed")
    ) {
      console.warn(
        "⚠️ Suppressed Playwright recoverable crash (uncaughtException):",
        msg
      );
      return;
    }

    console.error("❌ Uncaught exception (fatal):", err);
    process.exit(1);
  });

  /* ------------------------------------------
    small helpers
  -------------------------------------------*/
  async function wait_ms(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

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
          await p.close().catch(() => {});
        }
      }
    } catch (err) {
      console.warn("⚠️ close_extra_tabs error (ignored):", err?.message || err);
    }
  }

  function handles_dead({ browser, context, page }) {
    return !browser?.isConnected?.() || !context || !page || page.isClosed?.();
  }

  function is_cdp_disconnect_error(err) {
    const msg = String(err?.message || "");
    return (
      err?.code === "E_TARGET_CLOSED" ||
      msg.includes("Execution context was destroyed") ||
      msg.includes("Target page, context or browser has been closed") ||
      msg.includes("Target closed") ||
      msg.includes("Session closed") ||
      msg.includes("session closed") ||
      msg.includes("has been closed") ||
      msg.includes("CDP connection closed") ||
      msg.includes("WebSocket is not open") ||
      msg.includes("Frame was detached") ||
      msg.includes("Frame has been detached")
    );
  }

  async function attach_disconnected_logger_once(browser) {
    try {
      if (!browser) return;
      if (browser.__has_disconnect_logger) return;
      browser.__has_disconnect_logger = true;
      browser.on?.("disconnected", () =>
        console.warn("⚠️ CDP disconnected — Chrome closed")
      );
    } catch {
      // never crash on logger attach
    }
  }

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
          "⚠️ auto_login_select_season interrupted by navigation/context close; continuing..."
        );
        return;
      }
      throw e;
    }
  }

  async function safe_goto(page, url, opts = {}) {
    const timeout = opts.timeout ?? 30000;

    try {
      await page.goto(url, { waitUntil: "commit", timeout });
      return page.url();
    } catch (err) {
      const msg = String(err?.message || "");

      if (msg.includes("is interrupted by another navigation")) {
        console.warn("⚠️ Ignored navigation interruption, site redirected itself.");
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

  async function relogin(
    page,
    load_timeout_ms,
    wrestling_season,
    track_wrestling_category,
    url_login_page
  ) {
    await safe_goto(page, url_login_page, { timeout: load_timeout_ms });
    await page.waitForTimeout(1000);
    await safe_auto_login(page, wrestling_season, track_wrestling_category);
    await page.waitForTimeout(800);
  }

  /**
   * Wait until DevTools port is open (TCP)
   */
  async function wait_until_port_is_open(port, max_wait_ms = 5000, host = "127.0.0.1") {
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
   * DevTools readiness check (json/version)
   */
  async function wait_until_devtools_ready(port, max_wait_ms = 7000, host = "127.0.0.1") {
    const ok = await wait_until_port_is_open(port, max_wait_ms, host);
    if (!ok) return false;

    if (typeof fetch !== "function") return true;

    const endpoint = `http://${host}:${port}/json/version`;
    const deadline = Date.now() + max_wait_ms;

    while (Date.now() < deadline) {
      try {
        const res = await fetch(endpoint, { cache: "no-store" });
        if (res.ok) {
          const j = await res.json().catch(() => null);
          if (j && j.Browser) return true;
          return true;
        }
      } catch {}
      await wait_ms(200);
    }

    console.warn(`⚠️ DevTools not ready at ${endpoint}`);
    return false;
  }

  /**
   * Soft reset: new tab in the SAME context (do not kill Chrome via browser.close()).
   */
  async function soft_reset_page_and_relogin({
    context,
    page,
    load_timeout_ms,
    wrestling_season,
    track_wrestling_category,
    url_login_page,
  }) {
    try {
      if (context) await close_extra_tabs(context, page);
    } catch {}

    try {
      if (page && !page.isClosed?.()) {
        await page.close().catch(() => {});
      }
    } catch {}

    const new_page = await context.newPage();
    new_page.setDefaultTimeout(load_timeout_ms);
    new_page.setDefaultNavigationTimeout(load_timeout_ms);

    await relogin(
      new_page,
      load_timeout_ms,
      wrestling_season,
      track_wrestling_category,
      url_login_page
    );

    return new_page;
  }

  /**
   * Recovery helper: soft-reset first, hard relaunch only if needed.
   */
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
    if (cause) console.warn(`♻️ ${cause} — recovering...`);
    else console.warn("♻️ recovering...");

    // 1) SOFT path: if CDP still connected and we have a context, rotate tab + relogin
    try {
      if (browser?.isConnected?.() && context) {
        page = await soft_reset_page_and_relogin({
          context,
          page,
          load_timeout_ms,
          wrestling_season,
          track_wrestling_category,
          url_login_page,
        });
        await attach_disconnected_logger_once(browser);
        return { browser, page, context };
      }
    } catch (e) {
      console.warn("⚠️ soft reset failed; will hard relaunch:", e?.message || e);
    }

    // 2) HARD path: relaunch + reconnect via step_0
    await wait_until_devtools_ready(port, 8000).catch(() => false);

    ({ browser, page, context } = await step_0_launch_chrome_developer_parallel_scrape(
      url_home_page,
      port,
      { force_relaunch: true }
    ));

    await attach_disconnected_logger_once(browser);

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

        let m = t.match(
          /^(\d{1,2})[\/\-](\d{1,2})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/
        );
        if (m) {
          const [, m1, d1, m2, d2, y2] = m;
          const start_obj = to_date(y2, m1, d1);
          const end_obj = to_date(y2, m2, d2);
          return { start_date: fmt_mdy(start_obj), end_date: fmt_mdy(end_obj) };
        }

        m = t.match(
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
          const m2 = href.match(/wrestlerId=(\d+)/);
          if (m2 && m2[1] && m2[1] !== wrestler_id) {
            opponent_id = m2[1];
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

  // dialog handler guard
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

    // optional external session
    page,
    browser,
    context,

    file_path,

    task_set_id,
    port,
    worker_id
  ) {
    if (!task_set_id) {
      throw new Error("task_set_id is required (wrestler_match_history_scrape_tasks scope)");
    }
    if (!port) {
      throw new Error("port is required (per-worker Chrome DevTools port)");
    }
    if (!worker_id) {
      throw new Error("worker_id is required (LOCKED task ownership)");
    }

    const load_timeout_ms = 30000;
    const MAX_ATTEMPTS_PER_WRESTLER = 2;

    let created_session_here = false;

    // self-managed attach if not provided
    if (!page || !browser || !context) {
      console.log(
        color_text(
          `🧩 No page/browser/context provided — attaching via DevTools port ${port}`,
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
    }

    await attach_disconnected_logger_once(browser);

    // dialog handler (safe)
    if (page && !dialog_handler_attached) {
      dialog_handler_attached = true;

      page.on("dialog", (dialog) => {
        try {
          console.log(
            color_text(
              `📣 JS dialog detected: "${dialog.message()}" → auto-accepting so navigation can continue`,
              "yellow"
            )
          );
        } catch {}

        dialog.accept().catch((err) => {
          const msg = String(err?.message || "");
          if (
            msg.includes("Page.handleJavaScriptDialog") ||
            msg.includes("session closed") ||
            msg.includes("Internal server error") ||
            msg.includes("Protocol error") ||
            msg.includes("Target closed") ||
            msg.includes("Session closed")
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

    // INITIAL LOGIN
    await relogin(
      page,
      load_timeout_ms,
      wrestling_season,
      track_wrestling_category,
      url_login_page
    );

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
      start_at: 0,
      limit: matches_page_limit,
      batch_size: 500,
      task_set_id,

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

          // ✅ only wait briefly for redirect; don't burn 5s on every loop
          console.log("step 3: wait for redirect (brief)");
          await page
            .waitForURL(/seasons\/index\.jsp/i, { timeout: 800 })
            .catch(() => {});

          if (/seasons\/index\.jsp/i.test(page.url())) {
            console.log(
              "step 3a: on index.jsp, starting auto login for season:",
              wrestling_season
            );

            await safe_auto_login(page, wrestling_season, track_wrestling_category);
            await page.waitForTimeout(800);

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
            await page.waitForTimeout(800);
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
            await tf.waitForLoadState?.("domcontentloaded").catch(() => {});
            await page.waitForTimeout(400);
            return await tf.evaluate(extractor_source());
          });

          // progress
          let prog = null;
          try {
            prog = await get_task_set_progress(task_set_id);
          } catch (e) {
            console.warn("⚠️ get_task_set_progress failed (ignored):", e?.message || e);
          }

          const total = prog?.total_count ?? total_rows_in_db;
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

          // delete existing match history for this wrestler in this season/category
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
  ♻️ Processed ${processed} wrestler pages — recycling session (soft reset preferred).
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
          if (is_cdp_disconnect_error(e) || e?.code === "E_GOTO_TIMEOUT") {
            const is_timeout = e?.code === "E_GOTO_TIMEOUT";
            const cause = is_timeout ? "navigation timeout" : "CDP/target closed";

            if (is_timeout) auto_recover_timeout_count += 1;
            else auto_recover_cdp_count += 1;

            const recover_attempt_no = auto_recover_cdp_count + auto_recover_timeout_count;

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

            await safe_goto(page, effective_url_after_reconnect, {
              timeout: load_timeout_ms,
            });

            continue; // retry this wrestler
          }

          console.error("❌ Fatal error while processing wrestler link", {
            index: i,
            url,
            attempts,
            msg: String(e?.message || ""),
          });

          throw e;
        }
      }
    }

    // Only close CDP connection if we created it here
    if (created_session_here) {
      try {
        await browser.close();
      } catch {}
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

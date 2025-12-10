// src/step_3_get_wrestler_match_history.js (ESM, snake_case)
import net from "net"; // for wait_until_port_is_open function

import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// Save files to csv & msyql
import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file.js";
import {
  upsert_wrestler_match_history,
  // >>> NEW: delete helper for per-wrestler snapshot <<< 
  delete_wrestler_match_history_for_wrestler,
} from "../utilities/mysql/upsert_wrestler_match_history.js";

import {
  count_rows_in_db_wrestler_links,
  iter_name_links_from_db,
  count_name_links_based_on_event_schedule,
  iter_name_links_based_on_event_schedule,
} from "../utilities/mysql/iter_name_links_from_db.js";

import { step_0_launch_chrome_developer } from "./step_0_launch_chrome_developer.js";
import { auto_login_select_season } from "../utilities/scraper_tasks/auto_login_select_season.js";

import { color_text } from "../utilities/console_logs/console_colors.js";
import { step_19_close_chrome_dev } from "./step_19_close_chrome_developer.js";

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

async function relogin(page, load_timeout_ms, wrestling_season, track_wrestling_category, url_login_page) {
  const login_url = url_login_page;
  await safe_goto(page, login_url, { timeout: load_timeout_ms });
  await page.waitForTimeout(1000);
  await page.evaluate(auto_login_select_season, { wrestling_season, track_wrestling_category });
  await page.waitForTimeout(800);
}

async function safe_goto(page, url, opts = {}) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", ...opts });
  } catch (err) {
    const msg = String(err?.message || "");

    if (msg.includes("is interrupted by another navigation")) {
      console.warn("⚠️ Ignored navigation interruption, site redirected itself.");
      await page.waitForLoadState("domcontentloaded").catch(() => {});
    } else if (msg.includes("Target page, context or browser has been closed")) {
      err.code = "E_TARGET_CLOSED"; // sentinel
      throw err;
    } else if (err?.name === "TimeoutError" || msg.includes("Timeout")) {
      // >>> NEW: mark page.goto timeouts so the outer loop can retry
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
    } catch {}
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

  await wait_until_devtools_ready(9222, 8000).catch(() => false);

  ({ browser, page, context } = await step_0_launch_chrome_developer(url_home_page));

  browser.on?.("disconnected", () => {
    console.warn("⚠️ CDP disconnected");
  });

  await relogin(page, load_timeout_ms, wrestling_season, track_wrestling_category, url_login_page);

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
    // === basic helper ===
    const norm = (s) =>
      (s || "")
        .normalize("NFKD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/\s+/g, " ")
        .trim();

    // >>> NEW: stable base normalizer for bout_index <<<
    const base_norm = (s) =>
      (s || "")
        .toLowerCase()
        .replace(/\(fall.*?\)/gi, "")
        .replace(/\(dec.*?\)/gi, "")
        .replace(/\(maj.*?\)/gi, "")
        .replace(/\(sv.*?\)/gi, "")
        .replace(/\(tb.*?\)/gi, "")
        .replace(/\(pin.*?\)/gi, "")
        .replace(/\(tech.*?\)/gi, "")
        .replace(/\d+-\d+/g, "")
        .replace(/[^\w\s]/g, "")
        .replace(/\s+/g, " ")
        .trim();

    // >>> NEW: hash function for bout_index <<<
    const make_hash = (str) => {
      let h = 0;
      for (let i = 0; i < str.length; i++) {
        h = (h * 31 + str.charCodeAt(i)) >>> 0;
      }
      return String(h);
    };

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

    // current wrestler context (from dropdown)
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
      const links = Array.from(details_cell.querySelectorAll('a[href*="wrestlerId="]'));
      for (const a of links) {
        const href = a.getAttribute("href") || "";
        const m = href.match(/wrestlerId=(\d+)/);
        if (m && m[1] && m[1] !== wrestler_id) {
          opponent_id = m[1];
          break;
        }
      }

      // >>> NEW: compute bout_index <<<
      const normalized_base = base_norm(details_text_raw);
      const bout_index = make_hash(normalized_base);

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

        // >>> NEW FIELD <<<
        bout_index,
      });

      match_order += 1;
    }

    return rows;
  };
}

/* ------------------------------------------
   main orchestrator
-------------------------------------------*/
async function main(
  url_home_page,
  url_login_page,
  matches_page_limit = 5,
  loop_start = 0,
  wrestling_season = "2024-25",
  track_wrestling_category = "High School Boys",
  gender,
  sql_where_filter_state_qualifier,
  sql_team_id_list,
  sql_wrestler_id_list,
  page,
  browser,
  context,
  file_path,
  use_scheduled_events_iterator_query = false,
  use_wrestler_list_iterator_query = true
) {
  const load_timeout_ms = 30000;
  const MAX_ATTEMPTS_PER_WRESTLER = 2;

  const mode = (() => {
    if (use_scheduled_events_iterator_query && !use_wrestler_list_iterator_query) return "events";
    if (!use_scheduled_events_iterator_query && use_wrestler_list_iterator_query) return "list";
    console.warn("⚠️ iterator flags ambiguous; defaulting to list");
    return "list";
  })();

  let total_rows_in_db;

  if (mode === "events") {
    total_rows_in_db = await count_name_links_based_on_event_schedule(
      wrestling_season,
      track_wrestling_category
    );
  } else {
    total_rows_in_db = await count_rows_in_db_wrestler_links(
      wrestling_season,
      track_wrestling_category,
      gender,
      sql_where_filter_state_qualifier,
      sql_team_id_list,
      sql_wrestler_id_list
    );
  }

  const no_of_urls = Math.min(matches_page_limit, total_rows_in_db);
  let headers_written = false;

  browser.on?.("disconnected", () =>
    console.warn("⚠️ CDP disconnected — Chrome closed")
  );

  await safe_goto(page, url_login_page, { timeout: load_timeout_ms });
  await page.waitForTimeout(2000);

  console.log("step 1: on index.jsp, auto login for:", wrestling_season);
  await page.evaluate(auto_login_select_season, { wrestling_season, track_wrestling_category });
  await page.waitForTimeout(1000);

  if (mode === "events") {
    console.log(color_text(`📄 DB has ${total_rows_in_db} wrestler links from scheduled events`, "green"));
  } else {
    console.log(color_text(`📄 DB has ${total_rows_in_db} wrestler links`, "green"));
  }

  console.log(color_text(`⚙️ Processing up to ${no_of_urls}`, "green"));

  let processed = 0;

  const iterator =
    mode === "events"
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
          sql_team_id_list,
          sql_wrestler_id_list,
        });

  console.log("find console.log=================");

  for await (const { i, url } of iterator) {
    if (handles_dead({ browser, context, page })) {
      ({ browser, page, context } = await helper_browser_close_restart_relogin(
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
    while (attempts < 2) {
      attempts += 1;
      try {
        const all_rows = [];

        const effective_url = build_wrestler_matches_url(url_home_page, page, url);
        console.log("step 2a: go to url:", effective_url);
        await safe_goto(page, effective_url, { timeout: load_timeout_ms });

        console.log("step 2b: find target frame");
        let target_frame =
          page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) ||
          page.mainFrame();

        console.log("step 3: wait for redirect");
        await page.waitForURL(/seasons\/index\.jsp/i, { timeout: 5000 }).catch(() => {});

        if (/seasons\/index\.jsp/i.test(page.url())) {
          console.log("step 3a: auto login again for:", wrestling_season);

          await page.evaluate(auto_login_select_season, {
            wrestling_season,
            track_wrestling_category,
          });
          await page.waitForTimeout(1000);

          const url2 = build_wrestler_matches_url(url_home_page, page, url);
          console.log("step 3b: re-navigate:", url2);

          await safe_goto(page, url2, { timeout: load_timeout_ms });
          await page.waitForTimeout(1000);

          target_frame =
            page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) ||
            page.mainFrame();
        }

        if (/MainFrame\.jsp/i.test(page.url())) {
          const url2 = build_wrestler_matches_url(url_home_page, page, url);

          await safe_goto(page, url2, { timeout: load_timeout_ms });
          target_frame =
            page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) ||
            page.mainFrame();
        }

        console.log("step 4: wait for dropdown");
        await safe_wait_for_selector(target_frame, "#wrestler", { timeout: load_timeout_ms });

        console.log("step 5: extract rows");
        await target_frame.waitForLoadState?.("domcontentloaded");
        await page.waitForTimeout(1000);

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
            ({ browser, page, context } = await helper_browser_close_restart_relogin(
              browser,
              page,
              context,
              url_home_page,
              load_timeout_ms,
              wrestling_season,
              track_wrestling_category,
              url_login_page,
              "frame died"
            ));

            const url2 = build_wrestler_matches_url(url_home_page, page, url);
            await safe_goto(page, url2, { timeout: load_timeout_ms });

            let tf =
              page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) ||
              page.mainFrame();

            rows = await tf.evaluate(extractor_source());
          } else {
            throw e;
          }
        }

        console.log(
          color_text(
            `✔ ${i} of ${no_of_urls}. rows returned: ${rows.length} from ${url}`,
            "red"
          )
        );

        all_rows.push(...rows);

        // >>> NEW: delete existing match history for this wrestler/season/category
        const this_wrestler_id = rows[0]?.wrestler_id;
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
        const hw = await save_to_csv_file(all_rows, i, headers_written, file_path);
        headers_written = hw;

        console.log("step 7: save to sql db\n");
        try {
          const { inserted, updated } = await upsert_wrestler_match_history(rows, {
            wrestling_season,
            track_wrestling_category,
            gender,
          });
          console.log(
            color_text(
              `🛠️ DB upsert — inserted: ${inserted}, updated: ${updated}`,
              "green"
            )
          );
        } catch (e) {
          console.error("❌ DB upsert failed:", e?.message || e);
        }

        processed += 1;

        const HARD_RESET_LIMIT = 50;
        if (processed % HARD_RESET_LIMIT === 0 && processed < no_of_urls) {
          console.log(
            color_text(
              `=================================
              HARD RESTART AT ${HARD_RESET_LIMIT}
              ♻️ Processed ${processed} pages — recycling browser session
              ===================================`,
              "yellow"
            )
          );
          ({ browser, page, context } = await helper_browser_close_restart_relogin(
            browser,
            page,
            context,
            url_home_page,
            load_timeout_ms,
            wrestling_season,
            track_wrestling_category,
            url_login_page,
            "processed 50 pages"
          ));
        }

        break;
      } catch (e) {
        if (e?.code === "E_TARGET_CLOSED" || e?.code === "E_GOTO_TIMEOUT") {
          const cause = e?.code === "E_GOTO_TIMEOUT" ? "navigation timeout" : "target closed";

          ({ browser, page, context } = await helper_browser_close_restart_relogin(
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

          const url2 = build_wrestler_matches_url(url_home_page, page, url);

          await safe_goto(page, url2, { timeout: load_timeout_ms });

          continue;
        }

        throw e;
      }
    }
  }

  await browser.close();

  console.log(
    `\n✅ done. processed ${processed} wrestler pages via ${mode} iterator`
  );
}

export { main as step_3_get_wrestler_match_history };

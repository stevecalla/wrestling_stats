// src/step_2_get_team_schedule.js (ESM, snake_case)
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// Save files to csv & msyql
import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file.js";
import { upsert_wrestler_match_history } from "../utilities/mysql/upsert_wrestler_match_history.js";
import { count_rows_in_db_wrestler_links, iter_name_links_from_db } from "../utilities/mysql/iter_name_links_from_db.js";

import { step_0_launch_chrome_developer } from "./step_0_launch_chrome_developer.js";
import { auto_login_select_season } from "../utilities/scraper_tasks/auto_login_select_season.js";

import { color_text } from "../utilities/console_logs/console_colors.js";

/* ------------------------------------------
   small helpers
-------------------------------------------*/
function handles_dead({ browser, context, page }) {
  return !browser?.isConnected?.() || !context || !page || page.isClosed?.();
}

async function relogin(page, load_timeout_ms, wrestling_season, url_login_page) {
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
      await page.waitForLoadState("domcontentloaded").catch(() => { });
    } else if (msg.includes("Target page, context or browser has been closed")) {
      err.code = "E_TARGET_CLOSED"; // sentinel
      throw err;
    } else {
      throw err;
    }
  }
  return page.url();
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
      err.code = "E_TARGET_CLOSED"; // sentinel
    }
    throw err;
  }
}

/* ------------------------------------------
   extractor for team schedule (Results.jsp)
   - runs IN THE BROWSER context
-------------------------------------------*/
function schedule_extractor_source() {
  return ({ wrestling_season, track_wrestling_category, page_index, base_span_row_counter }) => {
    function parse_dates(date_raw) {
      if (!date_raw) return { start_date: null, end_date: null };

      const cleaned = date_raw.replace(/\s+/g, " ").trim();
      const hasRange = cleaned.includes("-");

      if (!hasRange) {
        return { start_date: cleaned, end_date: cleaned };
      }

      // example: "12/05 - 12/06/2025"
      let [left, right] = cleaned.split("-").map((s) => s.trim());
      const rightParts = right.split("/");
      const end_year = rightParts[2]; // e.g. "2025"

      if (left.split("/").length === 2) {
        left = `${left}/${end_year}`;
      }

      return { start_date: left, end_date: right };
    }

    const out = [];
    const trNodes = Array.from(
      document.querySelectorAll("table.dataGrid tr.dataGridRow, table.dataGrid tr.dataGridAltRow")
    );

    let spanCounter = base_span_row_counter || 0;

    for (const tr of trNodes) {
      const cells = tr.querySelectorAll("td");
      if (cells.length < 3) continue;

      // ✅ increment ONCE per grid row
      spanCounter += 1;
      const row_index_in_span = spanCounter;

      const date_raw = (cells[1].innerText || "").trim();
      const { start_date, end_date } = parse_dates(date_raw);

      const eventCell = cells[2];
      const link = eventCell.querySelector("a");
      const event_name = (
        (link && link.innerText) ||
        eventCell.innerText ||
        ""
      ).trim();

      const event_js = (link && link.getAttribute("href")) || "";

      if (!date_raw && !event_name) continue;

      const hasAt = event_name.includes("@");

      if (hasAt) {
        const [leftRaw, rightRaw] = event_name.split("@");
        const team_left = (leftRaw || "").trim();
        const team_right = (rightRaw || "").trim();

        const teams = [
          { team_name_raw: team_left,  team_role: "away", team_index: 1 },
          { team_name_raw: team_right, team_role: "home", team_index: 2 },
        ];

        for (const t of teams) {
          out.push({
            wrestling_season,
            track_wrestling_category,
            grid_page_index: page_index,

            date_raw,
            start_date,
            end_date,

            event_name,
            event_js,

            team_name_raw: t.team_name_raw,
            team_role: t.team_role,
            team_index: t.team_index,

            // ✅ this is the per-span row count you want
            row_index_in_span,
          });
        }
      } else {
        out.push({
          wrestling_season,
          track_wrestling_category,
          grid_page_index: page_index,

          date_raw,
          start_date,
          end_date,

          event_name,
          event_js,

          team_name_raw: null,
          team_role: null,
          team_index: null,

          // ✅ single-row event still gets the same label
          row_index_in_span,
        });
      }
    }

    return out;
  };
}

/* ------------------------------------------
   main orchestrator (DB-backed)
-------------------------------------------*/
async function main(
  url_home_page,
  url_login_page,
  matches_page_limit = 5,   // now used as max grid pages per date span
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
  file_path
) {
  const load_timeout_ms = 30000;
  let headers_written = false;
  let processed = 0;               // total pages scraped across all spans
  let global_page_index = 0;       // page counter across spans
  let global_row_counter = 0;      // ✅ total rows scraped across all spans

  browser.on?.("disconnected", () => {
    console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
  });

  // --- login + season selection (existing, working flow) ---
  await safe_goto(page, url_login_page, { timeout: load_timeout_ms });
  await page.waitForTimeout(2000);

  console.log("step 1: on index.jsp, starting auto login for season:", wrestling_season);
  await page.evaluate(auto_login_select_season, { wrestling_season, track_wrestling_category });
  await page.waitForTimeout(1000);

  console.log("step 0: team schedule — waiting for MainFrame / Results to load");
  console.log("step 0: initial top-level URL:", page.url());

  // --- helper: find the frame that actually holds the Results grid ---
  async function find_results_frame(page, timeout_ms) {
    const start = Date.now();

    while (Date.now() - start < timeout_ms) {
      const frames = page.frames();
      const urls = frames.map(f => f.url());
      console.log("step 0a: current frames:", urls);

      // 1) Prefer the real /seasons/Results.jsp content frame
      const candidateFrames = frames.filter(f =>
        /\/seasons\/Results\.jsp/i.test(f.url())
      );

      for (const f of candidateFrames) {
        try {
          const grid = await f.$("table.dataGrid");
          if (grid) {
            console.log("step 0b: found Results.jsp content frame with table.dataGrid");
            return f;
          }
        } catch {
          // ignore and keep looking
        }
      }

      // 2) Fallback: any frame that actually has the data grid
      for (const f of frames) {
        try {
          const grid = await f.$("table.dataGrid");
          if (grid) {
            console.log("step 0c: found frame with table.dataGrid via DOM check");
            return f;
          }
        } catch {
          // ignore
        }
      }

      await page.waitForTimeout(500);
    }

    throw new Error("❌ Unable to find Results frame with table.dataGrid within timeout");
  }

  // --- wait for MainFrame.jsp if it happens, but don't hard-fail if it doesn't ---
  try {
    await page.waitForURL(/MainFrame\.jsp/i, { timeout: load_timeout_ms });
    console.log("step 0d: now on MainFrame.jsp URL:", page.url());
  } catch {
    console.log("step 0d: MainFrame.jsp wait timed out, continuing with URL:", page.url());
  }

  // ---------- helpers for SEARCH modal + date ranges ----------

  function makeDate(y, m, d) {
    return new Date(y, m - 1, d); // JS months 0-based
  }

  const OVERALL_START = makeDate(2025, 11, 1);
  const OVERALL_END = makeDate(2026, 3, 31);
  const SPAN_DAYS = 7;  // inclusive span length

  // NOTE: all of these now operate on the *Results frame*, not the top-level page.
  async function open_search_modal(results_frame) {
    console.log("🔍 opening Search modal (inside Results frame)...");
    await results_frame.click("#searchButton", { timeout: 5000 });
    await results_frame.waitForSelector("#searchFrame", {
      state: "visible",
      timeout: 5000,
    });
  }

  async function set_date_inputs(results_frame, startDate, endDate) {
    const pad2 = (n) => String(n).padStart(2, "0");

    const sMonth = pad2(startDate.getMonth() + 1);
    const sDay = pad2(startDate.getDate());
    const sYear = String(startDate.getFullYear());

    const eMonth = pad2(endDate.getMonth() + 1);
    const eDay = pad2(endDate.getDate());
    const eYear = String(endDate.getFullYear());

    console.log(`📆 setting date range: ${sMonth}/${sDay}/${sYear} → ${eMonth}/${eDay}/${eYear}`);

    await results_frame.fill("#s_startDateMonth", sMonth);
    await results_frame.fill("#s_startDateDay", sDay);
    await results_frame.fill("#s_startDateYear", sYear);

    await results_frame.fill("#s_endDateMonth", eMonth);
    await results_frame.fill("#s_endDateDay", eDay);
    await results_frame.fill("#s_endDateYear", eYear);
  }

  async function submit_search(results_frame) {
    console.log("🔎 submitting search...");
    await results_frame.click('#searchFrame .inputButton input[value="Search"]');
    // Let Results reload; scrape_all_pages_for_current_search will wait for the grid.
    await page.waitForTimeout(1000);
  }

  // ✅ FIXED: only wait for the grid, not specific row classes
  async function wait_for_rows_in_frame(target_frame) {
    console.log("step 1: wait for schedule grid table.dataGrid (rows or 'no results')");
    await safe_wait_for_selector(target_frame, "table.dataGrid", {
      timeout: load_timeout_ms,
    });
  }

  function format_span_label(startDate, endDate) {
    const pad2 = (n) => String(n).padStart(2, "0");
    const fmt = (d) =>
      `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    return `${fmt(startDate)} → ${fmt(endDate)}`;
  }

  // Try to click "Next" in the grid pager.
  // Returns { hasNext: boolean, clicked: boolean, reason: string }
  async function go_to_next_page_if_any(target_frame) {
    return await target_frame.evaluate(() => {
      function findNavContainers() {
        const list = [];
        const top = document.querySelector("#dataGridNextPrev_top .dataGridNextPrev");
        const bottom = document.querySelector("#dataGridNextPrev_bottom .dataGridNextPrev");
        if (top) list.push(top);
        if (bottom) list.push(bottom);

        // fallback: any .dataGridNextPrev on the page
        const generic = Array.from(document.querySelectorAll(".dataGridNextPrev"));
        for (const g of generic) {
          if (!list.includes(g)) list.push(g);
        }

        return list;
      }

      const navs = findNavContainers();
      if (!navs.length) {
        return { hasNext: false, clicked: false, reason: "no_nav_containers" };
      }

      // 1) Try to find a "Next" icon/link that is not disabled
      for (const nav of navs) {
        const anchors = Array.from(nav.querySelectorAll("a"));
        for (const a of anchors) {
          const text = (a.textContent || "").trim().toLowerCase();
          const i = a.querySelector("i");
          const iconCls = (i?.className || "").toLowerCase();
          const aCls = (a.className || "").toLowerCase();
          const html = a.innerHTML.toLowerCase();

          const looksLikeNext =
            text.includes("next") ||
            iconCls.includes("dgnext") ||
            aCls.includes("dgnext") ||
            html.includes("dgnext");

          if (!looksLikeNext) continue;

          const isDisabled =
            iconCls.includes("disabled") ||
            aCls.includes("disabled") ||
            html.includes("dgnext_disabled");

          if (isDisabled) {
            continue;
          }

          // ✅ Found an enabled "Next"
          a.click();
          return { hasNext: true, clicked: true, reason: "next_icon_clicked" };
        }
      }

      // 2) Fallback: numeric page links, e.g. "1 2 3"
      for (const nav of navs) {
        const pageLinks = Array.from(nav.querySelectorAll("a"))
          .filter(a => /^\d+$/.test((a.textContent || "").trim()));

        if (!pageLinks.length) continue;

        // Try to detect current page
        const current =
          pageLinks.find(a => (a.className || "").toLowerCase().includes("current")) ||
          pageLinks.find(a => (a.parentElement?.className || "").toLowerCase().includes("current"));

        let currentIndex = -1;

        if (current) {
          currentIndex = pageLinks.indexOf(current);
        } else {
          // If we can't detect, assume first link is current (page 1)
          currentIndex = 0;
        }

        if (currentIndex < 0 || currentIndex >= pageLinks.length - 1) {
          continue; // already on last page
        }

        const nextPageLink = pageLinks[currentIndex + 1];
        if (!nextPageLink) continue;

        nextPageLink.click();
        return { hasNext: true, clicked: true, reason: "numeric_page_clicked" };
      }

      // No usable next found
      return { hasNext: false, clicked: false, reason: "no_next_found" };
    });
  }

  // ---------- pagination for a *single* date-span search ----------
  // ✅ FIXED: accept the frame we already know is correct
  async function scrape_all_pages_for_current_search(date_span_label, target_frame) {
    console.log("step 1: using target_frame url:", target_frame.url());

    let local_page_index = 0;
    let rows_seen_in_span = 0;  // count of output rows in this span
    const seenSignatures = new Set();  // ✅ all signatures for this span

    while (local_page_index < matches_page_limit) {
      console.log(`\n========== grid page ${local_page_index + 1} (${date_span_label}) ==========`);

      await wait_for_rows_in_frame(target_frame);
      await target_frame.waitForLoadState?.("domcontentloaded");
      await page.waitForTimeout(500);

      const rows = await target_frame.evaluate(
        schedule_extractor_source(),
        {
          wrestling_season,
          track_wrestling_category,
          page_index: global_page_index,
          base_span_row_counter: rows_seen_in_span,  // feeds row_index_in_span
        }
      );

      console.log(
        color_text(
          `✔ schedule rows returned: ${rows.length} from Results.jsp`,
          "green"
        )
      );

      if (!rows.length) {
        console.log("⚠️ No rows returned for this grid page, stopping pagination for this span.");
        break;
      }

      // --- Build a signature that ignores dynamic fields so we can detect cycles ---
      const signature = JSON.stringify(
        rows.map(r => {
          const {
            grid_page_index,
            row_index_in_span,
            row_index_global,
            search_span_label,
            ...rest
          } = r;
          return rest;
        })
      );

      if (seenSignatures.has(signature)) {
        console.log(
          "⚠️ This page's rows match a previously-seen page in this span; " +
          "assuming we are cycling pages. Stopping pagination for this span."
        );
        break;
      }
      seenSignatures.add(signature);

      // --- Enrich rows with audit fields before saving ---
      const enrichedRows = rows.map((r, idx) => {
        const row_index_global = global_row_counter + idx + 1;

        return {
          ...r,
          search_span_label: date_span_label, // e.g. "2025-11-29 → 2025-12-05"
          row_index_global,
        };
      });

      rows_seen_in_span += rows.length;
      global_row_counter += rows.length;

      console.log("step 3: save schedule rows to csv");
      const headers_written_now = await save_to_csv_file(
        enrichedRows,
        global_page_index,
        headers_written,
        file_path
      );
      headers_written = headers_written_now;
      console.log(`\x1b[33m➕ tracking headers_written: ${headers_written}\x1b[0m\n`);

      processed += 1;
      global_page_index += 1;
      local_page_index += 1;

      // --- Try to move to the next grid page ---
      const navResult = await go_to_next_page_if_any(target_frame);

      if (!navResult.hasNext || !navResult.clicked) {
        console.log(
          `⛔ No NEXT page available (reason: ${navResult.reason}); finished pagination for this span.`
        );
        break;
      }

      console.log(`➡️ Clicked NEXT (reason: ${navResult.reason}), waiting for grid to reload...`);
      await page.waitForTimeout(1000);
    }
  }

  // ---------- MAIN DATE-SPAN LOOP ----------

  console.log(`📆 Running date-span loop from 2025-11-01 to 2026-03-31 in ${SPAN_DAYS}-day chunks`);

  let spanIndex = 0;
  for (
    let cursor = new Date(OVERALL_START.getTime());
    cursor <= OVERALL_END;
    cursor.setDate(cursor.getDate() + SPAN_DAYS)
  ) {
    spanIndex += 1;

    const spanStart = new Date(cursor.getTime());
    const spanEnd = new Date(cursor.getTime());
    spanEnd.setDate(spanEnd.getDate() + SPAN_DAYS - 1);
    if (spanEnd > OVERALL_END) spanEnd.setTime(OVERALL_END.getTime());

    const spanLabel = format_span_label(spanStart, spanEnd);
    console.log(`\n==================== DATE SPAN #${spanIndex}: ${spanLabel} ====================`);

    // Get the Results frame first, then operate inside it
    const results_frame_for_search = await find_results_frame(page, load_timeout_ms);

    try {
      await open_search_modal(results_frame_for_search);
      await set_date_inputs(results_frame_for_search, spanStart, spanEnd);
      await submit_search(results_frame_for_search);

      // scrape all pages for this search using the SAME frame
      await scrape_all_pages_for_current_search(spanLabel, results_frame_for_search);
    } catch (err) {
      console.error(`❌ Error scraping span ${spanLabel}:`, err);
      // continue to next span
    }
  }

  // await browser.close();
  console.log(
    `\n✅ done. processed ${processed} Results.jsp page(s) into CSV: ${file_path}`
  );
}

export { main as step_2_get_team_schedule };




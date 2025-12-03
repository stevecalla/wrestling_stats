// src/step_2_get_team_schedule.js (ESM, snake_case)
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// Save files to csv & mysql
import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file.js";
import { upsert_team_schedule } from "../utilities/mysql/upsert_team_scheudule_data.js";

import { auto_login_select_season } from "../utilities/scraper_tasks/auto_login_select_season.js";

import { color_text } from "../utilities/console_logs/console_colors.js";

/* ------------------------------------------
   small helpers
-------------------------------------------*/
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
    } else {
      throw err;
    }
  }
  return page.url();
}

async function safe_wait_for_selector(frame_or_page, selector, opts = {}) {
  try {
    await frame_or_page.waitForSelector(selector, {
      state: "visible",
      ...opts,
    });
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
   Event Team modal helpers (multi-team popup)
-------------------------------------------*/

async function close_event_team_modal_if_open(target_frame, page) {
  try {
    await target_frame
      .evaluate(() => {
        const modal = document.querySelector("#eventTeamFrame");
        if (!modal) return;

        // Explicit close icon: <i class="icon-close" onclick="hideModal()">
        const close_icon = modal.querySelector("i.icon-close");

        if (close_icon && close_icon instanceof HTMLElement) {
          close_icon.click();
        } else {
          // Fallback: force hide
          modal.classList.remove("active");
          if (modal instanceof HTMLElement) {
            modal.style.display = "none";
          }
        }
      })
      .catch(() => {});

    await page.waitForTimeout(150).catch(() => {});
  } catch {
    // Last resort: ESC
    try {
      await page.keyboard.press("Escape");
      await page.waitForTimeout(150).catch(() => {});
    } catch {
      // ignore
    }
  }
}

// Click javascript:openEvent(...) link in the Results frame,
// open the Event Team Select modal, and scrape each team link.
async function get_teams_from_multi_team_event(
  target_frame,
  page,
  event_name,
  event_js
) {
  const modal_selector = "#eventTeamFrame";

  // Make sure no stale modal is open first
  await close_event_team_modal_if_open(target_frame, page);
  await page.waitForTimeout(200); // small buffer after closing old modal

  // Try to match by exact href first; fallback to text filter
  let event_link_locator = target_frame.locator(`a[href="${event_js}"]`);
  let link_count = await event_link_locator.count();

  if (!link_count) {
    event_link_locator = target_frame
      .locator('a[href^="javascript:openEvent"]')
      .filter({ hasText: event_name });
    link_count = await event_link_locator.count();
  }

  if (!link_count) {
    console.log(
      color_text(
        `⚠️ no clickable link found for multi-team event: "${event_name}" (${event_js})`,
        "yellow"
      )
    );
    return [];
  }

  try {
    await Promise.all([
      target_frame
        .waitForSelector(modal_selector, {
          state: "visible",
          timeout: 5000,
        })
        .catch(() => null),
      event_link_locator.first().click(),
    ]);

    const modal_locator = target_frame.locator(modal_selector);

    if (!(await modal_locator.isVisible())) {
      console.log(
        color_text(
          `⚠️ event team modal did not become visible for "${event_name}"`,
          "yellow"
        )
      );
      return [];
    }

    // give the modal a moment to fully render its contents
    await page.waitForTimeout(400);

    // search anywhere inside modal for team links by `teamId=`
    const teams_scope_locator = modal_locator;
    try {
      await teams_scope_locator.waitForSelector(
        'a[target="_blank"][href*="teamId="]',
        { timeout: 4000 }
      );
    } catch {
      // no links found in time — might really be a no-team / weird popup
    }

    const team_links_locator = teams_scope_locator.locator(
      'a[target="_blank"][href*="teamId="]'
    );

    const team_link_count = await team_links_locator.count();
    const teams = [];

    for (let i = 0; i < team_link_count; i++) {
      const link = team_links_locator.nth(i);

      const team_name_raw = (await link.innerText()).trim();
      const href = (await link.getAttribute("href")) || "";

      const match = href.match(/teamId=(\d+)/i);
      const team_id = match ? match[1] : null;

      teams.push({
        team_name_raw,
        team_id,
      });
    }

    console.log(
      color_text(
        `✅ multi-team popup: "${event_name}" → ${teams.length} team(s)`,
        "green"
      )
    );

    // Close modal after scraping
    await close_event_team_modal_if_open(target_frame, page);
    await page.waitForTimeout(150).catch(() => {});

    return teams;
  } catch (err) {
    console.error(
      `⚠️ error while loading multi-team popup for event "${event_name}" (${event_js}):`,
      err
    );
    await close_event_team_modal_if_open(target_frame, page);
    await page.waitForTimeout(150).catch(() => {});
    return [];
  }
}

// Expand rows that have NO '@' and a javascript:openEvent(...) link
// by clicking the popup and creating one row per team.
async function expand_multi_team_rows(rows, target_frame, page) {
  const expanded_rows = [];
  const popup_cache_by_event_js = {};

  for (const row of rows) {
    const event_name = row.event_name || "";
    const event_js = row.event_js || "";

    const has_at = event_name.includes("@");
    const is_clickable_js =
      event_js.startsWith("javascript:") && event_js.includes("openEvent");

    // Already a dual meet or not clickable → keep as-is
    if (has_at || !is_clickable_js) {
      expanded_rows.push(row);
      continue;
    }

    if (!popup_cache_by_event_js[event_js]) {
      popup_cache_by_event_js[event_js] = await get_teams_from_multi_team_event(
        target_frame,
        page,
        event_name,
        event_js
      );
    }

    const teams = popup_cache_by_event_js[event_js] || [];

    if (!teams.length) {
      // Popup didn't yield anything; keep original row
      expanded_rows.push(row);
      continue;
    }

    teams.forEach((t, index) => {
      expanded_rows.push({
        ...row,
        team_name_raw: t.team_name_raw,
        team_id: t.team_id,
        team_role: row.team_role ?? null,
        team_index: index + 1,
      });
    });
  }

  return expanded_rows;
}

/* ------------------------------------------
   extractor for team schedule (Results.jsp)
   - runs IN THE BROWSER context
-------------------------------------------*/
function schedule_extractor_source() {
  return ({
    wrestling_season,
    track_wrestling_category,
    page_index,
    base_span_row_counter,
  }) => {
    function parse_dates(date_raw) {
      if (!date_raw) return { start_date: null, end_date: null };

      const cleaned = date_raw.replace(/\s+/g, " ").trim();
      const has_range = cleaned.includes("-");

      if (!has_range) {
        return { start_date: cleaned, end_date: cleaned };
      }

      // example: "12/05 - 12/06/2025"
      let [left, right] = cleaned.split("-").map((s) => s.trim());
      const right_parts = right.split("/");
      const end_year = right_parts[2]; // e.g. "2025"

      if (left.split("/").length === 2) {
        left = `${left}/${end_year}`;
      }

      return { start_date: left, end_date: right };
    }

    const out = [];
    const tr_nodes = Array.from(
      document.querySelectorAll(
        "table.dataGrid tr.dataGridRow, table.dataGrid tr.dataGridAltRow"
      )
    );

    let span_counter = base_span_row_counter || 0;

    for (const tr of tr_nodes) {
      const cells = tr.querySelectorAll("td");
      if (cells.length < 3) continue;

      // increment ONCE per grid row in this span
      span_counter += 1;
      const row_index_in_span = span_counter;

      const date_raw = (cells[1].innerText || "").trim();
      const { start_date, end_date } = parse_dates(date_raw);

      const event_cell = cells[2];
      const link = event_cell.querySelector("a");
      const event_name =
        ((link && link.innerText) || event_cell.innerText || "").trim();

      const event_js = (link && link.getAttribute("href")) || "";

      if (!date_raw && !event_name) continue;

      const has_at = event_name.includes("@");

      if (has_at) {
        const split = event_name.split("@");
        const left_raw = split[0] || "";
        const right_raw = split[1] || "";
        const team_left = left_raw.trim();
        const team_right = right_raw.trim();

        const teams = [
          { team_name_raw: team_left, team_role: "away", team_index: 1 },
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
            team_id: null, // filled only for multi-team

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
          team_id: null,

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
  matches_page_limit = 5, // max grid pages per date span
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

  // CSV: overwrite if exists, always start fresh
  let headers_written = false;
  try {
    if (fs.existsSync(file_path)) {
      fs.unlinkSync(file_path);
      console.log(
        color_text(
          `🧹 existing CSV deleted at ${file_path} → starting fresh`,
          "yellow"
        )
      );
    } else {
      console.log(
        color_text(`📄 creating new CSV at ${file_path}`, "yellow")
      );
    }
  } catch (e) {
    console.log(
      color_text(
        `⚠️ could not reset CSV at ${file_path}: ${e.message}`,
        "yellow"
      )
    );
  }

  let processed = 0; // total pages scraped
  let global_page_index = 0; // page counter
  let global_row_counter = 0; // total expanded rows written

  browser.on?.("disconnected", () => {
    console.warn(
      "⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep)."
    );
  });

  // --- login + season selection ---
  await safe_goto(page, url_login_page, { timeout: load_timeout_ms });
  await page.waitForTimeout(2000);

  console.log(
    "step 1: on index.jsp, starting auto login for season:",
    wrestling_season
  );
  await page.evaluate(auto_login_select_season, {
    wrestling_season,
    track_wrestling_category,
  });
  await page.waitForTimeout(1000);

  console.log("step 0: team schedule — waiting for MainFrame / Results to load");
  console.log("step 0: initial top-level URL:", page.url());

  // --- helper: find the frame that actually holds the Results grid ---
  async function find_results_frame(page, timeout_ms) {
    const start = Date.now();

    while (Date.now() - start < timeout_ms) {
      const frames = page.frames();
      const urls = frames.map((f) => f.url());
      console.log("step 0a: current frames:", urls);

      // Prefer the real /seasons/Results.jsp content frame
      const candidate_frames = frames.filter((f) =>
        /\/seasons\/Results\.jsp/i.test(f.url())
      );

      for (const f of candidate_frames) {
        try {
          const grid = await f.$("table.dataGrid");
          if (grid) {
            console.log(
              "step 0b: found Results.jsp content frame with table.dataGrid"
            );
            return f;
          }
        } catch {
          // ignore and keep looking
        }
      }

      // Fallback: any frame that actually has the data grid
      for (const f of frames) {
        try {
          const grid = await f.$("table.dataGrid");
          if (grid) {
            console.log(
              "step 0c: found frame with table.dataGrid via DOM check"
            );
            return f;
          }
        } catch {
          // ignore
        }
      }

      await page.waitForTimeout(500);
    }

    throw new Error(
      "❌ Unable to find Results frame with table.dataGrid within timeout"
    );
  }

  // (Optional) wait for MainFrame.jsp
  try {
    await page.waitForURL(/MainFrame\.jsp/i, { timeout: load_timeout_ms });
    console.log("step 0d: now on MainFrame.jsp URL:", page.url());
  } catch {
    console.log(
      "step 0d: MainFrame.jsp wait timed out, continuing with URL:",
      page.url()
    );
  }

  // ---------- helpers for SEARCH modal + date ranges ----------

  function format_span_label(start_date, end_date) {
    const pad2 = (n) => String(n).padStart(2, "0");
    const fmt = (d) =>
      `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    return `${fmt(start_date)} → ${fmt(end_date)}`;
  }

  // Read "1 - 50 of 109" from the pager and parse totals
  async function get_pager_info(target_frame) {
    return await target_frame.evaluate(() => {
      const container = document.querySelector(
        "#dataGridNextPrev_top .dataGridNextPrev"
      );
      if (!container) return null;

      const spans = container.querySelectorAll("span");
      if (spans.length < 2) return null;

      const text = (spans[1].textContent || "").trim(); // e.g. "1 - 50 of 109"
      const m = text.match(/(\d+)\s*-\s*(\d+)\s*of\s*(\d+)/i);
      if (!m) return null;

      return {
        start: parseInt(m[1], 10),
        end: parseInt(m[2], 10),
        total: parseInt(m[3], 10),
        raw: text,
      };
    });
  }

  // Rolling date window: last 7 days and next 5 days from "today"
  function get_rolling_date_range() {
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const start_date = new Date(today);
    start_date.setDate(start_date.getDate() - 7);

    const end_date = new Date(today);
    end_date.setDate(end_date.getDate() + 5);

    return { start_date, end_date };
  }

  async function open_search_modal(results_frame, page) {
    console.log("🔍 opening Search modal (inside Results frame)...");

    // Close any leftover Event Team modal first
    await close_event_team_modal_if_open(results_frame, page);

    await results_frame.click("#searchButton", { timeout: 5000 });
    await results_frame.waitForSelector("#searchFrame", {
      state: "visible",
      timeout: 5000,
    });
  }

  async function set_date_inputs(results_frame, start_date, end_date) {
    const pad2 = (n) => String(n).padStart(2, "0");

    const s_month = pad2(start_date.getMonth() + 1);
    const s_day = pad2(start_date.getDate());
    const s_year = String(start_date.getFullYear());

    const e_month = pad2(end_date.getMonth() + 1);
    const e_day = pad2(end_date.getDate());
    const e_year = String(end_date.getFullYear());

    console.log(
      `📆 setting date range: ${s_month}/${s_day}/${s_year} → ${e_month}/${e_day}/${e_year}`
    );

    await results_frame.fill("#s_startDateMonth", s_month);
    await results_frame.fill("#s_startDateDay", s_day);
    await results_frame.fill("#s_startDateYear", s_year);

    await results_frame.fill("#s_endDateMonth", e_month);
    await results_frame.fill("#s_endDateDay", e_day);
    await results_frame.fill("#s_endDateYear", e_year);
  }

  async function submit_search(results_frame) {
    console.log("🔎 submitting search...");
    await results_frame.click(
      '#searchFrame .inputButton input[value="Search"]'
    );
    await page.waitForTimeout(1000);
  }

  async function wait_for_rows_in_frame(target_frame) {
    console.log(
      "step 1: wait for schedule grid table.dataGrid (rows or 'no results')"
    );
    await safe_wait_for_selector(target_frame, "table.dataGrid", {
      timeout: load_timeout_ms,
    });
  }

  // Try to click "Next" in the grid pager.
  async function go_to_next_page_if_any(target_frame) {
    return await target_frame.evaluate(() => {
      function find_nav_containers() {
        const list = [];
        const top = document.querySelector(
          "#dataGridNextPrev_top .dataGridNextPrev"
        );
        const bottom = document.querySelector(
          "#dataGridNextPrev_bottom .dataGridNextPrev"
        );
        if (top) list.push(top);
        if (bottom) list.push(bottom);

        const generic = Array.from(
          document.querySelectorAll(".dataGridNextPrev")
        );
        for (const g of generic) {
          if (!list.includes(g)) list.push(g);
        }

        return list;
      }

      const navs = find_nav_containers();
      if (!navs.length) {
        return { hasNext: false, clicked: false, reason: "no_nav_containers" };
      }

      // 1) Try a "Next" icon/link
      for (const nav of navs) {
        const anchors = Array.from(nav.querySelectorAll("a"));
        for (const a of anchors) {
          const text = (a.textContent || "").trim().toLowerCase();
          const i = a.querySelector("i");
          const icon_cls = (i?.className || "").toLowerCase();
          const a_cls = (a.className || "").toLowerCase();
          const html = a.innerHTML.toLowerCase();

          const looks_like_next =
            text.includes("next") ||
            icon_cls.includes("dgnext") ||
            a_cls.includes("dgnext") ||
            html.includes("dgnext");

          if (!looks_like_next) continue;

          const is_disabled =
            icon_cls.includes("disabled") ||
            a_cls.includes("disabled") ||
            html.includes("dgnext_disabled");

          if (is_disabled) {
            continue;
          }

          a.click();
          return { hasNext: true, clicked: true, reason: "next_icon_clicked" };
        }
      }

      // 2) Fallback: numeric page links
      for (const nav of navs) {
        const page_links = Array.from(nav.querySelectorAll("a")).filter((a) =>
          /^\d+$/.test((a.textContent || "").trim())
        );

        if (!page_links.length) continue;

        const current =
          page_links.find((a) =>
            (a.className || "").toLowerCase().includes("current")
          ) ||
          page_links.find((a) =>
            (a.parentElement?.className || "")
              .toLowerCase()
              .includes("current")
          );

        let current_index = -1;

        if (current) {
          current_index = page_links.indexOf(current);
        } else {
          current_index = 0;
        }

        if (current_index < 0 || current_index >= page_links.length - 1) {
          continue;
        }

        const next_page_link = page_links[current_index + 1];
        if (!next_page_link) continue;

        next_page_link.click();
        return {
          hasNext: true,
          clicked: true,
          reason: "numeric_page_clicked",
        };
      }

      return { hasNext: false, clicked: false, reason: "no_next_found" };
    });
  }

  // ---------- pagination for the rolling date-span search ----------
  async function scrape_all_pages_for_current_search(
    date_span_label,
    target_frame
  ) {
    console.log("step 1: using target_frame url:", target_frame.url());

    let local_page_index = 0;
    let written_rows_seen_in_span = 0; // expanded rows written to CSV

    // Track pager ranges we've already seen (to avoid re-scraping same page)
    const seen_page_ranges = new Set();

    while (local_page_index < matches_page_limit) {
      console.log(
        `\n========== grid page ${
          local_page_index + 1
        } (${date_span_label}) ==========`
      );

      await wait_for_rows_in_frame(target_frame);
      await target_frame.waitForLoadState?.("domcontentloaded");
      await page.waitForTimeout(500);

      const pager_info = await get_pager_info(target_frame);

      let base_span_row_counter = 0;

      if (pager_info) {
        console.log(
          color_text(
            `🔢 pager text: "${pager_info.raw}" (total=${pager_info.total})`,
            "cyan"
          )
        );

        const rangeKey = `${pager_info.start}-${pager_info.end}-${pager_info.total}`;
        if (seen_page_ranges.has(rangeKey)) {
          console.log(
            color_text(
              `⚠️ pager range ${rangeKey} already seen; assuming page cycle, stopping pagination for this span.`,
              "yellow"
            )
          );
          break;
        }
        seen_page_ranges.add(rangeKey);

        console.log(
          color_text(
            `📝 page showing ${pager_info.start}–${pager_info.end} of ${pager_info.total} total rows`,
            "magenta"
          )
        );

        // base row index in span comes from pager "start"
        base_span_row_counter = pager_info.start - 1;
      } else {
        console.log(
          color_text("🔢 pager text not found (continuing anyway)", "yellow")
        );
        base_span_row_counter = 0;
      }

      // Use pager-based base_span_row_counter so row_index_in_span lines up with "row in span"
      const raw_rows = await target_frame.evaluate(
        schedule_extractor_source(),
        {
          wrestling_season,
          track_wrestling_category,
          page_index: global_page_index,
          base_span_row_counter,
        }
      );

      console.log(
        color_text(
          `✔ schedule rows returned (raw): ${raw_rows.length} from Results.jsp`,
          "green"
        )
      );

      if (raw_rows.length) {
        console.log(
          "   sample events:",
          raw_rows
            .slice(0, 3)
            .map((r) => `${r.date_raw} | ${r.event_name} | ${r.event_js}`)
        );
      }

      if (!raw_rows.length) {
        console.log(
          "⚠️ No rows returned for this grid page, stopping pagination for this span."
        );
        break;
      }

      // Expand multi-team events (no '@' + javascript:openEvent)
      const rows = await expand_multi_team_rows(
        raw_rows,
        target_frame,
        page
      );

      if (rows.length !== raw_rows.length) {
        console.log(
          color_text(
            `🔁 expanded multi-team events: ${raw_rows.length} → ${rows.length} rows`,
            "yellow"
          )
        );
      }

      // Enrich rows with audit fields before saving.
      const enriched_rows = rows.map((r, idx) => {
        const row_index_global = global_row_counter + idx + 1;

        return {
          ...r,
          search_span_label: date_span_label,
          row_index_global,
        };
      });

      written_rows_seen_in_span += rows.length;
      global_row_counter += rows.length;

      console.log("step 3: save schedule rows to csv");
      const headers_written_now = await save_to_csv_file(
        enriched_rows,
        global_page_index,
        headers_written,
        file_path
      );
      headers_written = headers_written_now;
      console.log(
        `\x1b[33m➕ tracking headers_written: ${headers_written}\x1b[0m`
      );
      console.log(
        color_text(
          `📊 written rows so far this span (expanded): ${written_rows_seen_in_span}`,
          "cyan"
        )
      );

      // 🆕 DB upsert, consistent with step_3 pattern
      console.log("step 7: save to sql db\n");
      try {
        const { inserted, updated } = await upsert_team_schedule(
          enriched_rows,
          { wrestling_season, track_wrestling_category, gender }
        );
        console.log(
          color_text(
            `🛠️ DB upsert (team schedule) — inserted: ${inserted}, updated: ${updated}`,
            "green"
          )
        );
      } catch (e) {
        console.error(
          "❌ DB upsert (team schedule) failed:",
          e?.message || e
        );
      }

      processed += 1;
      global_page_index += 1;
      local_page_index += 1;

      // Stop condition: when pager says we've reached the end of the span
      if (pager_info && pager_info.end >= pager_info.total) {
        console.log(
          color_text(
            `✅ pager end (${pager_info.end}) >= pager total (${pager_info.total}); stopping pagination for this span.`,
            "green"
          )
        );
        break;
      }

      const nav_result = await go_to_next_page_if_any(target_frame);

      if (!nav_result.hasNext || !nav_result.clicked) {
        console.log(
          `⛔ No NEXT page available (reason: ${nav_result.reason}); finished pagination for this span.`
        );
        break;
      }

      console.log(
        `➡️ Clicked NEXT (reason: ${nav_result.reason}), waiting for grid to reload...`
      );
      await page.waitForTimeout(1000);
    }

    console.log(
      color_text(
        `📦 span summary "${date_span_label}": expanded rows written=${written_rows_seen_in_span}`,
        "magenta"
      )
    );
  }

  // ---------- SINGLE ROLLING DATE-SPAN EXECUTION ----------
  const { start_date, end_date } = get_rolling_date_range();
  const span_label = format_span_label(start_date, end_date);

  console.log(
    `\n📆 Running rolling window for last 7 and next 5 days: ${span_label}`
  );

  const results_frame_for_search = await find_results_frame(
    page,
    load_timeout_ms
  );

  try {
    await open_search_modal(results_frame_for_search, page);
    await set_date_inputs(results_frame_for_search, start_date, end_date);
    await submit_search(results_frame_for_search);

    await scrape_all_pages_for_current_search(
      span_label,
      results_frame_for_search
    );
  } catch (err) {
    console.error(`❌ Error scraping rolling span ${span_label}:`, err);
  }

  // await browser.close();
  console.log(
    `\n✅ done. processed ${processed} Results.jsp page(s) into CSV: ${file_path}`
  );
}

export { main as step_2_get_team_schedule };

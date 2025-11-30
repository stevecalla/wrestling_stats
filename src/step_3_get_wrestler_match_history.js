// src/step_3_get_wrestler_match_history.js (ESM, snake_case)
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
   extractor_source runs in the page (frame) context
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

      // B: MM/DD/YYYY - MM/DD/YYYY
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
    let match_order = 1;   // 👈 per wrestler-page order

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
        match_order,         // 👈 store the order
        opponent_id,
        raw_details: details_text_raw,
      });

      match_order += 1;      // 👈 increment for next row
    }

    return rows;
  };
}


/* ------------------------------------------
   main orchestrator (DB-backed)
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
  file_path
) {
  const load_timeout_ms = 30000;

  // DB: count + cap (memory-efficient streaming)
  const total_rows_in_db = await count_rows_in_db_wrestler_links( 
    wrestling_season, 
    track_wrestling_category,
    gender, 
    sql_where_filter_state_qualifier, 
    sql_team_id_list, 
    sql_wrestler_id_list
  );
  const no_of_urls = Math.min(matches_page_limit, total_rows_in_db);

  let headers_written = false;

  browser.on?.("disconnected", () => {
    console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
  });

  // initial login
  await safe_goto(page, url_login_page, { timeout: load_timeout_ms });
  await page.waitForTimeout(2000);

  console.log("step 1: on index.jsp, starting auto login for season:", wrestling_season);
  await page.evaluate(auto_login_select_season, { wrestling_season, track_wrestling_category });
  await page.waitForTimeout(1000);

  console.log(color_text(`📄 DB has ${total_rows_in_db} wrestler links`, "green"));
  console.log(
    color_text(
      `\x1b[33m⚙️ Processing up to ${no_of_urls} (min of page limit vs DB size)\x1b[0m\n`,
      "green"
    )
  );

  let processed = 0;

  // const test_link = [{ // Boyd Thomas (Roman)
  //   i: 0,
  //   url: "https://www.trackwrestling.com/seasons/WrestlerMatches.jsp?TIM=1762492149718&twSessionId=fmuycnbgon&wrestlerId=30579778132"
  // }];
  // const test_link = [{ // Colt Jones
  //   i: 0,
  //   url: "https://www.trackwrestling.com/seasons/WrestlerMatches.jsp?TIM=1762492149718&twSessionId=fmuycnbgon&wrestlerId=30253039132"
  // }];

  // for (const { i, url } of test_link) {
  for await (const { i, url } of iter_name_links_from_db({
    start_at: loop_start,
    limit: matches_page_limit,
    batch_size: 500,
    wrestling_season,
    track_wrestling_category,
    gender,
    sql_where_filter_state_qualifier,
    sql_team_id_list,
    sql_wrestler_id_list,
  })) {
    if (handles_dead({ browser, context, page })) {
      console.warn("♻️ handles_dead — reconnecting via step_0_launch_chrome_developer...");
      ({ browser, page, context } = await step_0_launch_chrome_developer(url_home_page));
      browser.on?.("disconnected", () => {
        console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
      });
      await relogin(page, load_timeout_ms, wrestling_season, url_login_page);
    }

    let attempts = 0;
    while (attempts < 2) {
      attempts += 1;
      try {
        const all_rows = [];

        console.log("step 2a: go to url:", url);
        await safe_goto(page, url, { timeout: load_timeout_ms });

        console.log("step 2b: find target frame");
        let target_frame =
          page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();

        console.log("step 3: wait to see if redirected to seasons/index.jsp");
        await page.waitForURL(/seasons\/index\.jsp/i, { timeout: 5000 }).catch(() => { });

        if (/seasons\/index\.jsp/i.test(page.url())) {
          console.log("step 3a: on index.jsp, starting auto login for season:", wrestling_season);
          await page.evaluate(auto_login_select_season, { wrestling_season, track_wrestling_category });
          await page.waitForTimeout(1000);
          console.log("step 3b: re-navigating to original URL after login:", url);
          await safe_goto(page, url, { timeout: load_timeout_ms });
          await page.waitForTimeout(1000);
          target_frame =
            page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
        }

        if (/MainFrame\.jsp/i.test(page.url())) {
          await safe_goto(page, url, { timeout: load_timeout_ms });
          target_frame =
            page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
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
            console.warn("♻️ frame died during evaluate — reconnecting and retrying once...");
            ({ browser, page, context } = await step_0_launch_chrome_developer(url_home_page));
            browser.on?.("disconnected", () =>
              console.warn("⚠️ CDP disconnected — Chrome was closed.")
            );
            await relogin(page, load_timeout_ms, wrestling_season, url_login_page);
            await safe_goto(page, url, { timeout: load_timeout_ms });
            let tf =
              page.frames().find((f) => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
            rows = await tf.evaluate(extractor_source());
          } else {
            throw e;
          }
        }

        console.log(
          color_text(
            `✔ ${i} of ${no_of_urls}. rows returned: ${rows.length} rows from: ${url}`,
            "red"
          )
        );
        all_rows.push(...rows);

        console.log("step 6: save to csv");
        const headers_written_now = await save_to_csv_file(all_rows, i, headers_written, file_path);
        headers_written = headers_written_now;
        console.log(`\x1b[33m➕ tracking headers_written: ${headers_written}\x1b[0m\n`);

        console.log("step 7: save to sql db\n");
        try {
          const { inserted, updated } = await upsert_wrestler_match_history(rows, { wrestling_season, track_wrestling_category, gender });
          console.log(color_text(`🛠️ DB upsert — inserted: ${inserted}, updated: ${updated}`, "green"));
        } catch (e) {
          console.error("❌ DB upsert failed:", e?.message || e);
        }

        processed += 1;
        break; // success → break retry loop
      } catch (e) {
        if (e?.code === "E_TARGET_CLOSED" || e?.code === "E_GOTO_TIMEOUT") {
          const cause =
            e?.code === "E_GOTO_TIMEOUT" ? "navigation timeout" : "page/context/browser closed";
          console.warn(`♻️ ${cause} — reconnecting and retrying this url once...`);

          ({ browser, page, context } = await step_0_launch_chrome_developer(url_home_page));
          browser.on?.("disconnected", () => {
            console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
          });
          await relogin(page, load_timeout_ms, wrestling_season, url_login_page);

          if (attempts >= 2) throw e;
          continue;
        }
        throw e;
      }
    }
  }

  await browser.close(); // closes CDP connection (not the external Chrome instance)
  console.log(`\n✅ done. processed ${processed} wrestler pages from DB (wrestler_list.name_link)`);
}

export { main as step_3_get_wrestler_match_history };



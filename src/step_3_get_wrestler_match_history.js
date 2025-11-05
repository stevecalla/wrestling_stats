// src/step_3_get_wrestler_match_history.js (ESM, snake_case)
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

// keep import specifiers but use snake_case bindings locally
import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file.js";
import { upsert_wrestler_match_history } from "../utilities/mysql/upsert_wrestler_match_history.js";
import {
  count_rows_in_db_wrestler_links,
  iter_name_links_from_db,
} from "../utilities/mysql/iter_name_links_from_db.js";

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
  await page.evaluate(auto_login_select_season, { WRESTLING_SEASON: wrestling_season });
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
    // === helpers in-page ===
    const norm = (s) =>
      (s || "")
        .normalize("NFKD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/\s+/g, " ")
        .trim();
    const lc = (s) => norm(s).toLowerCase();
    const esc_reg = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

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
      if (!t) return { start_date: "", end_date: "", start_obj: null, end_obj: null };

      // A: MM/DD - MM/DD/YYYY
      let m =
        t.match(
          /^(\d{1,2})[\/\-](\d{1,2})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/
        );
      if (m) {
        const [, m1, d1, m2, d2, y2] = m;
        const start_obj = to_date(y2, m1, d1);
        const end_obj = to_date(y2, m2, d2);
        return { start_date: fmt_mdy(start_obj), end_date: fmt_mdy(end_obj), start_obj, end_obj };
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
        return { start_date: fmt_mdy(start_obj), end_date: fmt_mdy(end_obj), start_obj, end_obj };
      }

      // C: MM/DD/YYYY
      m = t.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
      if (m) {
        const [, mm, dd, yy] = m;
        const d = to_date(yy, mm, dd);
        return { start_date: fmt_mdy(d), end_date: "", start_obj: d, end_obj: null };
      }

      // fallback: first full date token
      m = t.match(/(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/);
      if (m) {
        const [token] = m;
        const [mm, dd, yy] = token.split(/[\/\-]/);
        const d = to_date(yy, mm, dd);
        return { start_date: fmt_mdy(d), end_date: "", start_obj: d, end_obj: null };
      }

      return { start_date: "", end_date: "", start_obj: null, end_obj: null };
    };

    // NEW: parse first/last name from a full name string
    function parse_name(full) {
      const raw = String(full || "").trim();
      if (!raw) return { first_name: null, last_name: null };

      if (raw.includes(",")) {
        // "Last, First Middle"
        const [last, rest] = raw.split(",").map(s => s.trim()).filter(Boolean);
        if (!last) return { first_name: null, last_name: null };
        if (!rest) return { first_name: null, last_name: last };
        const first = rest.split(/\s+/)[0] || null;
        return { first_name: first || null, last_name: last || null };
      }

      const parts = raw.split(/\s+/).filter(Boolean);
      if (parts.length === 1) {
        return { first_name: null, last_name: parts[0] || null };
      }
      const first = parts[0] || null;
      const last = parts[parts.length - 1] || null;
      return { first_name: first || null, last_name: last || null };
    }

    // current wrestler context (from dropdown)
    const sel = document.querySelector("#wrestler");
    const sel_opt = sel?.selectedOptions?.[0] || document.querySelector("#wrestler option[selected]");
    const current_id = (sel_opt?.value || "").trim();
    const opt_text = norm(sel_opt?.textContent || "");
    const current_name = opt_text.includes(" - ")
      ? opt_text.split(" - ").slice(1).join(" - ").trim()
      : opt_text;
    const current_name_n = lc(current_name);

    const name_re = new RegExp(`\\b${esc_reg(current_name)}\\b`, "gi");
    const scrub = (s) => norm((s || "").replace(name_re, "").replace(/\s{2,}/g, " "));

    const rows = [];
    for (const tr of document.querySelectorAll("tr.dataGridRow")) {
      const tds = tr.querySelectorAll("td");
      if (tds.length < 5) continue;

      const date_raw = norm(tds[1]?.innerText);
      const { start_date, end_date, start_obj, end_obj } = parse_date_range_text(date_raw);

      const event_raw = norm(tds[2]?.innerText);
      const weight_raw = norm(tds[3]?.innerText);
      const details_text_raw = norm(tds[4]?.innerText);
      const details_cell = tds[4];

      const round = (details_text_raw.match(/^(.*?)\s*-\s*/)?.[1] || "").trim();
      const is_bye = /\b(received a bye|bye)\b/i.test(details_text_raw);
      const is_unknown_forfeit = /\bover\s+Unknown\s*\(\s*For\.\s*\)/i.test(details_text_raw);

      const link_infos = Array.from(details_cell.querySelectorAll('a[href*="wrestlerId="], td a'))
        .map((a) => {
          const href = a.getAttribute("href") || "";
          const id = (href.match(/wrestlerId=(\d+)/) || [])[1] || "";
          const name = norm(a.textContent || "");
          return { id, name, name_n: lc(name) };
        })
        .filter((x) => x.name_n);

      const name_school_pairs = [];
      const rx_ns = /([A-Z][A-Za-z'’.\- ]+)\s*\(([^)]+)\)/g;
      for (const m of details_text_raw.matchAll(rx_ns)) {
        name_school_pairs.push({ name: m[1].trim(), name_n: lc(m[1]), opponent_school: m[2].trim() });
      }

      let me = { id: current_id, name: current_name, name_n: current_name_n };
      const me_by_id = current_id && link_infos.find((li) => li.id === current_id);
      if (me_by_id) me = me_by_id;
      const me_by_name = link_infos.find((li) => li.name_n === current_name_n);
      if (!me_by_id && me_by_name) me = me_by_name;

      const candidates = [];
      for (const li of link_infos) {
        if (li.name_n && li.name_n !== me.name_n) candidates.push({ id: li.id || "", name: li.name, name_n: li.name_n });
      }
      for (const ns of name_school_pairs) {
        if (ns.name_n && ns.name_n !== me.name_n && !candidates.some((c) => c.name_n === ns.name_n)) {
          candidates.push({ id: "", name: ns.name, name_n: ns.name_n });
        }
      }

      let opponent = { id: "", name: "", name_n: "" };
      let opponent_school = "";
      if (is_unknown_forfeit && !is_bye) {
        opponent = { id: "", name: "Unknown", name_n: "unknown" };
      } else {
        opponent = candidates[0] || opponent;
        if (opponent.name && lc(opponent.name) === me.name_n) opponent = { id: "", name: "", name_n: "" };
        if (opponent.name && lc(opponent.name) !== "unknown") {
          const hit = name_school_pairs.find((ns) => ns.name_n === lc(opponent.name));
          if (hit) opponent_school = hit.opponent_school;
        }
      }

      const result_token =
        (
          details_text_raw.match(
            /\b(over|def\.?|maj\.?\s*dec\.?|dec(?:ision)?|tech(?:nical)?\s*fall|fall|pinned|bye)\b/i
          ) || []
        )[0]?.toLowerCase() || (is_unknown_forfeit ? "over" : "");

      let score_details = (details_text_raw.match(/\(([^()]+)\)\s*$/)?.[1] || "").trim();
      if (/^for\.?$/i.test(score_details)) score_details = "";
      if (lc(opponent.name) === "unknown") score_details = "Unknown";
      if (result_token === "bye" || is_bye) score_details = "Bye";

      if (!opponent_school) opponent_school = event_raw;
      opponent_school = opponent_school.replace(/\bvs\.\s*/i, "").trim();

      const details_lc = lc(details_text_raw);
      const over_idx = details_lc.indexOf(" over ");
      const def_idx = details_lc.indexOf(" def.");
      const token_idx = over_idx >= 0 ? over_idx : def_idx;
      const names_in_order = [...details_text_raw.matchAll(/([A-Z][A-Za-z'’.\- ]+)\s*\(([^)]+)\)/g)].map((m) =>
        m[1].trim()
      );

      rows.push({
        start_date,
        end_date,
        sort_date_obj: start_obj || end_obj || new Date(NaN),
        raw_details: details_text_raw,

        event: event_raw,
        weight: weight_raw,
        round,
        opponent: is_bye ? "" : opponent.name || "",
        opponent_id: is_bye ? "" : opponent.id || "",
        opponent_school: opponent_school,
        result: result_token || (is_bye ? "bye" : ""),
        score_details,
        details: details_text_raw,
        token_idx,
        names_in_order,
      });
    }

    // compute per-row w-l-t from top to bottom
    rows.sort(
      (a, b) =>
        +a.sort_date_obj - +b.sort_date_obj ||
        String(a.start_date).localeCompare(String(b.start_date))
    );

    let w = 0,
      l = 0,
      t = 0;

    // Parse current wrestler's first/last once (used for every returned row)
    const { first_name, last_name } = parse_name(current_name);

    const with_record = rows.map((r) => {
      let outcome = "U";

      if (r.result === "bye") {
        outcome = "W";
      } else if (/\b(tie|draw)\b/i.test(r.details)) {
        outcome = "T";
      } else if (/\bover\s+unknown\s*\(\s*for\.\s*\)/i.test(r.details)) {
        outcome = "W";
      } else if (r.token_idx >= 0) {
        const before = r.details.slice(0, r.token_idx).toLowerCase();
        const after = r.details.slice(r.token_idx).toLowerCase();
        const me_before = before.includes(lc(current_name));
        const me_after = after.includes(lc(current_name));
        if (me_before && !me_after) outcome = "W";
        else if (!me_before && me_after) outcome = "L";
      }

      if (outcome === "W") w++;
      else if (outcome === "L") l++;
      else if (outcome === "T") t++;

      let winner_name = "";
      if (r.result === "bye" || /\bover\s+unknown\s*\(\s*for\.\s*\)/i.test(r.details)) {
        winner_name = current_name;
      } else if (r.token_idx >= 0 && r.names_in_order.length) {
        winner_name = r.names_in_order[0];
      } else if (outcome === "W") {
        winner_name = current_name;
      } else if (outcome === "L") {
        winner_name = r.opponent;
      }

      if (outcome === "L") winner_name = r.opponent || winner_name;
      if (outcome === "W") winner_name = current_name;
      if (outcome === "T") winner_name = "";

      // NEW: parse opponent first/last for this row
      const { first_name: opponent_first_name, last_name: opponent_last_name } = parse_name(r.opponent);

      const now_utc = new Date().toISOString();
      return {
        page_url: location.href,
        wrestler_id: current_id,
        wrestler: current_name,
        first_name,              // parsed first name
        last_name,               // parsed last name

        start_date: r.start_date,
        end_date: r.end_date,

        event: scrub(r.event),
        weight_category: scrub(r.weight),
        round: scrub(r.round),
        opponent: scrub(r.opponent),
        opponent_first_name,     // NEW
        opponent_last_name,      // NEW
        opponent_school: scrub(r.opponent_school),
        result: scrub(r.result),
        score_details: scrub(r.score_details),
        winner_name,
        outcome,
        record: `${w}-${l}-${t} W-L-T`, // text-safe for Excel

        raw_details: r.raw_details,
        created_at_utc: now_utc,
      };
    });

    return with_record;
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
  page,
  browser,
  context,
  file_path
) {
  const load_timeout_ms = 30000;

  // DB: count + cap (memory-efficient streaming)
  const total_rows_in_db = await count_rows_in_db_wrestler_links();
  const no_of_urls = Math.min(matches_page_limit, total_rows_in_db);

  let headers_written = false;

  browser.on?.("disconnected", () => {
    console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
  });

  // initial login
  await safe_goto(page, url_login_page, { timeout: load_timeout_ms });
  await page.waitForTimeout(2000);

  console.log("step 1: on index.jsp, starting auto login for season:", wrestling_season);
  await page.evaluate(auto_login_select_season, { WRESTLING_SEASON: wrestling_season });
  await page.waitForTimeout(1000);

  console.log(color_text(`📄 DB has ${total_rows_in_db} wrestler links`, "green"));
  console.log(
    color_text(
      `\x1b[33m⚙️ Processing up to ${no_of_urls} (min of page limit vs DB size)\x1b[0m\n`,
      "green"
    )
  );

  let processed = 0;

  for await (const { i, url } of iter_name_links_from_db({
    start_at: loop_start,
    limit: matches_page_limit,
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
        await page.waitForURL(/seasons\/index\.jsp/i, { timeout: 5000 }).catch(() => {});

        if (/seasons\/index\.jsp/i.test(page.url())) {
          console.log("step 3a: on index.jsp, starting auto login for season:", wrestling_season);
          await page.evaluate(auto_login_select_season, { WRESTLING_SEASON: wrestling_season });
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
          const { inserted, updated } = await upsert_wrestler_match_history(rows);
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

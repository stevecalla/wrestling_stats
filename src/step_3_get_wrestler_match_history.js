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

    // --- helper: sanitize a display name before splitting ---
    function clean_display_name(raw) {
      let s = String(raw || "").trim();
      s = s.replace(/\s*\([^)]+\)\s*$/u, "");  // remove ONE trailing (...), e.g. "(JR)"
      s = s.replace(/\s+/g, " ").trim();
      const suffixRe = /(?:,?\s+(?:Jr\.?|Sr\.?|II|III|IV|V|VI))$/iu;
      s = s.replace(suffixRe, "").trim();
      return s;
    }

    function parse_name(full) {
      const cleaned = clean_display_name(full);
      if (!cleaned) return { first_name: null, last_name: null };

      if (cleaned.includes(",")) {
        const [last, restRaw] = cleaned.split(",").map(s => s.trim()).filter(Boolean);
        const rest = restRaw || "";
        const first = rest.split(/\s+/)[0] || null;
        return { first_name: first || null, last_name: last || null };
      }

      const parts = cleaned.split(/\s+/).filter(Boolean);
      if (parts.length === 1) return { first_name: null, last_name: parts[0] || null };

      const particles = new Set(["van", "von", "de", "da", "del", "der", "di", "du", "la", "le"]);
      let first = parts[0];
      let last = parts[parts.length - 1];
      const penult = parts[parts.length - 2]?.toLowerCase();
      if (particles.has(penult)) last = parts.slice(parts.length - 2).join(" ");
      return { first_name: first || null, last_name: last || null };
    }

    // NEW: parenthetical looks like a school (not score/time/result tokens)
    const looks_like_school = (s) =>
      !!s &&
      !/\d|:/.test(s) &&
      !/^(fall|tf|tech|dec|maj|md|sv|ot|for)\b/i.test(s);

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

      // (CHANGED) capture raw round first; final round decided later
      const round_raw = (details_text_raw.match(/^(.*?)\s*-\s*/)?.[1] || "").trim();

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

      // Build name→school pairs
      const name_school_pairs = [];

      // Pass 1: catch "Name (Alias) (School)" → map to School (2nd paren)
      for (const m of details_text_raw.matchAll(/([A-Za-z][A-Za-z'’.\- ]+?)\s*\(([^)]+)\)\s*\(([^)]+)\)/g)) {
        const nm = (m[1] || "").trim();
        const maybeSchool = (m[3] || "").trim();
        if (nm && looks_like_school(maybeSchool)) {
          name_school_pairs.push({ name: nm, name_n: lc(nm), school: maybeSchool });
        }
      }

      // Pass 2: regular "Name (School)" — ignore score/time/result tokens
      for (const m of details_text_raw.matchAll(/([A-Za-z][A-Za-z'’.\- ]+?)\s*\(([^)]+)\)/g)) {
        const nm = (m[1] || "").trim();
        const sc = (m[2] || "").trim();
        if (!nm || !looks_like_school(sc)) continue;
        name_school_pairs.push({ name: nm, name_n: lc(nm), school: sc });
      }

      // identify "me"
      let me = { id: current_id, name: current_name, name_n: current_name_n };
      const me_by_id = current_id && link_infos.find((li) => li.id === current_id);
      if (me_by_id) me = me_by_id;
      const me_by_name = link_infos.find((li) => li.name_n === current_name_n);
      if (!me_by_id && me_by_name) me = me_by_name;

      // opponent candidates
      const candidates = [];
      for (const li of link_infos) {
        if (li.name_n && li.name_n !== me.name_n) candidates.push({ id: li.id || "", name: li.name, name_n: li.name_n });
      }
      for (const ns of name_school_pairs) {
        if (ns.name_n && ns.name_n !== me.name_n && !candidates.some((c) => c.name_n === ns.name_n)) {
          candidates.push({ id: "", name: ns.name, name_n: ns.name_n });
        }
      }

      // resolve opponent + opponent_school
      let opponent = { id: "", name: "", name_n: "" };
      let opponent_school = "";

      if (is_unknown_forfeit && !is_bye) {
        opponent = { id: "", name: "Unknown", name_n: "unknown" };
      } else {
        opponent = candidates[0] || opponent;
        if (opponent.name && lc(opponent.name) === me.name_n) opponent = { id: "", name: "", name_n: "" };

        if (opponent.name && lc(opponent.name) !== "unknown") {
          // First: if the opponent name itself contains a paren (alias), the next paren is likely the school
          if (!opponent_school && /\)/.test(opponent.name)) {
            const reNext = new RegExp(`${esc_reg(opponent.name)}\\s*\\(([^)]+)\\)`, "i");
            const m = details_text_raw.match(reNext);
            if (m && looks_like_school(m[1])) opponent_school = m[1].trim();
          }

          // Pair-map
          if (!opponent_school) {
            const hit = name_school_pairs.find((ns) => ns.name_n === lc(opponent.name));
            if (hit) opponent_school = hit.school;
          }

          // Fallback: generic single-paren after opponent name
          if (!opponent_school) {
            const re = new RegExp(`\\b${esc_reg(opponent.name)}\\b\\s*\\(([^)]+)\\)`, "i");
            const m = details_text_raw.match(re);
            if (m) {
              const sc = (m[1] || "").trim();
              if (looks_like_school(sc)) opponent_school = sc;
            }
          }
        }
      }

      // opponent_id by matched name
      let opponent_id = "";
      if (!is_bye && !is_unknown_forfeit && opponent?.name) {
        const link_nodes = Array.from(details_cell.querySelectorAll('a[href*="wrestlerId="]'));
        const li = link_nodes
          .map(a => {
            const href = a.getAttribute("href") || "";
            const id = (href.match(/wrestlerId=(\d+)/) || [])[1] || "";
            const name = norm(a.textContent || "");
            return { id, name_n: lc(name) };
          })
          .find(x => x.id && x.id !== current_id && x.name_n === lc(opponent.name));
        if (li) opponent_id = li.id;
      }

      // Resolve wrestler_school
      let wrestler_school = "";

      // If current name includes an alias paren, the very next paren in details is the school
      if (!wrestler_school && /\)/.test(me.name)) {
        const reNext = new RegExp(`${esc_reg(me.name)}\\s*\\(([^)]+)\\)`, "i");
        const m = details_text_raw.match(reNext);
        if (m && looks_like_school(m[1])) wrestler_school = m[1].trim();
      }

      // Pair-map fallback
      if (!wrestler_school) {
        const me_hit = name_school_pairs.find((ns) => ns.name_n === me.name_n);
        if (me_hit) wrestler_school = me_hit.school;
      }

      // Generic fallback: "Me (School)"
      if (!wrestler_school && me.name) {
        const reMe = new RegExp(`\\b${esc_reg(me.name)}\\b\\s*\\(([^)]+)\\)`, "i");
        const m = details_text_raw.match(reMe);
        if (m) {
          const sc = (m[1] || "").trim();
          if (looks_like_school(sc)) wrestler_school = sc;
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

      const details_lc = lc(details_text_raw);
      const over_idx = details_lc.indexOf(" over ");
      const def_idx = details_lc.indexOf(" def.");
      const token_idx = over_idx >= 0 ? over_idx : def_idx;
      const names_in_order = [...details_text_raw.matchAll(/([A-Z][A-Za-z'’.\- ]+)\s*\(([^)]+)\)/g)].map((m) =>
        m[1].trim()
      );

      // ===== ROUND FILTERING (ONLY CHANGE) =====
      let round = round_raw;

      // If the full raw details starts with wrestler or opponent name, treat as individual match => no round
      const starts_with_wrestler = current_name
        ? new RegExp(`^\\s*${esc_reg(current_name)}\\b`, "i").test(details_text_raw)
        : false;
      const starts_with_opponent = opponent?.name
        ? new RegExp(`^\\s*${esc_reg(opponent.name)}\\b`, "i").test(details_text_raw)
        : false;

      if (starts_with_wrestler || starts_with_opponent) {
        round = "";
      } else if (round) {
        const rlc = lc(round);
        const badTokens = [
          lc(current_name || ""),
          lc(opponent?.name || ""),
          lc(wrestler_school || ""),
          lc(opponent_school || "")
        ].filter(Boolean);

        if (badTokens.some(tok => tok && rlc.includes(tok))) {
          round = "";
        }
      }
      // ===== END ROUND FILTERING =====

      rows.push({
        start_date,
        end_date,
        sort_date_obj: start_obj || end_obj || new Date(NaN),
        raw_details: details_text_raw,

        event: event_raw,
        weight: weight_raw,
        round, // <- filtered round
        opponent: is_bye ? "" : opponent.name || "",
        opponent_id: is_bye ? "" : opponent.id || "",
        opponent_school,
        wrestler_school,
        result: result_token || (is_bye ? "bye" : ""),
        score_details,
        details: details_text_raw,
        token_idx,
        names_in_order,
      });
    }

    // ------------------------------------------
    // Compute win-loss-tie record for wrestler
    // ------------------------------------------
    rows.sort(
      (a, b) =>
        +a.sort_date_obj - +b.sort_date_obj ||
        String(a.start_date).localeCompare(String(b.start_date))
    );

    let wins_all = 0, losses_all = 0, ties_all = 0;
    let wins_var = 0, losses_var = 0, ties_var = 0;

    function classify_row(row, current_name) {
      const txt = (row.details || "").toLowerCase();

      const is_bye = row.result === "bye" || /\b(received a bye|bye)\b/i.test(row.details);
      const is_exhibition = /\bexhibition\b/i.test(txt);
      const is_unknown_forfeit = /\bover\s+unknown\s*\(\s*for\.\s*\)/i.test(row.details);
      const is_forfeit = /\b(for\.|fft|forfeit)\b/i.test(txt);
      const is_med_forfeit = /\b(mff|med(?:ical)?\s*for(?:feit)?)\b/i.test(txt);
      const is_injury_default = /\b(inj\.?\s*def\.?|injury\s*default)\b/i.test(txt);
      const is_dq = /\b(dq|disqualification)\b/i.test(txt);
      const is_tie = /\b(tie|draw)\b/i.test(txt);

      const is_varsity = /^varsity\b/i.test(String(row.round || ""));

      let outcome = "U";
      let counts_in_record = true;

      if (is_bye) {
        outcome = "bye";
        counts_in_record = false;
        return { outcome, counts_in_record, is_varsity };
      }

      if (is_exhibition) {
        outcome = "U";
        counts_in_record = false;
        return { outcome, counts_in_record, is_varsity };
      }

      if (is_tie) {
        outcome = "T";
        return { outcome, counts_in_record, is_varsity };
      }

      if (is_unknown_forfeit || is_forfeit || is_med_forfeit || is_injury_default || is_dq) {
        if (is_unknown_forfeit) {
          outcome = "W";
          return { outcome, counts_in_record, is_varsity };
        }

        if (row.token_idx >= 0) {
          const before = row.details.slice(0, row.token_idx).toLowerCase();
          const after = row.details.slice(row.token_idx).toLowerCase();
          const me_before = before.includes(current_name.toLowerCase());
          const me_after = after.includes(current_name.toLowerCase());
          outcome = (me_before && !me_after) ? "W" : (!me_before && me_after) ? "L" : "U";
        } else {
          outcome = "U";
        }
        return { outcome, counts_in_record, is_varsity };
      }

      if (row.token_idx >= 0) {
        const before = row.details.slice(0, row.token_idx).toLowerCase();
        const after = row.details.slice(row.token_idx).toLowerCase();
        const me_before = before.includes(current_name.toLowerCase());
        const me_after = after.includes(current_name.toLowerCase());
        if (me_before && !me_after) outcome = "W";
        else if (!me_before && me_after) outcome = "L";
      }

      return { outcome, counts_in_record, is_varsity };
    }

    const { first_name, last_name } = parse_name(current_name);

    const with_record = rows.map((row) => {
      const { outcome, counts_in_record, is_varsity } = classify_row(row, current_name);

      if (counts_in_record) {
        if (outcome === "W") wins_all++;
        else if (outcome === "L") losses_all++;
        else if (outcome === "T") ties_all++;
      }

      if (counts_in_record && is_varsity) {
        if (outcome === "W") wins_var++;
        else if (outcome === "L") losses_var++;
        else if (outcome === "T") ties_var++;
      }

      const opponent_clean = scrub(row.opponent);
      const wrestler_clean = current_name;

      let winner_name = "";
      if (outcome === "W" || outcome === "bye") {
        winner_name = wrestler_clean;
      } else if (outcome === "L") {
        winner_name = opponent_clean;
      } else {
        winner_name = "";
      }

      const { first_name: opponent_first_name, last_name: opponent_last_name } = parse_name(row.opponent);
      const now_utc = new Date().toISOString();

      return {
        page_url: location.href,
        wrestler_id: (document.querySelector("#wrestler")?.value || "").trim(),
        wrestler: current_name,
        first_name,
        last_name,
        wrestler_school: scrub(row.wrestler_school),

        start_date: row.start_date,
        end_date: row.end_date,

        event: scrub(row.event),
        weight_category: scrub(row.weight),
        round: scrub(row.round),

        opponent: scrub(row.opponent),
        opponent_id: row.opponent_id || "",
        opponent_first_name,
        opponent_last_name,
        opponent_school: scrub(row.opponent_school),

        result: scrub(row.result),
        score_details: scrub(row.score_details),
        winner_name,
        outcome,
        counts_in_record,

        record: `${wins_all}-${losses_all}-${ties_all} W-L-T`,
        record_varsity: `${wins_var}-${losses_var}-${ties_var} W-L-T (Varsity)`,

        raw_details: row.raw_details,
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
  track_wrestling_category = "High School Boys",
  gender,
  page,
  browser,
  context,
  file_path
) {
  const load_timeout_ms = 30000;

  // DB: count + cap (memory-efficient streaming)
  const total_rows_in_db = await count_rows_in_db_wrestler_links( wrestling_season, gender );
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
    gender,
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



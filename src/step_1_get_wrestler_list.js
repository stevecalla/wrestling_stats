import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { URL_WRESTLERS } from "../data/input/urls_wrestlers.js";
import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file";
import { auto_login_select_season } from "../utilities/scraper_tasks/auto_login_select_season";
import { click_on_wrestler_menu } from "../utilities/scraper_tasks/click_on_wrestler_menu";

// === Main function to get wrestler match history ===
// function extractor_source() {
//   return async (opts = {}) => {
//     const {
//       startPrefix = "a",
//       delayMs = 700,
//       guardMaxSteps = 200, // safety; bump if needed
//     } = opts;

//     // ---- tiny utils ----
//     const sleep = (ms) => new Promise(r => setTimeout(r, ms));

//     function getPager(which = 'top') {
//       const id = which === 'top' ? '#dataGridNextPrev_top' : '#dataGridNextPrev_bottom';
//       return document.querySelector(id) || document.querySelector('.dataGridNextPrev') || null;
//     }
//     function getPagerRangeText(which = 'top') {
//       const pager = getPager(which);
//       if (!pager) return '';
//       const spans = pager.querySelectorAll('span');
//       for (const s of spans) {
//         const t = (s.textContent || '').trim();
//         if (/^\d+\s*-\s*\d+\s+of\s+\d+$/i.test(t)) return t;
//       }
//       return '';
//     }
//     function parseRange(which = 'top') {
//       const t = getPagerRangeText(which);
//       const m = t.match(/(\d+)\s*-\s*(\d+)\s+of\s+(\d+)/i);
//       return m ? { start: +m[1], end: +m[2], total: +m[3], text: t } : null;
//     }
//     function hasNextPage(which = 'top') {
//       const r = parseRange(which);
//       return r ? r.end < r.total : false;
//     }
//     function clickNext(which = 'top') {
//       const pager = getPager(which);
//       if (!pager) return false;
//       const nextAnchor =
//         pager.querySelector('a .dgNext')?.closest('a') ||
//         pager.querySelector('a[href^="javascript:defaultGrid.next"]') ||
//         pager.querySelector('a i.icon-arrow_r')?.closest('a');
//       if (!nextAnchor) return false;
//       nextAnchor.click();
//       return true;
//     }
//     function getGridRows() {
//       return document.querySelectorAll('tr.dataGridRow');
//     }
//     function lastRowSignature() {
//       const rows = getGridRows();
//       const last = rows[rows.length - 1];
//       if (!last) return '';
//       const txt = last.innerText.replace(/\s+/g, ' ').trim();
//       const hrefs = Array.from(last.querySelectorAll('a')).map(a => a.href).join('|');
//       return `${txt}::${hrefs}`;
//     }
//     async function waitForLastRowChange(prevSig, { timeoutMs = 12000, pollMs = 120 } = {}) {
//       const start = performance.now();
//       while (performance.now() - start < timeoutMs) {
//         const now = lastRowSignature();
//         if (now && now !== prevSig) return true;
//         await sleep(pollMs);
//       }
//       return false;
//     }
//     async function ensurePageSize50(which = 'top') {
//       const pager = getPager(which);
//       const input = pager?.querySelector('input[type="text"][onchange*="updateLimit"]');
//       if (!input) return;
//       const current = parseInt(input.value || '0', 10);
//       if (current === 50) return;
//       const before = lastRowSignature();
//       input.value = '50';
//       input.dispatchEvent(new Event('change', { bubbles: true }));
//       await waitForLastRowChange(before).catch(() => { });
//     }
//     async function openSearchPanel() {
//       const openBtn = document.querySelector('#searchButton, input[type="button"][onclick*="openSearchWrestlers"]');
//       if (openBtn) openBtn.click();
//       await sleep(500);
//     }
//     async function runSearchWithPrefix(prefix) {
//       await openSearchPanel();
//       const lastNameInput = document.querySelector('#s_lastName');
//       if (!lastNameInput) throw new Error('Last Name input #s_lastName not found');
//       lastNameInput.focus();
//       lastNameInput.value = prefix;
//       lastNameInput.dispatchEvent(new Event('input', { bubbles: true }));
//       lastNameInput.dispatchEvent(new Event('change', { bubbles: true }));
//       const innerSearch = document.querySelector('input[type="button"][onclick*="searchWrestlers"], input.segment-track[value="Search"]');
//       if (!innerSearch) throw new Error('Inner Search button not found');
//       const before = lastRowSignature();
//       innerSearch.click();
//       await waitForLastRowChange(before);
//       await ensurePageSize50('top');
//     }
//     function get_wrestler_table_data() {
//       const rows = document.querySelectorAll('tr.dataGridRow');
//       const data = Array.from(rows).map((row) => {
//         const cols = row.querySelectorAll('td div');
//         const getText = (i) => (cols[i]?.textContent || '').trim();
//         const getLink = (i) => cols[i]?.querySelector('a')?.href || null;
//         return {
//           name: getText(2),
//           name_link: getLink(2),
//           team: getText(3),
//           team_link: getLink(3),
//           weight_class: getText(4),
//           gender: getText(5),
//           grade: getText(6),
//           record: getText(8),
//           has_check_icon: !!cols[1]?.querySelector('.greenIcon'),
//           row_index: row.rowIndex
//         };
//       });
//       return data;
//     }
//     function parseLastNameFromNameCell(nameText) {
//       if (!nameText) return '';
//       const t = nameText.trim();
//       if (t.includes(',')) return t.split(',')[0].trim();      // "Doe, John"
//       const parts = t.split(/\s+/);
//       if (parts.length > 1) return parts[parts.length - 1];    // "John Doe" → "Doe"
//       return t;
//     }
//     function twoLetterPrefixFromLastRow() {
//       const rows = getGridRows();
//       const last = rows[rows.length - 1];
//       if (!last) return '';
//       const cols = last.querySelectorAll('td div');
//       const nameText = (cols[2]?.textContent || '').trim();
//       const lastName = parseLastNameFromNameCell(nameText).toLowerCase().replace(/[^a-z]/g, '');
//       return lastName.slice(0, 2);
//     }
//     function bumpTwoLetterPrefix(prefix) {
//       if (!/^[a-z]{2}$/.test(prefix)) return null;
//       const a = prefix.charCodeAt(0), b = prefix.charCodeAt(1);
//       if (b < 122) return String.fromCharCode(a) + String.fromCharCode(b + 1);
//       if (a < 122) return String.fromCharCode(a + 1) + 'a';
//       return null;
//     }
//     function nextFirstLetter(c) {
//       if (!/^[a-z]$/.test(c)) return null;
//       return c === 'z' ? null : String.fromCharCode(c.charCodeAt(0) + 1);
//     }
//     async function scrapeAllPagesCollect(which = 'top', delay = 800) {
//       const collected = [];
//       const firstData = get_wrestler_table_data();
//       collected.push(...firstData);
//       while (hasNextPage(which)) {
//         const before = lastRowSignature();
//         const clicked = clickNext(which);
//         if (!clicked) break;
//         const changed = await waitForLastRowChange(before);
//         if (!changed) break;
//         await ensurePageSize50(which);
//         const data = get_wrestler_table_data();
//         collected.push(...data);
//         await sleep(delay);
//       }
//       return collected;
//     }

//     // ---- MAIN alpha sweep (returns ALL rows to Node) ----
//     const masterRows = [];
//     let prefix = startPrefix.toLowerCase();
//     let steps = 0;

//     while (prefix && steps < guardMaxSteps) {
//       steps++;
//       await runSearchWithPrefix(prefix);

//       // no results → advance first letter
//       if (getGridRows().length === 0) {
//         const nf = nextFirstLetter(prefix[0]);
//         if (!nf) break;
//         prefix = nf;
//         continue;
//       }

//       // collect current block
//       const block = await scrapeAllPagesCollect('top', delayMs);
//       masterRows.push(...block);

//       // decide next prefix based on last row
//       const nxt2 = twoLetterPrefixFromLastRow();
//       if (nxt2 && (/^[a-z]{2}$/i.test(nxt2))) {
//         prefix = (nxt2 > prefix ? nxt2 : bumpTwoLetterPrefix(prefix.length === 1 ? prefix + 'a' : prefix)) || nextFirstLetter(prefix[0]);
//       } else {
//         prefix = nextFirstLetter(prefix[0]);
//       }
//     }

//     // stamp created_at_utc and page_url for traceability
//     const now = new Date().toISOString();
//     const withMeta = masterRows.map(r => ({
//       ...r,
//       created_at_utc: now,
//       page_url: location.href
//     }));

//     return withMeta;
//   };
// }

function extractor_source() {
  return async (opts = {}) => {
    const {
      prefix = "a",
      delay_ms = 700,
    } = opts;

    // ---- utils (same logic you already had, trimmed to what's needed) ----
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    function get_pager(which = "top") {
      const id = which === "top" ? "#dataGridNextPrev_top" : "#dataGridNextPrev_bottom";
      return document.querySelector(id) || document.querySelector(".dataGridNextPrev") || null;
    }
    function get_pager_range_text(which = "top") {
      const pager = get_pager(which);
      if (!pager) return "";
      const spans = pager.querySelectorAll("span");
      for (const s of spans) {
        const t = (s.textContent || "").trim();
        if (/^\d+\s*-\s*\d+\s+of\s+\d+$/i.test(t)) return t; // e.g., "1 - 50 of 387"
      }
      return "";
    }
    function parse_range(which = "top") {
      const t = get_pager_range_text(which);
      const m = t.match(/(\d+)\s*-\s*(\d+)\s+of\s+(\d+)/i);
      return m ? { start: +m[1], end: +m[2], total: +m[3], text: t } : null;
    }
    function has_next_page(which = "top") {
      const r = parse_range(which);
      return r ? r.end < r.total : false;
    }
    function click_next(which = "top") {
      const pager = get_pager(which);
      if (!pager) return false;
      const next_anchor =
        pager.querySelector("a .dgNext")?.closest("a") ||
        pager.querySelector('a[href^="javascript:defaultGrid.next"]') ||
        pager.querySelector("a i.icon-arrow_r")?.closest("a");
      if (!next_anchor) return false;
      next_anchor.click();
      return true;
    }
    function get_grid_rows() {
      return document.querySelectorAll("tr.dataGridRow");
    }
    function last_row_signature() {
      const rows = get_grid_rows();
      const last = rows[rows.length - 1];
      if (!last) return "";
      const txt = last.innerText.replace(/\s+/g, " ").trim();
      const hrefs = Array.from(last.querySelectorAll("a")).map(a => a.href).join("|");
      return `${txt}::${hrefs}`;
    }
    async function wait_for_last_row_change(prev_sig, { timeout_ms = 12000, poll_ms = 120 } = {}) {
      const start = performance.now();
      while (performance.now() - start < timeout_ms) {
        const now = last_row_signature();
        if (now && now !== prev_sig) return true;
        await sleep(poll_ms);
      }
      return false;
    }
    async function ensure_page_size_50(which = "top") {
      const pager = get_pager(which);
      const input = pager?.querySelector('input[type="text"][onchange*="updateLimit"]');
      if (!input) return;
      const current = parseInt(input.value || "0", 10);
      if (current === 50) return;
      const before = last_row_signature();
      input.value = "50";
      input.dispatchEvent(new Event("change", { bubbles: true }));
      await wait_for_last_row_change(before).catch(() => {});
    }
    async function open_search_panel() {
      const open_btn = document.querySelector('#searchButton, input[type="button"][onclick*="openSearchWrestlers"]');
      if (open_btn) open_btn.click();
      await sleep(400);
    }
    async function run_search_with_prefix(pfx) {
      await open_search_panel();
      const last_name_input = document.querySelector("#s_lastName");
      if (!last_name_input) throw new Error("Last Name input #s_lastName not found");
      last_name_input.focus();
      last_name_input.value = pfx;
      last_name_input.dispatchEvent(new Event("input", { bubbles: true }));
      last_name_input.dispatchEvent(new Event("change", { bubbles: true }));
      const inner_search = document.querySelector(
        'input[type="button"][onclick*="searchWrestlers"], input.segment-track[value="Search"]'
      );
      if (!inner_search) throw new Error("Inner Search button not found");
      const before = last_row_signature();
      inner_search.click();
      await wait_for_last_row_change(before);
      await ensure_page_size_50("top");
    }
    function get_wrestler_table_data() {
      const rows = document.querySelectorAll("tr.dataGridRow");
      const data = Array.from(rows).map((row) => {
        const cols = row.querySelectorAll("td div");
        const get_text = (i) => (cols[i]?.textContent || "").trim();
        const get_link = (i) => cols[i]?.querySelector("a")?.href || null;
        return {
          name: get_text(2),
          name_link: get_link(2),
          team: get_text(3),
          team_link: get_link(3),
          weight_class: get_text(4),
          gender: get_text(5),
          grade: get_text(6),
          record: get_text(8),
          has_check_icon: !!cols[1]?.querySelector(".greenIcon"),
          row_index: row.rowIndex,
        };
      });
      return data;
    }
    function parse_last_name_from_name_cell(name_text) {
      if (!name_text) return "";
      const t = name_text.trim();
      if (t.includes(",")) return t.split(",")[0].trim();
      const parts = t.split(/\s+/);
      if (parts.length > 1) return parts[parts.length - 1];
      return t;
    }
    function two_letter_prefix_from_last_row() {
      const rows = get_grid_rows();
      const last = rows[rows.length - 1];
      if (!last) return "";
      const cols = last.querySelectorAll("td div");
      const name_text = (cols[2]?.textContent || "").trim();
      const last_name = parse_last_name_from_name_cell(name_text).toLowerCase().replace(/[^a-z]/g, "");
      return last_name.slice(0, 2);
    }
    function bump_two_letter_prefix(pfx) {
      if (!/^[a-z]{2}$/.test(pfx)) return null;
      const a = pfx.charCodeAt(0), b = pfx.charCodeAt(1);
      if (b < 122) return String.fromCharCode(a) + String.fromCharCode(b + 1);
      if (a < 122) return String.fromCharCode(a + 1) + "a";
      return null;
    }
    function next_first_letter(c) {
      if (!/^[a-z]$/.test(c)) return null;
      return c === "z" ? null : String.fromCharCode(c.charCodeAt(0) + 1);
    }

    // ---- do one prefix step ----
    await run_search_with_prefix(prefix);

    // no results → advance first letter and return early (no rows)
    if (get_grid_rows().length === 0) {
      const nf = next_first_letter(prefix[0]);
      return { rows: [], next_prefix: nf, pages_advanced: 0, range_text: "" };
    }

    // collect all pages for this search
    const rows_collected = [];
    let pages_advanced = 0;

    const first_data = get_wrestler_table_data();
    rows_collected.push(...first_data);

    while (has_next_page("top")) {
      const before = last_row_signature();
      const clicked = click_next("top");
      if (!clicked) break;
      const changed = await wait_for_last_row_change(before);
      if (!changed) break;
      await ensure_page_size_50("top");
      const page_rows = get_wrestler_table_data();
      rows_collected.push(...page_rows);
      pages_advanced += 1;
      await sleep(delay_ms);
    }

    // decide next prefix from last row (two-letter) or fallback
    const nxt2 = two_letter_prefix_from_last_row();
    let next_prefix = null;
    if (nxt2 && /^[a-z]{2}$/i.test(nxt2)) {
      next_prefix = (nxt2 > prefix ? nxt2 : bump_two_letter_prefix(prefix.length === 1 ? prefix + "a" : prefix)) || next_first_letter(prefix[0]);
    } else {
      next_prefix = next_first_letter(prefix[0]);
    }

    const now = new Date().toISOString();
    const with_meta = rows_collected.map(r => ({ ...r, created_at_utc: now, page_url: location.href }));

    return {
      rows: with_meta,
      next_prefix,
      pages_advanced,
      range_text: get_pager_range_text("top") || ""
    };
  };
}

/**
 * In-page helper that performs exactly ONE alpha step:
 *  - run search with the given prefix
 *  - paginate (50/page) and collect rows
 *  - decide the next prefix (two-letter from last row or fallback)
 * Returns { rows, next_prefix, pages_advanced, range_text }
 */
async function main(MIN_URLS = 5, WRESTLING_SEASON = "2024-2025", page, browser) {
  const URLS = URL_WRESTLERS;
  const LOAD_TIMEOUT_MS = 30000;
  // const NO_OF_URLS = Math.min(MIN_URLS, URLS.length);
  // let headersWritten = false; // stays true once header is created

  const LOGIN_URL = "https://www.trackwrestling.com/seasons/index.jsp";
  await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
  await page.waitForTimeout(2000); // small settle

  // Step 1: select wrestling season
  console.log("step 1: on index.jsp, starting auto login for season:", WRESTLING_SEASON);
  const loginArgs = { WRESTLING_SEASON: WRESTLING_SEASON };
  await page.evaluate(auto_login_select_season, loginArgs); // <-- pass the function itself
  await page.waitForTimeout(1000);

  // 2) land on MainFrame and open Wrestlers menu
  await page.waitForURL(/seasons\/MainFrame\.jsp/i, { timeout: 8000 }).catch(() => { });
  await page.evaluate(click_on_wrestler_menu);
  await page.waitForTimeout(1500);

  // TODO =============
  // // 3) find Wrestlers frame
  // const wrestlersFrame = page.frames().find(f => /Wrestlers\.jsp/i.test(f.url())) || page.mainFrame();
  // await wrestlersFrame.waitForSelector('#searchButton', { timeout: LOAD_TIMEOUT_MS });

  // // 4) run alpha sweep *in the page* and get data back
  // console.log("step 2: running alpha sweep in-page…");
  // const all_rows = await wrestlersFrame.evaluate(extractor_source(), {
  //   startPrefix: "a",
  //   delayMs: 700,
  //   guardMaxSteps: 5     // adjust if you want to sweep deeper
  // });
  // console.log(`alpha sweep returned ${all_rows.length} rows`);

  // // 5) save with your existing CSV utility
  // const FILE_NAME = `wrestlers_alpha_${WRESTLING_SEASON}.csv`;
  // const FOLDER_NAME = "input";
  // const iterationIndex = 0;
  // save_to_csv_file(all_rows, iterationIndex, headersWritten, FOLDER_NAME, FILE_NAME);
  // TODO =============

  // TODO 2 =============
  // 3) ALPHA SWEEP LOOP
  let headers_written = false;
  let cumulative_rows = 0;
  let step = 0;
  let prefix = "a";                                    // start at top of alphabet
  const guard_max_steps = Math.min(MIN_URLS, 200);;    // safety guard
  const delay_ms = 700;

  const wrestlers_frame = page.frames().find(f => /Wrestlers\.jsp/i.test(f.url())) || page.mainFrame();
  await wrestlers_frame.waitForSelector("#searchButton", { timeout: LOAD_TIMEOUT_MS });

  // --- LOOP: one alpha step → write file → progress → compute next prefix
  const FILE_NAME = `wrestlers_alpha_${WRESTLING_SEASON}.csv`;
  const FOLDER_NAME = "input";

  while (prefix && step < guard_max_steps) {
    step += 1;

    const result = await wrestlers_frame.evaluate(extractor_source(), {
      prefix,
      delay_ms
    });

    const rows = result?.rows || [];
    const wrote = rows.length;

    if (wrote > 0) {
      // write after EACH prefix block
      let iterationIndex = step - 1;
      save_to_csv_file(rows, iterationIndex, headers_written, FOLDER_NAME, FILE_NAME);
      headers_written = true;
      cumulative_rows += wrote;
    }

    const pages_info = result?.pages_advanced ?? 0;
    const range_text = result?.range_text || "";
    console.log(`step ${step}: prefix="${prefix}" || pages=${pages_info + 1} ||   rows_written=${wrote} || total_rows=${cumulative_rows}`);

    // next prefix
    prefix = result?.next_prefix || null;

    // done?
    if (!prefix) {
      console.log(`[alpha] completed sweep. total_rows=${cumulative_rows}, steps=${step}`);
      break;
    }
  }
  // TODO 2 =============
  // }

  await browser.close(); // closes CDP connection (not your Chrome instance)
}

export { main as step_1_run_alpha_wrestler_list };

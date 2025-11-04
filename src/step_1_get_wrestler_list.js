// src/step_1_get_wrestler_list.js  (ESM)

import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file.js";
import { auto_login_select_season } from "../utilities/scraper_tasks/auto_login_select_season.js";
import { click_on_wrestler_menu } from "../utilities/scraper_tasks/click_on_wrestler_menu.js";
import { color_text } from "../utilities/console_logs/console_colors.js";

/* ------------------------------------------
   Helpers
-------------------------------------------*/

// robust wait for selector on a Page or Frame
async function safe_wait_for_selector(frameOrPage, selector, opts = {}) {
  try {
    await frameOrPage.waitForSelector(selector, { state: "visible", ...opts });
  } catch (err) {
    const msg = String(err?.message || "");
    if (
      msg.includes("Target page, context or browser has been closed") ||
      msg.includes("Frame was detached") ||
      msg.includes("Execution context was destroyed")
    ) {
      err.code = "E_TARGET_CLOSED";
    }
    throw err;
  }
}

// wait until the Wrestlers.jsp frame exists AND has a stable element
async function wait_for_wrestlers_frame(page, timeoutMs = 30000) {
  const start = Date.now();
  const STABLE_SELECTOR = [
    "#searchButton", // often the "Open Search" toggle
    'input[type="button"][onclick*="openSearchWrestlers"]',
    "table.dataGrid",      // the grid
    ".dataGridNextPrev"    // the pager
  ].join(", ");

  while (Date.now() - start < timeoutMs) {
    const f = page.frames().find(fr => /Wrestlers\.jsp/i.test(fr.url()));
    if (f) {
      try { await f.waitForLoadState?.("domcontentloaded", { timeout: 3000 }); } catch {}
      try {
        await f.waitForSelector(STABLE_SELECTOR, { state: "attached", timeout: 3000 });
        return f;
      } catch { /* keep polling */ }
    }
    await page.waitForTimeout(200);
  }
  throw new Error("Timed out waiting for Wrestlers.jsp frame to attach.");
}

// in-page scraper factory; returns a function executed inside the Wrestlers frame
function extractor_source() {
  return async (opts = {}) => {
    const { prefix = "a", grade, level, delay_ms = 100 } = opts;

    // tiny utils for in-page context
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    const getPager = (which = "top") =>
      document.querySelector(which === "top" ? "#dataGridNextPrev_top" : "#dataGridNextPrev_bottom")
      || document.querySelector(".dataGridNextPrev")
      || null;

    const getRangeText = (which = "top") => {
      const p = getPager(which);
      if (!p) return "";
      for (const s of p.querySelectorAll("span")) {
        const t = (s.textContent || "").trim();
        if (/^\d+\s*-\s*\d+\s+of\s+\d+$/i.test(t)) return t; // "1 - 50 of 387"
      }
      return "";
    };

    const parseRange = (which = "top") => {
      const m = getRangeText(which).match(/(\d+)\s*-\s*(\d+)\s+of\s+(\d+)/i);
      return m ? { start: +m[1], end: +m[2], total: +m[3] } : null;
    };

    const hasNextPage = (which = "top") => {
      const r = parseRange(which);
      return r ? r.end < r.total : false;
    };

    const clickNext = (which = "top") => {
      const p = getPager(which);
      if (!p) return false;
      const a =
        p.querySelector("a .dgNext")?.closest("a") ||
        p.querySelector('a[href^="javascript:defaultGrid.next"]') ||
        p.querySelector("a i.icon-arrow_r")?.closest("a");
      if (!a) return false;
      a.click();
      return true;
    };

    const gridRows = () => document.querySelectorAll("tr.dataGridRow");

    const lastRowSignature = () => {
      const rows = gridRows();
      const last = rows[rows.length - 1];
      if (!last) return "";
      const txt = last.innerText.replace(/\s+/g, " ").trim();
      const hrefs = Array.from(last.querySelectorAll("a")).map(a => a.href).join("|");
      return `${txt}::${hrefs}`;
    };

    const waitForLastRowChange = async (prevSig, { timeout_ms = 12000, poll_ms = 120 } = {}) => {
      const start = performance.now();
      while (performance.now() - start < timeout_ms) {
        const now = lastRowSignature();
        if (now && now !== prevSig) return true;
        await sleep(poll_ms);
      }
      return false;
    };

    const ensure_page_size_50 = async (which = "top") => {
      const p = getPager(which);
      const input = p?.querySelector('input[type="text"][onchange*="updateLimit"]');
      if (!input) return;
      const current = parseInt(input.value || "0", 10);
      if (current === 50) return;
      const before = lastRowSignature();
      input.value = "50";
      input.dispatchEvent(new Event("change", { bubbles: true }));
      await waitForLastRowChange(before).catch(() => {});
    };

    const openSearchPanel = async () => {
      const btn = document.querySelector('#searchButton, input[type="button"][onclick*="openSearchWrestlers"]');
      if (btn) btn.click();
      await sleep(800);
    };

    // select by visible text (not value)
    const setSelectByText = (selectEl, text) => {
      if (!selectEl || !text) return false;
      const opts = Array.from(selectEl.options || []);
      const found = opts.find(o => (o.textContent || "").trim().toLowerCase() === String(text).toLowerCase());
      if (!found) return false;
      selectEl.value = found.value;
      selectEl.dispatchEvent(new Event("input", { bubbles: true }));
      selectEl.dispatchEvent(new Event("change", { bubbles: true }));
      selectEl.dispatchEvent(new Event("blur", { bubbles: true }));
      return true;
    };

    const runSearchWithPrefix = async (pfx) => {
      await sleep(200 + Math.random() * 300);

      // ensure search UI fields exist
      await openSearchPanel();

      // 1) Last name starts with prefix
      const lastName = document.querySelector("#s_lastName");
      if (!lastName) throw new Error("Last Name input #s_lastName not found");
      lastName.focus();
      lastName.value = pfx;
      lastName.dispatchEvent(new Event("input", { bubbles: true }));
      lastName.dispatchEvent(new Event("change", { bubbles: true }));

      // 2) Grade (by text only, per your requirement)
      const gradeSelect = document.querySelector("#s_gradeId");
      if (gradeSelect && grade) {
        const ok = setSelectByText(gradeSelect, grade);
        if (!ok) console.warn(`Option "${grade}" not found in #s_gradeId.`);
      }

      // 3) Level (optional)
      const levelSelect = document.querySelector("#s_levelId");
      if (levelSelect && level) {
        const ok = setSelectByText(levelSelect, level);
        if (!ok) console.warn(`Option "${level}" not found in #s_levelId.`);
      }

      // 4) Trigger inner Search
      const go = document.querySelector(
        'input[type="button"][onclick*="searchWrestlers"], input.segment-track[value="Search"]'
      );
      if (!go) throw new Error("Inner Search button not found");
      const before = lastRowSignature();
      go.click();

      await waitForLastRowChange(before);
      await ensure_page_size_50("top");
    };

    const readTable = () => {
      const rows = document.querySelectorAll("tr.dataGridRow");
      return Array.from(rows).map((row) => {
        const cols = row.querySelectorAll("td div");
        const txt = (i) => (cols[i]?.textContent || "").trim();
        const href = (i) => cols[i]?.querySelector("a")?.href || null;
        return {
          name: txt(2),
          name_link: href(2),
          team: txt(3),
          team_link: href(3),
          weight_class: txt(4),
          gender: txt(5),
          grade: txt(6),
          level: txt(7),
          record: `${txt(8).trim()} W-L`,
        };
      });
    };

    // run once for this prefix
    await runSearchWithPrefix(prefix);

    // collect current + next pages
    const collected = [];
    collected.push(...readTable());

    let pages_advanced = 0;
    while (hasNextPage("top")) {
      const before = lastRowSignature();
      if (!clickNext("top")) break;
      const changed = await waitForLastRowChange(before);
      if (!changed) break;
      await ensure_page_size_50("top");
      collected.push(...readTable());
      pages_advanced += 1;
      await sleep(delay_ms);
    }

    const now = new Date().toISOString();
    const rows_with_meta = collected.map(r => ({ ...r, created_at_utc: now, page_url: location.href }));

    return {
      rows: rows_with_meta,
      pages_advanced,
      range_text: getRangeText("top") || ""
    };
  };
}

/* ------------------------------------------
   Main Orchestrator
-------------------------------------------*/

/**
 * Executes an A→Z sweep for HS Freshman/Sophomore/Junior/Senior (Varsity),
 * writing each letter’s results as they’re fetched.
 *
 * @param {string} URL_LOGIN_PAGE
 * @param {number} ALPHA_WRESTLER_LIST_LIMIT - max letters to iterate (<=26)
 * @param {string}  WRESTLING_SEASON         - e.g., "2024-2025" or "2025-26"
 * @param {import('playwright').Page} page
 * @param {import('playwright').Browser} browser
 * @param {string} folder_name               - output folder (your save_to_csv_file handles it)
 * @param {string} file_name                 - base file name
 */
async function main(
  URL_LOGIN_PAGE,
  ALPHA_WRESTLER_LIST_LIMIT = 26,
  WRESTLING_SEASON = "2024-2025",
  page,
  browser,
  folder_name,
  file_name
) {
  const LOAD_TIMEOUT_MS = 30000;

  // 1) Go to season index and complete auto login / season selection
  const LOGIN_URL = URL_LOGIN_PAGE;
  await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
  await page.waitForTimeout(800);

  console.log("step 1: on index.jsp, starting auto login for season:", WRESTLING_SEASON);
  await page.evaluate(auto_login_select_season, { WRESTLING_SEASON });

  // 2) REQUIRE landing on MainFrame
  await page.waitForURL(/seasons\/MainFrame\.jsp/i, { timeout: 20000 });

  // 3) Open Wrestlers menu
  await page.evaluate(click_on_wrestler_menu);

  // 4) Wait for the Wrestlers frame and ensure a stable element is present
  const wrestlers_frame = await wait_for_wrestlers_frame(page, LOAD_TIMEOUT_MS);

  // If the search panel is collapsed, open it so inputs exist for extractor_source
  try {
    await wrestlers_frame.waitForSelector("#s_lastName, #searchButton", { timeout: 1500 });
  } catch {
    await wrestlers_frame.evaluate(() => {
      const btn = document.querySelector('#searchButton, input[type="button"][onclick*="openSearchWrestlers"]');
      if (btn) btn.click();
    });
    await wrestlers_frame.waitForSelector("#s_lastName", { timeout: 5000 });
  }

  // 5) A→Z loop across grade categories
  let headers_written = false;
  let cumulative_rows = 0;
  const delay_ms = 800;

  const LETTERS = "abcdefghijklmnopqrstuvwxyz".split("");
  const ALPHA_LIMIT = Math.min(ALPHA_WRESTLER_LIST_LIMIT, LETTERS.length);

  const GRADE_CATEGORY = ["HS Freshman", "HS Sophomore", "HS Junior", "HS Senior"];
  const LEVEL_CATEGORY = ["Varsity"]; // extend if needed

  for (let j = 0; j < GRADE_CATEGORY.length; j++) {
    for (let i = 0; i < ALPHA_LIMIT; i++) {
      const prefix = LETTERS[i];
      const grade = GRADE_CATEGORY[j];
      const level = LEVEL_CATEGORY[0];

      // 6) Run one in-page extraction step for this letter+grade
      const result = await wrestlers_frame.evaluate(extractor_source(), { prefix, grade, level, delay_ms });
      const rows = result?.rows || [];
      const wrote = rows.length;

      if (wrote > 0) {
        // write (first write can create headers; your util handles deletion/append)
        const iterationIndex = i; // 0-based index you already use
        save_to_csv_file(rows, iterationIndex, headers_written, folder_name, file_name);
        headers_written = true;
        cumulative_rows += wrote;
      }

      const pages_info = result?.pages_advanced ?? 0;
      console.log(color_text(
        `step ${j + 1}-${i + 1}: prefix="${prefix}" | grade="${grade}" | level="${level}" | pages=${pages_info + 1
        } | rows_written=${wrote} | total_rows=${cumulative_rows}`, "green"
      ));
    }
  }

  console.log(`\n✅ Wrestler list by alpha completed. Total rows=${cumulative_rows}`);

  // Close just the CDP connection (keeps your external Chrome alive if attached)
  await browser.close();
}

export { main as step_1_run_alpha_wrestler_list };

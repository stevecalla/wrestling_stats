// import path from "path";
// import { fileURLToPath } from "url";

// import dotenv from "dotenv";
// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);
// dotenv.config({ path: path.resolve(__dirname, "../.env") });

// import { URL_WRESTLERS } from "../data/input/urls_wrestlers.js";
// import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file";
// import { auto_login_select_season } from "../utilities/scraper_tasks/auto_login_select_season";
// import { click_on_wrestler_menu } from "../utilities/scraper_tasks/click_on_wrestler_menu";

// // === helper: resilient navigation that ignores TW's mid-flight redirects ===
// async function safe_wait_for_selector(frameOrPage, selector, opts = {}) {
//   try {
//     await frameOrPage.waitForSelector(selector, { state: "visible", ...opts });
//   } catch (err) {
//     const msg = String(err?.message || "");
//     if (
//       msg.includes("Target page, context or browser has been closed") ||
//       msg.includes("Frame was detached") ||
//       msg.includes("Execution context was destroyed")
//     ) {
//       err.code = "E_TARGET_CLOSED"; // sentinel for revive
//     }
//     throw err;
//   }
// }

// // === Main function to get wrestler match history ===
// function extractor_source() {
//   return async (opts = {}) => {
//     const { prefix = "a", grade, level, delay_ms = 100 } = opts;

//     // --- tiny utils ---
//     const sleep = (ms) => new Promise(r => setTimeout(r, ms));

//     const getPager = (which = "top") =>
//       document.querySelector(which === "top" ? "#dataGridNextPrev_top" : "#dataGridNextPrev_bottom")
//       || document.querySelector(".dataGridNextPrev")
//       || null;

//     const getRangeText = (which = "top") => {
//       const p = getPager(which);
//       if (!p) return "";
//       for (const s of p.querySelectorAll("span")) {
//         const t = (s.textContent || "").trim();
//         if (/^\d+\s*-\s*\d+\s+of\s+\d+$/i.test(t)) return t; // "1 - 50 of 387"
//       }
//       return "";
//     };

//     const parseRange = (which = "top") => {
//       const m = getRangeText(which).match(/(\d+)\s*-\s*(\d+)\s+of\s+(\d+)/i);
//       return m ? { start: +m[1], end: +m[2], total: +m[3] } : null;
//     };

//     const hasNextPage = (which = "top") => {
//       const r = parseRange(which);
//       return r ? r.end < r.total : false;
//     };

//     const clickNext = (which = "top") => {
//       const p = getPager(which);
//       if (!p) return false;
//       const a =
//         p.querySelector("a .dgNext")?.closest("a") ||
//         p.querySelector('a[href^="javascript:defaultGrid.next"]') ||
//         p.querySelector("a i.icon-arrow_r")?.closest("a");
//       if (!a) return false;
//       a.click();
//       return true;
//     };

//     const gridRows = () => document.querySelectorAll("tr.dataGridRow");

//     const lastRowSignature = () => {
//       const rows = gridRows();
//       const last = rows[rows.length - 1];
//       if (!last) return "";
//       const txt = last.innerText.replace(/\s+/g, " ").trim();
//       const hrefs = Array.from(last.querySelectorAll("a")).map(a => a.href).join("|");
//       return `${txt}::${hrefs}`;
//     };

//     const waitForLastRowChange = async (prevSig, { timeout_ms = 12000, poll_ms = 120 } = {}) => {
//       const start = performance.now();
//       while (performance.now() - start < timeout_ms) {
//         const now = lastRowSignature();
//         if (now && now !== prevSig) return true;
//         await sleep(poll_ms);
//       }
//       return false;
//     };

//     const ensure_page_size_50 = async (which = "top") => {
//       const p = getPager(which);
//       const input = p?.querySelector('input[type="text"][onchange*="updateLimit"]');
//       if (!input) return;
//       const current = parseInt(input.value || "0", 10);
//       if (current === 50) return;
//       const before = lastRowSignature();
//       input.value = "50";
//       input.dispatchEvent(new Event("change", { bubbles: true }));
//       await waitForLastRowChange(before).catch(() => { });
//     };

//     // --- search helpers ---
//     const openSearchPanel = async () => {
//       const btn = document.querySelector('#searchButton, input[type="button"][onclick*="openSearchWrestlers"]');
//       if (btn) btn.click();
//       await sleep(1000);
//     };

//     // NEW: select helper using visible text only
//     const setSelectByText = (selectEl, text) => {
//       if (!selectEl) return false;
//       const opts = Array.from(selectEl.options);
//       const found = opts.find(o => (o.textContent || "").trim().toLowerCase() === text.toLowerCase());
//       if (!found) return false;
//       selectEl.value = found.value;
//       selectEl.dispatchEvent(new Event("input", { bubbles: true }));
//       selectEl.dispatchEvent(new Event("change", { bubbles: true }));
//       selectEl.dispatchEvent(new Event("blur", { bubbles: true }));
//       return true;
//     };

//     const runSearchWithPrefix = async (pfx) => {
//       // insert delay to avoid server overload
//       await sleep(200 + Math.random() * 300);

//       await openSearchPanel();

//       // 1) Last name
//       const lastName = document.querySelector("#s_lastName");
//       if (!lastName) throw new Error("Last Name input #s_lastName not found");
//       lastName.focus();
//       lastName.value = pfx;
//       lastName.dispatchEvent(new Event("input", { bubbles: true }));
//       lastName.dispatchEvent(new Event("change", { bubbles: true }));

//       // 2) Grade = HS Senior (by text only)
//       const gradeSelect = document.querySelector("#s_gradeId");
//       if (gradeSelect) {
//         const ok = setSelectByText(gradeSelect, grade);
//         if (!ok) console.warn(`Option "${grade}" not found in grade dropdown.`);
//       } else {
//         console.warn("Grade select #s_gradeId not found; continuing without grade filter.");
//       }

//       // 3) Level = Varsity
//       // const levelSelect = document.querySelector("#s_levelId");
//       // if (levelSelect) {
//       //   const ok = setSelectByText(levelSelect, level);
//       //   if (!ok) console.warn(`Option "${level}" not found in level dropdown.`);
//       // } else {
//       //   console.warn("Level select #s_levelId not found; continuing without level filter.");
//       // }

//       // 4) Click inner Search
//       const go = document.querySelector(
//         'input[type="button"][onclick*="searchWrestlers"], input.segment-track[value="Search"]'
//       );
//       if (!go) throw new Error("Inner Search button not found");

//       const before = lastRowSignature();
//       go.click();
//       await waitForLastRowChange(before);
//       await ensure_page_size_50("top");
//     };

//     const readTable = () => {
//       const rows = document.querySelectorAll("tr.dataGridRow");
//       return Array.from(rows).map((row) => {
//         const cols = row.querySelectorAll("td div");
//         const txt = (i) => (cols[i]?.textContent || "").trim();
//         const href = (i) => cols[i]?.querySelector("a")?.href || null;
//         return {
//           name: txt(2),
//           name_link: href(2),
//           team: txt(3),
//           team_link: href(3),
//           weight_class: txt(4),
//           gender: txt(5),
//           grade: txt(6),
//           level: txt(7), record: txt(8),
//         };
//       });
//     };

//     // --- 1) run the search for this prefix (with grade filter) ---
//     await runSearchWithPrefix(prefix);

//     // --- 2) collect current page + all next pages ---
//     const collected = [];
//     collected.push(...readTable());

//     let pages_advanced = 0;
//     while (hasNextPage("top")) {
//       const before = lastRowSignature();
//       if (!clickNext("top")) break;
//       const changed = await waitForLastRowChange(before);
//       if (!changed) break;
//       await ensure_page_size_50("top");
//       collected.push(...readTable());
//       pages_advanced += 1;
//       await sleep(delay_ms);
//     }

//     // --- 3) return results (outer loop handles A→Z) ---
//     const now = new Date().toISOString();
//     const rows_with_meta = collected.map(r => ({ ...r, created_at_utc: now, page_url: location.href }));

//     return {
//       rows: rows_with_meta,
//       pages_advanced,
//       range_text: getRangeText("top") || ""
//     };
//   };
// }

// /**
//  * In-page helper that performs exactly ONE alpha step:
//  *  - run search with the given prefix
//  *  - paginate (200/page) and collect rows
//  *  - decide the next prefix (two-letter from last row or fallback)
//  * Returns { rows, next_prefix, pages_advanced, range_text }
//  */
// async function main(ALPHA_WRESTLER_LIST_LIMIT = 5, WRESTLING_SEASON = "2024-2025", page, browser, folder_name, file_name) {
//   const URLS = URL_WRESTLERS;
//   const LOAD_TIMEOUT_MS = 30000;

//   const LOGIN_URL = "https://www.trackwrestling.com/seasons/index.jsp";
//   await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
//   await page.waitForTimeout(2000); // small settle

//   // Step 1: select wrestling season
//   console.log("step 1: on index.jsp, starting auto login for season:", WRESTLING_SEASON);
//   const loginArgs = { WRESTLING_SEASON: WRESTLING_SEASON };
//   await page.evaluate(auto_login_select_season, loginArgs); // <-- pass the function itself
//   await page.waitForTimeout(1000);

//   // 2) land on MainFrame and open Wrestlers menu
//   await page.waitForURL(/seasons\/MainFrame\.jsp/i, { timeout: 8000 }).catch(() => { });
//   await page.evaluate(click_on_wrestler_menu);

//   await page.waitForTimeout(2000);
//   const wrestlers_frame = page.frames().find(f => /Wrestlers\.jsp/i.test(f.url())) || page.mainFrame();
//   // await wrestlers_frame.waitForSelector("#searchButton", { timeout: LOAD_TIMEOUT_MS });
//   await safe_wait_for_selector(wrestlers_frame, "#searchButton", { timeout: LOAD_TIMEOUT_MS });

//   // --- LOOP: one alpha step → write file → progress → compute next prefix
//   let headers_written = false;
//   let cumulative_rows = 0;
//   const delay_ms = 1000;

//   const LETTERS = [
//     "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
//     "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"
//   ];
//   const ALPHA_LIMIT = Math.min(ALPHA_WRESTLER_LIST_LIMIT, LETTERS.length);
//   const GRADE_CATEGORY = ["HS Freshman", "HS Sophomore", "HS Junior", "HS Senior"];
//   const LEVEL_CATEGORY = ["Varsity"];

//   // --- LOOP: cycle through each letter explicitly (a to z)
//   for (let j = 0; j < GRADE_CATEGORY.length; j++) {
//     for (let i = 0; i < ALPHA_LIMIT; i++) {
//       const prefix = LETTERS[i];
//       const grade = GRADE_CATEGORY[j];
//       const level = LEVEL_CATEGORY[0]; // Always use "Varsity"

//       const result = await wrestlers_frame.evaluate(extractor_source(), { prefix, grade, level, delay_ms });

//       const rows = result?.rows || [];
//       const wrote = rows.length;

//       if (wrote > 0) {
//         const iterationIndex = i; // 0-based
//         save_to_csv_file(rows, iterationIndex, headers_written, folder_name, file_name);
//         headers_written = true;
//         cumulative_rows += wrote;
//       }

//       const pages_info = result?.pages_advanced ?? 0;
//       console.log(`step ${j + 1}-${i + 1}: prefix="${prefix}" || grade="${grade}" || level="${level}" || pages=${pages_info + 1} || rows_written=${wrote} || total_rows=${cumulative_rows}`);
//     }
//   }

//   console.log(`\n Wrestler list by alpha successufully. Total rows=${cumulative_rows}`);

//   await browser.close(); // closes CDP connection (not your Chrome instance)
// }

// export { main as step_1_run_alpha_wrestler_list };

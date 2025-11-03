import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { wrestler_match_urls_2024_25 } from "../data_tracker_wrestling/input/wrestler_match_urls_2024_25.js";
import { wrestler_match_urls_2025_26 } from "../data_tracker_wrestling/input/wrestler_match_urls_2025_26.js";
import { save_to_csv_file } from "../utilities/create_and_load_csv_files/save_to_csv_file";
import { auto_login_select_season } from "../utilities/scraper_tasks/auto_login_select_season";

import { step_0_launch_chrome_developer } from "./step_0_launch_chrome_developer.js";

// === helper: resilient navigation that ignores TW's mid-flight redirects ===
function handlesDead({ browser, context, page }) {
  return !browser?.isConnected?.() || !context || !page || page.isClosed?.();
}

async function relogin(page, LOAD_TIMEOUT_MS, WRESTLING_SEASON) {
  const LOGIN_URL = "https://www.trackwrestling.com/seasons/index.jsp";
  await safe_goto(page, LOGIN_URL, { timeout: LOAD_TIMEOUT_MS });
  await page.waitForTimeout(1000);
  await page.evaluate(auto_login_select_season, { WRESTLING_SEASON });
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
      err.code = "E_TARGET_CLOSED";  // ← sentinel
      throw err;
    } else {
      throw err;
    }
  }
  return page.url();
}

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
      err.code = "E_TARGET_CLOSED"; // sentinel for revive
    }
    throw err;
  }
}

// === Main function to get wrestler match history ===
function extractor_source() {
  return () => {
    // === TW COLLECT (FULL LOGIC with improved start_date / end_date parsing) ===
    // Handles "12/13 - 12/14/2024" → start_date=12/13/2024, end_date=12/14/2024
    // ---------- helpers ----------
    const norm = s => (s || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").replace(/\s+/g, " ").trim();
    const lc = s => norm(s).toLowerCase();
    const escReg = s => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

    const toDate = (y, m, d) => {
      const Y = +y < 100 ? (+y + 2000) : +y;
      const mm = +m, dd = +d;
      const dt = new Date(Y, mm - 1, dd);
      return isNaN(+dt) ? null : dt;
    };
    const fmtMDY = d => {
      if (!(d instanceof Date) || isNaN(+d)) return "";
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const yy = String(d.getFullYear());
      return `${mm}/${dd}/${yy}`;
    };

    // Robustly parse date cell text into start/end
    // Supports:
    //   MM/DD - MM/DD/YYYY   (year missing on first → borrow from second)
    //   MM/DD/YYYY - MM/DD/YYYY
    //   MM/DD/YYYY
    const parseDateRangeText = (raw) => {
      const t = norm(raw);
      if (!t) return { start_date: "", end_date: "", startObj: null, endObj: null };

      // Pattern A: "MM/DD - MM/DD/YYYY"
      let m = t.match(/^(\d{1,2})[\/\-](\d{1,2})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
      if (m) {
        const [, m1, d1, m2, d2, y2] = m;
        const startObj = toDate(y2, m1, d1);
        const endObj = toDate(y2, m2, d2);
        return { start_date: fmtMDY(startObj), end_date: fmtMDY(endObj), startObj, endObj };
      }

      // Pattern B: "MM/DD/YYYY - MM/DD/YYYY"
      m = t.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\s*[-–—]\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
      if (m) {
        const [, m1, d1, y1, m2, d2, y2] = m;
        const startObj = toDate(y1, m1, d1);
        const endObj = toDate(y2, m2, d2);
        return { start_date: fmtMDY(startObj), end_date: fmtMDY(endObj), startObj, endObj };
      }

      // Pattern C: single "MM/DD/YYYY"
      m = t.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
      if (m) {
        const [, mm, dd, yy] = m;
        const d = toDate(yy, mm, dd);
        return { start_date: fmtMDY(d), end_date: "", startObj: d, endObj: null };
      }

      // Fallback: try to pick the first full date present, ignore others
      m = t.match(/(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})/);
      if (m) {
        const [token] = m;
        const [mm, dd, yy] = token.split(/[\/\-]/);
        const d = toDate(yy, mm, dd);
        return { start_date: fmtMDY(d), end_date: "", startObj: d, endObj: null };
      }

      return { start_date: "", end_date: "", startObj: null, endObj: null };
    };

    // Selected wrestler from dropdown
    const sel = document.querySelector("#wrestler");
    const selOpt = sel?.selectedOptions?.[0] || document.querySelector("#wrestler option[selected]");
    const currentId = (selOpt?.value || "").trim();
    const optText = norm(selOpt?.textContent || "");
    const currentName = optText.includes(" - ") ? optText.split(" - ").slice(1).join(" - ").trim() : optText;
    const currentNameN = lc(currentName);

    // Scrubber for non-wrestler columns
    const nameRe = new RegExp(`\\b${escReg(currentName)}\\b`, "gi");
    const scrub = s => norm((s || "").replace(nameRe, "").replace(/\s{2,}/g, " "));

    // ---------- extract rows ----------
    const rows = [];
    for (const row of document.querySelectorAll("tr.dataGridRow")) {
      const tds = row.querySelectorAll("td");
      if (tds.length < 5) continue;

      // NEW: parse start/end
      const dateStrRaw = norm(tds[1]?.innerText);
      const { start_date, end_date, startObj, endObj } = parseDateRangeText(dateStrRaw);

      const eventRaw = norm(tds[2]?.innerText);
      const weight = norm(tds[3]?.innerText);
      const detailsTextRaw = norm(tds[4]?.innerText);
      const detailsCell = tds[4];

      const round = (detailsTextRaw.match(/^(.*?)\s*-\s*/)?.[1] || "").trim();
      const isBye = /\b(received a bye|bye)\b/i.test(detailsTextRaw);
      const isUnknownForfeit = /\bover\s+Unknown\s*\(\s*For\.\s*\)/i.test(detailsTextRaw);

      // Linked names
      const linkInfos = Array.from(detailsCell.querySelectorAll('a[href*="wrestlerId="], td a'))
        .map(a => {
          const href = a.getAttribute("href") || "";
          const id = (href.match(/wrestlerId=(\d+)/) || [])[1] || "";
          const name = norm(a.textContent || "");
          return { id, name, nameN: lc(name) };
        })
        .filter(x => x.nameN);

      // Name (School) pairs
      const nameSchoolPairs = [];
      const rxNS = /([A-Z][A-Za-z'’.\- ]+)\s*\(([^)]+)\)/g;
      for (const m of detailsTextRaw.matchAll(rxNS)) {
        nameSchoolPairs.push({ name: m[1].trim(), nameN: lc(m[1]), school: m[2].trim() });
      }

      // Resolve "me"
      let me = { id: currentId, name: currentName, nameN: currentNameN };
      const meById = currentId && linkInfos.find(li => li.id === currentId);
      if (meById) me = meById;
      const meByName = linkInfos.find(li => li.nameN === currentNameN);
      if (!meById && meByName) me = meByName;

      // Candidate opponents (exclude me)
      const cand = [];
      for (const li of linkInfos) if (li.nameN && li.nameN !== me.nameN) cand.push({ id: li.id || "", name: li.name, nameN: li.nameN });
      for (const ns of nameSchoolPairs) if (ns.nameN && ns.nameN !== me.nameN && !cand.some(c => c.nameN === ns.nameN)) cand.push({ id: "", name: ns.name, nameN: ns.nameN });

      // Opponent selection with special rule
      let opponent = { id: "", name: "", nameN: "" };
      let opponentSchool = "";
      if (isUnknownForfeit && !isBye) {
        opponent = { id: "", name: "Unknown", nameN: "unknown" };
      } else {
        opponent = cand[0] || opponent;
        if (opponent.name && lc(opponent.name) === me.nameN) opponent = { id: "", name: "", nameN: "" };
        if (opponent.name && lc(opponent.name) !== "unknown") {
          const nsHit = nameSchoolPairs.find(ns => ns.nameN === lc(opponent.name));
          if (nsHit) opponentSchool = nsHit.school;
        }
      }

      // Result token and score details
      const resultToken =
        (detailsTextRaw.match(/\b(over|def\.?|maj\.?\s*dec\.?|dec(?:ision)?|tech(?:nical)?\s*fall|fall|pinned|bye)\b/i)?.[0] ||
          (isUnknownForfeit ? "over" : "")).toLowerCase();

      let scoreDetails = (detailsTextRaw.match(/\(([^()]+)\)\s*$/)?.[1] || "").trim();
      if (/^for\.?$/i.test(scoreDetails)) scoreDetails = "";
      if (lc(opponent.name) === "unknown") scoreDetails = "Unknown";
      if (resultToken === "bye" || isBye) scoreDetails = "Bye";

      // If school empty, backfill with event text; THEN strip leading "vs. " from school only
      if (!opponentSchool) opponentSchool = eventRaw;
      opponentSchool = opponentSchool.replace(/\bvs\.\s*/i, "").trim();

      // Precompute for winner detection
      const detailsLC = lc(detailsTextRaw);
      const overIdx = detailsLC.indexOf(" over ");
      const defIdx = detailsLC.indexOf(" def.");
      const tokenIdx = overIdx >= 0 ? overIdx : defIdx;
      const namesInOrder = [...detailsTextRaw.matchAll(/([A-Z][A-Za-z'’.\- ]+)\s*\(([^)]+)\)/g)].map(m => m[1].trim());

      rows.push({
        // dates
        start_date: start_date,
        end_date: end_date,
        sortDateObj: startObj || endObj || new Date(NaN),

        // raw details (new)
        raw_details: detailsTextRaw,

        // fields for outcome/winner
        event: eventRaw,
        weight,
        round,
        opponent: isBye ? "" : (opponent.name || ""),
        opponentId: isBye ? "" : (opponent.id || ""),
        opponentSchool,
        result: resultToken || (isBye ? "bye" : ""),
        score_details: scoreDetails,
        details: detailsTextRaw,
        tokenIdx,
        namesInOrder
      });
    }

    // ---------- compute W-L-T + winner (enforce consistency) ----------
    rows.sort((a, b) => (+a.sortDateObj - +b.sortDateObj) || String(a.start_date).localeCompare(String(b.start_date)));

    let W = 0, L = 0, T = 0;
    const withRecord = rows.map(r => {
      let outcome = "U";

      if (r.result === "bye") {
        outcome = "W";
      } else if (/\b(tie|draw)\b/i.test(r.details)) {
        outcome = "T";
      } else if (/\bover\s+unknown\s*\(\s*for\.\s*\)/i.test(r.details)) {
        outcome = "W";
      } else if (r.tokenIdx >= 0) {
        const before = r.details.slice(0, r.tokenIdx).toLowerCase();
        const after = r.details.slice(r.tokenIdx).toLowerCase();
        const meBefore = before.includes(currentNameN);
        const meAfter = after.includes(currentNameN);
        if (meBefore && !meAfter) outcome = "W";
        else if (!meBefore && meAfter) outcome = "L";
      }

      if (outcome === "W") W++;
      else if (outcome === "L") L++;
      else if (outcome === "T") T++;

      // Winner determination
      let winnerRaw = "";
      if (r.result === "bye" || /\bover\s+unknown\s*\(\s*for\.\s*\)/i.test(r.details)) {
        winnerRaw = currentName;
      } else if (r.tokenIdx >= 0 && r.namesInOrder.length) {
        winnerRaw = r.namesInOrder[0];
      } else if (outcome === "W") {
        winnerRaw = currentName;
      } else if (outcome === "L") {
        winnerRaw = r.opponent;
      }

      if (outcome === "L") winnerRaw = r.opponent || winnerRaw;
      if (outcome === "W") winnerRaw = currentName;
      if (outcome === "T") winnerRaw = "";

      const now_timestamp_utc = new Date().toISOString(); // created_at (UTC ISO 8601)
      return {
        page_url: location.href,
        wrestler_id: currentId,
        wrestler: currentName,

        // date columns
        start_date: r.start_date,
        end_date: r.end_date,

        event: scrub(r.event),
        weight_c: scrub(r.weight),
        round: scrub(r.round),
        opponent: scrub(r.opponent),
        school: scrub(r.opponentSchool),
        result: scrub(r.result),
        score_details: scrub(r.score_details),
        winner_name: winnerRaw,
        outcome,
        record: `${W}-${L}-${T} W-L-T`,  // keep as text for Excel

        raw_details: r.raw_details,     // NEW: preserve original text
        created_at_utc: now_timestamp_utc,             // NEW
      };
    });

    return withRecord;

  };
}

async function main(MIN_URLS = 5, WRESTLING_SEASON = "2024-25", page, browser, context, folder_name, file_name, loop_start = 0) {
  const URLS = WRESTLING_SEASON === '2024-25' ? wrestler_match_urls_2024_25 : wrestler_match_urls_2025_26;
  const LOAD_TIMEOUT_MS = 30000;
  const NO_OF_URLS = Math.min(MIN_URLS, URLS.length);
  let headersWritten = false; // stays true once header is created

  // This sets up an event listener on the Playwright browser object that fires whenever the CDP (Chrome DevTools Protocol) connection between Playwright and Chrome is lost.
  browser.on?.("disconnected", () => {
    console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
  });

  const LOGIN_URL = "https://www.trackwrestling.com/seasons/index.jsp";
  await safe_goto(page, LOGIN_URL, { timeout: LOAD_TIMEOUT_MS });
  await page.waitForTimeout(2000); // small settle

  // Step 1: select wrestling season
  console.log("step 1: on index.jsp, starting auto login for season:", WRESTLING_SEASON);
  const loginArgs = { WRESTLING_SEASON: WRESTLING_SEASON };
  await page.evaluate(auto_login_select_season, loginArgs); // <-- pass the function itself
  await page.waitForTimeout(1000);

  // Step 2: go to each URL and extract rows
  for (let i = loop_start; i < NO_OF_URLS; i++) {
    // revive handles if needed
    if (handlesDead({ browser, context, page })) {
      console.warn("♻️ Handles dead — reconnecting via step_0_launch_chrome_developer...");
      ({ browser, page, context } = await step_0_launch_chrome_developer("https://www.trackwrestling.com/"));

      // ✅ Reattach listener for the new browser instance
      browser.on?.("disconnected", () => {
        console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
      });

      await relogin(page, LOAD_TIMEOUT_MS, WRESTLING_SEASON);
    }


    const url = URLS[i];
    let attempts = 0;

    while (attempts < 2) {
      attempts++;

      try {
        const all_rows = [];

        console.log('step 2a: go to url:', url);
        await safe_goto(page, url, { timeout: LOAD_TIMEOUT_MS });

        // find frame AFTER nav
        console.log('step 2b: find target frame');
        let targetFrame = page.frames().find(f => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();

        // check redirect to seasons/index.jsp
        console.log('step 3: wait to see if redirected to seasons/index.jsp');
        await page.waitForURL(/seasons\/index\.jsp/i, { timeout: 5000 }).catch(() => { });

        if (/seasons\/index\.jsp/i.test(page.url())) {
          console.log("step 3a: on index.jsp, starting auto login for season:", WRESTLING_SEASON);
          await page.evaluate(auto_login_select_season, { WRESTLING_SEASON });
          await page.waitForTimeout(1000);
          console.log('step 3b: re-navigating to original URL after login:', url);
          await safe_goto(page, url, { timeout: LOAD_TIMEOUT_MS });
          await page.waitForTimeout(1000);
          targetFrame = page.frames().find(f => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
        }

        // bounce off MainFrame.jsp if it happens
        if (/MainFrame\.jsp/i.test(page.url())) {
          await safe_goto(page, url, { timeout: LOAD_TIMEOUT_MS });
          targetFrame = page.frames().find(f => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
        }

        // Wait for the dropdown (proof we’re on the right doc)
        console.log('step 4: wait for dropdown');
        // await targetFrame.waitForSelector("#wrestler", { timeout: LOAD_TIMEOUT_MS });
        await safe_wait_for_selector(targetFrame, "#wrestler", { timeout: LOAD_TIMEOUT_MS });

        console.log('step 5: extract rows');
        await targetFrame.waitForLoadState?.("domcontentloaded");
        await page.waitForTimeout(1000);

        // const rows = await targetFrame.evaluate(extractor_source());

        let rows;
        try {
          rows = await targetFrame.evaluate(extractor_source());
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
            console.warn("♻️ Frame died during evaluate — reconnecting and retrying once...");
            ({ browser, page, context } = await step_0_launch_chrome_developer("https://www.trackwrestling.com/"));
            browser.on?.("disconnected", () => console.warn("⚠️ CDP disconnected — Chrome was closed."));
            await relogin(page, LOAD_TIMEOUT_MS, WRESTLING_SEASON);
            await safe_goto(page, url, { timeout: LOAD_TIMEOUT_MS });
            let targetFrame = page.frames().find(f => /WrestlerMatches\.jsp/i.test(f.url())) || page.mainFrame();
            rows = await targetFrame.evaluate(extractor_source());
          } else {
            throw e;
          }
        }

        console.log(`✔ ${i} of ${NO_OF_URLS}. Rows returned: ${rows.length} rows from: ${url}`);
        all_rows.push(...rows);

        console.log('step 6: save to CSV');
        headersWritten = await save_to_csv_file(all_rows, i, headersWritten, folder_name, file_name);
        console.log(`\x1b[33m➕ Tracking headersWritten: ${headersWritten}\x1b[0m\n`);

        // success → break retry loop
        break;

      } catch (e) {
        if (e?.code === "E_TARGET_CLOSED" || e?.code === "E_GOTO_TIMEOUT") {
          const cause = e?.code === "E_GOTO_TIMEOUT" ? "navigation timeout" : "page/context/browser closed";
          console.warn(`♻️ ${cause} — reconnecting and retrying this URL once...`);

          ({ browser, page, context } = await step_0_launch_chrome_developer("https://www.trackwrestling.com/"));

          // ✅ Reattach listener for the new browser instance
          browser.on?.("disconnected", () => {
            console.warn("⚠️ CDP disconnected — Chrome was closed (manual, crash, or sleep).");
          });

          await relogin(page, LOAD_TIMEOUT_MS, WRESTLING_SEASON);

          if (attempts >= 2) throw e; // stop after retry
          continue; // retry same URL
        }
        throw e; // not one of our recoverable cases
      }
  }
}

await browser.close(); // closes CDP connection (not your Chrome instance)
}

export { main as step_3_get_wrestler_match_history };

import fs from "fs";
import path from "path";

import { URL_WRESTLERS } from "../data/input/urls_wrestlers.js";

// ================== USER CONFIG ==================
const URLS = URL_WRESTLERS;

const LOAD_TIMEOUT_MS = 30000;
const AFTER_LOAD_PAUSE_MS = 500;

// const OUTPUT_DIR = path.resolve("output");
const OUTPUT_DIR = "/Users/stevecalla/wrestling/data/output/";
const OUT_CSV = path.join(OUTPUT_DIR, "tw_matches_full.csv");

// ---------- CSV helper ----------
function to_csv_file(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const esc = v => {
    if (v == null) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [headers.join(","), ...rows.map(r => headers.map(h => esc(r[h])).join(","))].join("\n");
}

function save_to_csv_file(all_rows, iterationIndex, headersWritten) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  // Skip empty batches until we get data
  if (!Array.isArray(all_rows) || all_rows.length === 0) {
    console.log(`ℹ️ Iteration ${iterationIndex}: no data, waiting for next batch...`);
    return;
  }

  // Build CSV normally
  const csvFull = to_csv_file(all_rows);
  const lines = csvFull.split(/\r?\n/).filter(Boolean);
  const headerLine = lines.shift();  // first line
  const rowsOnly = lines.join("\n");

  // If we haven’t written headers yet, start fresh file
  if (!headersWritten) {
    fs.writeFileSync(OUT_CSV, headerLine + "\n" + rowsOnly + "\n", "utf8");
    headersWritten = true;
    console.log(`🧾 Created ${OUT_CSV} with headers + ${all_rows.length} rows`);
  } else {
    // Append only rows
    fs.appendFileSync(OUT_CSV, "\n" + rowsOnly + "\n", "utf8");
    console.log(`➕ Appended ${all_rows.length} rows → ${OUT_CSV}`);
  }
}

// === Your exact table-only extractor, wrapped to run in a frame ===
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

async function auto_login() {
  // === tw_auto_login_with_highlight_and_dryrun.js ===
  // Runs on trackwrestling_page "seasons/index.jsp" page.
  // DRY_RUN=true → preview (highlights + toasts only). DRY_RUN=false → perform clicks.

  // ---- config ----
  const DRY_RUN = false; // ← set to false to perform actions

  // ---- tiny utils ----
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  function toast(msg) {
    let tray = document.getElementById("__tw_toast_tray__");
    if (!tray) {
      tray = document.createElement("div");
      tray.id = "__tw_toast_tray__";
      Object.assign(tray.style, {
        position: "fixed", right: "12px", top: "12px", zIndex: 999999,
        display: "flex", flexDirection: "column", gap: "8px", pointerEvents: "none",
        fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
      });
      document.body.appendChild(tray);
    }
    const t = document.createElement("div");
    Object.assign(t.style, {
      background: "rgba(20,20,20,.9)", color: "#fff", padding: "8px 12px",
      borderRadius: "8px", boxShadow: "0 6px 18px rgba(0,0,0,.35)", fontSize: "12px",
      opacity: "0", transform: "translateY(-6px)", transition: "all .2s ease",
    });
    t.textContent = (DRY_RUN ? "[DRY] " : "") + msg;
    tray.appendChild(t);
    requestAnimationFrame(() => { t.style.opacity = "1"; t.style.transform = "translateY(0)"; });
    setTimeout(() => { t.style.opacity = "0"; t.style.transform = "translateY(-6px)"; }, 1600);
    setTimeout(() => t.remove(), 2000);
  }

  function highlight(el, label = "") {
    if (!el) return;
    const r = el.getBoundingClientRect();
    const box = document.createElement("div");
    Object.assign(box.style, {
      position: "fixed",
      left: `${r.left - 4}px`,
      top: `${r.top - 4}px`,
      width: `${r.width + 8}px`,
      height: `${r.height + 8}px`,
      border: "3px solid #4ade80",
      borderRadius: "8px",
      background: "rgba(74, 222, 128, 0.10)",
      boxShadow: "0 0 0 2px rgba(0,0,0,.15) inset",
      zIndex: 999998,
      pointerEvents: "none",
      transition: "opacity .25s ease",
    });

    const tag = document.createElement("div");
    Object.assign(tag.style, {
      position: "fixed",
      left: `${r.left}px`,
      top: `${Math.max(6, r.top - 22)}px`,
      padding: "2px 6px",
      background: "#111827",
      color: "#fff",
      fontSize: "11px",
      borderRadius: "6px",
      zIndex: 999999,
      pointerEvents: "none",
      boxShadow: "0 2px 8px rgba(0,0,0,.35)",
      whiteSpace: "nowrap",
    });
    tag.textContent = (DRY_RUN ? "[DRY] " : "") + label;

    document.body.appendChild(box);
    document.body.appendChild(tag);
    setTimeout(() => { box.style.opacity = "0"; tag.style.opacity = "0"; }, 1000);
    setTimeout(() => { box.remove(); tag.remove(); }, 1300);
  }

  // ---- main flow ----
  console.log("[snippet] start — DRY_RUN=", DRY_RUN);
  if (!location.href.includes("seasons/index.jsp")) {
    toast("Not on seasons/index.jsp — aborting");
    console.warn("[snippet] Not on seasons/index.jsp — aborting.");
    return;
  }

  // 1) Scroll right (defaultGrid.nextX)
  const scrollBtn = document.querySelector('a[href="javascript:defaultGrid.nextX()"]');
  if (scrollBtn) {
    highlight(scrollBtn, "Scroll right");
    toast("Click: Scroll right");
    await sleep(350);
    if (!DRY_RUN) scrollBtn.click();
    await sleep(800);
  } else {
    toast("Scroll-right arrow not found");
    console.warn("[snippet] Scroll arrow not found");
  }

  // 2) Open "2024–25 High School Boys"
  const seasonLink = Array.from(document.querySelectorAll("a[href^='javascript:seasonSelected']"))
    .find(a => (a.textContent || "").trim().includes("2024-25 High School Boys"));
  if (seasonLink) {
    highlight(seasonLink, "Open: 2024–25 High School Boys");
    toast("Open: 2024–25 High School Boys");
    await sleep(400);
    if (!DRY_RUN) seasonLink.click();
    await sleep(1500);
  } else {
    toast("Season link not found");
    console.warn("[snippet] Season link not found");
  }

  // 3) Select Colorado governing body
  const select = document.querySelector("select#gbId");
  if (select) {
    const option = Array.from(select.options)
      .find(o => /Colorado High School Activities Association/i.test(o.textContent || ""));
    highlight(select, "Select: Governing Body");
    if (option) {
      toast("Select: Colorado HS Activities Association");
      await sleep(300);
      if (!DRY_RUN) {
        select.value = option.value;
        select.dispatchEvent(new Event("change", { bubbles: true }));
      }
      await sleep(600);
    } else {
      toast("Colorado option not found");
      console.warn("[snippet] Colorado option not found");
    }
  } else {
    toast("<select id='gbId'> not found");
    console.warn("[snippet] Governing-body <select> not found");
  }

  // 4) Click Login
  const loginBtn = Array.from(document.querySelectorAll('input[type="button"][value="Login"]'))
    .find(el => (el.getAttribute("onclick") || "").includes("publicLogin"));
  if (loginBtn) {
    highlight(loginBtn, "Click: Login");
    toast("Click: Login");
    await sleep(350);
    if (!DRY_RUN) loginBtn.click();
  } else {
    toast("Login button not found");
    console.warn("[snippet] Login button not found");
  }

  toast("Done " + (DRY_RUN ? "(dry run) ✅" : "✅"));
  console.log("[snippet] done — DRY_RUN=", DRY_RUN);

}

async function step_3_get_wrestler_match_history(MIN_URLS = 5, browser, trackwrestling_page) {
  const NO_OF_URLS = Math.min(MIN_URLS, URLS.length);
  let headersWritten = false; // stays true once header is created

  for (let i = 0; i < NO_OF_URLS; i++) {
    const all_rows = [];

    const url = URLS[i];
    console.log('step 1: go to url:', url);
    await trackwrestling_page.goto(url, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });

    // Some pages embed the content in frames—find the right one
    console.log('step 2: find target frame');
    const targetFrame = trackwrestling_page.frames().find(f => /WrestlerMatches\.jsp/i.test(f.url())) || trackwrestling_page.mainFrame();

    // Wait to see if TW bounces us to seasons/index.jsp; ignore if it doesn't.
    console.log('step 3: wait to see if redirected to seasons/index.jsp');
    await trackwrestling_page.waitForURL(/seasons\/index\.jsp/i, { timeout: 5000 }).catch(() => { });

    // If we ARE on seasons/index.jsp, run the in-page auto login UI flow
    if (/seasons\/index\.jsp/i.test(trackwrestling_page.url())) {
      console.log('step 3a: on index.jsp, running auto login');
      
      const loginArgs = {WREStling_SEASON: "2024-2025"};
      await trackwrestling_page.evaluate(auto_login, loginArgs); // <-- pass the function itself

      await trackwrestling_page.waitForTimeout(1000);

      console.log('step 3b: re-navigating to original URL after login:', url);
      await trackwrestling_page.goto(url, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
    }

    // Wait for the dropdown (proof we’re on the right doc)
    console.log('step 4: wait for dropdown');
    await targetFrame.waitForSelector("#wrestler", { timeout: LOAD_TIMEOUT_MS });

    console.log('step 5: extract rows');
    await targetFrame.waitForLoadState?.("domcontentloaded");
    await trackwrestling_page.waitForTimeout(AFTER_LOAD_PAUSE_MS);

    const rows = await targetFrame.evaluate(extractor_source());
    console.log(`✔ ${i} of ${NO_OF_URLS}. Rows returned: ${rows.length} rows from: ${url}`);
    all_rows.push(...rows);

    console.log('step 6: save to CSV');
    save_to_csv_file(all_rows, i, headersWritten); // pass iteration index to determine if first run

  }

  await browser.close(); // closes CDP connection (not your Chrome instance)
}

// this code runs only when the file is executed directly
if (import.meta.url === process.argv[1] || import.meta.url === `file://${process.argv[1]}`) {
  try {
    step_3_get_wrestler_match_history().catch(err => {
      console.error(err);
      process.exit(1);
    });

  } catch (err) {
    console.error("❌ Error:", err.message);
  }
}

export { step_3_get_wrestler_match_history };

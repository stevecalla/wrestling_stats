// tw_scrape_urls_cdp.js
// Node 18+ / Playwright 1.47+
// pnpm add -D playwright  OR  npm i -D playwright

// === START CHROME WITH REMOTE DEBUGGING ===
// (do this once, in another terminal, before running this script)
// mkdir -p ~/chrome-tw-profile
// "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
//   --remote-debugging-port=9222 \
//   --user-data-dir="$HOME/chrome-tw-profile"

import { chromium } from "playwright";
import fs from "fs";
import path from "path";

const URLS = [
  // put hundreds here:
  "https://www.trackwrestling.com/seasons/WrestlerMatches.jsp?TIM=1761945162951&twSessionId=csxlguiedi&wrestlerId=30517266132",
  "https://www.trackwrestling.com/seasons/WrestlerMatches.jsp?TIM=1761945162951&twSessionId=csxlguiedi&wrestlerId=29894397132",
];

const OUT_CSV = "tw_matches_full.csv";
const CONNECT_URL = "http://localhost:9222"; // your Chrome --remote-debugging-port
const LOAD_TIMEOUT_MS = 30000;
const AFTER_LOAD_PAUSE_MS = 500;

function toCSV(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const esc = v => {
    if (v == null) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [headers.join(","), ...rows.map(r => headers.map(h => esc(r[h])).join(","))].join("\n");
}

// === Your exact table-only extractor, wrapped to run in a frame ===
function extractorSource() {
  return () => {
    const norm = s => (s || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").replace(/\s+/g, " ").trim();
    const lc = s => norm(s).toLowerCase();
    const parseMDY = s => {
      const m = (s || "").match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
      if (!m) return null;
      const mm = +m[1], dd = +m[2], yy = +m[3];
      const yyyy = yy < 100 ? yy + 2000 : yy;
      return new Date(yyyy, mm - 1, dd);
    };
    const escReg = s => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

    const sel = document.querySelector("#wrestler");
    const selOpt = sel?.selectedOptions?.[0] || document.querySelector("#wrestler option[selected]");
    const currentId = (selOpt?.value || "").trim();
    const optText = norm(selOpt?.textContent || "");
    const currentName = optText.includes(" - ") ? optText.split(" - ").slice(1).join(" - ").trim() : optText;
    const currentNameN = lc(currentName);

    const nameRe = new RegExp(`\\b${escReg(currentName)}\\b`, "gi");
    const scrub = s => norm((s || "").replace(nameRe, "").replace(/\s{2,}/g, " "));

    const rows = [];
    for (const row of document.querySelectorAll("tr.dataGridRow")) {
      const tds = row.querySelectorAll("td");
      if (tds.length < 5) continue;

      const dateStr = norm(tds[1]?.innerText);
      const eventRaw = norm(tds[2]?.innerText);
      const weight = norm(tds[3]?.innerText);
      const detailsText = norm(tds[4]?.innerText);
      const detailsCell = tds[4];

      const round = (detailsText.match(/^(.*?)\s*-\s*/)?.[1] || "").trim();
      const isBye = /\b(received a bye|bye)\b/i.test(detailsText);
      const isUnknownForfeit = /\bover\s+Unknown\s*\(\s*For\.\s*\)/i.test(detailsText);

      const linkInfos = Array.from(detailsCell.querySelectorAll('a[href*="wrestlerId="], td a'))
        .map(a => {
          const href = a.getAttribute("href") || "";
          const id = (href.match(/wrestlerId=(\d+)/) || [])[1] || "";
          const name = norm(a.textContent || "");
          return { id, name, nameN: lc(name) };
        })
        .filter(x => x.nameN);

      const nameSchoolPairs = [];
      const rxNS = /([A-Z][A-Za-z'’.\- ]+)\s*\(([^)]+)\)/g;
      for (const m of detailsText.matchAll(rxNS)) {
        nameSchoolPairs.push({ name: m[1].trim(), nameN: lc(m[1]), school: m[2].trim() });
      }

      let me = { id: currentId, name: currentName, nameN: currentNameN };
      const meById = currentId && linkInfos.find(li => li.id === currentId);
      if (meById) me = meById;
      const meByName = linkInfos.find(li => li.nameN === currentNameN);
      if (!meById && meByName) me = meByName;

      const cand = [];
      for (const li of linkInfos) if (li.nameN && li.nameN !== me.nameN) cand.push({ id: li.id || "", name: li.name, nameN: li.nameN });
      for (const ns of nameSchoolPairs) if (ns.nameN && ns.nameN !== me.nameN && !cand.some(c => c.nameN === ns.nameN)) cand.push({ id: "", name: ns.name, nameN: ns.nameN });

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

      const resultToken =
        (detailsText.match(/\b(over|def\.?|maj\.?\s*dec\.?|dec(?:ision)?|tech(?:nical)?\s*fall|fall|pinned|bye)\b/i)?.[0] ||
          (isUnknownForfeit ? "over" : "")).toLowerCase();

      let scoreDetails = (detailsText.match(/\(([^()]+)\)\s*$/)?.[1] || "").trim();
      if (/^for\.?$/i.test(scoreDetails)) scoreDetails = "";
      if (lc(opponent.name) === "unknown") scoreDetails = "Unknown";
      if (resultToken === "bye" || isBye) scoreDetails = "Bye";

      if (!opponentSchool) opponentSchool = eventRaw;
      opponentSchool = opponentSchool.replace(/\bvs\.\s*/i, "").trim();

      const detailsLC = lc(detailsText);
      const overIdx = detailsLC.indexOf(" over ");
      const defIdx = detailsLC.indexOf(" def.");
      const tokenIdx = overIdx >= 0 ? overIdx : defIdx;
      const namesInOrder = [...detailsText.matchAll(/([A-Z][A-Za-z'’.\- ]+)\s*\(([^)]+)\)/g)].map(m => m[1].trim());

      rows.push({
        dateStr,
        dateObj: parseMDY(dateStr) || new Date(NaN),
        event: eventRaw,
        weight,
        round,
        opponent: isBye ? "" : (opponent.name || ""),
        opponentId: isBye ? "" : (opponent.id || ""),
        opponentSchool,
        result: resultToken || (isBye ? "bye" : ""),
        score_details: scoreDetails,
        details: detailsText,
        tokenIdx,
        namesInOrder
      });
    }

    rows.sort((a, b) => (a.dateObj - b.dateObj) || a.dateStr.localeCompare(b.dateStr));
    let W = 0, L = 0, T = 0;
    const withRecord = rows.map(r => {
      let outcome = "U";
      if (r.result === "bye") outcome = "W";
      else if (/\b(tie|draw)\b/i.test(r.details)) outcome = "T";
      else if (/\bover\s+unknown\s*\(\s*for\.\s*\)/i.test(r.details)) outcome = "W";
      else if (r.tokenIdx >= 0) {
        const before = r.details.slice(0, r.tokenIdx).toLowerCase();
        const after = r.details.slice(r.tokenIdx).toLowerCase();
        const sel = document.querySelector("#wrestler");
        const optText = (sel?.selectedOptions?.[0]?.textContent || document.querySelector("#wrestler option[selected]")?.textContent || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").replace(/\s+/g, " ").trim();
        const meName = optText.includes(" - ") ? optText.split(" - ").slice(1).join(" - ").trim().toLowerCase() : optText.toLowerCase();
        const meBefore = before.includes(meName);
        const meAfter = after.includes(meName);
        if (meBefore && !meAfter) outcome = "W";
        else if (!meBefore && meAfter) outcome = "L";
      }

      if (outcome === "W") W++; else if (outcome === "L") L++; else if (outcome === "T") T++;

      let winnerRaw = "";
      const sel2 = document.querySelector("#wrestler");
      const optText2 = (sel2?.selectedOptions?.[0]?.textContent || document.querySelector("#wrestler option[selected]")?.textContent || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").replace(/\s+/g, " ").trim();
      const meFull = optText2.includes(" - ") ? optText2.split(" - ").slice(1).join(" - ").trim() : optText2;

      if (r.result === "bye" || /\bover\s+unknown\s*\(\s*for\.\s*\)/i.test(r.details)) {
        winnerRaw = meFull;
      } else if (r.tokenIdx >= 0 && r.namesInOrder.length) {
        winnerRaw = r.namesInOrder[0];
      } else if (outcome === "W") {
        winnerRaw = meFull;
      } else if (outcome === "L") {
        winnerRaw = r.opponent;
      }
      if (outcome === "L") winnerRaw = r.opponent || winnerRaw;
      if (outcome === "W") winnerRaw = meFull;
      if (outcome === "T") winnerRaw = "";

      // Scrub using currentName
      const currentName = meFull;
      const nameRe = new RegExp(`\\b${currentName.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&")}\\b`, "gi");
      const scrub = s => (s || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").replace(/\s+/g, " ").trim().replace(nameRe, "").replace(/\s{2,}/g, " ");

      return {
        page_url: location.href,
        wrestler_id: sel2?.value?.trim() || "",
        wrestler: meFull,
        date: r.dateStr,
        event: scrub(r.event),
        wt: scrub(r.weight),
        round: scrub(r.round),
        opponent: scrub(r.opponent),
        school: scrub(r.opponentSchool),
        result: scrub(r.result),
        score_details: scrub(r.score_details),
        winner_name: winnerRaw,
        outcome,
        record: `${W}-${L}-${T} W-L-T`,
      };
    });

    return withRecord;
  };
}

async function run() {
  // Connect to the already-open Chrome
  const browser = await chromium.connectOverCDP(CONNECT_URL);
  const contexts = browser.contexts();
  if (!contexts.length) throw new Error("No contexts from CDP connection.");
  const context = contexts[0]; // use the default profile

  const page = await context.newPage();
  page.setDefaultTimeout(LOAD_TIMEOUT_MS);

  const allRows = [];

  for (const url of URLS) {
    // Navigate directly; keep it simple
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });

    // Some pages embed the content in frames—find the right one
    const targetFrame = page.frames().find(f =>
      /WrestlerMatches\.jsp/i.test(f.url())
    ) || page.mainFrame();

    // Wait for the dropdown (proof we’re on the right doc)
    await targetFrame.waitForSelector("#wrestler", { timeout: LOAD_TIMEOUT_MS });

    // Prefer table rows like your original code
    // If table rows are slow, still proceed—extractor just returns 0
    await targetFrame.waitForLoadState?.("domcontentloaded");
    // Slight settle
    await page.waitForTimeout(AFTER_LOAD_PAUSE_MS);

    const rows = await targetFrame.evaluate(extractorSource());
    console.log(`✔ ${rows.length} rows from: ${url}`);
    allRows.push(...rows);
  }

  // Write CSV
  // const csv = toCSV(allRows);
  // fs.writeFileSync(OUT_CSV, csv, "utf8");
  // console.log(`\nSaved ${allRows.length} rows → ${OUT_CSV}`);

  // Directory for output
  const OUTPUT_DIR = path.resolve("output");
  const OUT_CSV = path.join(OUTPUT_DIR, "tw_matches_full.csv");

  // === WRITE CSV TO ./output ===
  const csv = toCSV(allRows);
  fs.writeFileSync(OUT_CSV, csv, "utf8");
  console.log(`\n✅ Saved ${allRows.length} rows → ${OUT_CSV}`);


  // Ensure directory exists
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }


  await browser.close(); // only closes CDP pipe, not your Chrome
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});

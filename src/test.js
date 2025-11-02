function extractor_source_new() {
  return () => {
    // === TW COLLECT (FULL LOGIC + raw_details + created_at) ===
    // Handles "12/13 - 12/14/2024" → start_date=12/13/2024, end_date=12/14/2024

    (() => {
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

        // Fallback: pick the first full date present, ignore others
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

      // Scrubber for non-wrestler columns (display-only)
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
        const detailsTextRaw = norm(tds[4]?.innerText); // keep ORIGINAL for raw_details
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
          start_date,
          end_date,
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
          const meBefore = before.includes(lc(currentName));
          const meAfter = after.includes(lc(currentName));
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

        const nowISO = new Date().toISOString(); // created_at (UTC ISO 8601)
        return {
          page_url: location.href,
          wrestler_id: currentId,
          wrestler: currentName,

          start_date: r.start_date,
          end_date: r.end_date,

          event: scrub(r.event),
          wt: scrub(r.weight),
          round: scrub(r.round),
          opponent: scrub(r.opponent),
          school: scrub(r.opponentSchool),
          result: scrub(r.result),
          score_details: scrub(r.score_details),
          winner_name: winnerRaw,
          outcome,
          record: `${W}-${L}-${T} W-L-T`,  // keep as text for Excel

          raw_details: r.raw_details,     // NEW: preserve original text
          created_at: nowISO,             // NEW
        };
      });

      // // Append to localStorage
      // const KEY = "tw_matches_full_v1";
      // const prior = JSON.parse(localStorage.getItem(KEY) || "[]");
      // const combined = prior.concat(withRecord);
      // localStorage.setItem(KEY, JSON.stringify(combined));

      // console.table(withRecord);
      // console.log(`[TW] +${withRecord.length} rows from this page. Stored total: ${combined.length} (key=${KEY}).`);

      return withRecord;
    })();
  };
}

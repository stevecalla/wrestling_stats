// if url contains seasons/MainFrame.jsp?
// then click on wrestler menu
// <a href="javascript:changeFrame('Wrestlers.jsp?TIM=1762035889765&amp;twSessionId=csxlguiedi')">Wrestlers	</a>

// === Function to login automatically by selecting the correct season ===
async function click_on_wrestler_menu() {
  // === tw_mainframe_open_wrestlers.js ===
  // Runs on TrackWrestling "seasons/MainFrame.jsp" (frameset)
  // Finds and clicks the Wrestlers menu across frames.

    const DRY_RUN = false; // set true to preview without clicking
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));

    function toast(msg) {
      let tray = document.getElementById("__tw_toast_tray__");
      if (!tray) {
        tray = document.createElement("div");
        tray.id = "__tw_toast_tray__";
        Object.assign(tray.style, {
          position: "fixed", right: "12px", top: "12px", zIndex: 2147483647,
          display: "flex", flexDirection: "column", gap: "8px", pointerEvents: "none",
          fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
        });
        document.body.appendChild(tray);
      }
      const t = document.createElement("div");
      Object.assign(t.style, {
        background: "rgba(20,20,20,.92)", color: "#fff", padding: "8px 12px",
        borderRadius: "8px", boxShadow: "0 6px 18px rgba(0,0,0,.35)", fontSize: "12px",
        opacity: "0", transform: "translateY(-6px)", transition: "all .2s ease",
      });
      t.textContent = (DRY_RUN ? "[DRY] " : "") + msg;
      tray.appendChild(t);
      requestAnimationFrame(() => { t.style.opacity = "1"; t.style.transform = "translateY(0)"; });
      setTimeout(() => { t.style.opacity = "0"; t.style.transform = "translateY(-6px)"; }, 1600);
      setTimeout(() => t.remove(), 2000);
    }

    function highlight(el, label = "Wrestlers") {
      if (!el || el.ownerDocument !== document) return; // only highlight if same doc
      const r = el.getBoundingClientRect();
      const box = document.createElement("div");
      Object.assign(box.style, {
        position: "fixed", left: `${r.left - 4}px`, top: `${r.top - 4}px`,
        width: `${r.width + 8}px`, height: `${r.height + 8}px`,
        border: "3px solid #4ade80", borderRadius: "8px",
        background: "rgba(74,222,128,0.10)", boxShadow: "0 0 0 2px rgba(0,0,0,.15) inset",
        zIndex: 2147483646, pointerEvents: "none", transition: "opacity .25s ease",
      });
      const tag = document.createElement("div");
      Object.assign(tag.style, {
        position: "fixed", left: `${r.left}px`, top: `${Math.max(6, r.top - 22)}px`,
        padding: "2px 6px", background: "#111827", color: "#fff", fontSize: "11px",
        borderRadius: "6px", zIndex: 2147483647, pointerEvents: "none",
        boxShadow: "0 2px 8px rgba(0,0,0,.35)", whiteSpace: "nowrap",
      });
      tag.textContent = (DRY_RUN ? "[DRY] " : "") + label;
      document.body.appendChild(box); document.body.appendChild(tag);
      setTimeout(() => { box.style.opacity = "0"; tag.style.opacity = "0"; }, 1000);
      setTimeout(() => { box.remove(); tag.remove(); }, 1300);
    }

    // Search all same-origin frames for an element or function
    async function findInAllFrames(testFn, { timeout = 8000, pollMs = 250 } = {}) {
      const start = performance.now();
      while (performance.now() - start < timeout) {
        const queue = [window];
        while (queue.length) {
          const w = queue.shift();
          try {
            const doc = w.document;
            const res = testFn(doc, w);
            if (res) return res;
            for (let i = 0; i < w.frames.length; i++) queue.push(w.frames[i]);
          } catch { /* cross-origin frames ignored */ }
        }
        await sleep(pollMs);
      }
      return null;
    }

    // Ensure we’re on MainFrame.jsp (frameset shell)
    if (!/seasons\/MainFrame\.jsp/i.test(location.href)) {
      toast("Not on seasons/MainFrame.jsp — aborting");
      console.warn("[snippet] Not on seasons/MainFrame.jsp");
      return;
    }

    toast("Scanning frames for Wrestlers link…");

    // 1) Try to find an <a href="javascript:changeFrame('Wrestlers.jsp?...')">
    let wrestlersAnchor = await findInAllFrames((doc) => {
      return Array.from(doc.querySelectorAll("a[href^='javascript:changeFrame']"))
        .find(a => /Wrestlers\.jsp/i.test(a.getAttribute("href") || ""));
    });

    // 2) Fallback: anchor whose text includes "Wrestlers"
    if (!wrestlersAnchor) {
      wrestlersAnchor = await findInAllFrames((doc) => {
        return Array.from(doc.querySelectorAll("a")).find(a => /Wrestlers/i.test(a.textContent || ""));
      });
    }

    // 3) If still not found, call changeFrame directly if exposed
    if (!wrestlersAnchor) {
      const fnWin = await findInAllFrames((doc, win) => (typeof win.changeFrame === "function" ? win : null), { timeout: 4000 });
      if (fnWin) {
        toast("Calling changeFrame('Wrestlers.jsp') directly");
        if (!DRY_RUN) fnWin.changeFrame("Wrestlers.jsp");
        return;
      }
      toast("Wrestlers not found in any frame");
      console.warn("[snippet] Wrestlers not found");
      return;
    }

    // Click in the frame that owns the anchor
    const ownerWin = wrestlersAnchor.ownerDocument?.defaultView;
    if (ownerWin === window) {
      highlight(wrestlersAnchor, "Open: Wrestlers");
    } else {
      toast("Found Wrestlers in another frame");
    }
    await sleep(250);
    if (!DRY_RUN) wrestlersAnchor.click();
    toast("Wrestlers clicked ✅");

}

export { click_on_wrestler_menu };
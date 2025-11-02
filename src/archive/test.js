  // // 1) Scroll right (defaultGrid.nextX)
  // if (WRESTLING_SEASON === "2024-2025") {
  //   toast("WRESTLING_SEASON = 2024-2025");
  //   const scrollBtn = document.querySelector('a[href="javascript:defaultGrid.nextX()"]');
  //   if (scrollBtn) {
  //     highlight(scrollBtn, "Scroll right");
  //     toast("Click: Scroll right");
  //     await sleep(350);
  //     if (!DRY_RUN) scrollBtn.click();
  //     await sleep(800);
  //   } else {
  //     toast("Scroll-right arrow not found");
  //     console.warn("[snippet] Scroll arrow not found");
  //   }

  //   // 2) Open "2024–25 High School Boys"
  //   const seasonLink = Array.from(document.querySelectorAll("a[href^='javascript:seasonSelected']"))
  //     .find(a => (a.textContent || "").trim().includes("2024-25 High School Boys"));
  //   if (seasonLink) {
  //     highlight(seasonLink, "Open: 2024–25 High School Boys");
  //     toast("Open: 2024–25 High School Boys");
  //     await sleep(400);
  //     if (!DRY_RUN) seasonLink.click();
  //     await sleep(1500);
  //   } else {
  //     toast("Season link not found");
  //     console.warn("[snippet] Season link not found");
  //   }
  // } else if (WRESTLING_SEASON === "2025-2026") {
  //   // 2) Open "2025–2026 High School Boys"
  //   toast("WRESTLING_SEASON = 2025-2026");
  //   const seasonLink = Array.from(document.querySelectorAll("a[href^='javascript:seasonSelected']"))
  //     .find(a => (a.textContent || "").trim().includes("2025-26 High School Boys"));
  //   if (seasonLink) {
  //     highlight(seasonLink, "Open: 2025–26 High School Boys");
  //     toast("Open: 2025–26 High School Boys");
  //     await sleep(400);
  //     if (!DRY_RUN) seasonLink.click();
  //     await sleep(1500);
  //   } else {
  //     toast("Season link not found");
  //     console.warn("[snippet] Season link not found");
  //   }
  // }
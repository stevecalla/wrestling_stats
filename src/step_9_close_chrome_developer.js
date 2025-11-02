import { exec } from "child_process";

async function step_9_close_chrome_dev(browser, context) {
  try {
    if (context) await context.close();    // closes all pages
    if (browser) await browser.close();    // disconnects from process

    // On macOS/Linux — close only the Chrome process launched with your specific profile dir
    const userDataDir = process.env.STORE_CHROME_DATA;
    exec(`pkill -f "${userDataDir}"`, (err) => {
      if (err) console.warn("⚠️ Could not close Chrome Dev instance:", err.message);
      else console.log("🧹 Closed Chrome Dev instance cleanly.");
    });
  } catch (e) {
    console.error("Failed to close Chrome Dev:", e);
  }
}

export { step_9_close_chrome_dev };
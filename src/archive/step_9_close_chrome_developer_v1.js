import { exec } from "child_process";
import os from "os";
import path from "path";

async function step_9_close_chrome_dev(browser, context) {
  try {
    if (context) await context.close();    // closes all pages
    if (browser) await browser.close();    // disconnects from process

    // On macOS/Linux — close only the Chrome process launched with your specific profile dir
    const userDataDir = path.join(os.homedir(), "chrome-tw-user-data");

    exec(`pkill -f "${userDataDir}"`, (err) => {
      if (err) console.warn("⚠️ Could not close Chrome Dev instance:", err.message);
      else console.log("🧹 Closed Chrome Dev instance cleanly.");
    });
  } catch (e) {
    console.error("Failed to close Chrome Dev:", e);
  }
}

// step_9_close_chrome_dev();

export { step_9_close_chrome_dev };
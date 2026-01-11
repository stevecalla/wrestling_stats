// step_0_launch_chrome_developer.js (ESM, modular, cross-platform, saves profile at ~/chrome-tw-user-data)
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { chromium } from "playwright";

// import all three launchers
import { launch_chrome_win_v3 } from "../../utilities/chrome_dev_tools/launch_chrome_win_parallel_scrape_v3.js";
import { launch_chrome_linux } from "../../utilities/chrome_dev_tools/launch_chrome_linux.js";
import { launch_chrome_mac } from "../../utilities/chrome_dev_tools/launch_chrome_mac.js";

import { step_19_close_chrome_dev } from "../step_19_close_chrome_developer.js";

async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

async function is_chrome_dev_up(CONNECT_URL) {

  const url = `${CONNECT_URL}/json/version`;

  try {
    const res = await fetch(url, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

async function go_to_website_in_chrome(CONNECT_URL, URL, LOAD_TIMEOUT_MS) { 
  const browser = await chromium.connectOverCDP(CONNECT_URL, { timeout: LOAD_TIMEOUT_MS });

  const contexts = browser.contexts();
  if (!contexts.length) throw new Error("No contexts from CDP connection.");
  const context = contexts[0];

  const page = await context.newPage();

  page.setDefaultTimeout(LOAD_TIMEOUT_MS);
  if (URL) {
    await page.goto(URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
    await page.waitForTimeout(2000);
  }
  return { browser, page, context };
}

async function main(URL = process.env.TARGET_URL || "https://www.google.com", PORT) {

  // const PORT = String(process.env.CHROME_DEVTOOLS_PORT || 9222);
  const CONNECT_URL = `http://localhost:${PORT}`;
  const LOAD_TIMEOUT_MS = 45000;
  // const LOAD_TIMEOUT_MS = 30000;

  const platform = process.platform;
  console.log(`[INFO] Detected platform → ${platform}`);

  // If DevTools is already up, skip launching and just connect
  if (await is_chrome_dev_up(CONNECT_URL)) {
    console.log(`[CDP] Chrome DevTools already listening on ${PORT}. Connecting…`);
    return go_to_website_in_chrome(CONNECT_URL, URL, LOAD_TIMEOUT_MS);
  }

  // --- unified default profile dir in the user's home (~ works on all OS) ---
  const USER_DATA_DIR_DEFAULT = path.join(
    os.homedir(),
    `chrome-tw-user-data-${PORT}` // adding port so each chrome instance has a unique user profile
  );
  fs.mkdirSync(USER_DATA_DIR_DEFAULT, { recursive: true });

  console.log(`[INFO] Using Chrome profile dir → ${USER_DATA_DIR_DEFAULT}`);
  console.log(`[INFO] DevTools port → ${PORT}`);

  // Delegate launch to the per-OS helpers (they will read STORE_CHROME_DATA we just set)
  if (platform === "win32") {
    await launch_chrome_win_v3(URL, USER_DATA_DIR_DEFAULT, PORT);
  } else if (platform === "linux") {
    await launch_chrome_linux(URL, USER_DATA_DIR_DEFAULT, PORT);
  } else if (platform === "darwin") {
    await launch_chrome_mac(URL, USER_DATA_DIR_DEFAULT, PORT);
  } else {
    console.warn(`[WARN] Unsupported platform: ${platform}.`);
    return;
  }

  // Wait for Chrome to become available, then connect
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (await is_chrome_dev_up(CONNECT_URL)) {
      console.log(`[CDP] Chrome DevTools active on ${PORT}. Connecting via Playwright…`);
      return go_to_website_in_chrome(CONNECT_URL, URL, LOAD_TIMEOUT_MS);
    }
    await wait(400);
  }


  throw new Error(`Timed out waiting for Chrome DevTools on port ${PORT}.`);
}

// step_0_launch_chrome_developer_v3();

export { main as step_0_launch_chrome_developer_v3 };

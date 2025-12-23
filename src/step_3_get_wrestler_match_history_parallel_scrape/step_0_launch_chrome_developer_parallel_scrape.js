// src/step_3_get_wrestler_match_history_parallel_scrape/step_0_launch_chrome_developer_parallel_scrape.js
// (ESM, modular, cross-platform, saves profile at ~/chrome-tw-user-data)
//
// ✅ Improvements:
// - Reuse an existing page in the persistent CDP context (avoid tab explosion)
// - Set both default timeout + default navigation timeout
// - Fallback to waitUntil="commit" if domcontentloaded is flaky/slow
// - Add opts.force_relaunch to avoid reconnecting to a zombie Chrome after CDP disconnects/timeouts

import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

import { chromium } from "playwright";

// Windows launcher (parallel scrape variant)
import { launch_chrome_win } from "../../utilities/chrome_dev_tools/launch_chrome_win_parallel_scrape.js";
import { launch_chrome_linux } from "../../utilities/chrome_dev_tools/launch_chrome_linux_parallel_scrape.js";
import { launch_chrome_mac } from "../../utilities/chrome_dev_tools/launch_chrome_mac_parallel_scrape.js";

// ---- small helpers ----
async function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// NOTE: take a fully qualified URL string (e.g. `${CONNECT_URL}/json/version`)
async function is_chrome_dev_up(url) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

async function go_to_website_in_chrome(URL, CONNECT_URL, LOAD_TIMEOUT_MS) {
  const browser = await chromium.connectOverCDP(CONNECT_URL);

  const contexts = browser.contexts();
  if (!contexts.length) throw new Error("No contexts from CDP connection.");
  const context = contexts[0];

  // ✅ reuse an existing page if possible (persistent context often already has one)
  let page = context.pages()[0];
  if (!page) page = await context.newPage();

  // ✅ set BOTH (Playwright separates these)
  page.setDefaultTimeout(LOAD_TIMEOUT_MS);
  page.setDefaultNavigationTimeout(LOAD_TIMEOUT_MS);

  if (URL) {
    try {
      await page.bringToFront().catch(() => {});
      await page.goto(URL, {
        waitUntil: "domcontentloaded",
        timeout: LOAD_TIMEOUT_MS,
      });
    } catch (e) {
      // ✅ fallback: "commit" is much harder to deadlock on slow/blocked resources
      console.warn(
        `[WARN] goto(domcontentloaded) failed; retry waitUntil=commit: ${e?.message || e}`
      );
      await page.goto(URL, { waitUntil: "commit", timeout: LOAD_TIMEOUT_MS });
    }

    await page.waitForTimeout(500);
  }

  return { browser, page, context };
}

/**
 * Main launcher / connector
 *
 * @param {string} URL - Target URL to open (optional)
 * @param {string|number} PORT - DevTools port to use (required)
 * @param {{ force_relaunch?: boolean }} opts - if true, do NOT reuse an existing listening port
 */
async function main(
  URL = process.env.TARGET_URL || "https://www.google.com",
  PORT,
  opts = {}
) {
  const { force_relaunch = false } = opts;

  if (!PORT) {
    throw new Error(
      `[ERR] DevTools PORT is required. Pass PORT or set env (e.g. CHROME_DEVTOOLS_PORT).`
    );
  }

  const CONNECT_URL = `http://localhost:${PORT}`;
  const LOAD_TIMEOUT_MS = 30000;

  const platform = process.platform;
  console.log(`[INFO] Detected platform → ${platform}`);

  // --- unified default profile dir in the user's home (~ works on all OS) ---
  const USER_DATA_DIR_DEFAULT = path.join(os.homedir(), "chrome-tw-user-data");
  fs.mkdirSync(USER_DATA_DIR_DEFAULT, { recursive: true });

  console.log(`[INFO] Using Chrome profile dir → ${USER_DATA_DIR_DEFAULT}`);
  console.log(`[INFO] DevTools port → ${PORT}`);

  const version_endpoint = `${CONNECT_URL}/json/version`;

  // ✅ Only reuse existing DevTools session if NOT forcing relaunch
  if (!force_relaunch && (await is_chrome_dev_up(version_endpoint))) {
    console.log(
      `[CDP] Chrome DevTools already listening on ${PORT}. Connecting…`
    );
    return go_to_website_in_chrome(URL, CONNECT_URL, LOAD_TIMEOUT_MS);
  }

  // Delegate launch to the per-OS helpers (we pass USER_DATA_DIR_DEFAULT + PORT)
  if (platform === "win32") {
    await launch_chrome_win(URL, USER_DATA_DIR_DEFAULT, PORT);
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
    if (await is_chrome_dev_up(version_endpoint)) {
      console.log(
        `[CDP] Chrome DevTools active on ${PORT}. Connecting via Playwright…`
      );
      return go_to_website_in_chrome(URL, CONNECT_URL, LOAD_TIMEOUT_MS);
    }
    await wait(400);
  }

  throw new Error(`Timed out waiting for Chrome DevTools on port ${PORT}.`);
}

export { main as step_0_launch_chrome_developer_parallel_scrape };

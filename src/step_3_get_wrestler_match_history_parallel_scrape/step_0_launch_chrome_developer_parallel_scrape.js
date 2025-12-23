// src/step_3_get_wrestler_match_history_parallel_scrape/step_0_launch_chrome_developer_parallel_scrape.js
// (ESM, modular, cross-platform)
//
// ✅ Improvements:
// - CDP health check to avoid zombie listeners on Linux
// - Retry connect a couple times before giving up
// - force_relaunch triggers OS launch helper (best-effort), then connect+health check
// - Reuse existing page in persistent CDP context (avoid tab explosion)
// - Set both default timeout + default navigation timeout
// - Fallback to waitUntil="commit" if domcontentloaded is flaky/slow

import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

import { chromium } from "playwright";

// OS launchers (parallel scrape variants)
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

// a little stronger: make sure /json/list is parseable
async function is_chrome_dev_healthy(connect_url) {
  try {
    const v = await fetch(`${connect_url}/json/version`, { cache: "no-store" });
    if (!v.ok) return false;

    const l = await fetch(`${connect_url}/json/list`, { cache: "no-store" });
    if (!l.ok) return false;

    await l.json().catch(() => null); // parse probe
    return true;
  } catch {
    return false;
  }
}

function looks_like_cdp_zombie_error(e) {
  const msg = String(e?.message || "");
  return (
    msg.includes("Frame has been detached") ||
    msg.includes("Frame was detached") ||
    msg.includes("Execution context was destroyed") ||
    msg.includes("Target page, context or browser has been closed") ||
    msg.includes("CDP connection closed") ||
    msg.includes("WebSocket is not open") ||
    msg.includes("Protocol error") ||
    msg.includes("session closed")
  );
}

async function connect_and_prepare_page(URL, CONNECT_URL, LOAD_TIMEOUT_MS) {
  const browser = await chromium.connectOverCDP(CONNECT_URL);

  const contexts = browser.contexts();
  if (!contexts.length) {
    try { await browser.close(); } catch {}
    throw new Error("No contexts from CDP connection.");
  }

  const context = contexts[0];

  // ✅ reuse an existing page if possible (persistent context often already has one)
  let page = context.pages()[0];
  if (!page) page = await context.newPage();

  // ✅ set BOTH (Playwright separates these)
  page.setDefaultTimeout(LOAD_TIMEOUT_MS);
  page.setDefaultNavigationTimeout(LOAD_TIMEOUT_MS);

  // ---- CDP HEALTH CHECK ----
  // On Linux, "DevTools is up" can still be a zombie. This forces real activity.
  // If this fails with detach/target-closed, caller should retry/relaunch.
  try {
    if (URL) {
      await page.bringToFront().catch(() => {});
      await page.goto(URL, { waitUntil: "commit", timeout: 15000 });
      await page.waitForTimeout(250);
    } else {
      // minimal no-op that still touches the session
      await page.waitForTimeout(50);
    }
  } catch (e) {
    const msg = String(e?.message || e);
    try { await browser.close(); } catch {}
    const err = new Error(`CDP health check failed: ${msg}`);
    err.code = "E_CDP_ZOMBIE";
    throw err;
  }

  // If caller wants domcontentloaded, do best-effort after health check
  if (URL) {
    try {
      await page.goto(URL, {
        waitUntil: "domcontentloaded",
        timeout: LOAD_TIMEOUT_MS,
      });
    } catch (e) {
      console.warn(
        `[WARN] goto(domcontentloaded) failed; retry waitUntil=commit: ${e?.message || e}`
      );
      await page.goto(URL, { waitUntil: "commit", timeout: LOAD_TIMEOUT_MS });
    }
    await page.waitForTimeout(250);
  }

  return { browser, page, context };
}

async function connect_with_retries(URL, CONNECT_URL, LOAD_TIMEOUT_MS, attempts = 2) {
  let last_err = null;

  for (let i = 1; i <= attempts; i++) {
    try {
      return await connect_and_prepare_page(URL, CONNECT_URL, LOAD_TIMEOUT_MS);
    } catch (e) {
      last_err = e;
      const msg = String(e?.message || e);

      if (e?.code === "E_CDP_ZOMBIE" || looks_like_cdp_zombie_error(e)) {
        console.warn(`[WARN] CDP connect attempt ${i}/${attempts} failed (zombie-ish): ${msg}`);
        await wait(600);
        continue;
      }

      // non-zombie errors should surface immediately
      throw e;
    }
  }

  throw last_err || new Error("CDP connect failed after retries.");
}

/**
 * Main launcher / connector
 *
 * @param {string} URL - Target URL to open (optional)
 * @param {string|number} PORT - DevTools port to use (required)
 * @param {{ force_relaunch?: boolean }} opts - if true, attempt OS launch even if port already responds
 */
async function main(
  URL = process.env.TARGET_URL || "https://www.trackwrestling.com/",
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

  console.log(`[INFO] Using Chrome profile base dir → ${USER_DATA_DIR_DEFAULT}`);
  console.log(`[INFO] DevTools port → ${PORT}`);

  const version_endpoint = `${CONNECT_URL}/json/version`;

  // If DevTools is already listening and we are NOT forcing relaunch:
  // connect + CDP health check (this is the key fix for Linux zombies)
  if (!force_relaunch && (await is_chrome_dev_up(version_endpoint))) {
    console.log(`[CDP] Chrome DevTools already listening on ${PORT}. Connecting…`);

    // tiny readiness probe for /json/list, etc (best-effort)
    const healthy_http = await is_chrome_dev_healthy(CONNECT_URL).catch(() => false);
    if (!healthy_http) {
      console.warn(`[WARN] DevTools responds but /json/list not healthy yet; waiting briefly…`);
      await wait(500);
    }

    return await connect_with_retries(URL, CONNECT_URL, LOAD_TIMEOUT_MS, 2);
  }

  // Otherwise, try to launch via per-OS helper (best-effort)
  if (platform === "win32") {
    await launch_chrome_win(URL, USER_DATA_DIR_DEFAULT, PORT);
  } else if (platform === "linux") {
    // ✅ headless desired
    await launch_chrome_linux(URL, USER_DATA_DIR_DEFAULT, PORT, { headless: true });
  } else if (platform === "darwin") {
    await launch_chrome_mac(URL, USER_DATA_DIR_DEFAULT, PORT);
  } else {
    console.warn(`[WARN] Unsupported platform: ${platform}.`);
    return;
  }

  // Wait for Chrome to become available, then connect+health check
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (await is_chrome_dev_up(version_endpoint)) {
      console.log(`[CDP] Chrome DevTools active on ${PORT}. Connecting via Playwright…`);
      try {
        return await connect_with_retries(URL, CONNECT_URL, LOAD_TIMEOUT_MS, 3);
      } catch (e) {
        const msg = String(e?.message || e);
        console.warn(`[WARN] connect/health failed after launch attempt: ${msg}`);
        await wait(800);
      }
    }
    await wait(400);
  }

  // Helpful Linux guidance when a port is held by a bad instance
  if (platform === "linux") {
    throw new Error(
      `Timed out waiting for healthy Chrome DevTools on port ${PORT}.\n` +
        `If the port is held by a zombie Chrome, kill it and retry:\n` +
        `  pkill -f "remote-debugging-port=${PORT}" || true\n` +
        `  pkill -f chrome || true\n`
    );
  }

  throw new Error(`Timed out waiting for Chrome DevTools on port ${PORT}.`);
}

export { main as step_0_launch_chrome_developer_parallel_scrape };

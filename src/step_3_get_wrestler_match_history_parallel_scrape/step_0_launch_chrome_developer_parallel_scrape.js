// src/step_3_get_wrestler_match_history_parallel_scrape/step_0_launch_chrome_developer_parallel_scrape.js
// (ESM, modular, cross-platform)

import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

import { chromium } from "playwright";

import { launch_chrome_win } from "../../utilities/chrome_dev_tools/launch_chrome_win_parallel_scrape.js";
import { launch_chrome_linux } from "../../utilities/chrome_dev_tools/launch_chrome_linux_parallel_scrape.js";
import { launch_chrome_mac } from "../../utilities/chrome_dev_tools/launch_chrome_mac_parallel_scrape.js";

async function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function wait_for_cdp_ws(connect_url, max_wait_ms = 15000) {
  const deadline = Date.now() + max_wait_ms;

  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${connect_url}/json/version`, { cache: "no-store" });
      if (res.ok) {
        const j = await res.json().catch(() => null);
        if (j?.webSocketDebuggerUrl) return true;
      }
    } catch { }
    await wait(250);
  }

  return false;
}

async function fetch_ok(url) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

async function is_chrome_dev_up(connect_url) {
  return fetch_ok(`${connect_url}/json/version`);
}

async function is_chrome_dev_healthy(connect_url) {
  try {
    const v = await fetch(`${connect_url}/json/version`, { cache: "no-store" });
    if (!v.ok) return false;

    const l = await fetch(`${connect_url}/json/list`, { cache: "no-store" });
    if (!l.ok) return false;

    await l.json().catch(() => null);
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
    msg.includes("session closed") ||
    msg.includes("Session closed")
  );
}

async function connect_and_prepare_page(URL, CONNECT_URL, LOAD_TIMEOUT_MS) {
  const ok = await wait_for_cdp_ws(CONNECT_URL, 15000);
  if (!ok) {
    throw new Error(`CDP preflight failed: webSocketDebuggerUrl not ready at ${CONNECT_URL}`);
  }

  const browser = await chromium.connectOverCDP(CONNECT_URL, { timeout: 90000 });

  const contexts = browser.contexts();
  if (!contexts.length) {
    try {
      await browser.close();
    } catch { }
    throw new Error("No contexts from CDP connection.");
  }

  const context = contexts[0];

  // Reuse existing page if possible (persistent context usually has one)
  let page = context.pages()[0];
  if (!page) page = await context.newPage();

  page.setDefaultTimeout(LOAD_TIMEOUT_MS);
  page.setDefaultNavigationTimeout(LOAD_TIMEOUT_MS);

  // CDP health check: do something that forces a real session interaction
  try {
    if (URL) {
      await page.bringToFront().catch(() => { });
      await page.goto(URL, { waitUntil: "commit", timeout: 15000 });
      await page.waitForTimeout(150);
    } else {
      await page.waitForTimeout(25);
    }
  } catch (e) {
    const msg = String(e?.message || e);
    try {
      await browser.close();
    } catch { }
    const err = new Error(`CDP health check failed: ${msg}`);
    err.code = "E_CDP_ZOMBIE";
    throw err;
  }

  // Best-effort domcontentloaded (optional)
  if (URL) {
    try {
      await page.goto(URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
    } catch (e) {
      console.warn(
        `[WARN] goto(domcontentloaded) failed; fallback waitUntil=commit: ${e?.message || e}`
      );
      await page.goto(URL, { waitUntil: "commit", timeout: LOAD_TIMEOUT_MS });
    }
    await page.waitForTimeout(150);
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
        console.warn(`[WARN] CDP connect attempt ${i}/${attempts} failed: ${msg}`);
        await wait(650);
        continue;
      }
      throw e;
    }
  }

  throw last_err || new Error("CDP connect failed after retries.");
}

async function main(
  URL = process.env.TARGET_URL || "https://www.trackwrestling.com/",
  PORT,
  opts = {}
) {
  const { force_relaunch = false } = opts;

  if (!PORT) {
    throw new Error(`[ERR] DevTools PORT is required.`);
  }

  const CONNECT_URL = `http://127.0.0.1:${PORT}`;
  const LOAD_TIMEOUT_MS = 30000;

  const platform = process.platform;
  console.log(`[INFO] Detected platform → ${platform}`);

  const USER_DATA_DIR_DEFAULT = path.join(os.homedir(), "chrome-tw-user-data");
  fs.mkdirSync(USER_DATA_DIR_DEFAULT, { recursive: true });

  console.log(`[INFO] Using Chrome profile base dir → ${USER_DATA_DIR_DEFAULT}`);
  console.log(`[INFO] DevTools port → ${PORT}`);

  // If DevTools is already listening and not forcing relaunch, connect.
  if (!force_relaunch && (await is_chrome_dev_up(CONNECT_URL))) {
    console.log(`[CDP] DevTools already up on ${PORT}. Connecting…`);

    const healthy_http = await is_chrome_dev_healthy(CONNECT_URL).catch(() => false);
    if (!healthy_http) {
      console.warn(`[WARN] DevTools responds but /json/list not healthy yet; waiting…`);
      await wait(500);
    }

    try {
      return await connect_with_retries(URL, CONNECT_URL, LOAD_TIMEOUT_MS, 3);
    } catch (e) {
      console.warn(`[WARN] connect failed on existing DevTools; force_relaunch once: ${e?.message || e}`);
      // best-effort relaunch (will create a new Chrome instance/profile-per-port)
      if (platform === "win32") await launch_chrome_win(URL, USER_DATA_DIR_DEFAULT, PORT);
      else if (platform === "linux") await launch_chrome_linux(URL, USER_DATA_DIR_DEFAULT, PORT, { headless: true });
      else if (platform === "darwin") await launch_chrome_mac(URL, USER_DATA_DIR_DEFAULT, PORT);

      return await connect_with_retries(URL, CONNECT_URL, LOAD_TIMEOUT_MS, 3);
    }
  }

  // Otherwise launch best-effort via OS helper
  if (platform === "win32") {
    await launch_chrome_win(URL, USER_DATA_DIR_DEFAULT, PORT);
  } else if (platform === "linux") {
    await launch_chrome_linux(URL, USER_DATA_DIR_DEFAULT, PORT, { headless: true });
  } else if (platform === "darwin") {
    await launch_chrome_mac(URL, USER_DATA_DIR_DEFAULT, PORT);
  } else {
    throw new Error(`[ERR] Unsupported platform: ${platform}`);
  }

  const deadline = Date.now() + 25000;
  while (Date.now() < deadline) {
    if (await is_chrome_dev_up(CONNECT_URL)) {
      console.log(`[CDP] DevTools active on ${PORT}. Connecting…`);
      try {
        return await connect_with_retries(URL, CONNECT_URL, LOAD_TIMEOUT_MS, 3);
      } catch (e) {
        console.warn(`[WARN] connect/health failed after launch: ${e?.message || e}`);
        await wait(900);
      }
    }
    await wait(450);
  }

  throw new Error(`Timed out waiting for healthy Chrome DevTools on port ${PORT}.`);
}

export { main as step_0_launch_chrome_developer_parallel_scrape };

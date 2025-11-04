// step_0_launch_chrome_developer.js (ESM, minimal changes, cross-platform)
import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawn } from "child_process";
import { chromium } from "playwright";

// ⬇️ NEW: delegate launch on Windows/Linux
import { launch_chrome_win } from "../utilities/chrome_dev_tools/launch_chrome_win.js";
import { launch_chrome_linux } from "../utilities/chrome_dev_tools/launch_chrome_linux.js";

const PORT = process.env.CHROME_DEVTOOLS_PORT || 9222;
const CONNECT_URL = `http://localhost:${PORT}`; // Chrome DevTools 
const LOAD_TIMEOUT_MS = 30000;

const USER_DATA_DIR = process.env.STORE_CHROME_DATA; // persistent across reboots

// ---------- tiny utils ----------
async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

async function is_chrome_dev_up(url = `${CONNECT_URL}/json/version`) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

function find_mac_chrome() {
  const candidates = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
  ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) return p;
    } catch {}
  }
  return null;
}

async function go_to_website_in_chrome(URL) {
  const browser = await chromium.connectOverCDP(CONNECT_URL);
  const contexts = browser.contexts();
  if (!contexts.length) throw new Error("No contexts from CDP connection.");
  const context = contexts[0];

  const page = await context.newPage();
  page.setDefaultTimeout(LOAD_TIMEOUT_MS);
  if (URL) {
    await page.goto(URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
    await page.waitForTimeout(2000); // small settle
  }

  return { browser, page, context };
}

async function step_0_launch_chrome_developer(
  URL = process.env.TARGET_URL || "https://www.google.com"
) {
  // Windows: delegate to small helper, then connect via CDP (unchanged connect logic)
  if (process.platform === "win32") {
    console.log("[INFO] Detected Windows → delegating to launch_chrome_win()");
    await launch_chrome_win(URL);
    return go_to_website_in_chrome(URL);
  }

  // Linux: delegate to small helper, then connect via CDP
  if (process.platform === "linux") {
    console.log("[INFO] Detected Linux → delegating to launch_chrome_linux()");
    await launch_chrome_linux(URL);
    return go_to_website_in_chrome(URL);
  }

  // macOS flow (your original logic, minimally touched)
  if (!USER_DATA_DIR) {
    throw new Error("STORE_CHROME_DATA is not set. Add it to your .env for macOS launch.");
  }

  // Ensure the profile path exists before launch
  fs.mkdirSync(USER_DATA_DIR, { recursive: true });
  console.log("[ENV] STORE_CHROME_DATA:", USER_DATA_DIR);

  if (await is_chrome_dev_up()) {
    console.log(`[CDP] Chrome is already listening on ${PORT}.`);
    return go_to_website_in_chrome(URL);
  }

  if (process.platform !== "darwin") {
    console.warn("[CDP] Auto-launch is set up for macOS only here. (Windows/Linux handled above.)");
    return;
  }

  const chromeBin = find_mac_chrome();
  if (!chromeBin) {
    throw new Error("Could not find Chrome binary in /Applications. Install Google Chrome.");
  }

  console.log("[CDP] Launching Chrome with remote debugging…");
  const child = spawn(chromeBin, [
    `--remote-debugging-port=${PORT}`,
    `--user-data-dir=${USER_DATA_DIR}`,
    `--no-first-run`,
    `--no-default-browser-check`,
    // Keep your flags minimal; add more via env if needed
    // e.g., process.env.CHROME_ARGS split/pushed here
  ], {
    stdio: "ignore",
    detached: true,
  });
  child.unref();

  // Wait for DevTools to be ready
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (await is_chrome_dev_up()) {
      console.log(`[CDP] Chrome is now available on ${PORT}.`);
      return go_to_website_in_chrome(URL);
    }
    await wait(400);
  }
  throw new Error(`Timed out waiting for Chrome DevTools on port ${PORT}.`);
}

step_0_launch_chrome_developer();

export { step_0_launch_chrome_developer };

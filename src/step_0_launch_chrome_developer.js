import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { spawn } from "child_process";
import { chromium } from "playwright";

const PORT = process.env.CHROME_DEVTOOLS_PORT || 9222;
const CONNECT_URL = `http://localhost:${PORT}`; // Chrome DevTools 
const LOAD_TIMEOUT_MS = 30000;

console.log('process.env.STORE_CHROME_DATA ', process.env.STORE_CHROME_DATA);
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
    } catch { }
  }
  return null;
}

async function go_to_website_in_chrome(URL) {

  const browser = await chromium.connectOverCDP(CONNECT_URL);
  const contexts = browser.contexts();
  if (!contexts.length) throw new Error("No contexts from CDP connection.");
  const context = contexts[0];

  // const google = await context.newPage();
  // await google.goto("https://www.google.com");
  // await google.waitForTimeout(800); // small settle

  const page = await context.newPage();
  page.setDefaultTimeout(LOAD_TIMEOUT_MS);
  await page.goto(URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
  await page.waitForTimeout(2000); // small settle

  return { browser, page, context };
}

async function step_0_launch_chrome_developer(URL) {
  // Ensure the profile path exists before launch
  fs.mkdirSync(USER_DATA_DIR, { recursive: true });
  console.log("[ENV] STORE_CHROME_DATA:", USER_DATA_DIR);

  if (await is_chrome_dev_up()) {
    console.log(`[CDP] Chrome is already listening on ${PORT}.`);
    const { browser, page, context } = await go_to_website_in_chrome(URL);
    return { browser, page, context };
  }
  if (process.platform !== "darwin") {
    console.warn("[CDP] Auto-launch is set up for macOS only. Start Chrome manually if not on macOS.");
    return;
  }
  const chromeBin = find_mac_chrome();
  if (!chromeBin) {
    throw new Error("Could not find Chrome binary in /Applications. Install Google Chrome.");
  }

  // Ensure user-data-dir exists
  fs.mkdirSync(USER_DATA_DIR, { recursive: true });
  console.log('user data dir:', USER_DATA_DIR);

  console.log("[CDP] Launching Chrome with remote debugging…");
  const child = spawn(chromeBin, [
    `--remote-debugging-port=${PORT}`,
    `--user-data-dir=${USER_DATA_DIR}`,
    `--no-first-run`,
    `--no-default-browser-check`,
    // Open a blank page quickly
    // "about:blank",
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
        const { browser, page, context } = await go_to_website_in_chrome(URL);
        return { browser, page, context };
    }
    await wait(400);
  }
  throw new Error(`Timed out waiting for Chrome DevTools on port ${PORT}.`);
}

export { step_0_launch_chrome_developer };

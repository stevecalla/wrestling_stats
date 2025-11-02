import fs from "fs";
import path from "path";
import { spawn } from "child_process";

import { chromium } from "playwright";

const CONNECT_URL = "http://localhost:9222"; // Chrome DevTools 
const LOAD_TIMEOUT_MS = 30000;
const AFTER_LOAD_PAUSE_MS = 500;

const USER_DATA_DIR = process.env.STORE_CHROME_DATA || path.resolve(process.env.HOME || "~", "chrome-tw-user-data"); // persistent across reboots

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

async function go_to_website_in_chrome(url) {

  const browser = await chromium.connectOverCDP(CONNECT_URL);
  const contexts = browser.contexts();
  if (!contexts.length) throw new Error("No contexts from CDP connection.");
  const context = contexts[0];

  // const google = await context.newPage();
  // await google.goto("https://www.google.com");
  // await google.waitForTimeout(800); // small settle

  const trackwrestling_page = await context.newPage();
  trackwrestling_page.setDefaultTimeout(LOAD_TIMEOUT_MS);
  await trackwrestling_page.goto("https://www.trackwrestling.com/", { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
  await trackwrestling_page.waitForTimeout(800); // small settle

  return { browser, trackwrestling_page };
}

async function step_0_launch_chrome_developer() {
  if (await is_chrome_dev_up()) {
    console.log("[CDP] Chrome is already listening on 9222.");
    const { browser, trackwrestling_page } = await go_to_website_in_chrome();
    return { browser, trackwrestling_page };
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
    `--remote-debugging-port=9222`,
    `--user-data-dir=${USER_DATA_DIR}`,
    `--no-first-run`,
    `--no-default-browser-check`,
    // Open a blank page quickly
    "about:blank",
  ], {
    stdio: "ignore",
    detached: true,
  });
  child.unref();

  // Wait for DevTools to be ready
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (await is_chrome_dev_up()) {
        console.log("[CDP] Chrome is now available on 9222.");
        const { browser, trackwrestling_page } = await go_to_website_in_chrome();
        return { browser, trackwrestling_page };
    }
    await wait(400);
  }
  throw new Error("Timed out waiting for Chrome DevTools on port 9222.");
}

export { step_0_launch_chrome_developer };

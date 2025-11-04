// launch_chrome_mac.js
import fs from "fs";
import { spawn } from "child_process";

const port = process.env.CHROME_DEVTOOLS_PORT || "9222";
const target_url = process.env.TARGET_URL || "https://www.google.com";

function find_mac_chrome() {
  const candidates = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
  ];
  for (const p of candidates) {
    try { if (fs.existsSync(p)) return p; } catch {}
  }
  return null;
}

function ensure_dir(dir) {
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

async function is_devtools_up(url = `http://localhost:${port}/json/version`) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

export async function launch_chrome_mac(url = target_url, USER_DATA_DIR_DEFAULT) {
  const user_data_dir = USER_DATA_DIR_DEFAULT;
  if (!user_data_dir) {
    throw new Error("STORE_CHROME_DATA not set in .env for macOS launch.");
  }

  ensure_dir(user_data_dir);
  const chrome_bin = find_mac_chrome();
  if (!chrome_bin) throw new Error("Chrome not found under /Applications.");

  if (await is_devtools_up()) {
    console.log(`[CDP] Chrome already listening on ${port}.`);
    return;
  }

  console.log("[MAC] Launching Chrome with remote debugging…");
  const args = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${user_data_dir}`,
    `--no-first-run`,
    `--no-default-browser-check`,
    `--new-window`,
    url,
  ];

  const child = spawn(chrome_bin, args, { stdio: "ignore", detached: true });
  child.unref();

  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (await is_devtools_up()) {
      console.log(`[CDP] Chrome DevTools listening on ${port}.`);
      return;
    }
    await wait(400);
  }

  throw new Error(`Timed out waiting for Chrome DevTools on port ${port}.`);
}

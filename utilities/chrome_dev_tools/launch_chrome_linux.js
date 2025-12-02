// launch_chrome_linux.js (Linux-only)
import fs from "fs";
import { spawn } from "child_process";

const port = String(process.env.CHROME_DEVTOOLS_PORT || "9222");
const target_url = (process.env.TARGET_URL && process.env.TARGET_URL.trim()) || "https://www.google.com";

// Prefer CHROME_PATH; otherwise try common Chrome/Chromium locations
function find_linux_chrome() {
  const candidates = [
    process.env.CHROME_PATH,
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
    "/snap/bin/chromium",
  ].filter(Boolean);
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

export async function launch_chrome_linux(url = target_url, USER_DATA_DIR_DEFAULT) {    
  const user_data_dir = USER_DATA_DIR_DEFAULT;

  ensure_dir(user_data_dir);

  const chrome_bin = find_linux_chrome();
  if (!chrome_bin) {
    throw new Error("No Chrome/Chromium binary found. Install Chrome/Chromium or set CHROME_PATH.");
  }

  // Already running with DevTools?
  if (await is_devtools_up()) {
    console.log(`[CDP] Chrome already listening on ${port}.`);
    return;
  }

  const args = [
    `--remote-debugging-port=${port}`,
    `--remote-debugging-address=127.0.0.1`,
    `--user-data-dir=${user_data_dir}`,
    `--no-first-run`,
    `--no-default-browser-check`,
    // `--new-window`,
    `--headless=new`,
    // Optional Linux niceties (uncomment if needed):
    // `--password-store=basic`,
    // `--use-mock-keychain`,
    // `--ozone-platform-hint=auto`,
    url,
  ];

  console.log(`[LINUX] Launching ${chrome_bin} with DevTools port ${port}…`);
  const child = spawn(chrome_bin, args, { stdio: "ignore", detached: true });
  child.unref();

  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (await is_devtools_up()) {
      console.log(`[CDP] Chrome DevTools listening on ${port}.`);
      return;
    }
    await wait(300);
  }

  throw new Error(`[ERR] DevTools not responding on port ${port}`);
}

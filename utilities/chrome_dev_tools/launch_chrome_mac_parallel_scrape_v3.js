// launch_chrome_mac.js
import fs from "fs";
import { spawn } from "child_process";

// const port = process.env.CHROME_DEVTOOLS_PORT || "9222";
const target_url = process.env.TARGET_URL || "https://www.google.com";

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

function ensure_dir(dir) {
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

async function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function is_devtools_up(port) {
  // ✅ use 127.0.0.1 to avoid localhost resolution quirks
  const url = `http://127.0.0.1:${port}/json/version`;

  try {
    const res = await fetch(url, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

async function main(url = target_url, USER_DATA_DIR_DEFAULT, port) {
  if (process.platform !== "darwin") {
    // ✅ fix message (was "Linux-only script.")
    throw new Error("[ERR] macOS-only script.");
  }

  const user_data_dir = USER_DATA_DIR_DEFAULT;
  if (!user_data_dir) {
    throw new Error("STORE_CHROME_DATA not set in .env for macOS launch.");
  }

  ensure_dir(user_data_dir);
  const chrome_bin = find_mac_chrome();
  if (!chrome_bin) throw new Error("Chrome not found under /Applications.");

  // ✅ BUGFIX: must pass port
  if (await is_devtools_up(port)) {
    console.log(`[CDP] Chrome already listening on ${port}.`);
    return;
  }

  console.log(`[INFO] platform=darwin`);
  console.log(`[INFO] port=${port}`);
  console.log(`[INFO] target_url=${target_url}`);

  // ✅ log existence accurately (and avoid undefined exists())
  console.log(
    `[INFO] chrome_bin=${chrome_bin} exists=${fs.existsSync(chrome_bin)}`
  );
  console.log(
    `[INFO] user_data_dir=${user_data_dir} exists=${fs.existsSync(user_data_dir)}`
  );

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
    // ✅ BUGFIX: must pass port
    if (await is_devtools_up(port)) {
      console.log(`[CDP] Chrome DevTools listening on ${port}.`);
      return;
    }
    await wait(400);
  }

  throw new Error(`Timed out waiting for Chrome DevTools on port ${port}.`);
}

export { main as launch_chrome_mac_v3 };

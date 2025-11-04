// OPEN IN COMMAND PROMPT CLI WITH COMMAND BELOW
// "" "%ProgramFiles%\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\tmp\chrome-tw-profile" --disable-features=SameSiteByDefaultCookies,CookiesWithoutSameSiteMustBeSecure
// http://localhost:9222/json/version

// launch_chrome_win.js (Windows-only, ESM, snake_case)
import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const port = process.env.CHROME_DEVTOOLS_PORT || process.env.DEVTOOLS_PORT || "9222";
const target_url = process.env.TARGET_URL || "https://www.google.com";

// Prefer Chrome; CHROME_PATH can override
const chrome_bin = (
  process.env.CHROME_PATH ||
  "C:/Program Files/Google/Chrome/Application/chrome.exe"
).replace(/\\/g, "/");

// ---------- helpers ----------
function q(s) {
  return `"${String(s).replace(/"/g, '""')}"`;
}

function exists(p) {
  try {
    return fs.existsSync(p);
  } catch {
    return false;
  }
}

async function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function is_devtools_up(current_port = port) {
  try {
    const res = await fetch(`http://localhost:${current_port}/json/version`, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

async function open_new_tab_via_devtools(url, current_port = port) {
  try {
    const res = await fetch(
      `http://localhost:${current_port}/json/new?${encodeURIComponent(url)}`,
      { cache: "no-store" }
    );
    return res.ok;
  } catch {
    return false;
  }
}

function ensure_writable_dir(preferred, fallback) {
  for (const dir of [preferred, fallback]) {
    try {
      fs.mkdirSync(dir, { recursive: true });
      const probe = path.posix.join(dir, ".write_test.tmp");
      fs.writeFileSync(probe, "ok");
      fs.unlinkSync(probe);
      return dir;
    } catch {
      // try next path
    }
  }
  throw new Error(
    `Failed to create writable user-data-dir.\nTried:\n - ${preferred}\n - ${fallback}`
  );
}

function launch_via_powershell(bin, args_str) {
  const ps_args = [
    "-NoProfile",
    "-Command",
    `Start-Process ${q(bin)} -ArgumentList ${q(args_str)} -WindowStyle Normal`,
  ];
  return spawn("powershell", ps_args, { stdio: "ignore", detached: true, shell: false });
}

function launch_direct(bin, args_arr) {
  return spawn(bin, args_arr, { stdio: "ignore", detached: true, shell: false });
}

// ---------- main ----------
async function launch_chrome_win(URL, USER_DATA_DIR_DEFAULT) {
  if (process.platform !== "win32") {
    console.error("[ERR] Windows-only script.");
    process.exit(1);
  }

  console.log(`[INFO] platform=win32`);
  console.log(`[INFO] port=${port}`);
  console.log(`[INFO] target_url=${target_url}`);
  console.log(`[INFO] chrome_bin=${chrome_bin} exists=${exists(chrome_bin)}`);

  if (!exists(chrome_bin)) {
    console.error("[ERR] Chrome binary not found. Set CHROME_PATH to chrome.exe.");
    process.exit(1);
  }

  let user_data_dir
  try {
    user_data_dir = ensure_writable_dir(USER_DATA_DIR_DEFAULT);
  } catch (e) {
    console.error("[ERR]", e.message);
    process.exit(1);
  }
  console.log(`[INFO] user_data_dir=${user_data_dir}`);

  const base_args_arr = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${user_data_dir}`,
    `--no-first-run`,
    `--no-default-browser-check`,
    `--new-window`,
  ];
  const base_args_str = base_args_arr.join(" ");

  // If DevTools already up, open a tab and exit
  if (await is_devtools_up(port)) {
    console.log(`[OK] DevTools already listening on ${port}. Opening tab → ${target_url}`);
    const ok = await open_new_tab_via_devtools(target_url, port);
    console.log(ok ? "[OK] New tab opened via /json/new" : "[WARN] /json/new failed; open manually.");
    console.log(`[TIP] DevTools endpoint: http://localhost:${port}/json/version`);
    process.exit(0);
  }

  // 1) Try PowerShell (best visibility from Git Bash/MINGW)
  console.log(`[INFO] Launch via PowerShell…`);
  const ps_child = launch_via_powershell(chrome_bin, `${base_args_str} ${target_url}`);
  ps_child.unref();

  // Wait up to 7.5s for DevTools
  const ps_deadline = Date.now() + 7500;
  while (Date.now() < ps_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (PS). Ensuring visible tab…`);
      const ok = await open_new_tab_via_devtools(target_url, port);
      console.log(
        ok
          ? "[OK] New tab opened via /json/new"
          : "[WARN] /json/new failed; a tab should already be open."
      );
      console.log(`[TIP] http://localhost:${port}/json/version`);
      process.exit(0);
    }
    await wait(250);
  }

  // 2) Fallback: direct spawn
  console.log(`[WARN] PowerShell path didn’t surface. Trying direct spawn…`);
  const direct_args = [...base_args_arr, target_url];
  const direct_child = launch_direct(chrome_bin, direct_args);
  direct_child.unref();

  const direct_deadline = Date.now() + 12000;
  while (Date.now() < direct_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (direct). Opening tab…`);
      const ok = await open_new_tab_via_devtools(target_url, port);
      console.log(
        ok
          ? "[OK] New tab opened via /json/new"
          : "[WARN] /json/new failed; a tab should already be open."
      );
      console.log(`[TIP] http://localhost:${port}/json/version`);
      process.exit(0);
    }
    await wait(300);
  }

  console.error(
    `[ERR] Couldn’t detect DevTools on ${port}. Close all Chrome windows, try a different port:\n` +
      `    set CHROME_DEVTOOLS_PORT=9333 && node launch_chrome_win.js`
  );
  process.exit(2);
};

// TEST
// import os from "os";
// const USER_DATA_DIR_DEFAULT = path.join(os.homedir(), "chrome-tw-user-data");
// fs.mkdirSync(USER_DATA_DIR_DEFAULT, { recursive: true });
// launch_chrome_win("", USER_DATA_DIR_DEFAULT);

export { launch_chrome_win };



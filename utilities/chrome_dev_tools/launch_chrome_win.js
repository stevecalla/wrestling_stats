// launch_chrome_win.js (Windows-only, ESM)
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
const q = (s) => `"${String(s).replace(/"/g, '""')}"`;

const exists = (p) => {
  try { return fs.existsSync(p); } catch { return false; }
};

const wait = (ms) => new Promise(r => setTimeout(r, ms));

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
  for (const dir of [preferred, fallback].filter(Boolean)) {
    try {
      fs.mkdirSync(dir, { recursive: true });
      const probe = path.join(dir, ".write_test.tmp");   // use path.join on Windows
      fs.writeFileSync(probe, "ok");
      fs.unlinkSync(probe);
      return dir;
    } catch {
      // try next path
    }
  }
  throw new Error(
    `Failed to create writable user-data-dir.\nTried:\n - ${preferred}\n - ${fallback || "(none)"}`
  );
}

function launch_via_powershell(bin, args_str) {
  const ps_args = [
    "-NoProfile",
    "-Command",
    `Start-Process ${q(bin)} -ArgumentList ${q(args_str)} -WindowStyle Normal`,
  ];
  const child = spawn("powershell", ps_args, { stdio: "ignore", detached: true, shell: false });
  child.unref();
  return child;
}

function launch_direct(bin, args_arr) {
  const child = spawn(bin, args_arr, { stdio: "ignore", detached: true, shell: false });
  child.unref();
  return child;
}

// ---------- main ----------
async function launch_chrome_win(URL, USER_DATA_DIR_DEFAULT) {
  if (process.platform !== "win32") {
    throw new Error("[ERR] Windows-only script.");
  }

  console.log(`[INFO] platform=win32`);
  console.log(`[INFO] port=${port}`);
  console.log(`[INFO] target_url=${target_url}`);
  console.log(`[INFO] chrome_bin=${chrome_bin} exists=${exists(chrome_bin)}`);

  if (!exists(chrome_bin)) {
    throw new Error("Chrome binary not found. Set CHROME_PATH to chrome.exe.");
  }

  const fallback_dir = path.join("C:", "tmp", "chrome-tw-user-data");
  const user_data_dir = ensure_writable_dir(USER_DATA_DIR_DEFAULT, fallback_dir);
  console.log(`[INFO] user_data_dir=${user_data_dir}`);

  const base_args_arr = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${user_data_dir}`,
    `--no-first-run`,
    `--no-default-browser-check`,
    `--new-window`,
  ];
  const base_args_str = base_args_arr.join(" ");

  // If DevTools already up: try to open a tab and return
  if (await is_devtools_up(port)) {
    console.log(`[OK] DevTools already listening on ${port}. Opening tab → ${target_url}`);
    const ok = await open_new_tab_via_devtools(target_url, port);
    console.log(ok ? "[OK] New tab opened via /json/new" : "[WARN] /json/new failed; open manually.");
    console.log(`[TIP] DevTools endpoint: http://localhost:${port}/json/version`);
    return; // ← success; hand control back to caller
  }

  // 1) Try PowerShell
  console.log(`[INFO] Launch via PowerShell…`);
  launch_via_powershell(chrome_bin, `${base_args_str} ${URL || target_url}`);

  // Wait up to 7.5s for DevTools
  const ps_deadline = Date.now() + 7500;
  while (Date.now() < ps_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (PS). Ensuring visible tab…`);
      const ok = await open_new_tab_via_devtools(URL || target_url, port);
      console.log(ok ? "[OK] New tab opened via /json/new"
                     : "[WARN] /json/new failed; a tab should already be open.");
      console.log(`[TIP] http://localhost:${port}/json/version`);
      return; // ← success; hand control back to caller
    }
    await wait(250);
  }

  // 2) Fallback: direct spawn
  console.log(`[WARN] PowerShell path didn’t surface. Trying direct spawn…`);
  launch_direct(chrome_bin, [...base_args_arr, URL || target_url]);

  const direct_deadline = Date.now() + 12000;
  while (Date.now() < direct_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (direct). Opening tab…`);
      const ok = await open_new_tab_via_devtools(URL || target_url, port);
      console.log(ok ? "[OK] New tab opened via /json/new"
                     : "[WARN] /json/new failed; a tab should already be open.");
      console.log(`[TIP] http://localhost:${port}/json/version`);
      return; // ← success; hand control back to caller
    }
    await wait(300);
  }

  // If we get here, we never detected DevTools
  throw new Error(
    `[ERR] Couldn’t detect DevTools on ${port}. Close all Chrome windows, or try a different port:\n` +
    `    set CHROME_DEVTOOLS_PORT=9333 && node launch_chrome_win.js`
  );
}

export { launch_chrome_win };

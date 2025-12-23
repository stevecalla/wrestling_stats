// src/utilities/chrome_dev_tools/launch_chrome_win_parallel_scrape.js
// (Windows-only, ESM)
//
// ✅ UPDATE (Unique user-data-dir per worker/port):
// - Instead of using a shared USER_DATA_DIR_DEFAULT for all workers,
//   we create a per-port subdirectory: <base>/port_<PORT>
// - This prevents Chrome profile locking conflicts when running multiple
//   parallel Chrome instances (9223, 9224, ...)

import fs from "fs";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Prefer Chrome; CHROME_PATH can override
const chrome_bin = (
  process.env.CHROME_PATH ||
  "C:/Program Files/Google/Chrome/Application/chrome.exe"
).replace(/\\/g, "/");

// ---------- helpers ----------
const q = (s) => `"${String(s).replace(/"/g, '""')}"`;

const exists = (p) => {
  try {
    return fs.existsSync(p);
  } catch {
    return false;
  }
};

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

function resolve_powershell_exe_candidates() {
  const system_root = process.env.SystemRoot || "C:\\Windows";

  // Windows PowerShell
  const win_ps = path.join(
    system_root,
    "System32",
    "WindowsPowerShell",
    "v1.0",
    "powershell.exe"
  );

  // PowerShell 7 common locations
  const pf = process.env.ProgramFiles || "C:\\Program Files";
  const pf86 = process.env["ProgramFiles(x86)"] || "C:\\Program Files (x86)";
  const pwsh_1 = path.join(pf, "PowerShell", "7", "pwsh.exe");
  const pwsh_2 = path.join(pf86, "PowerShell", "7", "pwsh.exe");

  return [win_ps, pwsh_1, pwsh_2, "powershell.exe", "pwsh.exe"];
}

function pick_first_existing_path(candidates) {
  for (const c of candidates) {
    if (!c) continue;
    if (c.endsWith(".exe")) {
      if (exists(c)) return c;
    } else {
      // bare name "powershell.exe" / "pwsh.exe" — may resolve via PATH
      return c;
    }
  }
  return "powershell.exe";
}

async function fetch_ok(url) {
  try {
    const res = await fetch(url, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

async function is_devtools_up(current_port) {
  return fetch_ok(`http://localhost:${current_port}/json/version`);
}

async function open_new_tab_via_devtools(url, current_port, opts = {}) {
  const { retries = 5, delay_ms = 250, debug = false } = opts;

  const endpoint = `http://localhost:${current_port}/json/new?${encodeURIComponent(
    url
  )}`;

  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(endpoint, { cache: "no-store" });
      if (res.ok) return true;

      if (debug && i === retries - 1) {
        const body = await res.text().catch(() => "");
        console.warn(
          `[WARN] /json/new failed status=${res.status} body=${body.slice(
            0,
            250
          )}`
        );
      }
    } catch (e) {
      if (debug && i === retries - 1) {
        console.warn(`[WARN] /json/new fetch error: ${e?.message || e}`);
      }
    }
    await wait(delay_ms);
  }

  return false;
}

function ensure_writable_dir(preferred, fallback) {
  for (const dir of [preferred, fallback].filter(Boolean)) {
    try {
      fs.mkdirSync(dir, { recursive: true });
      const probe = path.join(dir, ".write_test.tmp");
      fs.writeFileSync(probe, "ok");
      fs.unlinkSync(probe);
      return dir;
    } catch {
      // try next path
    }
  }
  throw new Error(
    `Failed to create writable user-data-dir.\nTried:\n - ${preferred}\n - ${
      fallback || "(none)"
    }`
  );
}

// ✅ NEW: make a per-port profile directory to avoid lock conflicts
function ensure_dir(p) {
  fs.mkdirSync(p, { recursive: true });
  return p;
}

// ✅ NEW: use a stable per-port subfolder so multiple Chromes can run in parallel
function resolve_user_data_dir_per_port(base_dir, port) {
  const safe_port = String(port || "").trim() || "noport";
  return ensure_dir(path.join(base_dir, `port_${safe_port}`));
}

function launch_via_powershell(bin, args_arr) {
  const ps_exe = pick_first_existing_path(resolve_powershell_exe_candidates());

  // Build a single argument string for Chrome (keeps quoting simple)
  const args_str = args_arr.join(" ");

  // IMPORTANT: surface errors — do not fail silently
  const ps_args = [
    "-NoProfile",
    "-Command",
    `Start-Process ${q(bin)} -ArgumentList ${q(
      args_str
    )} -WindowStyle Normal`,
  ];

  const child = spawn(ps_exe, ps_args, {
    stdio: ["ignore", "ignore", "pipe"], // keep stderr
    detached: true,
    shell: false,
  });

  child.on("error", (e) => {
    console.error(`[ERR] PowerShell spawn failed (${ps_exe}): ${e?.message || e}`);
  });

  child.stderr?.on("data", (d) => {
    const msg = String(d || "").trim();
    if (msg) console.error(`[ERR] PowerShell stderr: ${msg}`);
  });

  child.unref();
  return child;
}

function launch_direct(bin, args_arr) {
  const child = spawn(bin, args_arr, {
    stdio: "ignore",
    detached: true,
    shell: false,
  });

  child.on("error", (e) => {
    console.error(`[ERR] Direct spawn failed: ${e?.message || e}`);
  });

  child.unref();
  return child;
}

// ---------- main ----------
async function main(URL, USER_DATA_DIR_DEFAULT, port) {
  const target_url = process.env.TARGET_URL || "https://www.google.com";

  if (process.platform !== "win32") {
    throw new Error("[ERR] Windows-only script.");
  }

  if (!port) {
    throw new Error("[ERR] port is required.");
  }

  console.log(`[INFO] platform=win32`);
  console.log(`[INFO] port=${port}`);
  console.log(`[INFO] target_url=${target_url}`);
  console.log(`[INFO] chrome_bin=${chrome_bin} exists=${exists(chrome_bin)}`);

  if (!exists(chrome_bin)) {
    throw new Error("Chrome binary not found. Set CHROME_PATH to chrome.exe.");
  }

  const fallback_dir = path.join("C:", "tmp", "chrome-tw-user-data");

  // 1) ensure base dir is writable
  const user_data_dir_base = ensure_writable_dir(
    USER_DATA_DIR_DEFAULT,
    fallback_dir
  );

  // 2) ✅ derive a per-port profile directory under the base
  const user_data_dir = resolve_user_data_dir_per_port(user_data_dir_base, port);

  console.log(`[INFO] user_data_dir_base=${user_data_dir_base}`);
  console.log(`[INFO] user_data_dir=${user_data_dir}`);

  const open_url = URL || target_url;

  // Keep args as an array for direct spawn
  const base_args_arr = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${user_data_dir}`,

    // reduce profile/first-run noise
    `--no-first-run`,
    `--no-default-browser-check`,

    // `--new-window`,
    `--headless=new`,
    open_url, // include URL as arg so Chrome opens a visible tab
  ];

  // If DevTools already up: try to open a tab (optional) and return
  if (await is_devtools_up(port)) {
    console.log(`[OK] DevTools already listening on ${port}.`);
    const ok = await open_new_tab_via_devtools(open_url, port, {
      retries: 3,
      delay_ms: 250,
      debug: false,
    });
    console.log(
      ok
        ? "[OK] New tab opened via /json/new"
        : "[WARN] /json/new failed; continuing (Playwright can open a page)."
    );
    console.log(`[TIP] DevTools endpoint: http://localhost:${port}/json/version`);
    return;
  }

  // 1) Try PowerShell
  console.log(`[INFO] Launch via PowerShell…`);
  launch_via_powershell(chrome_bin, base_args_arr);

  // Wait up to 10s for DevTools
  const ps_deadline = Date.now() + 10000;
  while (Date.now() < ps_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (PS).`);

      // /json/new is best-effort only
      const ok = await open_new_tab_via_devtools(open_url, port, {
        retries: 5,
        delay_ms: 250,
        debug: false,
      });
      console.log(
        ok
          ? "[OK] New tab opened via /json/new"
          : "[WARN] /json/new failed; a tab should already be open (or Playwright will create one)."
      );

      console.log(`[TIP] http://localhost:${port}/json/version`);
      return;
    }
    await wait(250);
  }

  // 2) Fallback: direct spawn
  console.log(`[WARN] PowerShell launch didn’t surface DevTools. Trying direct spawn…`);
  launch_direct(chrome_bin, base_args_arr);

  const direct_deadline = Date.now() + 12000;
  while (Date.now() < direct_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (direct).`);

      const ok = await open_new_tab_via_devtools(open_url, port, {
        retries: 5,
        delay_ms: 250,
        debug: false,
      });
      console.log(
        ok
          ? "[OK] New tab opened via /json/new"
          : "[WARN] /json/new failed; a tab should already be open (or Playwright will create one)."
      );

      console.log(`[TIP] http://localhost:${port}/json/version`);
      return;
    }
    await wait(300);
  }

  // If we get here, we never detected DevTools
  throw new Error(
    `[ERR] Couldn’t detect DevTools on ${port}. Close all Chrome windows, or try a different port:\n` +
      `    set CHROME_DEVTOOLS_PORT=9333 && node launch_chrome_win_parallel_scrape.js`
  );
}

export { main as launch_chrome_win };

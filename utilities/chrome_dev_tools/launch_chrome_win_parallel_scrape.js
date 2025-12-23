// src/utilities/chrome_dev_tools/launch_chrome_win_parallel_scrape.js
// (Windows-only, ESM)
//
// ✅ Stable parallel Chrome launcher for CDP workers.
// - Per-port user-data-dir to avoid profile lock conflicts
// - Extra stability flags (GPU/background throttling, etc.)
// - Optional per-port disk cache dir
// - DevTools readiness checks (json/version)
// - Best-effort open tab via /json/new

import fs from "fs";
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

  const win_ps = path.join(
    system_root,
    "System32",
    "WindowsPowerShell",
    "v1.0",
    "powershell.exe"
  );

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
      return c; // bare name via PATH
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
  return fetch_ok(`http://127.0.0.1:${current_port}/json/version`);
}

async function open_new_tab_via_devtools(url, current_port, opts = {}) {
  const { retries = 5, delay_ms = 250, debug = false } = opts;

  const endpoint = `http://127.0.0.1:${current_port}/json/new?${encodeURIComponent(
    url
  )}`;

  for (let i = 0; i < retries; i++) {
    try {
      const res = await fetch(endpoint, { cache: "no-store" });
      if (res.ok) return true;

      if (debug && i === retries - 1) {
        const body = await res.text().catch(() => "");
        console.warn(
          `[WARN] /json/new failed status=${res.status} body=${body.slice(0, 250)}`
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

function ensure_dir(p) {
  fs.mkdirSync(p, { recursive: true });
  return p;
}

function resolve_user_data_dir_per_port(base_dir, port) {
  const safe_port = String(port || "").trim() || "noport";
  return ensure_dir(path.join(base_dir, `port_${safe_port}`));
}

function resolve_cache_dir_per_port(base_dir, port) {
  const safe_port = String(port || "").trim() || "noport";
  return ensure_dir(path.join(base_dir, `cache_${safe_port}`));
}

function launch_via_powershell(bin, args_arr) {
  const ps_exe = pick_first_existing_path(resolve_powershell_exe_candidates());
  const args_str = args_arr.join(" ");

  const ps_args = [
    "-NoProfile",
    "-Command",
    `Start-Process ${q(bin)} -ArgumentList ${q(args_str)} -WindowStyle Normal`,
  ];

  const child = spawn(ps_exe, ps_args, {
    stdio: ["ignore", "ignore", "pipe"],
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

  // Prefer user home base dir, fallback to C:\tmp
  const fallback_dir = path.join("C:", "tmp", "chrome-tw-user-data");

  const user_data_dir_base = ensure_writable_dir(USER_DATA_DIR_DEFAULT, fallback_dir);
  const user_data_dir = resolve_user_data_dir_per_port(user_data_dir_base, port);

  // Optional: separate disk cache to reduce I/O contention
  const cache_base = ensure_writable_dir(path.join("C:", "tmp", "chrome-tw-cache"), null);
  const disk_cache_dir = resolve_cache_dir_per_port(cache_base, port);

  console.log(`[INFO] user_data_dir_base=${user_data_dir_base}`);
  console.log(`[INFO] user_data_dir=${user_data_dir}`);
  console.log(`[INFO] disk_cache_dir=${disk_cache_dir}`);

  const open_url = URL || target_url;

  const base_args_arr = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${user_data_dir}`,
    `--disk-cache-dir=${disk_cache_dir}`,

    // reduce profile/first-run noise
    `--no-first-run`,
    `--no-default-browser-check`,

    // stability flags (Windows parallel automation)
    `--disable-gpu`,
    `--disable-software-rasterizer`,
    `--disable-background-timer-throttling`,
    `--disable-backgrounding-occluded-windows`,
    `--disable-renderer-backgrounding`,
    `--disable-features=TranslateUI`,
    `--disable-popup-blocking`,
    `--disable-notifications`,
    `--mute-audio`,

    // good hygiene
    `--new-window`,
    `--headless=new`,
    open_url,
  ];

  if (await is_devtools_up(port)) {
    console.log(`[OK] DevTools already listening on ${port}.`);
    const ok = await open_new_tab_via_devtools(open_url, port, {
      retries: 3,
      delay_ms: 250,
      debug: false,
    });
    console.log(ok ? "[OK] New tab opened via /json/new" : "[WARN] /json/new failed.");
    console.log(`[TIP] DevTools endpoint: http://127.0.0.1:${port}/json/version`);
    return;
  }

  console.log(`[INFO] Launch via PowerShell…`);
  launch_via_powershell(chrome_bin, base_args_arr);

  const ps_deadline = Date.now() + 12000;
  while (Date.now() < ps_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (PS).`);
      await open_new_tab_via_devtools(open_url, port, {
        retries: 5,
        delay_ms: 250,
        debug: false,
      }).catch(() => {});
      console.log(`[TIP] http://127.0.0.1:${port}/json/version`);
      return;
    }
    await wait(250);
  }

  console.log(`[WARN] PowerShell launch didn’t surface DevTools. Trying direct spawn…`);
  launch_direct(chrome_bin, base_args_arr);

  const direct_deadline = Date.now() + 15000;
  while (Date.now() < direct_deadline) {
    if (await is_devtools_up(port)) {
      console.log(`[OK] DevTools is listening on ${port} (direct).`);
      await open_new_tab_via_devtools(open_url, port, {
        retries: 5,
        delay_ms: 250,
        debug: false,
      }).catch(() => {});
      console.log(`[TIP] http://127.0.0.1:${port}/json/version`);
      return;
    }
    await wait(300);
  }

  throw new Error(
    `[ERR] Couldn’t detect DevTools on ${port}.\n` +
      `Close all Chrome instances OR pick a different port.\n` +
      `Tip: make sure no regular Chrome is using this profile dir.\n`
  );
}

export { main as launch_chrome_win };

// src/utilities/chrome_dev_tools/launch_chrome_linux_parallel_scrape.js
// (Linux-only, ESM)
//
// ✅ Parity with Windows launcher:
// - main(URL, USER_DATA_DIR_DEFAULT, port)
// - ensure_writable_dir(preferred, fallback) with write probe
// - per-port user-data-dir: <base>/port_<PORT> to avoid profile lock conflicts
// - is_devtools_up() via /json/version
// - open_new_tab_via_devtools() via /json/new (best-effort)
// - spawn + poll until DevTools is detected

import fs from "fs";
import os from "os";
import path from "path";
import { spawn } from "child_process";

// ---------- helpers ----------
const exists = (p) => {
  try {
    return fs.existsSync(p);
  } catch {
    return false;
  }
};

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

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
      // try next
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
    try {
      if (exists(p)) return p;
    } catch {
      // ignore
    }
  }
  return null;
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

  if (process.platform !== "linux") {
    throw new Error("[ERR] Linux-only script.");
  }

  if (!port) {
    throw new Error("[ERR] port is required.");
  }

  const chrome_bin = find_linux_chrome();

  console.log(`[INFO] platform=linux`);
  console.log(`[INFO] port=${port}`);
  console.log(`[INFO] target_url=${target_url}`);
  console.log(`[INFO] chrome_bin=${chrome_bin || "(not found)"}`);

  if (!chrome_bin) {
    throw new Error(
      "No Chrome/Chromium binary found. Install Chrome/Chromium or set CHROME_PATH."
    );
  }

  // Linux fallback dir
  const fallback_dir =
    process.env.CHROME_USER_DATA_FALLBACK_DIR ||
    path.join(os.tmpdir(), "chrome-tw-user-data");

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

  const base_args_arr = [
    `--remote-debugging-port=${port}`,
    `--remote-debugging-address=127.0.0.1`,
    `--user-data-dir=${user_data_dir}`,

    // reduce profile/first-run noise
    `--no-first-run`,
    `--no-default-browser-check`,

    // common linux stability flags
    `--disable-dev-shm-usage`,

    // `--new-window`,
    `--headless=new`,
    open_url, // include URL as arg so Chrome opens a tab
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

  // Launch direct (Linux)
  console.log(`[INFO] Launch direct…`);
  launch_direct(chrome_bin, base_args_arr);

  // Wait up to 12s for DevTools
  const deadline = Date.now() + 12000;
  while (Date.now() < deadline) {
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
    `[ERR] Couldn’t detect DevTools on ${port}. Close all Chrome processes, or try a different port:\n` +
      `    CHROME_DEVTOOLS_PORT=9333 node launch_chrome_linux_parallel_scrape.js`
  );
}

export { main as launch_chrome_linux };

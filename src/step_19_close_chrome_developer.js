// step_9_close_chrome_dev.js (cross-platform, reads port & profile from .env)
import dotenv from "dotenv";
import { exec } from "child_process";
import os from "os";
import path from "path";

dotenv.config(); // load .env if present

// --- defaults ---
const PORT = process.env.CHROME_DEVTOOLS_PORT || "9222";
const USER_DATA_DIR = path.join(os.homedir(), "chrome-tw-user-data");

// --- main function ---
export async function step_19_close_chrome_dev({
  browser,
  context,
  userDataDir = USER_DATA_DIR,
  port = PORT,
} = {}) {
  try {
    // Close Playwright handles if provided
    if (context) {
      try { await context.close(); } catch { }
    }
    if (browser) {
      try { await browser.close(); } catch { }
    }

    const platform = process.platform;
    console.log(`[INFO] Closing Chrome Dev instance on ${platform}`);
    console.log(`[INFO] Target profile → ${userDataDir}`);
    console.log(`[INFO] Target port → ${port}`);

    const run = (cmd) =>
      new Promise((resolve) => {
        exec(cmd, { windowsHide: true }, (err, stdout, stderr) => {
          if (err) return resolve({ ok: false, err });
          resolve({ ok: true });
        });
      });

    if (platform === "win32") {
      // PowerShell: match chrome/msedge/chromium.exe processes by userDataDir and port
      const regexEsc = (s) => s.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
      const profileRe = regexEsc(userDataDir);
      const portRe = regexEsc(String(port));
      const psFilter =
        `$_.Name -match '(chrome|msedge|chromium)\\.exe' ` +
        `-and $_.CommandLine -match '${profileRe}' ` +
        `-and $_.CommandLine -match '${portRe}'`;
      const psCmd =
        `powershell -NoProfile -Command "` +
        `Get-CimInstance Win32_Process | Where-Object { ${psFilter} } | ` +
        `ForEach-Object { try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop } catch {} }"`;

      const { ok, err } = await run(psCmd);
      if (ok) console.log("🧹 Closed Chrome Dev instance cleanly (Windows).");
      else console.warn("⚠️ Could not close Chrome Dev instance:", err?.message || "unknown error");
      return;
    }

    // macOS / Linux
    for (const name of ["Google Chrome", "Google Chrome Canary", "Chromium"]) {
      const cmd = `pkill -if "${name}.*--user-data-dir=${userDataDir}"`;
      const { ok, err } = await run(cmd);

      if (ok) {
        console.log(`🧹 Closed ${name} Dev instance cleanly (macOS/Linux).`);
        return; // stop after the first successful kill
      } else if (err?.code !== 1) {
        // code 1 just means "no match found", which is fine
        console.warn(`⚠️ Error while closing ${name}:`, err?.message || "unknown error");
      }
    }
  } catch (e) {
    console.error("❌ Failed to close Chrome Dev:", e);
  }
}

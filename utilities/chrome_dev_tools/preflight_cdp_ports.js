/**
 * Preflight: Clear Chrome DevTools (CDP) Ports
 * --------------------------------------------
 * Ensures a clean starting state before launching / connecting to Chrome via CDP.
 *
 * - Kills any processes currently listening on the specified DevTools ports.
 * - Helps prevent Playwright connectOverCDP timeouts caused by stale/zombie Chrome.
 *
 * Platform support:
 * - Linux & macOS: uses `lsof` + `kill -9`
 * - Windows: uses `netstat` + `taskkill`
 *
 * Usage (as a module):
 *   import { preflight_cdp_ports } from "./scripts/preflight_cdp_ports.js";
 *   await preflight_cdp_ports([9222, 9223]);
 *
 * Usage (CLI):
 *   node scripts/preflight_cdp_ports.js
 */

import { execSync } from "child_process";
import os from "os";

const DEFAULT_PORTS = [9222, 9223, 9224, 9225, 9226];

function sh(cmd) {
  try {
    return execSync(cmd, { stdio: ["ignore", "pipe", "pipe"] }).toString().trim();
  } catch {
    return ""; // treat as "nothing found"
  }
}

function kill_pid(pid) {
  if (!pid) return;
  if (process.platform === "win32") sh(`taskkill /F /PID ${pid}`);
  else sh(`kill -9 ${pid}`);
}

function pids_listening_on_port(port) {
  if (process.platform === "win32") {
    const out = sh(`netstat -ano | findstr :${port}`);
    if (!out) return [];
    const pids = new Set();
    out.split("\n").forEach((line) => {
      const parts = line.trim().split(/\s+/);
      const pid = parts[parts.length - 1];
      if (pid && /^\d+$/.test(pid)) pids.add(pid);
    });
    return [...pids];
  }

  const out = sh(`lsof -ti :${port}`);
  return out ? out.split("\n").map((s) => s.trim()).filter(Boolean) : [];
}

// ✅ EXPORT THIS
export async function preflight_cdp_ports(ports = DEFAULT_PORTS) {
  console.log(`🧹 Preflight: clearing CDP ports ${ports.join(", ")} on ${os.platform()}`);

  for (const port of ports) {
    const pids = pids_listening_on_port(port);
    if (!pids.length) {
      console.log(`  - :${port} is free`);
      continue;
    }
    console.log(`  - :${port} killing PID(s): ${pids.join(", ")}`);
    for (const pid of pids) kill_pid(pid);
  }

  console.log("✅ Preflight complete.");
}

// (optional) allow direct CLI usage too
if (import.meta.url === `file://${process.argv[1]}`) {
  await preflight_cdp_ports();
}

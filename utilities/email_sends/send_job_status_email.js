// utilities/email_sends/send_job_status_email.js (ESM)
import os from "os";
import fs from "fs";
import { execSync } from "child_process";

import { send_mail } from "./nodemailer.js";

// ----------------------------------------------------
// System helpers
// ----------------------------------------------------
function safe_exec(cmd) {
  try {
    return execSync(cmd, { encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function get_root_disk_stats() {
  try {
    const out = execSync("df -P -B1 /", { encoding: "utf8" }).trim().split("\n");
    if (!out[1]) {
      return { pct_used: "N/A", total_gb: "N/A", used_gb: "N/A", avail_gb: "N/A" };
    }

    const cols = out[1].split(/\s+/);
    const total_bytes = Number(cols[1]);
    const used_bytes = Number(cols[2]);
    const avail_bytes = Number(cols[3]);
    const pct_used = cols[4] || "N/A";

    const to_gb = (b) => (Number.isFinite(b) ? (b / 1024 / 1024 / 1024).toFixed(2) : "N/A");

    return {
      pct_used,
      total_gb: to_gb(total_bytes),
      used_gb: to_gb(used_bytes),
      avail_gb: to_gb(avail_bytes),
    };
  } catch {
    return { pct_used: "N/A", total_gb: "N/A", used_gb: "N/A", avail_gb: "N/A" };
  }
}

function get_system_temp_f() {
  let temp_c = null;

  // Prefer sysfs: /sys/class/thermal/thermal_zone*/temp
  try {
    const base = "/sys/class/thermal";
    if (fs.existsSync(base)) {
      const zones = fs.readdirSync(base).filter((n) => n.startsWith("thermal_zone"));
      for (const z of zones) {
        const temp_path = `${base}/${z}/temp`;
        if (!fs.existsSync(temp_path)) continue;

        const raw = fs.readFileSync(temp_path, "utf8").trim();
        const val = Number(raw);
        if (Number.isNaN(val)) continue;

        temp_c = val > 1000 ? val / 1000 : val;
        if (temp_c > 0 && temp_c < 120) break; // sanity
      }
    }
  } catch {
    // ignore
  }

  // fallback: lm-sensors
  if (temp_c === null) {
    try {
      const out = execSync("sensors", { encoding: "utf8" });
      const m = out.match(/\+([0-9]{1,3}\.[0-9])°C|\+([0-9]{1,3})°C/);
      if (m) temp_c = Number(m[1] || m[2]);
    } catch {
      return "N/A";
    }
  }

  if (temp_c === null || Number.isNaN(temp_c)) return "N/A";
  return (((temp_c * 9) / 5 + 32)).toFixed(1);
}

function fmt_duration_ms(ms) {
  if (!Number.isFinite(ms)) return "N/A";
  const total_seconds = Math.floor(ms / 1000);

  const days = Math.floor(total_seconds / 86400);
  const rem1 = total_seconds % 86400;

  const hours = Math.floor(rem1 / 3600);
  const rem2 = rem1 % 3600;

  const minutes = Math.floor(rem2 / 60);
  const seconds = rem2 % 60;

  const parts = [];
  if (days) parts.push(`${days}d`);
  parts.push(`${hours}h`, `${minutes}m`, `${seconds}s`);
  return parts.join(" ");
}

function fmt_now_mtn() {
  const now = new Date();
  return now.toLocaleString("en-US", {
    timeZone: "America/Denver",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

function guess_stage_color(stage, status) {
  // defaults: gray
  let color = "#6c757d";

  if (status === "FAIL") return "#dc3545"; // red
  if (stage === "START") return "#0d6efd"; // blue
  if (stage === "AFTER_STEP_3_4") return "#fd7e14"; // orange
  if (stage === "END") return "#28a745"; // green

  return color;
}

function safe_json(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

// ----------------------------------------------------
// Main: send_job_status_email
// ----------------------------------------------------
async function send_job_status_email({
  stage,                 // "START" | "AFTER_STEP_3_4" | "END"
  status = "OK",         // "OK" | "FAIL"
  job_name = "TrackWrestling Pipeline",
  program_start_ms = null,
  ctx = null,            // your ctx object from orchestrator
  step_flags = null,
  test_flags = null,
  details = {},          // stage-specific add-ons: task_set_id, notes, etc.
  error = null,          // Error or stack string
}) {
  const email_sender = process.env.EMAIL_SENDER || "callasteven@gmail.com";
  const email_recipient = process.env.EMAIL_RECIPIENT || email_sender;

  const host = os.hostname();
  const pid = process.pid;

  const timestamp_mtn = fmt_now_mtn();
  const uptime_seconds = os.uptime();
  const load_1m = os.loadavg()?.[0]?.toFixed?.(2) ?? "N/A";

  const total_mem_gb = (os.totalmem() / 1024 / 1024 / 1024).toFixed(2);
  const free_mem_gb = (os.freemem() / 1024 / 1024 / 1024).toFixed(2);
  const used_mem_gb = (Number(total_mem_gb) - Number(free_mem_gb)).toFixed(2);
  const mem_used_pct = ((Number(used_mem_gb) / Number(total_mem_gb)) * 100).toFixed(1);

  const disk = get_root_disk_stats();
  const temp_f = get_system_temp_f();

  const node_version = process.version;
  const cwd = process.cwd();

  const git_branch = safe_exec("git rev-parse --abbrev-ref HEAD");
  const git_commit = safe_exec("git rev-parse --short HEAD");
  const git_dirty = safe_exec("git status --porcelain") ? "DIRTY" : "CLEAN";

  const elapsed_ms = program_start_ms ? (Date.now() - program_start_ms) : null;

  const subject = `[${status}] ${job_name} — ${stage} — ${timestamp_mtn} MT`;

  const banner_color = guess_stage_color(stage, status);

  const step_flags_pretty = step_flags ? safe_json(step_flags) : "N/A";
  const test_flags_pretty = test_flags ? safe_json(test_flags) : "N/A";
  const config_pretty = ctx?.config ? safe_json(ctx.config) : "N/A";
  const paths_pretty = ctx?.paths ? safe_json(ctx.paths) : "N/A";
  const details_pretty = safe_json(details);

  const error_text =
    error
      ? (typeof error === "string" ? error : (error?.stack || error?.message || String(error)))
      : "";

  const html_body = `
  <div style="font-family: Arial, sans-serif; max-width: 980px; margin: auto;">
    <h2 style="margin: 0 0 6px 0;">🧠 ${job_name}</h2>

    <div style="
      background-color: ${banner_color};
      color: white;
      padding: 10px 12px;
      border-radius: 10px;
      font-weight: bold;
      margin-bottom: 14px;
    ">
      Stage: ${stage} &nbsp;|&nbsp; Status: ${status}
      ${elapsed_ms ? `&nbsp;|&nbsp; Elapsed: ${fmt_duration_ms(elapsed_ms)}` : ""}
    </div>

    <table style="width:100%; border-collapse: collapse; margin-bottom: 16px;">
      <tr><td style="padding:6px 0;"><strong>Timestamp (MT)</strong></td><td style="padding:6px 0;">${timestamp_mtn}</td></tr>
      <tr><td style="padding:6px 0;"><strong>Host</strong></td><td style="padding:6px 0;">${host}</td></tr>
      <tr><td style="padding:6px 0;"><strong>PID</strong></td><td style="padding:6px 0;">${pid}</td></tr>
      <tr><td style="padding:6px 0;"><strong>Node</strong></td><td style="padding:6px 0;">${node_version}</td></tr>
      <tr><td style="padding:6px 0;"><strong>Uptime</strong></td><td style="padding:6px 0;">${fmt_duration_ms(uptime_seconds * 1000)}</td></tr>
      <tr><td style="padding:6px 0;"><strong>Load Avg (1m)</strong></td><td style="padding:6px 0;">${load_1m}</td></tr>
      <tr><td style="padding:6px 0;"><strong>Memory</strong></td><td style="padding:6px 0;">${used_mem_gb} GB used (${mem_used_pct}%) — ${free_mem_gb} GB free / ${total_mem_gb} GB total</td></tr>
      <tr><td style="padding:6px 0;"><strong>Temp</strong></td><td style="padding:6px 0;">${temp_f} °F</td></tr>
      <tr><td style="padding:6px 0;"><strong>Disk (/)</strong></td><td style="padding:6px 0;">${disk.used_gb} GB used / ${disk.total_gb} GB total — ${disk.avail_gb} GB available (${disk.pct_used} used)</td></tr>
      <tr><td style="padding:6px 0;"><strong>CWD</strong></td><td style="padding:6px 0;"><code>${cwd}</code></td></tr>
      <tr><td style="padding:6px 0;"><strong>Git</strong></td><td style="padding:6px 0;">${git_branch || "N/A"} @ ${git_commit || "N/A"} (${git_dirty})</td></tr>
    </table>

    <h3 style="margin: 0 0 6px 0;">Stage Details</h3>
    <pre style="background:#111;color:#eee;padding:12px;border-radius:10px;font-size:12px;overflow-x:auto;white-space:pre-wrap;">${details_pretty}</pre>

    <h3 style="margin: 0 0 6px 0;">Enabled Steps</h3>
    <pre style="background:#111;color:#eee;padding:12px;border-radius:10px;font-size:12px;overflow-x:auto;white-space:pre-wrap;">${step_flags_pretty}</pre>

    <h3 style="margin: 0 0 6px 0;">Test Flags</h3>
    <pre style="background:#111;color:#eee;padding:12px;border-radius:10px;font-size:12px;overflow-x:auto;white-space:pre-wrap;">${test_flags_pretty}</pre>

    <h3 style="margin: 0 0 6px 0;">Config Snapshot</h3>
    <pre style="background:#111;color:#eee;padding:12px;border-radius:10px;font-size:12px;overflow-x:auto;white-space:pre-wrap;">${config_pretty}</pre>

    <h3 style="margin: 0 0 6px 0;">Paths</h3>
    <pre style="background:#111;color:#eee;padding:12px;border-radius:10px;font-size:12px;overflow-x:auto;white-space:pre-wrap;">${paths_pretty}</pre>

    ${
      error_text
        ? `<h3 style="margin: 0 0 6px 0; color:#dc3545;">Error</h3>
           <pre style="background:#111;color:#ffb3b3;padding:12px;border-radius:10px;font-size:12px;overflow-x:auto;white-space:pre-wrap;">${error_text}</pre>`
        : ""
    }
  </div>`;

  const mail_options = {
    from: {
      name: `${job_name} (${host})`,
      address: email_sender,
    },
    to: email_recipient,
    subject,
    text: `${subject}\n\n${details_pretty}\n\n${error_text ? `ERROR:\n${error_text}\n\n` : ""}`,
    html: html_body,
  };

  await send_mail(mail_options);
}

export { send_job_status_email };
// send_daily_summary_email.js  (ESM)
// node send_daily_summary_email.js
// Reads metrics.jsonl (JSON Lines) from this folder and emails a 24h health summary.

import os from "os";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { send_mail, mail_details, close_mail_transport } from "../../../utilities/email_sends/nodemailer.js";

// -----------------------------
// Paths (ESM __dirname)
// -----------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Same as your ThinkPad version
const metrics_file = path.join(__dirname, "metrics.jsonl");

// -----------------------------
// Utils
// -----------------------------
function to_num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function gib_from_bytes(b) {
  const n = to_num(b);
  if (n === null) return null;
  return n / 1024 / 1024 / 1024;
}

function fmt(n, digits = 1) {
  if (n === null) return "n/a";
  return n.toFixed(digits);
}

function fmt_pct(n, digits = 1) {
  if (n === null) return "n/a";
  return n.toFixed(digits) + "%";
}

function pct(part, total) {
  const p = to_num(part);
  const t = to_num(total);
  if (p === null || t === null || t <= 0) return null;
  return (p / t) * 100;
}

function parse_file() {
  if (!fs.existsSync(metrics_file)) return [];
  const raw = fs.readFileSync(metrics_file, "utf8").trim();
  if (!raw) return [];
  return raw
    .split("\n")
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function ts_utc_ms(ts_utc) {
  if (!ts_utc) return null;
  // expects "YYYY-MM-DD HH:MM:SS" or ISO-ish; force UTC
  const t = Date.parse(String(ts_utc).replace(" ", "T") + "Z");
  return Number.isFinite(t) ? t : null;
}

function peak(arr, key) {
  return arr.reduce((m, r) => {
    const a = to_num(m[key]);
    const b = to_num(r[key]);
    if (a === null) return r;
    if (b === null) return m;
    return b > a ? r : m;
  }, arr[0]);
}

function min_by(arr, key) {
  return arr.reduce((m, r) => {
    const a = to_num(m[key]);
    const b = to_num(r[key]);
    if (a === null) return r;
    if (b === null) return m;
    return b < a ? r : m;
  }, arr[0]);
}

function first_last_delta(arr, key) {
  if (!arr.length) return { first: null, last: null, delta: null };
  const first = to_num(arr[0][key]);
  const last = to_num(arr[arr.length - 1][key]);
  if (first === null || last === null) return { first, last, delta: null };
  return { first, last, delta: last - first };
}

function arrow(delta, epsilon = 0.01) {
  if (delta === null) return "→";
  if (delta > epsilon) return "↑";
  if (delta < -epsilon) return "↓";
  return "→";
}

// Convert a "badness" percentage (0..100) into penalty points (0..max_penalty)
function penalty_from_pct(pct_val, warn, crit, max_penalty) {
  const x = to_num(pct_val);
  if (x === null) return 0;

  if (x <= warn) return 0;
  if (x >= crit) return max_penalty;

  // linear between warn..crit
  const t = (x - warn) / (crit - warn);
  return t * max_penalty;
}

// Similar but for absolute values (e.g., io wait)
function penalty_from_abs(val, warn, crit, max_penalty) {
  const x = to_num(val);
  if (x === null) return 0;

  if (x <= warn) return 0;
  if (x >= crit) return max_penalty;

  const t = (x - warn) / (crit - warn);
  return t * max_penalty;
}

function status(val, warn, crit) {
  const x = to_num(val);
  if (x === null) return "UNKNOWN";
  if (x >= crit) return "CRITICAL";
  if (x >= warn) return "WARNING";
  return "OK";
}

function status_color(s) {
  if (s === "CRITICAL") return "#dc3545"; // red
  if (s === "WARNING") return "#fd7e14"; // orange
  if (s === "OK") return "#28a745"; // green
  return "#6c757d"; // gray
}

function score_badge_color(score) {
  if (score >= 90) return "#28a745";
  if (score >= 75) return "#fd7e14";
  return "#dc3545";
}

function fmt_ts_mtn(now = new Date()) {
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

function fmt_ts_mtn_from_ts_utc(ts_utc) {
  const ms = ts_utc_ms(ts_utc);
  if (ms === null) return "n/a";
  return new Date(ms).toLocaleString("en-US", {
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

function fmt_peak_val_at_mtn(val_str, ts_utc) {
  const when = fmt_ts_mtn_from_ts_utc(ts_utc);
  return `${val_str} @ ${when} MT`;
}

// -----------------------------
// Build report from last 24h
// -----------------------------
function build_daily_summary_report() {
  const all = parse_file();
  const now = Date.now();
  const day_ms = 24 * 60 * 60 * 1000;

  const day = all
    .filter((r) => {
      const t = ts_utc_ms(r.ts_utc);
      return t !== null && t >= now - day_ms;
    })
    .sort((a, b) => (ts_utc_ms(a.ts_utc) || 0) - (ts_utc_ms(b.ts_utc) || 0));

  if (!day.length) {
    return {
      ok: false,
      reason: "no metrics in last 24h",
    };
  }

  const latest = day[day.length - 1];

  // Peaks / mins
  const peak_load = peak(day, "load_1");
  const peak_disk_pct = peak(day, "disk_use_pct");
  const peak_swap = peak(day, "swap_used_b");
  const peak_wa = peak(day, "vm_wa");
  const min_mem = min_by(day, "mem_available_b");

  // RAM
  const mem_total = gib_from_bytes(latest.mem_total_b);
  const mem_used = gib_from_bytes(latest.mem_used_b);
  const mem_avail = gib_from_bytes(latest.mem_available_b);
  const mem_used_pct = pct(latest.mem_used_b, latest.mem_total_b);
  const min_mem_avail = gib_from_bytes(min_mem.mem_available_b);

  // SWAP
  const swap_total = gib_from_bytes(latest.swap_total_b);
  const swap_used = gib_from_bytes(latest.swap_used_b);
  const swap_used_pct = pct(latest.swap_used_b, latest.swap_total_b);
  const peak_swap_used = gib_from_bytes(peak_swap.swap_used_b);

  // DISK
  const disk_total = gib_from_bytes(latest.disk_size_b);
  const disk_used = gib_from_bytes(latest.disk_used_b);
  const disk_pct = to_num(latest.disk_use_pct);
  const peak_disk = to_num(peak_disk_pct.disk_use_pct);

  // CPU utilization vs cores (approx)
  const cpu_load_pct = pct(latest.load_1, latest.cores);

  // Memory pressure (PSI)
  const psi_some = to_num(latest.psi_some_avg10);
  const psi_full = to_num(latest.psi_full_avg10);

  // Trends (first → last) over 24h window
  const t_load_1 = first_last_delta(day, "load_1");
  const t_mem_used_pct = (() => {
    const first = pct(day[0].mem_used_b, day[0].mem_total_b);
    const last = pct(latest.mem_used_b, latest.mem_total_b);
    if (first === null || last === null) return { first, last, delta: null };
    return { first, last, delta: last - first };
  })();
  const t_disk_pct = first_last_delta(day, "disk_use_pct");
  const t_swap_used_b = first_last_delta(day, "swap_used_b");
  const t_vm_wa = first_last_delta(day, "vm_wa");

  // Section status thresholds
  const mem_status = status(mem_used_pct, 75, 90);
  const disk_status = status(disk_pct, 75, 90);
  const cpu_status = status(cpu_load_pct, 70, 90);
  const swap_status = status(swap_used_pct, 70, 90);
  const io_status = status(to_num(latest.vm_wa), 5, 10);

  // Pressure status (simple)
  let pressure_status = "OK";
  // psi_full > 0 indicates real stalls; psi_some indicates reclaim contention
  if ((psi_full !== null && psi_full > 0.2) || (psi_some !== null && psi_some > 5)) {
    pressure_status = "CRITICAL";
  } else if ((psi_full !== null && psi_full > 0.0) || (psi_some !== null && psi_some > 1)) {
    pressure_status = "WARNING";
  }

  // Overall health
  const statuses = [mem_status, disk_status, cpu_status, swap_status, io_status, pressure_status];
  let overall = "OK";
  if (statuses.includes("CRITICAL")) overall = "CRITICAL";
  else if (statuses.includes("WARNING")) overall = "WARNING";

  // 0–100 Health Score (higher is better)
  const penalty_mem = penalty_from_pct(mem_used_pct, 75, 90, 25);
  const penalty_disk = penalty_from_pct(disk_pct, 75, 90, 20);
  const penalty_cpu = penalty_from_pct(cpu_load_pct, 70, 90, 20);
  const penalty_swap = penalty_from_pct(swap_used_pct, 70, 90, 15);
  const penalty_io = penalty_from_abs(to_num(latest.vm_wa), 5, 10, 10);

  // PSI penalty (very sensitive to full)
  let penalty_psi = 0;
  if (psi_full !== null) penalty_psi += penalty_from_abs(psi_full, 0.01, 0.2, 10);
  else if (psi_some !== null) penalty_psi += penalty_from_abs(psi_some, 1, 5, 5);

  let score = 100 - (penalty_mem + penalty_disk + penalty_cpu + penalty_swap + penalty_io + penalty_psi);
  score = Math.max(0, Math.min(100, Math.round(score)));

  const peak_load_str = fmt_peak_val_at_mtn(
    to_num(peak_load.load_1) === null ? "n/a" : to_num(peak_load.load_1).toFixed(2),
    peak_load.ts_utc
  );

  const peak_disk_str = fmt_peak_val_at_mtn(
    to_num(peak_disk_pct.disk_use_pct) === null ? "n/a" : to_num(peak_disk_pct.disk_use_pct).toFixed(0) + "%",
    peak_disk_pct.ts_utc
  );

  const peak_swap_str = fmt_peak_val_at_mtn(
    fmt(gib_from_bytes(peak_swap.swap_used_b), 2) + " GiB",
    peak_swap.ts_utc
  );

  const peak_wa_str = fmt_peak_val_at_mtn(
    to_num(peak_wa.vm_wa) === null ? "n/a" : to_num(peak_wa.vm_wa).toFixed(1) + "%",
    peak_wa.ts_utc
  );

  const min_mem_str = fmt_peak_val_at_mtn(
    fmt(gib_from_bytes(min_mem.mem_available_b), 2) + " GiB",
    min_mem.ts_utc
  );

  // Plain-text message (good for email text body / logs)
  const text_message =
    `[${latest.host_name}] 24h System Health Summary
Overall: ${overall}   Health Score: ${score}/100
Samples analyzed (last 24h): ${day.length}

RAM
- Used: ${fmt(mem_used)}/${fmt(mem_total)} GiB (${fmt_pct(mem_used_pct)})   Trend: ${arrow(t_mem_used_pct.delta)} (${fmt(t_mem_used_pct.delta, 1)} pp)
- Available: ${fmt(mem_avail)} GiB
- Min Available (24h): ${min_mem_str}
- Status: ${mem_status}
- PSI avg10: some=${psi_some === null ? "n/a" : psi_some.toFixed(2)}  full=${psi_full === null ? "n/a" : psi_full.toFixed(2)}   Status: ${pressure_status}

CPU
- Load (1/5/15): ${latest.load_1}/${latest.load_5}/${latest.load_15}   Trend (1m): ${arrow(t_load_1.delta)} (${fmt(t_load_1.delta, 2)})
- Peak 1m load (24h): ${peak_load_str}
- Util vs capacity (approx): ${fmt_pct(cpu_load_pct)}
- Cores: ${latest.cores}
- Status: ${cpu_status}

Disk (/)
- Used: ${fmt(disk_used)}/${fmt(disk_total)} GiB (${disk_pct === null ? "n/a" : disk_pct.toFixed(0)}%)   Trend: ${arrow(t_disk_pct.delta)} (${fmt(t_disk_pct.delta, 1)} pp)
- Peak usage % (24h): ${peak_disk_str}
- Status: ${disk_status}

Swap
- Used: ${fmt(swap_used)}/${fmt(swap_total)} GiB (${fmt_pct(swap_used_pct)})   Trend: ${arrow(t_swap_used_b.delta)} (${fmt(gib_from_bytes(t_swap_used_b.delta), 2)} GiB)
- Peak swap used (24h): ${peak_swap_str}
- Status: ${swap_status}

Disk IO Wait
- Current: ${latest.vm_wa}%   Trend: ${arrow(t_vm_wa.delta)} (${fmt(t_vm_wa.delta, 1)} pp)
- Peak (24h): ${peak_wa_str}
- Status: ${io_status}

Temps
- CPU: ${latest.cpu_temp_c || "n/a"}°C
- NVMe: ${latest.nvme_temp_c || "n/a"}°C
`;

  return {
    ok: true,
    latest,
    day_count: day.length,
    overall,
    score,
    mem_status,
    disk_status,
    cpu_status,
    swap_status,
    io_status,
    pressure_status,
    psi_some,
    psi_full,
    mem_total,
    mem_used,
    mem_avail,
    mem_used_pct,
    min_mem_avail,
    swap_total,
    swap_used,
    swap_used_pct,
    peak_swap_used,
    disk_total,
    disk_used,
    disk_pct,
    peak_disk,
    cpu_load_pct,
    t_load_1,
    t_mem_used_pct,
    t_disk_pct,
    t_swap_used_b,
    t_vm_wa,
    peak_load,
    peak_wa,
    text_message,
    peak_load_str,
    peak_disk_str,
    peak_swap_str,
    peak_wa_str,
    min_mem_str,
  };
}

// -----------------------------
// Email sender
// -----------------------------
async function send_daily_summary_via_email() {
  const report = build_daily_summary_report();
  const formattedDate = fmt_ts_mtn(new Date());

  // If no data, send a short email anyway (so you notice it)
  if (!report.ok) {
    const subject = `Daily System Health — NO DATA — ${formattedDate} MT`;
    const html = `
      <div style="font-family: Arial, sans-serif; max-width: 900px; margin: auto;">
        <h2 style="margin-bottom: 6px;">🖥️ Daily System Health Summary</h2>
        <div style="background:#dc3545;color:white;padding:10px;border-radius:6px;font-weight:bold;text-align:center;">
          NO METRICS IN LAST 24 HOURS
        </div>
        <p style="margin-top:14px;">metrics.jsonl path:</p>
        <pre style="background:#111;color:#eee;padding:12px;border-radius:6px;white-space:pre-wrap;">${metrics_file}</pre>
      </div>
    `;

    const mail_options = {
      from: mail_details?.from || {
        name: "System Health",
        address: process.env.MAIL_FROM || "callasteven@gmail.com",
      },
      to: mail_details?.to || process.env.MAIL_TO || "callasteven@gmail.com",
      subject,
      text: `NO METRICS IN LAST 24 HOURS\n\nmetrics.jsonl: ${metrics_file}\n`,
      html,
    };

    await send_mail(mail_options);
    return;
  }

  const host = report.latest.host_name || os.hostname();
  const overallColor = status_color(report.overall);
  const scoreColor = score_badge_color(report.score);

  const row = (label, value, color = null, bold = false) => `
    <tr>
      <td style="padding:6px 0; vertical-align: top;"><strong>${label}</strong></td>
      <td style="padding:6px 0; ${color ? `color:${color};` : ""} ${bold ? "font-weight:bold;" : ""}">${value}</td>
    </tr>
  `;

  const html_body = `
  <div style="font-family: Arial, sans-serif; max-width: 900px; margin: auto;">

    <h2 style="margin-bottom: 6px;">🖥️ ${host} — 24h System Health Summary</h2>

    <div style="
      background-color: ${overallColor};
      color: white;
      padding: 10px;
      border-radius: 8px;
      font-weight: bold;
      text-align: center;
      margin-bottom: 12px;
    ">
      Overall Status: ${report.overall}
    </div>

    <div style="
      background-color: ${scoreColor};
      color: white;
      padding: 10px;
      border-radius: 8px;
      font-weight: bold;
      text-align: center;
      margin-bottom: 18px;
    ">
      Health Score: ${report.score}/100
    </div>

    <table style="width:100%; border-collapse: collapse; margin-bottom: 18px;">
      ${row("Timestamp (MT)", formattedDate)}
      ${row("Samples (24h)", String(report.day_count))}
      ${row("RAM Status", report.mem_status, status_color(report.mem_status), true)}
      ${row("CPU Status", report.cpu_status, status_color(report.cpu_status), true)}
      ${row("Disk Status", report.disk_status, status_color(report.disk_status), true)}
      ${row("Swap Status", report.swap_status, status_color(report.swap_status), true)}
      ${row("IO Wait Status", report.io_status, status_color(report.io_status), true)}
      ${row("Memory Pressure Status", report.pressure_status, status_color(report.pressure_status), true)}
    </table>

    <h3 style="margin: 16px 0 8px 0;">Key Metrics</h3>
    <table style="width:100%; border-collapse: collapse; margin-bottom: 18px;">
      ${row("RAM Used", `${fmt(report.mem_used)}/${fmt(report.mem_total)} GiB (${fmt_pct(report.mem_used_pct)}) • Trend ${arrow(report.t_mem_used_pct.delta)} (${fmt(report.t_mem_used_pct.delta, 1)} pp)`)}
      ${row("RAM Available", `${fmt(report.mem_avail)} GiB • Min (24h) ${report.min_mem_str}`)}
      ${row("PSI avg10", `some=${report.psi_some === null ? "n/a" : report.psi_some.toFixed(2)} • full=${report.psi_full === null ? "n/a" : report.psi_full.toFixed(2)}`)}
      ${row("CPU Load (1/5/15)", `${report.latest.load_1}/${report.latest.load_5}/${report.latest.load_15} • Trend (1m) ${arrow(report.t_load_1.delta)} (${fmt(report.t_load_1.delta, 2)}) • Peak (24h) ${report.peak_load_str}`)}
      ${row("CPU Util (approx)", `${fmt_pct(report.cpu_load_pct)} • Cores ${report.latest.cores}`)}
      ${row("Disk (/)", `${fmt(report.disk_used)}/${fmt(report.disk_total)} GiB (${report.disk_pct === null ? "n/a" : report.disk_pct.toFixed(0)}%) • Trend ${arrow(report.t_disk_pct.delta)} (${fmt(report.t_disk_pct.delta, 1)} pp) • Peak (24h) ${report.peak_disk_str}`)}
      ${row("Swap", `${fmt(report.swap_used)}/${fmt(report.swap_total)} GiB (${fmt_pct(report.swap_used_pct)}) • Peak (24h) ${report.peak_swap_str} • Trend ${arrow(report.t_swap_used_b.delta)} (${fmt(gib_from_bytes(report.t_swap_used_b.delta), 2)} GiB)`)}
      ${row("IO Wait", `${report.latest.vm_wa}% • Peak (24h) ${report.peak_wa_str} • Trend ${arrow(report.t_vm_wa.delta)} (${fmt(report.t_vm_wa.delta, 1)} pp)`)}
      ${row("Temps", `CPU ${report.latest.cpu_temp_c || "n/a"}°C • NVMe ${report.latest.nvme_temp_c || "n/a"}°C`)}
    </table>

    <h3 style="margin: 16px 0 8px 0;">Full Text Summary</h3>
    <pre style="
      background: #111;
      color: #eee;
      padding: 12px;
      border-radius: 8px;
      font-size: 12px;
      overflow-x: auto;
      white-space: pre-wrap;
      margin-bottom: 0;
    ">${report.text_message}</pre>

  </div>
  `;

  const subject = `Daily System Health: Dell-${host} — ${report.overall} — ${report.score}/100 — ${formattedDate} MT`;

  const mail_options = {
    from: mail_details?.from || {
      name: `System Health ${host}`,
      address: process.env.MAIL_FROM || "callasteven@gmail.com",
    },
    to: mail_details?.to || process.env.MAIL_TO || "callasteven@gmail.com",
    subject,
    text: report.text_message,
    html: html_body,
  };

  await send_mail(mail_options);
  close_mail_transport();     // lets Node exit naturally
}

// -----------------------------
// Run (direct execution)
// -----------------------------
const is_main = process.argv[1] === __filename;
if (is_main) {
  send_daily_summary_via_email().catch((e) => {
    console.error("send_daily_summary_via_email failed:", e?.stack || e?.message || e);
    process.exitCode = 1;
  });
}

// -----------------------------
// Exports
// -----------------------------
export { build_daily_summary_report, send_daily_summary_via_email };
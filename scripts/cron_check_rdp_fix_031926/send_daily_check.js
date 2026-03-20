// send_rdp_fix_check_email.js  (ESM)
// node send_rdp_fix_check_email.js
// Reads check_rdp_fix.log from this folder and emails the latest status.

import os from "os";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { send_mail, mail_details, close_mail_transport } from "../../utilities/email_sends/nodemailer.js";

// -----------------------------
// Paths (ESM __dirname)
// -----------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const check_log_file = path.join(__dirname, "check_rdp_fix.log");

// -----------------------------
// Utils
// -----------------------------
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

function escape_html(str = "") {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function read_check_log() {
  if (!fs.existsSync(check_log_file)) {
    return {
      ok: false,
      reason: "check_rdp_fix.log not found",
      log_text: "",
      status: "UNKNOWN",
      action_needed: true,
    };
  }

  const log_text = fs.readFileSync(check_log_file, "utf8").trim();

  if (!log_text) {
    return {
      ok: false,
      reason: "check_rdp_fix.log is empty",
      log_text: "",
      status: "UNKNOWN",
      action_needed: true,
    };
  }

  let status = "UNKNOWN";
  let action_needed = true;

  if (
    log_text.includes("⚠️ Update is still the KNOWN BROKEN version") ||
    log_text.includes("👉 DO NOT upgrade") ||
    log_text.includes("👉 Keep packages on hold") ||
    log_text.includes("✅ No updates available — remaining on regression-safe version")
  ) {
    status = "NO ACTION NEEDED";
    action_needed = false;
  } else if (
    log_text.includes("🚨 Candidate new version(s) detected:") ||
    log_text.includes("👉 Recommended next steps:")
  ) {
    status = "ACTION REQUIRED";
    action_needed = true;
  }

  return {
    ok: true,
    log_text,
    status,
    action_needed,
  };
}

function status_color(status) {
  if (status === "ACTION REQUIRED") return "#fd7e14";
  if (status === "NO ACTION NEEDED") return "#28a745";
  return "#6c757d";
}

// -----------------------------
// Email sender
// -----------------------------
async function send_rdp_fix_check_via_email() {
  const report = read_check_log();
  const formatted_date = fmt_ts_mtn(new Date());
  const host = os.hostname();

  const subject_status = report.status || "UNKNOWN";
  const badge_color = status_color(subject_status);

  if (!report.ok) {
    const subject = `RDP Fix Check: ${host} — UNKNOWN — ${formatted_date} MT`;

    const html = `
      <div style="font-family: Arial, sans-serif; max-width: 900px; margin: auto;">
        <h2 style="margin-bottom: 6px;">🖥️ RDP Fix Check</h2>
        <div style="background:#6c757d;color:white;padding:10px;border-radius:6px;font-weight:bold;text-align:center;">
          STATUS: UNKNOWN
        </div>
        <p style="margin-top:14px;"><strong>Reason:</strong> ${escape_html(report.reason)}</p>
        <p><strong>Log path:</strong></p>
        <pre style="background:#111;color:#eee;padding:12px;border-radius:6px;white-space:pre-wrap;">${escape_html(check_log_file)}</pre>
      </div>
    `;

    const mail_options = {
      from: mail_details?.from || {
        name: "RDP Fix Check",
        address: process.env.MAIL_FROM || "callasteven@gmail.com",
      },
      to: mail_details?.to || process.env.MAIL_TO || "callasteven@gmail.com",
      subject,
      text: `STATUS: UNKNOWN\nReason: ${report.reason}\nLog path: ${check_log_file}\n`,
      html,
    };

    await send_mail(mail_options);
    close_mail_transport();
    return;
  }

  const html = `
    <div style="font-family: Arial, sans-serif; max-width: 900px; margin: auto;">
      <h2 style="margin-bottom: 6px;">🖥️ ${host} — RDP Fix Check</h2>

      <div style="
        background-color: ${badge_color};
        color: white;
        padding: 10px;
        border-radius: 8px;
        font-weight: bold;
        text-align: center;
        margin-bottom: 18px;
      ">
        STATUS: ${escape_html(report.status)}
      </div>

      <table style="width:100%; border-collapse: collapse; margin-bottom: 18px;">
        <tr>
          <td style="padding:6px 0; vertical-align: top;"><strong>Timestamp (MT)</strong></td>
          <td style="padding:6px 0;">${formatted_date}</td>
        </tr>
        <tr>
          <td style="padding:6px 0; vertical-align: top;"><strong>Host</strong></td>
          <td style="padding:6px 0;">${escape_html(host)}</td>
        </tr>
        <tr>
          <td style="padding:6px 0; vertical-align: top;"><strong>Action Needed</strong></td>
          <td style="padding:6px 0;">${report.action_needed ? "YES" : "NO"}</td>
        </tr>
        <tr>
          <td style="padding:6px 0; vertical-align: top;"><strong>Log File</strong></td>
          <td style="padding:6px 0;">${escape_html(check_log_file)}</td>
        </tr>
      </table>

      <h3 style="margin: 16px 0 8px 0;">Check Log</h3>
      <pre style="
        background: #111;
        color: #eee;
        padding: 12px;
        border-radius: 8px;
        font-size: 12px;
        overflow-x: auto;
        white-space: pre-wrap;
        margin-bottom: 0;
      ">${escape_html(report.log_text)}</pre>
    </div>
  `;

  const text = [
    `${host} — RDP Fix Check`,
    `Status: ${report.status}`,
    `Action Needed: ${report.action_needed ? "YES" : "NO"}`,
    `Timestamp (MT): ${formatted_date}`,
    `Log File: ${check_log_file}`,
    "",
    "Check Log:",
    report.log_text,
  ].join("\n");

  const subject = `RDP Fix Check: ${host} — ${subject_status} — ${formatted_date} MT`;

  const mail_options = {
    from: mail_details?.from || {
      name: `RDP Fix Check ${host}`,
      address: process.env.MAIL_FROM || "callasteven@gmail.com",
    },
    to: mail_details?.to || process.env.MAIL_TO || "callasteven@gmail.com",
    subject,
    text,
    html,
  };

  await send_mail(mail_options);
  close_mail_transport();
}

// -----------------------------
// Run (direct execution)
// -----------------------------
const is_main = process.argv[1] === __filename;
if (is_main) {
  send_rdp_fix_check_via_email().catch((e) => {
    console.error("send_rdp_fix_check_via_email failed:", e?.stack || e?.message || e);
    process.exitCode = 1;
  });
}

// -----------------------------
// Exports
// -----------------------------
export { read_check_log, send_rdp_fix_check_via_email };
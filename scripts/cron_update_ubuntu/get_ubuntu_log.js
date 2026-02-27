import os from "os";
import fs from "fs";
import { execSync } from "child_process";

import { determine_ubuntu_update_log_file_path } from "../../utilities/directory_tools/determine_os_path.js";
import { send_mail, mail_details } from "../../utilities/email_sends/nodemailer.js";

// const { slack_message_api } = require("../../schedule_slack/slack_message_api");

// ====================================================
// Helpers: Disk + Temperature
// ====================================================

function get_root_disk_stats() {
    // Returns totals for "/" in GB + percent used
    // Uses POSIX df output; stable for scripting
    try {
        const out = execSync("df -P -B1 /", { encoding: "utf8" }).trim().split("\n");
        if (!out[1]) {
            return {
                pct_used: "N/A",
                total_gb: "N/A",
                used_gb: "N/A",
                avail_gb: "N/A",
            };
        }

        const cols = out[1].split(/\s+/);
        // Filesystem 1B-blocks Used Available Capacity Mounted on
        //    0         1B        2     3        4       5
        const totalBytes = Number(cols[1]);
        const usedBytes = Number(cols[2]);
        const availBytes = Number(cols[3]);
        const capacity = cols[4]; // e.g. "63%"

        const toGB = (b) => (Number.isFinite(b) ? (b / 1024 / 1024 / 1024).toFixed(2) : "N/A");

        return {
            pct_used: capacity || "N/A",
            total_gb: toGB(totalBytes),
            used_gb: toGB(usedBytes),
            avail_gb: toGB(availBytes),
        };
    } catch (e) {
        return {
            pct_used: "N/A",
            total_gb: "N/A",
            used_gb: "N/A",
            avail_gb: "N/A",
        };
    }
}

function get_system_temp_f() {
    let tempC = null;

    // Prefer sysfs: /sys/class/thermal/thermal_zone*/temp
    try {
        const base = "/sys/class/thermal";
        if (fs.existsSync(base)) {
            const zones = fs.readdirSync(base).filter((n) => n.startsWith("thermal_zone"));
            for (const z of zones) {
                const tempPath = `${base}/${z}/temp`;
                if (!fs.existsSync(tempPath)) continue;

                const raw = fs.readFileSync(tempPath, "utf8").trim();
                const val = Number(raw);
                if (Number.isNaN(val)) continue;

                tempC = val > 1000 ? val / 1000 : val;

                // sanity range
                if (tempC > 0 && tempC < 120) break;
            }
        }
    } catch (e) {
        // ignore
    }

    // Fallback to lm-sensors if needed
    if (tempC === null) {
        try {
            const out = execSync("sensors", { encoding: "utf8" });
            const m = out.match(/\+([0-9]{1,3}\.[0-9])°C|\+([0-9]{1,3})°C/);
            if (m) tempC = Number(m[1] || m[2]);
        } catch (e) {
            return "N/A";
        }
    }

    if (tempC === null || Number.isNaN(tempC)) return "N/A";

    const tempF = (tempC * 9) / 5 + 32;
    return tempF.toFixed(1);
}

// ====================================================
// Read ubuntu update log
// ====================================================

async function read_ubuntu_update_log(char_limit = -500) {
    const file_name = "ubuntu-update.log";

    const ubuntu_update_log_content = await determine_ubuntu_update_log_file_path(file_name);
    const { file_path, platform } = ubuntu_update_log_content;

    console.log(platform);
    console.log(file_path);

    try {
        const data = fs.readFileSync(file_path, "utf8");

        // Character count = string length
        const char_count = data.length;

        // Get only the last N characters
        const last_n_char = data.slice(char_limit);

        console.log("Last characters:\n", last_n_char);
        console.log(`\nTotal Characters: ${char_count.toLocaleString()}`);

        return last_n_char;
    } catch (err) {
        console.error(`Error reading file at ${file_name}:`, err.message);
        return null;
    }
}

// ====================================================
// Slack sender (unchanged)
// ====================================================

async function send_ubuntu_log_via_slack() {
    let message = await read_ubuntu_update_log(-500);

    message = `\n================\nUBUNUTU SYSTEM UPDATE / UPGRADE\n================\n${message} \n\n================\nUBUNUTU SYSTEM UPDATE / UPGRADE \n================\n`;

    console.log(message);

    await slack_message_api(message, "steve_calla_slack_channel");
}

// ====================================================
// Email sender (enhanced)
// ====================================================

async function send_ubuntu_log_via_email() {
    // ---------------------------------------  
    // Read logs
    // ---------------------------------------
    const email_message = await read_ubuntu_update_log(-500);
    const message = await read_ubuntu_update_log(-1000);

    if (!message) {
        console.error("No log message found.");
        return;
    }

    // ---------------------------------------
    // Determine reboot status
    // ---------------------------------------
    let reboot_status = "Reboot Status Unknown";
    const lowerMessage = message.toLowerCase();

    if (lowerMessage.includes("run: sudo reboot")) {
        reboot_status = "⚠️ REBOOT REQUIRED";
    } else if (lowerMessage.includes("no reboot required")) {
        reboot_status = "✅ No Reboot Required";
    }

    // ---------------------------------------
    // Mountain Time timestamp
    // ---------------------------------------
    const now = new Date();
    const formattedDate = now.toLocaleString("en-US", {
        timeZone: "America/Denver",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
    });

    // ---------------------------------------
    // System metrics
    // ---------------------------------------
    const uptimeHours = (os.uptime() / 3600).toFixed(1);
    const kernel = os.release();
    const loadAvg = os.loadavg()[0].toFixed(2);

    const freeMemGB = (os.freemem() / 1024 / 1024 / 1024).toFixed(2);
    const totalMemGB = (os.totalmem() / 1024 / 1024 / 1024).toFixed(2);
    const usedMemGB = (Number(totalMemGB) - Number(freeMemGB)).toFixed(2);
    const memUsedPct = ((Number(usedMemGB) / Number(totalMemGB)) * 100).toFixed(1);

    const tempF = get_system_temp_f();

    const disk = get_root_disk_stats();
    const rootDiskPct = disk.pct_used;

    // Optional highlighting thresholds (Fahrenheit)
    const tempNum = Number(tempF);
    const isHot = !Number.isNaN(tempNum) && tempNum >= 176;     // ~80°C
    const isVeryHot = !Number.isNaN(tempNum) && tempNum >= 194; // ~90°C

    const diskNum = Number(String(rootDiskPct).replace("%", ""));
    const diskHigh = !Number.isNaN(diskNum) && diskNum >= 85;

    // ---------------------------------------
    // Status banner color
    // ---------------------------------------
    let statusColor = "#6c757d"; // default gray

    if (reboot_status.includes("REBOOT REQUIRED")) {
        statusColor = "#dc3545"; // red
    } else if (reboot_status.includes("No Reboot Required")) {
        statusColor = "#28a745"; // green
    }

    // ---------------------------------------
    // HTML Body
    // ---------------------------------------
    const html_body = `
    <div style="font-family: Arial, sans-serif; max-width: 800px; margin: auto;">

        <h2 style="margin-bottom: 5px;">
            🖥️ Dell Linux Server 7420
        </h2>

        <div style="
            background-color: ${statusColor};
            color: white;
            padding: 10px;
            border-radius: 6px;
            font-weight: bold;
            text-align: center;
            margin-bottom: 15px;
        ">
            ${reboot_status}
        </div>

        <table style="width:100%; border-collapse: collapse; margin-bottom: 20px;">
            <tr>
                <td style="padding:6px 0;"><strong>Timestamp (MT)</strong></td>
                <td style="padding:6px 0;">${formattedDate}</td>
            </tr>
            <tr>
                <td style="padding:6px 0;"><strong>Kernel</strong></td>
                <td style="padding:6px 0;">${kernel}</td>
            </tr>
            <tr>
                <td style="padding:6px 0;"><strong>Uptime</strong></td>
                <td style="padding:6px 0;">${uptimeHours} hours</td>
            </tr>
            <tr>
                <td style="padding:6px 0;"><strong>Load Avg (1m)</strong></td>
                <td style="padding:6px 0;">${loadAvg}</td>
            </tr>
            <tr>
                <td style="padding:6px 0;"><strong>Memory</strong></td>
                <td style="padding:6px 0;">${usedMemGB} GB used (${memUsedPct}%) — ${freeMemGB} GB free / ${totalMemGB} GB total</td>
            </tr>
            <tr>
                <td style="padding:6px 0;"><strong>System Temp</strong></td>
                <td style="padding:6px 0; font-weight:${isVeryHot ? "bold" : "normal"}; color:${isHot ? "#dc3545" : "inherit"};">
                    ${tempF} °F
                </td>
            </tr>
            <tr>
                <td style="padding:6px 0;"><strong>Disk ( / )</strong></td>
                <td style="padding:6px 0; font-weight:${diskHigh ? "bold" : "normal"}; color:${diskHigh ? "#dc3545" : "inherit"};">
                    ${disk.used_gb} GB used / ${disk.total_gb} GB total — ${disk.avail_gb} GB available (${disk.pct_used} used)
                </td>
            </tr>
        </table>

        <h3>Recent Update Log</h3>

        <pre style="
            background: #111;
            color: #eee;
            padding: 12px;
            border-radius: 6px;
            font-size: 12px;
            overflow-x: auto;
            white-space: pre-wrap;
        ">
${email_message || ""}
        </pre>

    </div>
    `;

    // ---------------------------------------
    // Build mail
    // ---------------------------------------
    const mail_options = {
        from: {
            name: "Dell Linux Server 7420 192.168.1.125",
            address: "callasteven@gmail.com",
        },
        to: "callasteven@gmail.com",
        subject: `Daily Dell Ubuntu Update — ${reboot_status} — ${formattedDate} MT`,
        text: email_message || "",
        html: html_body,
    };

    await send_mail(mail_options);
}

// ====================================================
// Run
// ====================================================

// send_ubuntu_log_via_slack().catch(e => {
//   console.log(e?.stack || e);
// });

send_ubuntu_log_via_email().catch((e) => {
    console.log(e?.stack || e);
});

// ====================================================
// Exports
// ====================================================

export { read_ubuntu_update_log, send_ubuntu_log_via_slack };
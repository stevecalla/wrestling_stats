import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import express from "express";
import os from "os";
import { execSync } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, ".env") });

const app = express();
const port = Number(process.env.SERVER_HEALTH_CHECK_PORT || 3000);

app.set("trust proxy", true);

//
// ---------- Helpers ----------
//

function format_duration(seconds) {
  seconds = Math.floor(seconds);

  const days = Math.floor(seconds / 86400);
  seconds %= 86400;

  const hours = Math.floor(seconds / 3600);
  seconds %= 3600;

  const minutes = Math.floor(seconds / 60);
  seconds %= 60;

  return `${days}d ${hours}h ${minutes}m ${seconds}s`;
}

function get_disk_usage() {
  try {
    const output = execSync("df -h /").toString().split("\n")[1];
    const parts = output.split(/\s+/);

    return {
      size: parts[1],
      used: parts[2],
      available: parts[3],
      use_percent: parts[4],
    };
  } catch {
    return {
      size: "unknown",
      used: "unknown",
      available: "unknown",
      use_percent: "unknown",
    };
  }
}

function log_block(title, obj) {
  console.log("***************************************");
  console.log(title);
  Object.entries(obj).forEach(([key, value]) => {
    console.log(`  ${key}: ${value}`);
  });
  console.log("=======================================");
}

//
// ---------- Request Logger ----------
//

app.use((req, res, next) => {
  const start = Date.now();

  res.on("finish", () => {
    const ms = Date.now() - start;

    console.log(
      `${new Date().toISOString()} | ${req.method} ${req.originalUrl} | ${res.statusCode} | ${ms}ms | ${req.ip}`
    );
  });

  next();
});

//
// ---------- Health Route ----------
//

app.get("/health", (req, res) => {
  const mem = process.memoryUsage();
  const disk = get_disk_usage();

  const status_details = {
    status: "ok",
    server: "health server is up and running. stands_ready.",
    requesting_ip: req.ip,
    uptime: format_duration(process.uptime()),
    load_avg: os.loadavg().map(n => n.toFixed(2)).join(", "),
    cpu_cores: os.cpus().length,
    memory_used_mb: (mem.rss / 1024 / 1024).toFixed(1),
    memory_total_mb: (os.totalmem() / 1024 / 1024).toFixed(0),
    memory_free_mb: (os.freemem() / 1024 / 1024).toFixed(0),
    disk_used: disk.used,
    disk_available: disk.available,
    disk_percent: disk.use_percent,
    local_time: new Date().toLocaleString(),
    timestamp: new Date().toISOString(),
  };

  log_block("status_details:", status_details);

  res.status(200).json(status_details);
});

//
// ---------- Start Server ----------
//

app.listen(port, "0.0.0.0", () => {
  console.log(`health_check_running_on_port ${port}`);
});
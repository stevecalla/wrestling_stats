// server_health_check_8000.js
import path from "path";
import rateLimit from "express-rate-limit";
import os from "os";
import { execSync } from "child_process";

import { fileURLToPath } from "url";
import dotenv from "dotenv";
import express from "express";


const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, ".env") });

const app = express();
const port = Number(process.env.SERVER_HEALTH_CHECK_PORT || 3000);

// IMPORTANT: if you're behind Cloudflare / reverse proxy, keep this true
app.set("trust proxy", true);

//
// ---------- Helpers ----------
//

// Best-effort "real client IP" + where it came from.
// This also helps rate limiting key off the real IP when behind Cloudflare.
function get_client_ip_info(req) {
  const cf_ip = (req.headers["cf-connecting-ip"] || "").toString().trim();
  const xff = (req.headers["x-forwarded-for"] || "").toString().trim();
  const x_real_ip = (req.headers["x-real-ip"] || "").toString().trim();

  // x-forwarded-for can be a comma-separated chain: "client, proxy1, proxy2"
  const xff_first = xff ? xff.split(",")[0].trim() : "";

  // express sets req.ip based on trust proxy
  const express_ip = (req.ip || "").toString().trim();

  // Prefer Cloudflare header if present, then XFF first hop, then X-Real-IP, then Express.
  const ip = cf_ip || xff_first || x_real_ip || express_ip || "unknown";

  let source = "unknown";
  if (cf_ip) source = "cf-connecting-ip";
  else if (xff_first) source = "x-forwarded-for:first";
  else if (x_real_ip) source = "x-real-ip";
  else if (express_ip) source = "express:req.ip";

  return {
    ip,
    source,
    cf_connecting_ip: cf_ip || null,
    x_forwarded_for: xff || null,
    x_real_ip: x_real_ip || null,
    express_req_ip: express_ip || null,
  };
}

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
// ---------- Rate Limiter ----------
//
// Install:
//   npm i express-rate-limit
//
// Defaults below are conservative for a public /health endpoint.
// Tune via env vars if desired.

const window_ms = Number(process.env.HEALTH_RL_WINDOW_MS || 60_000); // 1 minute
const max_requests = Number(process.env.HEALTH_RL_MAX || 60); // per IP per window

const health_rate_limiter = rateLimit({
  windowMs: window_ms,
  max: max_requests,
  standardHeaders: true, // adds RateLimit-* headers
  legacyHeaders: false,
  // Key by our "real IP" extraction (better behind proxies)
  keyGenerator: (req) => get_client_ip_info(req).ip,
  // Friendly JSON response on block
  handler: (req, res /*, next, options */) => {
    const ip_info = get_client_ip_info(req);
    const payload = {
      status: "rate_limited",
      message: "too many requests",
      requesting_ip: ip_info.ip,
      ip_source: ip_info.source,
      window_ms,
      max_requests,
      timestamp: new Date().toISOString(),
    };

    console.log(
      `${new Date().toISOString()} | RATE_LIMIT | ${req.method} ${req.originalUrl} | 429 | ${ip_info.ip} (${ip_info.source})`
    );

    return res.status(429).json(payload);
  },
});

//
// ---------- Request Logger ----------
//

app.use((req, res, next) => {
  const start = Date.now();
  const ip_info = get_client_ip_info(req);

  res.on("finish", () => {
    const ms = Date.now() - start;

    console.log(
      `${new Date().toISOString()} | ${req.method} ${req.originalUrl} | ${res.statusCode} | ${ms}ms | ${ip_info.ip} (${ip_info.source})`
    );
  });

  next();
});

//
// ---------- Health Route ----------
//

// Apply limiter only to /health (keeps other routes unaffected if you add them later)
// https://dell-home.kidderwise.org/health?key=SEE_ENV_SECRET
app.get("/health", health_rate_limiter, (req, res) => {
  const mem = process.memoryUsage();
  const disk = get_disk_usage();
  const ip_info = get_client_ip_info(req);

  console.log('req.query.key=', req.query.key);

  if (req.query.key !== process.env.SERVER_HEALTH_CHECK_KEY_SECRET) {
    return res.status(403).send("Forbidden");
  }

  const status_details = {
    status: "ok",
    server: "health server is up and running. stands_ready.",
    requesting_ip: ip_info.ip,
    ip_source: ip_info.source,

    // raw headers that often explain "where did this IP come from?"
    // (helpful behind Cloudflare / proxies)
    ip_debug: {
      cf_connecting_ip: ip_info.cf_connecting_ip,
      x_forwarded_for: ip_info.x_forwarded_for,
      x_real_ip: ip_info.x_real_ip,
      express_req_ip: ip_info.express_req_ip,
    },

    uptime: format_duration(process.uptime()),
    load_avg: os.loadavg().map((n) => n.toFixed(2)).join(", "),
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
  console.log(
    `rate_limit: window_ms=${window_ms} max_requests=${max_requests} (route=/health)`
  );
});
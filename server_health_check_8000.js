// server_health_check_8000.js
import path from "path";
import rateLimit from "express-rate-limit";
import os from "os";
import { execSync, execFile } from "child_process";

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

function get_client_ip_info(req) {
  const cf_ip = (req.headers["cf-connecting-ip"] || "").toString().trim();
  const xff = (req.headers["x-forwarded-for"] || "").toString().trim();
  const x_real_ip = (req.headers["x-real-ip"] || "").toString().trim();

  const xff_first = xff ? xff.split(",")[0].trim() : "";
  const express_ip = (req.ip || "").toString().trim();

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
    const output = execSync("df -h /", { encoding: "utf8" }).trim().split("\n")[1];
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

// function log_block(title, obj) {
//   console.log("***************************************");
//   console.log(title);
//   Object.entries(obj).forEach(([key, value]) => {
//     console.log(`  ${key}: ${value}`);
//   });
//   console.log("=======================================");
// }

function log_block(title, obj) {
  console.log("***************************************");
  console.log(title);

  for (const [key, value] of Object.entries(obj)) {
    const is_object =
      value !== null &&
      typeof value === "object" &&
      !(value instanceof Date);

    if (is_object) {
      console.log(`  ${key}:`);
      // indent JSON lines so it stays readable in pm2 logs
      const pretty = JSON.stringify(value, null, 2)
        .split("\n")
        .map((line) => `    ${line}`)
        .join("\n");
      console.log(pretty);
    } else {
      console.log(`  ${key}: ${value}`);
    }
  }

  console.log("=======================================");
}

function is_probably_ip(ip) {
  return typeof ip === "string" && ip.length >= 3 && ip.length <= 64 && (ip.includes(".") || ip.includes(":"));
}

//
// ---------- WHOIS lookup (best-effort + cached + timeout) ----------
//

const WHOIS_TTL_MS = Number(process.env.WHOIS_TTL_MS || 24 * 60 * 60 * 1000); // 24h
const WHOIS_TIMEOUT_MS = Number(process.env.WHOIS_TIMEOUT_MS || 1200); // 1.2s
const WHOIS_MAX_CACHE = Number(process.env.WHOIS_MAX_CACHE || 2000);

const whois_cache = new Map();

function prune_whois_cache_if_needed() {
  if (whois_cache.size <= WHOIS_MAX_CACHE) return;
  const to_delete = Math.max(1, Math.floor(WHOIS_MAX_CACHE * 0.1));
  let i = 0;
  for (const key of whois_cache.keys()) {
    whois_cache.delete(key);
    i += 1;
    if (i >= to_delete) break;
  }
}

function parse_whois_best_effort(raw) {
  const lines = raw
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean)
    .filter((l) => !l.startsWith("%") && !l.startsWith("#"));

  const find_first = (regex) => {
    for (const l of lines) {
      const m = l.match(regex);
      if (m) return (m[1] || "").trim();
    }
    return null;
  };

  const org =
    find_first(/^OrgName:\s*(.+)$/i) ||
    find_first(/^org-name:\s*(.+)$/i) ||
    find_first(/^Organization:\s*(.+)$/i) ||
    find_first(/^owner:\s*(.+)$/i) ||
    find_first(/^descr:\s*(.+)$/i) ||
    null;

  const city = find_first(/^City:\s*(.+)$/i) || find_first(/^city:\s*(.+)$/i) || null;

  const country = find_first(/^Country:\s*(.+)$/i) || find_first(/^country:\s*(.+)$/i) || null;

  const netname =
    find_first(/^NetName:\s*(.+)$/i) ||
    find_first(/^netname:\s*(.+)$/i) ||
    null;

  const asn =
    find_first(/^OriginAS:\s*(.+)$/i) ||
    find_first(/^origin:\s*(.+)$/i) ||
    null;

  return {
    org_name: org,
    city,
    country,
    net_name: netname,
    origin_as: asn,
  };
}

function whois_lookup(ip) {
  return new Promise((resolve) => {
    if (!is_probably_ip(ip)) return resolve({ ok: false, error: "invalid_ip" });

    const cached = whois_cache.get(ip);
    if (cached && cached.expires_at > Date.now()) {
      return resolve({ ok: true, cached: true, ...cached.data });
    }

    execFile(
      "whois",
      [ip],
      { timeout: WHOIS_TIMEOUT_MS, maxBuffer: 1024 * 1024 },
      (err, stdout, stderr) => {
        if (err) {
          const data = {
            ok: false,
            cached: false,
            error: err.killed ? "timeout" : "whois_failed",
            stderr: (stderr || "").toString().slice(0, 200),
          };
          whois_cache.set(ip, { expires_at: Date.now() + Math.min(WHOIS_TTL_MS, 10 * 60 * 1000), data });
          prune_whois_cache_if_needed();
          return resolve(data);
        }

        const parsed = parse_whois_best_effort((stdout || "").toString());
        const data = { ok: true, cached: false, ...parsed };

        whois_cache.set(ip, { expires_at: Date.now() + WHOIS_TTL_MS, data });
        prune_whois_cache_if_needed();
        return resolve(data);
      }
    );
  });
}

//
// ---------- Rate Limiter ----------
//

const window_ms = Number(process.env.HEALTH_RL_WINDOW_MS || 60_000);
const max_requests = Number(process.env.HEALTH_RL_MAX || 60);

const health_rate_limiter = rateLimit({
  windowMs: window_ms,
  max: max_requests,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => get_client_ip_info(req).ip,
  handler: (req, res) => {
    const ip_info = get_client_ip_info(req);
    console.log(
      `${new Date().toISOString()} | RATE_LIMIT | ${req.method} ${req.originalUrl} | 429 | ${ip_info.ip} (${ip_info.source})`
    );
    return res.status(429).json({
      status: "rate_limited",
      message: "too many requests",
      requesting_ip: ip_info.ip,
      ip_source: ip_info.source,
      window_ms,
      max_requests,
      timestamp: new Date().toISOString(),
    });
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

// https://dell-home.kidderwise.org/health?key=SEE_ENV_SECRET
app.get("/health", health_rate_limiter, async (req, res) => {
  const ip_info = get_client_ip_info(req);

  // IMPORTANT: do key check BEFORE any whois
  if (req.query.key !== process.env.SERVER_HEALTH_CHECK_KEY_SECRET) {
    return res.status(403).send("Forbidden");
  }

  const mem = process.memoryUsage();
  const disk = get_disk_usage();

  const whois = await whois_lookup(ip_info.ip);

  const status_details = {
    status: "ok",
    server: "health server is up and running. stands_ready.",
    requesting_ip: ip_info.ip,
    ip_source: ip_info.source,

    ip_debug: {
      cf_connecting_ip: ip_info.cf_connecting_ip,
      x_forwarded_for: ip_info.x_forwarded_for,
      x_real_ip: ip_info.x_real_ip,
      express_req_ip: ip_info.express_req_ip,
    },

    whois: {
      ok: whois.ok,
      cached: whois.cached || false,
      org_name: whois.org_name || null,
      city: whois.city || null,
      country: whois.country || null,
      net_name: whois.net_name || null,
      origin_as: whois.origin_as || null,
      error: whois.ok ? null : whois.error || "unknown",
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
  return res.status(200).json(status_details);
});

//
// ---------- Start Server ----------
//

app.listen(port, "0.0.0.0", () => {
  console.log(`health_check_running_on_port ${port}`);
  console.log(`rate_limit: window_ms=${window_ms} max_requests=${max_requests} (route=/health)`);
  console.log(`whois: ttl_ms=${WHOIS_TTL_MS} timeout_ms=${WHOIS_TIMEOUT_MS} max_cache=${WHOIS_MAX_CACHE}`);
});
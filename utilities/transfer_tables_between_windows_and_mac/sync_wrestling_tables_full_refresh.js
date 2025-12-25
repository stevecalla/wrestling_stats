// sync_wrestling_tables_full_refresh.js
import path from "path";
import { fileURLToPath } from "url";
import os from "os";
import dotenv from "dotenv";
import mysql from "mysql2/promise";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// -------------------------------------------------
// simple color + style logger (snake_case)
// -------------------------------------------------
const ansi_codes = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  dim: "\x1b[2m",
  fg_red: "\x1b[31m",
  fg_green: "\x1b[32m",
  fg_yellow: "\x1b[33m",
  fg_blue: "\x1b[34m",
  fg_magenta: "\x1b[35m",
  fg_cyan: "\x1b[36m",
  fg_white: "\x1b[37m",
};

function color_text(text, style = "info") {
  let prefix = "";
  const reset = ansi_codes.reset;

  switch (style) {
    case "success":
      prefix = ansi_codes.fg_green + ansi_codes.bright;
      break;
    case "warn":
      prefix = ansi_codes.fg_yellow + ansi_codes.bright;
      break;
    case "error":
      prefix = ansi_codes.fg_red + ansi_codes.bright;
      break;
    case "dim":
      prefix = ansi_codes.dim;
      break;
    case "info":
    default:
      prefix = ansi_codes.fg_cyan;
      break;
  }

  return `${prefix}${text}${reset}`;
}

// step icons for clarity
const step_icons = {
  init: "0️⃣",
  schema: "1️⃣",
  hash: "2️⃣",
  data: "3️⃣",
  skip: "⏭️",
  done: "✅",
  error: "❌",
};

// -------------------------------------------------
// Auto-select env file based on OS
// -------------------------------------------------

// If user provided --env= manually, that wins.
// Otherwise auto-select based on OS.
const arg_env_file = process.argv.find((a) => a.startsWith("--env="));
let env_file_path;

if (arg_env_file) {
  // Manual override: --env=...
  env_file_path = arg_env_file.replace("--env=", "");
  console.log(
    color_text(
      `${step_icons.init} [init] using manual env: ${env_file_path}`,
      "info"
    )
  );
} else {
  // Auto-select by OS
  const platform = os.platform(); // win32, darwin, linux

  if (platform === "win32") {
    env_file_path = ".env.sync.win";
  } else if (platform === "darwin" || platform === "linux") {
    env_file_path = ".env.sync.mac";
  } else {
    console.warn(
      color_text(
        `${step_icons.warn} [init] Unknown OS platform → defaulting to .env.sync.mac`,
        "warn"
      )
    );
    env_file_path = ".env.sync.mac";
  }

  console.log(
    color_text(
      `${step_icons.init} [init] auto-selected env → ${env_file_path} (platform=${platform})`,
      "info"
    )
  );
}

console.log(
  color_text(
    `${step_icons.init} [init] loading env file → ${env_file_path}`,
    "info"
  )
);

// -------------------------------------------------
// Now load the selected env file
// -------------------------------------------------
dotenv.config({
  path: path.isAbsolute(env_file_path)
    ? env_file_path
    : path.join(__dirname, env_file_path),
  override: true, // force .env.sync.* to overwrite anything from base .env / dotenvx
});

console.log("sync direction raw from env:", process.env.SYNC_DIRECTION);

// -------------------------------------------------
// tables to fully refresh
// -------------------------------------------------
const tables_to_sync = [
  // "test_wrestler_table_win",
  // "test_wrestler_table_mac",
  'reference_team_alias_map',
  'reference_wrestler_2026_state_qualifier_flags',
  'reference_wrestler_2026_team_division_flags',
  'reference_wrestler_cross_season_summary',
  'reference_wrestler_rankings_list',
  'team_schedule_scrape_data',
  'wrestler_list_scrape_data',
  'wrestler_list_scrape_data_2024_2025_boys_backup',
  'wrestler_match_history_2024_2025_boys_all',
  'wrestler_match_history_metrics_data',
  'wrestler_match_history_scrape_data',
  'wrestler_match_history_scrape_data_2025_2026_120225_120625',
  'wrestler_match_history_wrestler_ids_data',
  'wrestler_state_qualifier_and_place_reference',
  'wrestler_team_division_reference',
  'wrestler_match_history_scrape_tasks',
];

const sync_batch_size = parseInt(process.env.SYNC_BATCH_SIZE || "5000", 10);

// -------------------------------------------------
// direction + db config helpers
// -------------------------------------------------
const sync_direction = (
  process.env.SYNC_DIRECTION || "windows_to_mac"
).toLowerCase();

if (!["windows_to_mac", "mac_to_windows"].includes(sync_direction)) {
  console.error(
    color_text(
      `${step_icons.error} [init] invalid SYNC_DIRECTION: ${sync_direction}. Use windows_to_mac or mac_to_windows.`,
      "error"
    )
  );
  process.exit(1);
}

function get_db_config(prefix) {
  return {
    host: process.env[`${prefix}_HOST`],
    port: parseInt(process.env[`${prefix}_PORT`] || "3306", 10),
    user: process.env[`${prefix}_USER`],
    password: process.env[`${prefix}_PASSWORD`],
    database: process.env[`${prefix}_NAME`],
    multipleStatements: true,
  };
}

// resolve which side is source vs target based on direction
const source_prefix =
  sync_direction === "windows_to_mac" ? "WINDOWS_DB" : "MAC_DB";
const target_prefix =
  sync_direction === "windows_to_mac" ? "MAC_DB" : "WINDOWS_DB";

// -------------------------------------------------
// does table exist?
// -------------------------------------------------
async function table_exists(pool, table_name) {
  const [rows] = await pool.query(
    `
      SELECT COUNT(*) AS cnt
      FROM information_schema.tables
      WHERE table_schema = DATABASE()
        AND table_name = ?
    `,
    [table_name]
  );
  return rows[0].cnt > 0;
}

// -------------------------------------------------
// compute table signature
// - tries CHECKSUM TABLE
// - falls back to row_count + max(updated_at_utc)
// -------------------------------------------------
async function get_table_signature(pool, table_name) {
  const exists = await table_exists(pool, table_name);
  if (!exists) {
    return null;
  }

  let checksum_part = "nochecksum";

  // try CHECKSUM TABLE first
  try {
    const [checksum_rows] = await pool.query(
      `CHECKSUM TABLE \`${table_name}\``
    );
    if (
      checksum_rows &&
      checksum_rows.length > 0 &&
      checksum_rows[0].Checksum != null
    ) {
      checksum_part = String(checksum_rows[0].Checksum);
    }
  } catch (err) {
    // ignore, fall back
  }

  let row_count = 0;
  let max_updated = "null";

  // try to use updated_at_utc if it exists
  try {
    const [rows] = await pool.query(
      `
        SELECT
          COUNT(*) AS row_count,
          MAX(updated_at_utc) AS max_updated_at
        FROM \`${table_name}\`
      `
    );
    row_count = rows[0].row_count;
    if (rows[0].max_updated_at) {
      const val = rows[0].max_updated_at;
      max_updated =
        val instanceof Date ? val.toISOString() : String(val);
    }
  } catch (err) {
    // fall back to just row count
    const [rows] = await pool.query(
      `SELECT COUNT(*) AS row_count FROM \`${table_name}\``
    );
    row_count = rows[0].row_count;
  }

  return `${row_count}|${max_updated}|${checksum_part}`;
}

// -------------------------------------------------
// copy schema: drop target table + recreate
// -------------------------------------------------
async function copy_schema(table_name, source_pool, target_pool) {
  console.log(
    color_text(
      `\n${step_icons.schema} [schema] copying schema for ${table_name} ...`,
      "info"
    )
  );

  const [rows] = await source_pool.query(
    `SHOW CREATE TABLE \`${table_name}\``
  );
  if (!rows || rows.length === 0) {
    throw new Error(`SHOW CREATE TABLE returned no rows for ${table_name}`);
  }

  const create_sql = rows[0]["Create Table"];

  console.log(
    color_text(
      `[schema] dropping target table ${table_name} if exists...`,
      "warn"
    )
  );
  await target_pool.query(`DROP TABLE IF EXISTS \`${table_name}\``);

  console.log(
    color_text(
      `[schema] creating target table ${table_name} ...`,
      "info"
    )
  );
  await target_pool.query(create_sql);

  console.log(
    color_text(`[schema] schema copied for ${table_name}`, "success")
  );
}

// -------------------------------------------------
// copy data in batches (full refresh)
// -------------------------------------------------
async function copy_data(table_name, source_pool, target_pool) {
  console.log(
    color_text(
      `\n${step_icons.data} [data] copying data for ${table_name} ...`,
      "info"
    )
  );

  const [columns] = await source_pool.query(
    `SHOW COLUMNS FROM \`${table_name}\``
  );
  const column_names = columns.map((c) => c.Field);
  const column_list_sql = column_names.map((c) => `\`${c}\``).join(", ");

  let offset = 0;
  let total_copied = 0;

  while (true) {
    const [rows] = await source_pool.query(
      `SELECT * FROM \`${table_name}\` LIMIT ? OFFSET ?`,
      [sync_batch_size, offset]
    );

    if (rows.length === 0) break;

    const row_values = rows.map((r) => column_names.map((col) => r[col]));

    const insert_sql = `
      INSERT INTO \`${table_name}\` (${column_list_sql})
      VALUES ?
    `;

    await target_pool.query(insert_sql, [row_values]);

    offset += rows.length;
    total_copied += rows.length;

    console.log(
      color_text(
        `[data] ${table_name}: copied ${total_copied} rows so far...`,
        "dim"
      )
    );
  }

  console.log(
    color_text(
      `[data] ${table_name}: finished copying ${total_copied} rows.`,
      "success"
    )
  );
}

// -------------------------------------------------
// main
// -------------------------------------------------
async function main() {
  console.log(
    color_text(
      `\n${step_icons.init} [init] sync_direction = ${sync_direction}`,
      "info"
    )
  );
  console.log(
    color_text(
      `${step_icons.init} [init] source_prefix = ${source_prefix}, target_prefix = ${target_prefix}`,
      "dim"
    )
  );

  const source_db_config = get_db_config(source_prefix);
  const target_db_config = get_db_config(target_prefix);

  console.log(
    color_text(
      `${step_icons.init} [init] source db: ${source_db_config.host} / ${source_db_config.database}`,
      "info"
    )
  );
  console.log(
    color_text(
      `${step_icons.init} [init] target db: ${target_db_config.host} / ${target_db_config.database}`,
      "info"
    )
  );

  const source_pool = await mysql.createPool(source_db_config);
  const target_pool = await mysql.createPool(target_db_config);

  async function log_actual_db(pool, label) {
    const [rows] = await pool.query(
      "SELECT @@hostname AS host, DATABASE() AS db"
    );
    console.log(
      color_text(
        `${step_icons.init} [init] ${label} actual connection → host: ${rows[0].host}, db: ${rows[0].db}`,
        "info"
      )
    );
  }

  await log_actual_db(source_pool, "SOURCE");
  await log_actual_db(target_pool, "TARGET");

  try {
    for (const table_name of tables_to_sync) {
      console.log(
        color_text(
          `\n========== syncing table: ${table_name} ==========\n`,
          "info"
        )
      );

      // 1) compute signatures
      console.log(
        color_text(
          `${step_icons.hash} [hash] computing table signatures for ${table_name} ...`,
          "info"
        )
      );

      const source_sig = await get_table_signature(source_pool, table_name);
      const target_sig = await get_table_signature(target_pool, table_name);

      console.log(
        color_text(
          `${step_icons.hash} [hash] ${table_name} → source: ${source_sig}, target: ${target_sig}`,
          "dim"
        )
      );

      // 2) if signatures match and target table exists → skip
      const force_refresh = true; // toggle if you later want "skip when identical"
      if (
        !force_refresh &&
        source_sig &&
        target_sig &&
        source_sig === target_sig
      ) {
        console.log(
          color_text(
            `${step_icons.skip} [skip] ${table_name} unchanged → skipping full refresh`,
            "success"
          )
        );
        continue;
      }

      // 3) else, full refresh (drop + recreate + copy data)
      console.log(
        color_text(
          `${step_icons.schema} [schema] ${table_name} changed or missing → performing full refresh`,
          "warn"
        )
      );

      await copy_schema(table_name, source_pool, target_pool);
      await copy_data(table_name, source_pool, target_pool);

      console.log(
        color_text(
          `${step_icons.done} [done] full refresh completed for ${table_name}`,
          "success"
        )
      );
    }

    console.log(
      color_text(
        `\n${step_icons.done} full refresh process complete for all tables\n`,
        "success"
      )
    );
  } catch (err) {
    console.error(
      color_text(
        `\n${step_icons.error} error during sync: ${err.message}`,
        "error"
      )
    );
    console.error(err);
    process.exitCode = 1;
  } finally {
    await source_pool.end();
    await target_pool.end();
  }
}

// main().catch((err) => {
//   console.error(
//     color_text(
//       `${step_icons.error} [fatal] unhandled error: ${err.message}`,
//       "error"
//     )
//   );
//   console.error(err);
//   process.exit(1);
// });

  export { main as step_18_transfer_tables_between_windows_and_mac }

// /src/utilities/parsers/parse_wrestler_list_from_docx.js
// npm install mammoth xlsx mysql2

import fs from "fs";
import path from "path";
import mammoth from "mammoth";
import * as xlsx from "xlsx";

import { get_pool } from "../mysql/mysql_pool.js";
import { determine_os_path } from "../../utilities/directory_tools/determine_os_path.js";
import { create_directory } from "../../utilities/directory_tools/create_directory.js";
import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

// -----------------------------------------------------------------------------
// Parser regex
// -----------------------------------------------------------------------------
const division_regex = /Class\s+(\dA)\b/i;
const weight_regex = /^(\d+)\s*lb[s]?\b/i;

// -----------------------------------------------------------------------------
// Parse a DOCX file’s wrestler rankings
// -----------------------------------------------------------------------------
export async function parse_wrestler_list_from_docx(docx_path) {
  const { value: raw_text } = await mammoth.extractRawText({ path: docx_path });

  const lines = raw_text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  let current_division = null;
  let current_weight = null;
  let in_wrestler_section = false;
  let rank_counter = 0;

  const rows = [];

  for (const line of lines) {
    const div_match = division_regex.exec(line);
    if (div_match) {
      current_division = div_match[1].toUpperCase();
      current_weight = null;
      in_wrestler_section = false;
      rank_counter = 0;
      continue;
    }

    const wmatch = weight_regex.exec(line);
    if (wmatch && current_division) {
      current_weight = wmatch[1];
      in_wrestler_section = true;
      rank_counter = 0;
      continue;
    }

    if (in_wrestler_section && current_division && current_weight) {
      if (/^Class\s+\dA\b/i.test(line)) {
        const new_match = division_regex.exec(line);
        if (new_match) {
          current_division = new_match[1].toUpperCase();
          current_weight = null;
          in_wrestler_section = false;
          rank_counter = 0;
          continue;
        }
      }

      if (!line.includes(",")) continue;

      const [before_paren] = line.split("(", 1);
      const prefix = before_paren.trim().replace(/;$/, "");

      let wrestler_name = prefix;
      let school = "";

      const comma_idx = prefix.indexOf(",");
      if (comma_idx !== -1) {
        wrestler_name = prefix.slice(0, comma_idx).trim();
        school = prefix.slice(comma_idx + 1).trim();
      }

      rank_counter += 1;

      rows.push({
        wrestling_season: "2025-26",
        track_wrestling_category: "High School Boys",
        division: current_division,
        weight_lbs: current_weight,
        rank: rank_counter,
        wrestler_name,
        school,
        details_line: line,
      });
    }
  }

  return rows;
}

// -----------------------------------------------------------------------------
// Excel + JSON writers
// -----------------------------------------------------------------------------
export function ensure_dir(dir_path) {
  fs.mkdirSync(dir_path, { recursive: true });
}

export function write_wrestler_list_to_excel(rows, excel_path) {
  const header = [
    "division",
    "weight_lbs",
    "rank",
    "wrestler_name",
    "school",
    "details_line",
    "wrestling_season",
    "track_wrestling_category",
    "created_at_mtn",
    "created_at_utc",
    "updated_at_mtn",
    "updated_at_utc",
  ];

  const data = [
    header,
    ...rows.map((r) => [
      r.division,
      r.weight_lbs,
      r.rank,
      r.wrestler_name,
      r.school,
      r.details_line,
      r.wrestling_season,
      r.track_wrestling_category,
      r.created_at_mtn ?? "",
      r.created_at_utc ?? "",
      r.updated_at_mtn ?? "",
      r.updated_at_utc ?? "",
    ]),
  ];

  const worksheet = xlsx.utils.aoa_to_sheet(data);
  const workbook = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(workbook, worksheet, "wrestler_list");

  ensure_dir(path.dirname(excel_path));
  xlsx.writeFile(workbook, excel_path);
}


export function write_wrestler_list_to_json(rows, json_path) {
  ensure_dir(path.dirname(json_path));
  fs.writeFileSync(json_path, JSON.stringify(rows, null, 2), "utf8");
}

// -----------------------------------------------------------------------------
// Ensure table exists (TEAM-SCHEDULE style)
// -----------------------------------------------------------------------------
let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  const sql = `
    CREATE TABLE IF NOT EXISTS reference_wrestler_rankings_list (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      wrestling_season VARCHAR(32) NOT NULL,
      track_wrestling_category VARCHAR(64) NOT NULL,

      division VARCHAR(8) NOT NULL,
      weight_lbs INT NOT NULL,
      \`rank\` INT NOT NULL,

      wrestler_name VARCHAR(255) NOT NULL,
      school VARCHAR(255) NULL,
      details_line TEXT NOT NULL,
      source_file VARCHAR(255) NULL,

      created_at_mtn DATETIME NOT NULL,
      created_at_utc DATETIME NOT NULL,
      updated_at_mtn DATETIME NOT NULL,
      updated_at_utc DATETIME NOT NULL,

      UNIQUE KEY uk_division_weight_rank (division, weight_lbs, \`rank\`),
      PRIMARY KEY (id)
    ) ENGINE=InnoDB
      DEFAULT CHARSET=utf8mb4
      COLLATE=utf8mb4_0900_ai_ci;
  `;

  await pool.query(sql);
  _ensured = true;
}

// -----------------------------------------------------------------------------
// Upload to MySQL (TEAM-SCHEDULE timestamp technique)
// -----------------------------------------------------------------------------
export async function upload_wrestler_list_to_mysql(rows, { source_file } = {}) {
  if (!rows?.length) {
    console.log("[parse_wrestler_list] no rows to upload.");
    return;
  }

  await ensure_table();
  const pool = await get_pool();

  // Compute Mountain + UTC timestamps
  const now_utc = new Date();
  const offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + offset_hours * 3600 * 1000);

  const sql = `
    INSERT INTO reference_wrestler_rankings_list (
      \`division\`,
      \`weight_lbs\`,
      \`rank\`,
      \`wrestler_name\`,
      \`school\`,
      \`details_line\`,
      \`source_file\`,
      \`wrestling_season\`,
      \`track_wrestling_category\`,
      \`created_at_mtn\`,
      \`created_at_utc\`,
      \`updated_at_mtn\`,
      \`updated_at_utc\`
    )
    VALUES ?
    ON DUPLICATE KEY UPDATE
      \`wrestler_name\` = VALUES(\`wrestler_name\`),
      \`school\` = VALUES(\`school\`),
      \`details_line\` = VALUES(\`details_line\`),
      \`source_file\`  = VALUES(\`source_file\`),
      \`wrestling_season\` = VALUES(\`wrestling_season\`),
      \`track_wrestling_category\` = VALUES(\`track_wrestling_category\`),
      \`updated_at_mtn\` = VALUES(\`updated_at_mtn\`),
      \`updated_at_utc\` = VALUES(\`updated_at_utc\`);
  `;

  const values = rows.map((row) => [
    row.division,
    Number(row.weight_lbs),
    row.rank,
    row.wrestler_name,
    row.school || null,
    row.details_line,
    source_file || null,
    row.wrestling_season,
    row.track_wrestling_category,
    now_mtn,
    now_utc,
    now_mtn,
    now_utc,
  ]);

  console.log(`[parse_wrestler_list] uploading ${rows.length} rows to mysql...`);
  await pool.query(sql, [values]);
  console.log("[parse_wrestler_list] mysql upload complete ✅");
}

// -----------------------------------------------------------------------------
// Runner (standalone execution)
// -----------------------------------------------------------------------------
export async function run_parse_wrestler_docx({
  docx_path,
  excel_path,
  json_path,
  upload_to_mysql = true,
} = {}) {
  const directory = determine_os_path();
  const input_dir = await create_directory("input", directory);
  const output_dir = await create_directory("output", directory);

  const effective_docx_path =
    docx_path || path.join(input_dir, "week_0_high_school_boys_2026.docx");

  const effective_excel_path =
    excel_path ||
    path.join(
      output_dir,
      "week_0_high_school_boys_2026_wrestler_list_with_rank.xlsx"
    );

  const effective_json_path =
    json_path ||
    path.join(
      output_dir,
      "week_0_high_school_boys_2026_wrestler_list_with_rank.json"
    );

  const source_file = path.basename(effective_docx_path);

  console.log(`[parse_wrestler_list] reading docx: ${effective_docx_path}`);
  const rows = await parse_wrestler_list_from_docx(effective_docx_path);
  console.log(
    `[parse_wrestler_list] parsed ${rows.length} wrestlers`
  );

  write_wrestler_list_to_excel(rows, effective_excel_path);
  write_wrestler_list_to_json(rows, effective_json_path);

  if (upload_to_mysql) {
    await upload_wrestler_list_to_mysql(rows, { source_file });
  }

  return rows;
}

run_parse_wrestler_docx().catch((err) => {
  console.error("[parse_wrestler_list] error:", err);
  process.exit(1);
});

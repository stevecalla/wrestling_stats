// src/step_11_append_state_qualifier_to_match_metrics.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

async function ensure_team_columns(pool) {
  const alters = [
    `
      ALTER TABLE team_schedule_scrape_data
        ADD COLUMN team_id_source VARCHAR(100) NULL AFTER team_id
    `
  ];

  for (const sql of alters) {
    try {
      await pool.query(sql);
    } catch (err) {
      if (err.code === "ER_DUP_FIELDNAME") {
        continue; // ignore duplicate column errors
      }
      throw err;
    }
  }
}

async function step_2a_append_team_id_to_team_schedule_data() {
  const pool = await get_pool();

  // 1) Ensure columns exist (safe to run multiple times)
  await ensure_team_columns(pool);

  // 2) Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  // 3a) Update rows from the team division reference table (ONLY WHERE team_id IS NULL)
  const sql_from_reference = `
    UPDATE team_schedule_scrape_data ts

    LEFT JOIN wrestler_team_division_reference r
      ON  r.wrestler_team            = ts.team_name_raw
      AND r.wrestling_season         = ts.wrestling_season
      AND r.track_wrestling_category = ts.track_wrestling_category

    SET
      ts.team_id        = r.wrestler_team_id,
      ts.team_id_source = 'wrestler_team_division_reference',

      -- timestamps (match original order + placeholders)
      ts.updated_at_mtn = ?,
      ts.updated_at_utc = ?

    WHERE 1 = 1
      AND ts.team_id IS NULL
      AND r.wrestler_team_id IS NOT NULL
  `;

  const [result_from_reference] = await pool.query(sql_from_reference, [
    updated_at_mtn,
    updated_at_utc,
  ]);

  // 3b) Second pass: self-join on team_schedule_scrape_data to fill remaining NULL team_id
  const sql_from_self = `
    UPDATE team_schedule_scrape_data ts

    JOIN team_schedule_scrape_data src
      ON  src.team_name_raw            = ts.team_name_raw
      AND src.wrestling_season         = ts.wrestling_season
      AND src.track_wrestling_category = ts.track_wrestling_category
      AND src.team_id IS NOT NULL

    SET
      ts.team_id        = src.team_id,
      ts.team_id_source = 'team_schedule_scrape_data',

      ts.updated_at_mtn = ?,
      ts.updated_at_utc = ?

    WHERE 1 = 1
      AND ts.team_id IS NULL
  `;

  const [result_from_self] = await pool.query(sql_from_self, [
    updated_at_mtn,
    updated_at_utc,
  ]);

  console.log(
    "step_2_append_team_id_to_team_schedule_data →",
    `from_reference: ${result_from_reference.affectedRows},`,
    `from_self_join: ${result_from_self.affectedRows}`
  );

  return {
    from_reference: result_from_reference.affectedRows,
    from_self_join: result_from_self.affectedRows,
  };
}


// await step_2a_append_team_id_to_team_schedule_data();

export { step_2a_append_team_id_to_team_schedule_data };

// src/step_14_append_onthemat_rankings_to_wrestler_list.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

/* -------------------------------------------------
   Ensure OnTheMat columns exist on LIST table
--------------------------------------------------*/
async function ensure_onthemat_columns(pool) {
  const alters = [
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_is_name_match TINYINT NULL AFTER wrestler_state_tournament_place
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_name TEXT NULL AFTER onthemat_is_name_match
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_is_team_match TINYINT NULL AFTER onthemat_name
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_team TEXT NULL AFTER onthemat_is_team_match
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_rank INT NULL AFTER onthemat_team
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_weight_lbs INT NULL AFTER onthemat_rank
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_rankings_source_file VARCHAR(50) NULL AFTER onthemat_weight_lbs
    `,
  ];

  for (const sql of alters) {
    try {
      await pool.query(sql);
    } catch (err) {
      if (err.code === "ER_DUP_FIELDNAME") continue;
      throw err;
    }
  }
}

async function step_14_append_onthemat_rankings_to_wrestler_list() {
  const pool = await get_pool();

  // 1) Ensure columns exist
  await ensure_onthemat_columns(pool);

  // 2) Timestamps
  const now_utc = new Date();
  const offset = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + offset * 3600 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  // -------------------------------------------------
  // 3) UPDATE LIST TABLE from OnTheMat rankings
  //    (transaction managed in JS; no multi-statement query)
  // -------------------------------------------------
  const update_sql = `
    UPDATE wrestler_list_scrape_data l
      LEFT JOIN (
        SELECT
          wrestler_name,
          school,
          source_file,
          MIN(\`rank\`) AS onthemat_rank,
          MIN(weight_lbs) AS weight_lbs

        FROM reference_wrestler_rankings_list
        GROUP BY wrestler_name, school, source_file
      ) r
        ON r.wrestler_name = l.name
        AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1) -- removed the ", CO" from team name for comparsion in step 2

    SET
      -- match flags
      l.onthemat_is_name_match = 
        CASE 
          WHEN r.wrestler_name IS NULL THEN 0 
          ELSE 1 
      END,

      l.onthemat_name = r.wrestler_name,

      l.onthemat_is_team_match = 
        CASE
          WHEN r.wrestler_name IS NULL THEN NULL
          WHEN r.school LIKE SUBSTRING_INDEX(l.team, ',', 1) THEN 1
          ELSE 0
        END,
      
      l.onthemat_team = r.school,

      l.onthemat_rankings_source_file = r.source_file,

      -- appended fields
      l.onthemat_rank = r.onthemat_rank,
      l.onthemat_weight_lbs = r.weight_lbs,

      -- timestamps
      l.updated_at_mtn = ?,
      l.updated_at_utc = ?

    WHERE l.wrestling_season = '2025-26'
      AND l.track_wrestling_category = 'High School Boys'
  `;

  // NOTE:
  // - For a "test run", we ROLLBACK at the end.
  // - When you are ready to apply changes, switch to COMMIT.

  let update_result;
  // let rollback_or_commit = "ROLLBACK"; // change to "COMMIT" when ready
  let rollback_or_commit = "COMMIT"; // change to "COMMIT" when ready

  try {
    await pool.query("START TRANSACTION");

    const [result] = await pool.query(update_sql, [updated_at_mtn, updated_at_utc]);
    update_result = result;

    // Optional: show how many rows were updated inside the transaction
    // (ROW_COUNT() pertains to the last statement on this connection)
    const [[row_count]] = await pool.query("SELECT ROW_COUNT() AS rows_updated");
    console.log("rows_updated (ROW_COUNT) =", row_count?.rows_updated);

    await pool.query(rollback_or_commit);
  } catch (err) {
    try {
      await pool.query("ROLLBACK");
    } catch (rollback_err) {
      console.error("rollback failed:", rollback_err?.message || rollback_err);
    }
    throw err;
  }

  console.log(
    "OnTheMat → wrestler_list_scrape_data updates complete 🔗",
    "affectedRows =", update_result?.affectedRows,
    "changedRows =", update_result?.changedRows,
    "txn =", rollback_or_commit
  );

  // -------------------------------------------------
  // 4) Summary (LIST table perspective)
  // -------------------------------------------------
  const summary_sql = `
    SELECT
      SUM(CASE WHEN onthemat_is_name_match = 1 THEN 1 ELSE 0 END) AS matched_rows,
      SUM(CASE WHEN onthemat_is_name_match = 0 THEN 1 ELSE 0 END) AS unmatched_rows,
      COUNT(*) AS total_rows,
      SUM(CASE WHEN onthemat_rank IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_rank,
      SUM(CASE WHEN onthemat_weight_lbs IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_weight
    FROM wrestler_list_scrape_data
    WHERE wrestling_season = '2025-26'
      AND track_wrestling_category = 'High School Boys'
  `;

  const [rows] = await pool.query(summary_sql);
  console.log("OnTheMat match summary 📊", rows[0]);
}

async function step_14_drop_onthemat_columns_from_wrestler_list() {
  const pool = await get_pool();

  const drop_sql = `
    ALTER TABLE wrestler_list_scrape_data
      DROP COLUMN onthemat_is_name_match,
      DROP COLUMN onthemat_name,
      DROP COLUMN onthemat_is_team_match,
      DROP COLUMN onthemat_team,
      DROP COLUMN onthemat_rankings_source_file,
      DROP COLUMN onthemat_rank,
      DROP COLUMN onthemat_weight_lbs
  `;

  await pool.query(drop_sql);

  console.log("OnTheMat columns dropped from wrestler_list_scrape_data 🧹");
}

// await step_14_append_onthemat_rankings_to_wrestler_list();
// step_14_drop_onthemat_columns_from_wrestler_list();

export { step_14_append_onthemat_rankings_to_wrestler_list };

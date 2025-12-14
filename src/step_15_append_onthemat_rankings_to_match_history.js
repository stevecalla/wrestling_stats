// src/step_15_append_onthemat_rankings_to_match_history.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

/* -------------------------------------------------
   Ensure OnTheMat columns exist on MATCH HISTORY table
--------------------------------------------------*/
async function ensure_onthemat_columns(pool) {
  const alters = [
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN onthemat_is_name_match TINYINT NULL AFTER opponent_state_tournament_place
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN onthemat_name TEXT NULL AFTER onthemat_is_name_match
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN onthemat_is_team_match TINYINT NULL AFTER onthemat_name
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN onthemat_team TEXT NULL AFTER onthemat_is_team_match
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN onthemat_rank INT NULL AFTER onthemat_team
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN onthemat_weight_lbs INT NULL AFTER onthemat_rank
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
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

async function step_15_append_onthemat_rankings_to_match_history() {
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
  // 3) UPDATE MATCH HISTORY METRICS TABLE from LIST TABLE
  //
  // Key idea:
  //   match_history_metrics (m) -> wrestler_list (l) by wrestler_id + season + category
  //   then copy l.onthemat_* fields to m.onthemat_* fields
  //
  // Assumes step_14 has already populated wrestler_list_scrape_data.onthemat_*.
  // -------------------------------------------------
  const update_sql = `
    UPDATE wrestler_match_history_metrics_data m

    LEFT JOIN wrestler_list_scrape_data l
      ON l.wrestler_id = m.wrestler_id
      AND l.wrestling_season = m.wrestling_season
      AND l.track_wrestling_category = m.track_wrestling_category

    SET
      m.onthemat_is_name_match = l.onthemat_is_name_match,
      m.onthemat_name = l.onthemat_name,
      m.onthemat_is_team_match = l.onthemat_is_team_match,
      m.onthemat_team = l.onthemat_team,
      m.onthemat_rank = l.onthemat_rank,
      m.onthemat_weight_lbs = l.onthemat_weight_lbs,
      m.onthemat_rankings_source_file = l.onthemat_rankings_source_file,

      m.updated_at_mtn = ?,
      m.updated_at_utc = ?

    WHERE m.wrestling_season = '2025-26'
      AND m.track_wrestling_category = 'High School Boys'
  `;

  let update_result;
  // let rollback_or_commit = "ROLLBACK"; // change to "COMMIT" when ready
  let rollback_or_commit = "COMMIT"; // change to "ROLLBACK" when testing

  try {
    await pool.query("START TRANSACTION");

    const [result] = await pool.query(update_sql, [updated_at_mtn, updated_at_utc]);
    update_result = result;

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
    "wrestler_list_scrape_data → wrestler_match_history_metrics_data updates complete 🔗",
    "affectedRows =", update_result?.affectedRows,
    "changedRows =", update_result?.changedRows,
    "txn =", rollback_or_commit
  );

  // -------------------------------------------------
  // 4) Summary (MATCH HISTORY table perspective)
  // -------------------------------------------------
  const summary_sql = `
    SELECT
      SUM(CASE WHEN onthemat_is_name_match = 1 THEN 1 ELSE 0 END) AS matched_rows,
      SUM(CASE WHEN onthemat_is_name_match = 0 THEN 1 ELSE 0 END) AS unmatched_rows,
      COUNT(*) AS total_rows,
      SUM(CASE WHEN onthemat_rank IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_rank,
      SUM(CASE WHEN onthemat_weight_lbs IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_weight
    FROM wrestler_match_history_metrics_data
    WHERE wrestling_season = '2025-26'
      AND track_wrestling_category = 'High School Boys'
  `;

  const [rows] = await pool.query(summary_sql);
  console.log("OnTheMat match history summary 📊", rows[0]);
}

async function step_15_drop_onthemat_columns_from_match_history() {
  const pool = await get_pool();

  const drop_sql = `
    ALTER TABLE wrestler_match_history_metrics_data
      DROP COLUMN onthemat_is_name_match,
      DROP COLUMN onthemat_name,
      DROP COLUMN onthemat_is_team_match,
      DROP COLUMN onthemat_team,
      DROP COLUMN onthemat_rankings_source_file,
      DROP COLUMN onthemat_rank,
      DROP COLUMN onthemat_weight_lbs
  `;

  await pool.query(drop_sql);

  console.log("🧹 OnTheMat columns dropped from wrestler_match_history_metrics_data 🧹");
}

// await step_15_append_onthemat_rankings_to_match_history();
// step_15_drop_onthemat_columns_from_match_history();

export { step_15_append_onthemat_rankings_to_match_history };

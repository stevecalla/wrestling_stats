// C:\Users\calla\development\projects\wrestling_stats\utilities\raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql

import { get_pool } from "../../mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  // NOT DROPPING / TRUNCATE IN THE MAIN JOB
  // const drop_sql = `
  //   DROP TABLE IF EXISTS reference_wrestler_2026_state_qualifier_flags;
  // `;
  // await pool.query(drop_sql);

  const create_sql = `
    CREATE TABLE IF NOT EXISTS reference_wrestler_2026_state_qualifier_flags (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

        wrestler_id_2026                        BIGINT UNSIGNED NOT NULL,
        wrestler_is_state_tournament_qualifier_2025  TEXT NULL,
        wrestler_state_tournament_place_2025         TEXT NULL,

        -- Timestamps
        created_at_mtn           DATETIME NOT NULL,
        created_at_utc           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at_mtn           DATETIME NOT NULL,
        updated_at_utc           DATETIME NOT NULL,

        PRIMARY KEY (id),
        KEY idx_wrestler_id_2026 (wrestler_id_2026)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;

  await pool.query(create_sql);

  _ensured = true;
}

async function step_3_create_reference_wrestler_2026_state_qualifier_flags() {
  // ✅ Always ensure table exists
  await ensure_table();

  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const created_at_mtn = now_mtn;
  const created_at_utc = now_utc;
  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  let inserted = 0;
  let updated = 0;

  const truncate_sql = `      -- 
      -- Using trancate b/ INSERT PAIRED WITH "VALUES" WILL BE DEPRECIATED IN FUTURE
      TRUNCATE TABLE reference_wrestler_2026_state_qualifier_flags;
  `;
  await pool.query(truncate_sql);

  // raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql
  const insert_sql = `
      INSERT INTO reference_wrestler_2026_state_qualifier_flags (
          wrestler_id_2026,
          wrestler_is_state_tournament_qualifier_2025,
          wrestler_state_tournament_place_2025,
          created_at_mtn,
          created_at_utc,
          updated_at_mtn,
          updated_at_utc
      )
      SELECT
          CAST(SUBSTRING_INDEX(wrestler_ids_2026, ',', 1) AS UNSIGNED) AS wrestler_id_2026,
          wrestler_is_state_tournament_qualifier_2025,
          wrestler_state_tournament_place_2025,

          ? AS created_at_mtn,
          ? AS created_at_utc,
          ? AS updated_at_mtn,
          ? AS updated_at_utc

      FROM reference_wrestler_cross_season_summary
      WHERE
          wrestler_ids_2026 IS NOT NULL
          AND wrestler_ids_2026 <> ''
          AND cnt_ids_2026 = 1
          AND cnt_ids_2025 = 1
          AND wrestler_is_state_tournament_qualifier_2025 IS NOT NULL;
      ;
  `;

  const params = [
    created_at_mtn,
    created_at_utc,
    updated_at_mtn,
    updated_at_utc,
  ];

  const [res] = await pool.query(insert_sql, params);

  const affected = Number(res.affectedRows || 0);
  inserted += affected;

  return { inserted, updated };
}

// For your standalone script usage:
// step_3_create_reference_wrestler_2026_state_qualifier_flags().then(r => {
//   console.log("step_3_create_reference_wrestler_2026_state_qualifier_flags:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("step_3_create_reference_wrestler_2026_state_qualifier_flags error:", err);
//   process.exit(1);
// });


// upsert_wrestler_team_info();

export { step_3_create_reference_wrestler_2026_state_qualifier_flags };

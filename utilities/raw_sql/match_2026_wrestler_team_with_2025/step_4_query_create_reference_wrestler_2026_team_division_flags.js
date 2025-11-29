// C:\Users\calla\development\projects\wrestling_stats\utilities\raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql

import { get_pool } from "../../mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  // NOT DROPPING / TRUNCATE IN THE MAIN JOB
  // const drop_sql = `
  //   DROP TABLE IF EXISTS reference_wrestler_2026_team_division_flags;
  // `;
  // await pool.query(drop_sql);

  const create_sql = `
    CREATE TABLE IF NOT EXISTS reference_wrestler_2026_team_division_flags (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

        team_id_2026      BIGINT UNSIGNED NOT NULL,
        team_division_2025  TEXT NULL,
        team_region_2025    TEXT NULL,

        -- Timestamps
        created_at_mtn      DATETIME NOT NULL,
        created_at_utc      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at_mtn      DATETIME NOT NULL,
        updated_at_utc      DATETIME NOT NULL,

        PRIMARY KEY (id),
        KEY idx_team_id_2026 (team_id_2026)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;

  await pool.query(create_sql);

  _ensured = true;
}

async function step_4_create_reference_wrestler_2026_team_division_flags() {
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
      TRUNCATE TABLE reference_wrestler_2026_team_division_flags;
  `;
  await pool.query(truncate_sql);

  // raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql
  const insert_sql = `
      INSERT INTO reference_wrestler_2026_team_division_flags (
          team_id_2026,
          team_division_2025,
          team_region_2025,
          created_at_mtn,
          created_at_utc,
          updated_at_mtn,
          updated_at_utc
      )
      SELECT
          CAST(SUBSTRING_INDEX(original_team_id_2026, ',', 1) AS UNSIGNED) AS team_id_2026,
          team_division_2025,
          team_region_2025,

          ? AS created_at_mtn,
          ? AS created_at_utc,
          ? AS updated_at_mtn,
          ? AS updated_at_utc

      FROM reference_wrestler_cross_season_summary
      WHERE
          original_team_id_2026 IS NOT NULL
          AND original_team_id_2026 <> ''
          AND cnt_ids_2026 = 1
          AND cnt_ids_2025 = 1
      GROUP BY team_id_2026, team_division_2025, team_region_2025;
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
// step_4_create_reference_wrestler_2026_team_division_flags().then(r => {
//   console.log("step_4_create_reference_wrestler_2026_team_division_flags:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("step_4_create_reference_wrestler_2026_team_division_flags error:", err);
//   process.exit(1);
// });

export { step_4_create_reference_wrestler_2026_team_division_flags };

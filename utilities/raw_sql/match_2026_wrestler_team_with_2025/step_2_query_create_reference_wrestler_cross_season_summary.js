// C:\Users\calla\development\projects\wrestling_stats\utilities\raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql

import { get_pool } from "../../mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  // NOT DROPPING / TRUNCATE IN THE MAIN JOB
  // const drop_sql = `
  //   DROP TABLE IF EXISTS reference_team_alias_map;
  // `;
  // await pool.query(drop_sql);

  const create_sql = `
      CREATE TABLE IF NOT EXISTS reference_wrestler_cross_season_summary (
          id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

          -- Base identity fields
          last_name                  VARCHAR(255) NULL,
          first_name                 VARCHAR(255) NULL,
          name                       VARCHAR(255) NULL,
          track_wrestling_category   VARCHAR(255) NULL,
          team                       VARCHAR(255) NULL,    -- canonical final team

          -- Season lists
          seasons_present            TEXT NULL,

          -- Wrestler IDs per season
          wrestler_ids_2025          TEXT NULL,
          wrestler_ids_2026          TEXT NULL,

          -- Grades per season
          grades_2025                TEXT NULL,
          grades_2026                TEXT NULL,

          -- State qualifier fields (2024–25 only)
          wrestler_is_state_tournament_qualifier_2025  TEXT NULL,
          wrestler_state_tournament_place_2025         TEXT NULL,

          -- Original team names and IDs
          original_team_2025         TEXT NULL,
          original_team_id_2025      TEXT NULL,
          original_team_2026         TEXT NULL,
          original_team_id_2026      TEXT NULL,

          -- Division & region
          team_division_2025         TEXT NULL,
          team_region_2025           TEXT NULL,

          -- Counts
          cnt_ids_2025               INT NOT NULL DEFAULT 0,
          cnt_ids_2026               INT NOT NULL DEFAULT 0,

          -- Alias usage flag
          used_team_alias_flag       TINYINT(1) NOT NULL DEFAULT 0,

          -- timestamps
          created_at_mtn           DATETIME     NOT NULL,
          created_at_utc           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

          updated_at_mtn           DATETIME     NOT NULL,
          updated_at_utc           DATETIME     NOT NULL,

          PRIMARY KEY (id),

          -- Helpful for lookups
          KEY idx_last_first (last_name, first_name),
          KEY idx_team (team)

    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;

  await pool.query(create_sql);

  _ensured = true;
}

async function step_2_create_reference_wrestler_cross_season_summary() {
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
      TRUNCATE TABLE reference_wrestler_cross_season_summary;
  `;
  await pool.query(truncate_sql);

  // raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql
  const insert_sql = `
      INSERT INTO reference_wrestler_cross_season_summary (
          last_name,
          first_name,
          name,
          track_wrestling_category,
          team,
          seasons_present,
          wrestler_ids_2025,
          wrestler_ids_2026,
          grades_2025,
          grades_2026,
          wrestler_is_state_tournament_qualifier_2025,
          wrestler_state_tournament_place_2025,
          original_team_2025,
          original_team_id_2025,
          original_team_2026,
          original_team_id_2026,
          team_division_2025,
          team_region_2025,
          cnt_ids_2025,
          cnt_ids_2026,
          used_team_alias_flag,

          -- NEW timestamp fields
          created_at_mtn,
          created_at_utc,
          updated_at_mtn,
          updated_at_utc
      )
      WITH base AS (
          SELECT 
              w.last_name,
              w.first_name,
              w.name,

              COALESCE(a.team_canonical, w.team) AS team_canonical,
              w.team AS team_raw,

              CASE 
                WHEN a.team_raw IS NOT NULL AND a.team_canonical <> w.team THEN 1 
                ELSE 0 
              END AS alias_used_row,

              w.wrestling_season,
              w.track_wrestling_category,
              w.wrestler_id,
              w.grade,
              w.team_id,
              w.wrestler_is_state_tournament_qualifier,
              w.wrestler_state_tournament_place,
              w.team_division,
              w.team_region

          FROM wrestler_list_scrape_data w
          LEFT JOIN reference_team_alias_map a ON a.team_raw = w.team
      )
      SELECT 
          last_name,
          first_name,
          name,
          track_wrestling_category,
          team_canonical AS team,

          GROUP_CONCAT(DISTINCT wrestling_season ORDER BY wrestling_season) AS seasons_present,

          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN wrestler_id END ORDER BY wrestler_id) AS wrestler_ids_2025,
          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2025-26' THEN wrestler_id END ORDER BY wrestler_id) AS wrestler_ids_2026,

          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN grade END ORDER BY grade) AS grades_2025,
          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2025-26' THEN grade END ORDER BY grade) AS grades_2026,

          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN wrestler_is_state_tournament_qualifier END ORDER BY wrestler_is_state_tournament_qualifier) AS wrestler_is_state_tournament_qualifier_2025,
          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN wrestler_state_tournament_place END ORDER BY wrestler_is_state_tournament_qualifier) AS wrestler_state_tournament_place_2025,

          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN team_raw END ORDER BY team_raw) AS original_team_2025,
          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN team_id END ORDER BY team_id) AS original_team_id_2025,

          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2025-26' THEN team_raw END ORDER BY team_raw) AS original_team_2026,
          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2025-26' THEN team_id END ORDER BY team_id) AS original_team_id_2026,

          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN team_division END ORDER BY team_division) AS team_division_2025,
          GROUP_CONCAT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN team_region END ORDER BY team_region) AS team_region_2025,

          COUNT(DISTINCT CASE WHEN wrestling_season = '2024-25' THEN wrestler_id END) AS cnt_ids_2025,
          COUNT(DISTINCT CASE WHEN wrestling_season = '2025-26' THEN wrestler_id END) AS cnt_ids_2026,

          MAX(alias_used_row) AS used_team_alias_flag,

          -- Timestamp placeholders supplied by Node.js
          ? AS created_at_mtn,
          ? AS created_at_utc,
          ? AS updated_at_mtn,
          ? AS updated_at_utc

      FROM base
      GROUP BY last_name, first_name, name, track_wrestling_category, team_canonical
      ORDER BY last_name, first_name, name, track_wrestling_category, team_canonical
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
// step_2_create_reference_wrestler_cross_season_summary().then(r => {
//   console.log("step_2_create_reference_wrestler_cross_season_summary:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("step_2_create_reference_wrestler_cross_season_summary error:", err);
//   process.exit(1);
// });


// upsert_wrestler_team_info();

export { step_2_create_reference_wrestler_cross_season_summary };

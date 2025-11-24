// C:\Users\calla\development\projects\wrestling_stats\utilities\raw_sql\discovery_team_division.sql
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import { get_pool } from "../../mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  const sql = `
    -- DROP TABLE wrestler_team_division_reference;
    CREATE TABLE IF NOT EXISTS wrestler_team_division_reference (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

        wrestling_season         VARCHAR(32)  NOT NULL,
        track_wrestling_category VARCHAR(32)  NOT NULL,

        wrestler_team            VARCHAR(255) NULL,
        wrestler_team_id         BIGINT UNSIGNED NULL,
        
        event                    VARCHAR(255)  NULL,
        team_division            VARCHAR(32) NOT NULL,
        team_region              VARCHAR(32) NOT NULL,

        -- timestamps
        created_at_mtn           DATETIME     NOT NULL,
        created_at_utc           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

        updated_at_mtn           DATETIME     NOT NULL,
        updated_at_utc           DATETIME     NOT NULL,

        -- KEY / INDEXES
        UNIQUE KEY idx_alpha (wrestling_season, track_wrestling_category, wrestler_team_id),
        INDEX ix_team_id (wrestler_team_id),
        PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;
  await pool.query(sql);
  _ensured = true;
}

async function upsert_wrestler_team_info() {
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

  const sql = `
    INSERT INTO wrestler_team_division_reference (
        wrestling_season,
        track_wrestling_category,
        wrestler_team,
        wrestler_team_id,
        event,
        team_division,
        team_region,
        created_at_mtn,
        created_at_utc,
        updated_at_mtn,
        updated_at_utc
    )
    WITH division AS (
        SELECT 
            wrestling_season,
            track_wrestling_category,
            event, 
            wrestler_team, 
            wrestler_team_id,
            CASE
                WHEN event LIKE '%1A%' THEN '1A'
                WHEN event LIKE '%2A%' THEN '2A'
                WHEN event LIKE '%3A%' THEN '3A'
                WHEN event LIKE '%4A%' THEN '4A'
                WHEN event LIKE '%5A%' THEN '5A'
                ELSE 'unknown'
            END AS team_division,
            TRIM(SUBSTRING_INDEX(event, 'A', -1)) AS team_region,
            COUNT(DISTINCT wrestler_id) AS count_wrestlers_unique,
            FORMAT(COUNT(*), 0)        AS count_records
        FROM wrestler_match_history_metrics_data 
        WHERE 1 = 1
          AND event LIKE '%CHSAA%'
          AND event LIKE '%Region%'
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING 1 = 1
          AND RIGHT(team_division, 1) = 'A'
    ),
    src AS (
        SELECT
            l.wrestling_season,
            l.track_wrestling_category,
            l.team    AS wrestler_team,
            l.team_id AS wrestler_team_id,
            CASE 
              WHEN d.event IS NULL THEN 'no_regional_event' 
              ELSE d.event 
            END AS event,
            CASE 
              WHEN d.event IS NULL THEN 'unknown' 
              ELSE d.team_division 
            END AS team_division,
            CASE 
              WHEN d.event IS NULL THEN 'unknown' 
              ELSE d.team_region 
            END AS team_region
        FROM wrestler_list_scrape_data AS l
        LEFT JOIN division AS d 
          ON d.wrestler_team_id = l.team_id
    )
    SELECT
        s.wrestling_season,
        s.track_wrestling_category,
        s.wrestler_team,
        s.wrestler_team_id,
        s.event,
        s.team_division,
        s.team_region,
        ?  AS created_at_mtn,
        ?  AS created_at_utc,
        ?  AS updated_at_mtn,
        ?  AS updated_at_utc
    FROM src AS s
    ON DUPLICATE KEY UPDATE

      -- do NOT touch created_* on update:
      -- Only bump updated_* if any tracked column actually changed (NULL-safe)
      updated_at_mtn =
        CASE
          WHEN NOT (
            wrestler_team_division_reference.wrestling_season         <=> VALUES(wrestling_season) AND
            wrestler_team_division_reference.track_wrestling_category <=> VALUES(track_wrestling_category) AND
            wrestler_team_division_reference.wrestler_team            <=> VALUES(wrestler_team) AND
            wrestler_team_division_reference.wrestler_team_id         <=> VALUES(wrestler_team_id) AND
            wrestler_team_division_reference.event                    <=> VALUES(event) AND
            wrestler_team_division_reference.team_division            <=> VALUES(team_division) AND
            wrestler_team_division_reference.team_region              <=> VALUES(team_region)
          )
          THEN VALUES(updated_at_mtn)
          ELSE wrestler_team_division_reference.updated_at_mtn
        END,

      updated_at_utc =
        CASE
          WHEN NOT (
            wrestler_team_division_reference.wrestling_season         <=> VALUES(wrestling_season) AND
            wrestler_team_division_reference.track_wrestling_category <=> VALUES(track_wrestling_category) AND
            wrestler_team_division_reference.wrestler_team            <=> VALUES(wrestler_team) AND
            wrestler_team_division_reference.wrestler_team_id         <=> VALUES(wrestler_team_id) AND
            wrestler_team_division_reference.event                    <=> VALUES(event) AND
            wrestler_team_division_reference.team_division            <=> VALUES(team_division) AND
            wrestler_team_division_reference.team_region              <=> VALUES(team_region)
          )
          THEN CURRENT_TIMESTAMP
          ELSE wrestler_team_division_reference.updated_at_utc
        END,

      -- If either unique key hits (wrestling_season, track_wrestling_category, wrestler_team_id), update these fields:
      wrestler_team_division_reference.wrestling_season         = VALUES(wrestling_season),
      wrestler_team_division_reference.track_wrestling_category = VALUES(track_wrestling_category),
      wrestler_team_division_reference.wrestler_team            = VALUES(wrestler_team),
      wrestler_team_division_reference.wrestler_team_id         = VALUES(wrestler_team_id),
      wrestler_team_division_reference.event                    = VALUES(event),
      wrestler_team_division_reference.team_division            = VALUES(team_division),
      wrestler_team_division_reference.team_region              = VALUES(team_region)
  `;

  const params = [
    created_at_mtn,
    created_at_utc,
    updated_at_mtn,
    updated_at_utc,
  ];

  const [res] = await pool.query(sql, params);

  const affected = Number(res.affectedRows || 0);
  inserted += affected;

  return { inserted, updated };
}

// For your standalone script usage:
// upsert_wrestler_team_info().then(r => {
//   console.log("upsert_wrestler_team_info result:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("upsert_wrestler_team_info error:", err);
//   process.exit(1);
// });


// upsert_wrestler_team_info();

export { upsert_wrestler_team_info };

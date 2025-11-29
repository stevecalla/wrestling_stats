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
    CREATE TABLE IF NOT EXISTS reference_team_alias_map (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    
    team_raw        VARCHAR(255) NOT NULL, -- = legacy / variant name (usually 2024-25)
    team_canonical  VARCHAR(255) NOT NULL, -- = preferred name (2025-26)

    -- timestamps
    created_at_mtn           DATETIME     NOT NULL,
    created_at_utc           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at_mtn           DATETIME     NOT NULL,
    updated_at_utc           DATETIME     NOT NULL,

    PRIMARY KEY (id),
    KEY idx_team_raw (team_raw),
    KEY idx_team_canonical (team_canonical)

    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;

  await pool.query(create_sql);

  _ensured = true;
}

async function step_1_create_reference_team_alias_map() {
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
      TRUNCATE TABLE reference_team_alias_map;
  `;
  await pool.query(truncate_sql);
  
  // raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql
  const insert_sql = `
      INSERT INTO reference_team_alias_map (
        team_raw,
        team_canonical,
        created_at_mtn,
        created_at_utc,
        updated_at_mtn,
        updated_at_utc
      )
        WITH team_rollup AS (
              SELECT
                  team,
                  GROUP_CONCAT(DISTINCT wrestling_season ORDER BY wrestling_season) AS seasons,
                  COUNT(DISTINCT team_id) AS cnt_team_ids,

                  LOWER(
                    TRIM(
                      REGEXP_REPLACE(team, '[^0-9a-z ]', ' ')
                    )
                  ) AS team_clean,

                  MAX(CASE WHEN wrestling_season = '2024-25' THEN 1 ELSE 0 END) AS has_2025,
                  MAX(CASE WHEN wrestling_season = '2025-26' THEN 1 ELSE 0 END) AS has_2026
              FROM wrestler_list_scrape_data
              GROUP BY team
          ),

          team_words AS (
              SELECT
                  team,
                  seasons,
                  cnt_team_ids,
                  team_clean,
                  has_2025,
                  has_2026,

                  SUBSTRING_INDEX(team_clean, ' ', 1) AS word1,

                  CASE
                      WHEN team_clean LIKE '% %'
                      THEN SUBSTRING_INDEX(team_clean, ' ', -1)
                      ELSE NULL
                  END AS word2
              FROM team_rollup
          ),

          with_neighbors AS (
              SELECT
                  team,
                  seasons,
                  cnt_team_ids,
                  team_clean,
                  word1,
                  word2,
                  has_2025,
                  has_2026,

                  LAG(team_clean) OVER (ORDER BY team_clean) AS prev_team_clean,
                  LEAD(team_clean) OVER (ORDER BY team_clean) AS next_team_clean,

                  LAG(team)       OVER (ORDER BY team_clean) AS prev_team,
                  LEAD(team)      OVER (ORDER BY team_clean) AS next_team,

                  LAG(has_2025)   OVER (ORDER BY team_clean) AS prev_has_2025,
                  LAG(has_2026)   OVER (ORDER BY team_clean) AS prev_has_2026,
                  LEAD(has_2025)  OVER (ORDER BY team_clean) AS next_has_2025,
                  LEAD(has_2026)  OVER (ORDER BY team_clean) AS next_has_2026
              FROM team_words
          ),

          scored AS (
              SELECT
                  *,
                  CASE WHEN (has_2025 + has_2026) = 1 THEN 1 ELSE 0 END AS team_single_season,

                  -- 0 = none, 1 = similar to prev, 2 = similar to next
                  CASE
                    WHEN prev_team_clean IS NOT NULL
                        AND (
                              ((word1 IS NOT NULL) AND (word2 IS NULL) AND prev_team_clean LIKE CONCAT('%', word1, '%'))
                            OR ((word1 IS NOT NULL) AND (word2 IS NOT NULL)
                                AND prev_team_clean LIKE CONCAT('%', word1, '%')
                                AND prev_team_clean LIKE CONCAT('%', word2, '%'))
                        )
                    THEN 1
                    WHEN next_team_clean IS NOT NULL
                        AND (
                              ((word1 IS NOT NULL) AND (word2 IS NULL) AND next_team_clean LIKE CONCAT('%', word1, '%'))
                            OR ((word1 IS NOT NULL) AND (word2 IS NOT NULL)
                                AND next_team_clean LIKE CONCAT('%', word1, '%')
                                AND next_team_clean LIKE CONCAT('%', word2, '%'))
                        )
                    THEN 2
                    ELSE 0
                  END AS is_similar_to_neighbor,

                  -- simpler helper flags
                  CASE
                    WHEN prev_team_clean IS NOT NULL
                        AND (
                              ((word1 IS NOT NULL) AND (word2 IS NULL) AND prev_team_clean LIKE CONCAT('%', word1, '%'))
                            OR ((word1 IS NOT NULL) AND (word2 IS NOT NULL)
                                AND prev_team_clean LIKE CONCAT('%', word1, '%')
                                AND prev_team_clean LIKE CONCAT('%', word2, '%'))
                        )
                    THEN 1 ELSE 0
                  END AS similar_prev,

                  CASE
                    WHEN next_team_clean IS NOT NULL
                        AND (
                              ((word1 IS NOT NULL) AND (word2 IS NULL) AND next_team_clean LIKE CONCAT('%', word1, '%'))
                            OR ((word1 IS NOT NULL) AND (word2 IS NOT NULL)
                                AND next_team_clean LIKE CONCAT('%', word1, '%')
                                AND next_team_clean LIKE CONCAT('%', word2, '%'))
                        )
                    THEN 1 ELSE 0
                  END AS similar_next
              FROM with_neighbors
          ),

          alias_pairs AS (
              SELECT DISTINCT
                  -- 2024-25 name (raw / legacy)
                  COALESCE(
                    CASE WHEN has_2025 = 1 THEN team END,
                    CASE WHEN similar_prev = 1 AND prev_has_2025 = 1 THEN prev_team END,
                    CASE WHEN similar_next = 1 AND next_has_2025 = 1 THEN next_team END
                  ) AS team_2024,

                  -- 2025-26 name (canonical)
                  COALESCE(
                    CASE WHEN has_2026 = 1 THEN team END,
                    CASE WHEN similar_prev = 1 AND prev_has_2026 = 1 THEN prev_team END,
                    CASE WHEN similar_next = 1 AND next_has_2026 = 1 THEN next_team END
                  ) AS team_2025
              FROM scored
              WHERE
                  (has_2025 + has_2026) = 1
                  AND (
                      (
                        similar_prev = 1
                        AND (IFNULL(prev_has_2025,0) + IFNULL(prev_has_2026,0)) = 1
                        AND (has_2025 <> IFNULL(prev_has_2025,0)
                          OR has_2026 <> IFNULL(prev_has_2026,0))
                      )
                    OR (
                        similar_next = 1
                        AND (IFNULL(next_has_2025,0) + IFNULL(next_has_2026,0)) = 1
                        AND (has_2025 <> IFNULL(next_has_2025,0)
                          OR has_2026 <> IFNULL(next_has_2026,0))
                      )
                  )
          ),

          clean_pairs AS (
              SELECT DISTINCT
                  team_2024,
                  team_2025, 

                  ? AS created_at_mtn,
                  ? AS created_at_utc,
                  ? AS updated_at_mtn,
                  ? AS updated_at_utc
                  
              FROM alias_pairs
              WHERE team_2024 IS NOT NULL
                AND team_2025 IS NOT NULL
          )

          SELECT
              team_2024 AS team_raw,      -- 2024-25 / legacy
              team_2025 AS team_canonical, -- 2025-26 / preferred
              
              created_at_mtn,
              created_at_utc,
              updated_at_mtn,
              updated_at_utc

          FROM clean_pairs
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
// step_1_create_reference_team_alias_map().then(r => {
//   console.log("step_1_create_reference_team_alias_map:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("step_1_create_reference_team_alias_map error:", err);
//   process.exit(1);
// });


// upsert_wrestler_team_info();

export { step_1_create_reference_team_alias_map };

-- TABLE CHECKS & VALIDATION
SELECT * FROM wrestler_list_scrape_data WHERE;
SELECT wrestling_season, track_wrestling_category, FORMAT(COUNT(*), 0) FROM wrestler_list_scrape_data  GROUP BY 1, 2 WITH ROLLUP;

SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE "2025-26";
SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE "2025-26" AND team_division IS NOT NULL;
SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE "2025-26" AND wrestler_is_state_tournament_qualifier IS NOT NULL;
SELECT team, GROUP_CONCAT(DISTINCT wrestling_season), COUNT(DISTINCT team_id) FROM wrestler_list_scrape_data GROUP BY 1 ORDER BY 1, 2;

-- SIMPLE QUERY TO GROUP WRESTLERS BY WRESTER ID & TEAM ID SEASON OVER SEASON
SELECT 
      last_name, 
      first_name,
      name, 
      team, 

      MAX(CASE 
            WHEN wrestling_season = '2024-25' THEN wrestler_id 
          END) AS wrestler_id_2025,

      MAX(CASE 
            WHEN wrestling_season = '2025-26' THEN wrestler_id 
          END) AS wrestler_id_2026,
          
      MAX(CASE 
            WHEN wrestling_season = '2024-25' THEN team_id 
          END) AS team_id_2025,
          
      MAX(CASE 
            WHEN wrestling_season = '2025-26' THEN team_id 
          END) AS team_id_2026,

      GROUP_CONCAT(
        grade
        ORDER BY 
          CASE 
            WHEN wrestling_season = '2024-25' THEN 1
            WHEN wrestling_season = '2025-26' THEN 2
            ELSE 3
          END,
          grade
      ) AS grades_by_season,

      GROUP_CONCAT(
        team
        ORDER BY 
          CASE 
            WHEN wrestling_season = '2024-25' THEN 1
            WHEN wrestling_season = '2025-26' THEN 2
            ELSE 3
          END,
          team
      ) AS teams_by_season,

      COUNT(DISTINCT wrestler_id) AS distinct_wrestler_ids,
      COUNT(*) AS row_count
  FROM wrestler_list_scrape_data 
  GROUP BY 
      last_name, 
      first_name,
      name, 
      team
  ORDER BY 
      last_name ASC, 
      first_name ASC,
      name ASC, 
      team ASC
;

-- 1️⃣ STEP 1 CREATE REFERENCE TABLE TO BE ABLE TO MATCH WRESTLERS WITH TEAM NAMES THAT CHANGED VS PRIOR SEASON
-- DROP TABLE IF EXISTS reference_team_alias_map;
CREATE TABLE IF NOT EXISTS reference_team_alias_map (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    
    team_raw        VARCHAR(255) NOT NULL, -- = legacy / variant name (usually 2024-25)
    team_canonical  VARCHAR(255) NOT NULL, -- = preferred name (2025-26)

    PRIMARY KEY (id),
    KEY idx_team_raw (team_raw),
    KEY idx_team_canonical (team_canonical)
);

-- FIND TEAMS THAT HAVE SIMILAR NAMES IN DIFFERENT SEASONS
-- ********************
-- is_similar_to_neighbor flag = if a team name is similar to the previous or next team name (when sorted in alpha order)
-- likely_cross_season_flag = if team name only exists in either 2025 or 2026 season then it's likely the same team as another team named slightly differently
-- if is_similar_to_neighbor = 1 (previous) or 2 (next) & likely_cross_season_match is 1 then it's likely the same team
-- ********************
WITH team_rollup AS (
    SELECT
        team,
        GROUP_CONCAT(DISTINCT wrestling_season ORDER BY wrestling_season) AS seasons,
        COUNT(DISTINCT team_id) AS cnt_team_ids,

        -- cleaned name for matching
        LOWER(
          TRIM(
            REGEXP_REPLACE(team, '[^0-9a-z ]', ' ')
          )
        ) AS team_clean,

        -- season flags
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

        -- first word
        SUBSTRING_INDEX(team_clean, ' ', 1) AS word1,

        -- last word (if any)
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
  )

  SELECT
    team,
    seasons,
    cnt_team_ids,
    prev_team,
    next_team,
    team_clean,
    word1,
    word2,

    has_2025,
    has_2026,

    -- 1 = only in one season, 0 = in both
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

    -- combined "likely alias" flag:
    -- similar to neighbor + both are single-season + seasons differ
    CASE
      -- similar to PREVIOUS
      WHEN
        (
          CASE
            WHEN prev_team_clean IS NOT NULL
                 AND (
                       ((word1 IS NOT NULL) AND (word2 IS NULL) AND prev_team_clean LIKE CONCAT('%', word1, '%'))
                    OR ((word1 IS NOT NULL) AND (word2 IS NOT NULL)
                        AND prev_team_clean LIKE CONCAT('%', word1, '%')
                        AND prev_team_clean LIKE CONCAT('%', word2, '%'))
                 )
            THEN 1 ELSE 0
          END
        ) = 1
        AND (has_2025 + has_2026) = 1
        AND (IFNULL(prev_has_2025,0) + IFNULL(prev_has_2026,0)) = 1
        AND (has_2025 <> IFNULL(prev_has_2025,0)
          OR has_2026 <> IFNULL(prev_has_2026,0))

      THEN 1

      -- similar to NEXT
      WHEN
        (
          CASE
            WHEN next_team_clean IS NOT NULL
                 AND (
                       ((word1 IS NOT NULL) AND (word2 IS NULL) AND next_team_clean LIKE CONCAT('%', word1, '%'))
                    OR ((word1 IS NOT NULL) AND (word2 IS NOT NULL)
                        AND next_team_clean LIKE CONCAT('%', word1, '%')
                        AND next_team_clean LIKE CONCAT('%', word2, '%'))
                 )
            THEN 1 ELSE 0
          END
        ) = 1
        AND (has_2025 + has_2026) = 1
        AND (IFNULL(next_has_2025,0) + IFNULL(next_has_2026,0)) = 1
        AND (has_2025 <> IFNULL(next_has_2025,0)
          OR has_2026 <> IFNULL(next_has_2026,0))

      THEN 1

      ELSE 0
    END AS likely_cross_season_match

  FROM with_neighbors
  ORDER BY team_clean
;

-- MANUALLY ADD TEAMS WITH SIMILAR NAMES TO THIS REFERENCE TABLE TO MATCH THEM
INSERT INTO reference_team_alias_map (team_raw, team_canonical) VALUES
    ('University Schools, CO', 'University High School, CO'),
    ('Palmer H S, CO', 'Palmer, CO'),
    ('Prairie View High School, CO', 'Prairie View, CO'),
    ('Fossil Ridge High School, CO', 'Fossil Ridge, CO')
    ON DUPLICATE KEY UPDATE
    team_canonical = VALUES(team_canonical)
;

-- PROGRAMMATICALLY ADD TEAMS WITH SIMILAR NAMES TO THIS REFERENCE TABLE TO MATCH THEM
-- ********************
-- is_similar_to_neighbor flag = if a team name is similar to the previous or next team name (when sorted in alpha order)
-- likely_cross_season_flag = if team name only exists in either 2025 or 2026 season then it's likely the same team as another team named slightly differently
-- if is_similar_to_neighbor = 1 (previous) or 2 (next) & likely_cross_season_match is 1 then it's likely the same team
-- ********************
-- 2️⃣ STEP 2 Using trancate b/ INSERT PAIRED WITH "VALUES" WILL BE DEPRECIATED IN FUTURE
TRUNCATE TABLE reference_team_alias_map;
INSERT INTO reference_team_alias_map (team_raw, team_canonical)
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
          team_2025
      FROM alias_pairs
      WHERE team_2024 IS NOT NULL
        AND team_2025 IS NOT NULL
  )

  SELECT
      team_2024 AS team_raw,      -- 2024-25 / legacy
      team_2025 AS team_canonical -- 2025-26 / preferred
  FROM clean_pairs
;

SELECT * FROM reference_team_alias_map;

-- 2️⃣ Turn your final query into a table; CREATE WRESTLER 2025 & 2026 CROSS SEASON SUMMARY
-- NOTE: STEP 2 BUILD RESULTS TO MATCH 2025 & 2026 WRESTLERS THE TEAM MAP TABLE ABOVE 
DROP TABLE IF EXISTS reference_wrestler_cross_season_summary;
CREATE TABLE reference_wrestler_cross_season_summary AS
WITH base AS (
    SELECT 
        w.last_name,
        w.first_name,
        w.name,

        -- canonicalized team name
        COALESCE(a.team_canonical, w.team) AS team_canonical,

        -- raw team name
        w.team AS team_raw,

        -- flag at the row level: did alias change anything?
        CASE 
          WHEN a.team_raw IS NOT NULL AND a.team_canonical <> w.team 
          THEN 1 
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
      team_canonical AS team,   -- final canonical team name

      -- seasons present
      GROUP_CONCAT(
        DISTINCT wrestling_season
        ORDER BY wrestling_season
      ) AS seasons_present,

      -- wrestler_ids per season
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN wrestler_id 
        END
        ORDER BY wrestler_id
      ) AS wrestler_ids_2025,

      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2025-26' THEN wrestler_id 
        END
        ORDER BY wrestler_id
      ) AS wrestler_ids_2026,

      -- grades per season
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN grade 
        END
        ORDER BY grade
      ) AS grades_2025,

      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2025-26' THEN grade 
        END
        ORDER BY grade
      ) AS grades_2026,

      -- wrestler state qualifier per season
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN wrestler_is_state_tournament_qualifier
        END
        ORDER BY wrestler_is_state_tournament_qualifier
      ) AS wrestler_is_state_tournament_qualifier_2025,
      
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN wrestler_state_tournament_place
        END
        ORDER BY wrestler_is_state_tournament_qualifier
      ) AS wrestler_state_tournament_place_2025,

  --     GROUP_CONCAT(
  --       DISTINCT CASE 
  --         WHEN wrestling_season = '2025-26' THEN wrestler_id 
  --       END
  --       ORDER BY wrestler_id
  --     ) AS wrestler_ids_2026,
      
      -- Original (raw) team names per season, ordered 25 → 26
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN team_raw 
        END
        ORDER BY team_raw
      ) AS original_team_2025,
      
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN team_id 
        END
        ORDER BY team_id
      ) AS original_team_id_2025,

      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2025-26' THEN team_raw 
        END
        ORDER BY team_raw
      ) AS original_team_2026,

      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2025-26' THEN team_id
        END
        ORDER BY team_id
      ) AS original_team_id_2026,

      -- w.team_division,
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN team_division
        END
        ORDER BY team_division
      ) AS team_division_2025,

      -- w.team_region
      GROUP_CONCAT(
        DISTINCT CASE 
          WHEN wrestling_season = '2024-25' THEN team_region
        END
        ORDER BY team_region
      ) AS team_region_2025,

      -- counts of distinct ids per season (optional)
      COUNT(DISTINCT CASE 
        WHEN wrestling_season = '2024-25' THEN wrestler_id 
      END) AS cnt_ids_2025,

      COUNT(DISTINCT CASE 
        WHEN wrestling_season = '2025-26' THEN wrestler_id 
      END) AS cnt_ids_2026,

      -- Did ANY row inside this wrestler use an alias mapping?
      MAX(alias_used_row) AS used_team_alias_flag

  FROM base
  GROUP BY last_name, first_name, name, track_wrestling_category, team_canonical
  ORDER BY last_name, first_name, name, track_wrestling_category, team_canonical
;

SELECT * FROM reference_wrestler_cross_season_summary;
SELECT * FROM reference_wrestler_cross_season_summary WHERE seasons_present LIKE "%2025-26%";
SELECT * FROM reference_wrestler_cross_season_summary WHERE seasons_present LIKE "%2025-26%" AND team_division_2025 IS NOT NULL;
SELECT * FROM reference_wrestler_cross_season_summary WHERE seasons_present LIKE "%2025-26%" AND wrestler_is_state_tournament_qualifier_2025 IS NOT NULL;
SELECT team, GROUP_CONCAT(DISTINCT wrestling_season), COUNT(DISTINCT team_id) FROM reference_wrestler_cross_season_summary GROUP BY 1 ORDER BY 1, 2;

-- 3️⃣ Build a lookup table keyed by 2026 wrestler_id
DROP TABLE IF EXISTS reference_wrestler_2026_state_qualifier_flags;
CREATE TABLE reference_wrestler_2026_state_qualifier_flags AS
SELECT
    -- Take the (only) 2026 wrestler_id for unambiguous cases
    CAST(SUBSTRING_INDEX(wrestler_ids_2026, ',', 1) AS UNSIGNED) AS wrestler_id_2026,
    wrestler_is_state_tournament_qualifier_2025,
    wrestler_state_tournament_place_2025
    
FROM reference_wrestler_cross_season_summary
WHERE
    wrestler_ids_2026 IS NOT NULL
    AND wrestler_ids_2026 <> ''
    AND cnt_ids_2026 = 1         -- only one 2026 id for this person
    AND cnt_ids_2025 = 1         -- only one 2025 id (clean mapping)
    AND wrestler_is_state_tournament_qualifier_2025 IS NOT NULL
;

SELECT * FROM reference_wrestler_2026_state_qualifier_flags
ORDER BY wrestler_id_2026
LIMIT 10000;

DROP TABLE IF EXISTS reference_wrestler_2026_team_division_flags;
CREATE TABLE reference_wrestler_2026_team_division_flags AS
SELECT
    -- Take the (only) 2026 wrestler_id for unambiguous cases
    CAST(SUBSTRING_INDEX(original_team_id_2026, ',', 1) AS UNSIGNED) AS team_id_2026,
    team_division_2025,
    team_region_2025
    
FROM reference_wrestler_cross_season_summary
WHERE
    original_team_id_2026 IS NOT NULL
    AND original_team_id_2026 <> ''
    AND cnt_ids_2026 = 1         -- only one 2026 id for this person
    AND cnt_ids_2025 = 1         -- only one 2025 id (clean mapping)
GROUP BY team_id_2026, team_division_2025, team_region_2025
;

SELECT * FROM reference_wrestler_2026_team_division_flags
ORDER BY team_id_2026
LIMIT 10000;

-- 4️⃣ USE THE LOOKUP IN THE PRIOR STEP TO UPDATE THE WRESTLER LIST
-- Now use that mapping to push state qualifier flags into wrestler_list_scrape_data for the 2025-26 season
UPDATE wrestler_list_scrape_data w
	JOIN reference_wrestler_2026_state_flags r ON w.wrestler_id      = r.wrestler_id_2026
		AND w.wrestling_season = '2025-26'
SET
    w.wrestler_is_state_tournament_qualifier = r.wrestler_is_state_tournament_qualifier_2025,
    w.wrestler_state_tournament_place        = r.wrestler_state_tournament_place_2025
;

UPDATE wrestler_list_scrape_data w
	JOIN reference_wrestler_2026_team_division_flags r ON w.team_id     = r.team_id_2026
		AND w.wrestling_season = '2025-26'
SET
    w.team_division 						 = r.team_division_2025,
    w.team_region							 = r.team_region_2025
;
 
SELECT * FROM wrestler_list_scrape_data; -- 10,911 wrestlers
SELECT * FROM reference_wrestler_cross_season_summary; -- 8,443 unique wrestlers
SELECT * FROM reference_wrestler_cross_season_summary WHERE seasons_present LIKE "%2025-26%"; -- 4,310 unique wrestlers

-- ✅ Reset those four fields to NULL for all 2025-26 rows
UPDATE wrestler_list_scrape_data
SET
    team_division = NULL,
    team_region = NULL,
    wrestler_is_state_tournament_qualifier = NULL,
    wrestler_state_tournament_place = NULL
WHERE wrestling_season = '2025-26'
  AND (
        team_division IS NOT NULL
     OR team_region IS NOT NULL
     OR wrestler_is_state_tournament_qualifier IS NOT NULL
     OR wrestler_state_tournament_place IS NOT NULL
  );

SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE "2025-26"; -- 4,310
SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE "2025-26" AND team_division IS NOT NULL; -- 0 before insert above; after 4,310
SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE "2025-26" AND wrestler_is_state_tournament_qualifier IS NOT NULL; -- 0 before insert above; after 411

-- 4️⃣ USE THE LOOKUP IN THE PRIOR STEP TO UPDATE THE WRESTLER MATCH HISTORY METRICS TABLE
SELECT * FROM wrestler_match_history_metrics_data LIMIT 10;
SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE "2025-26"; -- 4,310
-- Now use that mapping to push state qualifier flags into wrestler_list_scrape_data for the 2025-26 season

UPDATE wrestler_match_history_metrics_data
SET
    wrestler_team_division = NULL,
    wrestler_team_region = NULL,
    wrestler_is_state_tournament_qualifier = NULL,
    wrestler_state_tournament_place = NULL,
    opponent_team_division = NULL,
    opponent_team_region = NULL,
    opponent_is_state_tournament_qualifier = NULL,
    opponent_state_tournament_place = NULL
WHERE wrestling_season = '2025-26'
  AND (
        wrestler_team_division IS NOT NULL
     OR wrestler_team_region IS NOT NULL
     OR wrestler_is_state_tournament_qualifier IS NOT NULL
     OR wrestler_state_tournament_place IS NOT NULL
     OR opponent_team_division IS NOT NULL
     OR opponent_team_region IS NOT NULL
     OR opponent_is_state_tournament_qualifier IS NOT NULL
     OR opponent_state_tournament_place IS NOT NULL
  );

UPDATE wrestler_match_history_metrics_data m
JOIN reference_wrestler_2026_state_qualifier_flags r
      ON m.wrestler_id = r.wrestler_id_2026
SET
    m.wrestler_is_state_tournament_qualifier = r.wrestler_is_state_tournament_qualifier_2025,
    m.wrestler_state_tournament_place        = r.wrestler_state_tournament_place_2025
WHERE m.wrestling_season = '2025-26';

UPDATE wrestler_match_history_metrics_data m
JOIN reference_wrestler_2026_state_qualifier_flags r
      ON m.opponent_id = r.wrestler_id_2026
SET
    m.opponent_is_state_tournament_qualifier = r.wrestler_is_state_tournament_qualifier_2025,
    m.opponent_state_tournament_place        = r.wrestler_state_tournament_place_2025
WHERE m.wrestling_season = '2025-26';

UPDATE wrestler_match_history_metrics_data m
JOIN reference_wrestler_2026_team_division_flags r
      ON m.wrestler_team_id = r.team_id_2026
SET
    m.wrestler_team_division = r.team_division_2025,
    m.wrestler_team_region   = r.team_region_2025
WHERE m.wrestling_season = '2025-26';

UPDATE wrestler_match_history_metrics_data m
JOIN reference_wrestler_2026_team_division_flags r
      ON m.opponent_team_id = r.team_id_2026
SET
    m.opponent_team_division = r.team_division_2025,
    m.opponent_team_region   = r.team_region_2025
WHERE m.wrestling_season = '2025-26';

SELECT * FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2025-26"; -- tbd
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2025-26" AND wrestler_team_division IS NOT NULL; -- 0 before insert above; after tbd
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2025-26" AND opponent_team_division IS NOT NULL; -- 0 before insert above; after tbd
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2025-26" AND wrestler_is_state_tournament_qualifier IS NOT NULL; -- 0 before insert above; after tbd
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2025-26" AND opponent_is_state_tournament_qualifier IS NOT NULL; -- 0 before insert above; after tbd

-- add some dummy records
ALTER TABLE wrestler_match_history_metrics_data
    ADD COLUMN is_dummy_record TINYINT(1) NULL DEFAULT 0;
    
INSERT INTO wrestler_match_history_metrics_data (
    wrestler_id,
    opponent_id,
    wrestler_team_id,
    opponent_team_id,
    wrestling_season,
    track_wrestling_category,
    weight_category,
    event,
    raw_details,
    start_date,
    end_date,
    match_order,
    is_dummy_record
)
SELECT
    r.wrestler_id_2026,
    r.wrestler_id_2026,
    t.team_id_2026,
    t.team_id_2026,
    '2025-26',
    'Dummy Category',       -- required
    'Dummy Weight',         -- required?
    'Dummy Event',          -- required?
    'Dummy details',        -- required?
    NOW(),                  -- required?
    NOW(),                  -- required?
    1,                      -- match_order must exist
    1
FROM reference_wrestler_2026_state_qualifier_flags r
JOIN reference_wrestler_2026_team_division_flags t
      ON 1 = 1
LIMIT 10;

DELETE FROM wrestler_match_history_metrics_data
WHERE wrestling_season = '2025-26' AND is_dummy_record = 1;

ALTER TABLE wrestler_match_history_metrics_data
    DROP COLUMN is_dummy_record;






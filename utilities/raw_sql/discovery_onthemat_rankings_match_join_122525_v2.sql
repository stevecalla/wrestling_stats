-- ⚠️ HARD RESET (use only when rebuilding the reference table from scratch)
-- DROP TABLE `wrestling_stats`.`reference_wrestler_rankings_list`;


/* ----------------------------------------------------------------------------
   1) Quick sanity checks: rankings list table
      Purpose:
      - Verify table exists and is populated
      - Spot obvious ingestion or duplication issues
---------------------------------------------------------------------------- */

-- Full raw scan (manual inspection)
SELECT * FROM reference_wrestler_rankings_list;

-- Row counts by ranking week
-- WITH ROLLUP provides a grand total row at the bottom
SELECT 
	ranking_week, 
    FORMAT(COUNT(*), 0) AS reference_wrestler_rankings_list_row_count 
FROM reference_wrestler_rankings_list
GROUP BY 1 WITH ROLLUP
;

-- Quick sanity check of scraped wrestler list
SELECT * FROM wrestler_list_scrape_data LIMIT 10;


/* ----------------------------------------------------------------------------
   5a) Rankings vs Scrape coverage (weeks 0 / 1 / 2)

   Goal:
   - Understand coverage of rankings data vs scraped wrestler list
   - Quantify how many ranked wrestlers appear in scrape
   - Identify missing names by week and overall union

   ⚠ IMPORTANT:
   - Matching is currently name-based (wrestler_name only)
   - Same caveats as prior analysis apply (homonyms, spelling variants, etc.)
---------------------------------------------------------------------------- */

WITH
-- ---------------------------------------------------------------------------
-- 1) Reference universe (rankings only, no joins)
--    One row per wrestler per ranking week
-- ---------------------------------------------------------------------------
ref_matches AS (
  SELECT DISTINCT
    r.ranking_week_number,
    r.wrestler_name
  FROM reference_wrestler_rankings_list r
  WHERE r.ranking_week_number IN (0,1,2)
),

-- Collapse reference data to one row per wrestler
-- Boolean-style flags indicate which ranking weeks they appear in
ref_per_name AS (
  SELECT
    wrestler_name,
    MAX(ranking_week_number = 0) AS has_week0,
    MAX(ranking_week_number = 1) AS has_week1,
    MAX(ranking_week_number = 2) AS has_week2
  FROM ref_matches
  GROUP BY wrestler_name
),

-- ---------------------------------------------------------------------------
-- 2) Matched universe (rankings joined to scraped wrestler list)
--    Join logic:
--      - Exact wrestler name match
--      - School prefix match (before comma)
--      - Same season + category
-- ---------------------------------------------------------------------------
matched_matches AS (
  SELECT DISTINCT
    r.ranking_week_number,
    r.wrestler_name
  FROM reference_wrestler_rankings_list r
  JOIN wrestler_list_scrape_data l
    ON r.wrestler_name = l.name
   AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1)
   AND l.wrestling_season = '2025-26'
   AND l.track_wrestling_category = 'High School Boys'
  WHERE r.ranking_week_number IN (0,1,2)
),

-- Reduce matched universe to one row per wrestler
-- Same week-presence flags as reference universe
matched_per_name AS (
  SELECT
    wrestler_name,
    MAX(ranking_week_number = 0) AS has_week0,
    MAX(ranking_week_number = 1) AS has_week1,
    MAX(ranking_week_number = 2) AS has_week2
  FROM matched_matches
  GROUP BY wrestler_name
),

-- ---------------------------------------------------------------------------
-- 3) Missing universe
--    Wrestlers that appear in rankings but never match the scrape
--    Anti-join performed on wrestler_name
-- ---------------------------------------------------------------------------
missing_per_name AS (
  SELECT r.*
  FROM ref_per_name r
  LEFT JOIN matched_per_name m
    ON m.wrestler_name = r.wrestler_name
  WHERE m.wrestler_name IS NULL
)

-- ---------------------------------------------------------------------------
-- 4) Coverage summary metrics
-- ---------------------------------------------------------------------------
SELECT
  /* ---------------------------
     Reference totals
     (how many ranked wrestlers exist)
  --------------------------- */
  (SELECT SUM(has_week0=1) FROM ref_per_name) AS ref_week0_total,
  (SELECT SUM(has_week1=1) FROM ref_per_name) AS ref_week1_total,
  (SELECT SUM(has_week2=1) FROM ref_per_name) AS ref_week2_total,
  (SELECT COUNT(*)        FROM ref_per_name) AS ref_union_total,

  /* ---------------------------
     Matched totals
     (how many ranked wrestlers appear in scrape)
  --------------------------- */
  (SELECT SUM(has_week0=1) FROM matched_per_name) AS matched_week0_total,
  (SELECT SUM(has_week1=1) FROM matched_per_name) AS matched_week1_total,
  (SELECT SUM(has_week2=1) FROM matched_per_name) AS matched_week2_total,
  (SELECT COUNT(*)        FROM matched_per_name) AS matched_union_total,

  /* ---------------------------
     Missing totals
     (reference minus matched)
  --------------------------- */
  (SELECT SUM(has_week0=1) FROM missing_per_name) AS missing_week0_total,
  (SELECT SUM(has_week1=1) FROM missing_per_name) AS missing_week1_total,
  (SELECT SUM(has_week2=1) FROM missing_per_name) AS missing_week2_total,
  (SELECT COUNT(*)        FROM missing_per_name) AS missing_union_total,

  /* ---------------------------
     Overall coverage percentage
  --------------------------- */
  ROUND(
    100 * (SELECT COUNT(*) FROM matched_per_name) /
          NULLIF((SELECT COUNT(*) FROM ref_per_name), 0),
    2
  ) AS pct_union_matched
;


-- *******************************
-- Distribution of rankings rows by wrestler + school
-- Useful for detecting duplicates or multi-weight entries
SELECT 
	wrestler_name,
    school,
    FORMAT(COUNT(*), 0)
FROM reference_wrestler_rankings_list
-- WHERE wrestler_name LIKE "Brennan White"
GROUP BY 1, 2
ORDER BY 1, 2
;

-- Ordered rankings view
-- Helps visually inspect ordering anomalies
SELECT 
    *
FROM reference_wrestler_rankings_list
ORDER BY division, weight_lbs, `rank`, wrestler_name
;

-- Scraped wrestler list entries where name matched OnTheMat
-- Used to compare expected vs ranked population
SELECT 
	name,
    team,
    onthemat_is_name_match,
    FORMAT(COUNT(*), 0)
FROM wrestler_list_scrape_data AS l
WHERE 1 = 1
	AND wrestling_season = '2025-26'
    AND onthemat_is_name_match = 1
    -- AND name IN ('Cam Benavidez')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


/* ----------------------------------------------------------------------------
   Name-based comparison between rankings and wrestler list
---------------------------------------------------------------------------- */

WITH rankings AS (
-- One row per ranked wrestler / school / weight
SELECT 
	wrestler_name AS name,
    school AS team,
    weight_lbs AS weight,
    FORMAT(COUNT(*), 0)
FROM reference_wrestler_rankings_list
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
)
-- SELECT * FROM ranking;

, list AS (
-- One row per scraped wrestler / team (OnTheMat-matched only)
SELECT 
	name,
    team,
    onthemat_is_name_match,
    FORMAT(COUNT(*), 0)
FROM wrestler_list_scrape_data AS l
WHERE 1 = 1
	AND wrestling_season = '2025-26'
    AND onthemat_is_name_match = 1
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
)
-- SELECT * FROM list;

, join_lists AS (
-- Left join rankings → list to detect missing names
SELECT 
	r.name AS rank_list,
    r.team AS rank_team,
    r.weight AS rank_weight,
    l.name AS name_list,
    GROUP_CONCAT(l.team) as team_list,
    FORMAT(COUNT(*), 0)
FROM rankings AS r
	LEFT JOIN list AS l on r.name = l.name
        -- AND r.team LIKE SUBSTRING_INDEX(l.team, ',', 1)
GROUP BY r.name, r.team, r.weight, l.name
)
-- SELECT * FROM join_lists

-- Only ranking entries that never matched the scraped list
SELECT * FROM join_lists WHERE name_list IS NULL
-- SELECT DISTINCT rank_team FROM join_lists WHERE name_list IS NULL
;


-- Distinct school counts in rankings
-- Used to understand school distribution and normalization needs
SELECT DISTINCT school, FORMAT(COUNT(*), 0)
FROM reference_wrestler_rankings_list
GROUP BY 1
ORDER BY 1;


WITH
-- Normalized ranking school names
r AS (
  SELECT DISTINCT
    school AS ranking_school,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(school,'[^A-Za-z0-9 ]',' '),'\\s+',' '))) AS ranking_school_norm
  FROM reference_wrestler_rankings_list
  WHERE wrestling_season = '2025-26' 
    AND track_wrestling_category = 'High School Boys'
),

-- Normalized scraped team school names (prefix only)
l AS (
  SELECT DISTINCT
    team AS list_team,
    SUBSTRING_INDEX(team, ',', 1) AS list_school,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(SUBSTRING_INDEX(team, ',', 1),'[^A-Za-z0-9 ]',' '),'\\s+',' '))) AS list_school_norm
  FROM wrestler_list_scrape_data
  WHERE wrestling_season = '2025-26'
    AND track_wrestling_category = 'High School Boys'
)

-- Candidate school matches + auto-generated NAME_FIXES helper
SELECT
  r.ranking_school,
  GROUP_CONCAT(DISTINCT l.list_team ORDER BY l.list_team SEPARATOR ' , ') AS list_school,
  
  -- JS-style array literal you can copy/paste directly into NAME_FIXES
  CONCAT(
    "['",
    REPLACE(GROUP_CONCAT(DISTINCT l.list_team ORDER BY l.list_team SEPARATOR ' , '), "'", "\\'"),
    "', '",
    REPLACE(r.ranking_school, "'", "\\'"),
    "']"
  ) AS name_fix_array,
  
  -- Number of possible matching teams for this school
  COUNT(DISTINCT l.list_team) AS candidate_team_count
FROM r
LEFT JOIN l
  ON l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%')
  OR r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')
WHERE 1 = 1
GROUP BY r.ranking_school
HAVING 1 = 1
	AND candidate_team_count >=0
ORDER BY candidate_team_count DESC, r.ranking_school;

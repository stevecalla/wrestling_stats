/* ============================================================================
   OnTheMat rankings → wrestler_list_scrape_data mapping & validation
   Database: wrestling_stats

   Purpose
   -------
   This file contains:
   1) quick sanity checks for the source tables
   2) matching/overlap diagnostics across ranking weeks (week_0 vs week_1)
   3) preview queries to validate join logic before UPDATEs
   4) downstream checks for match-history metrics enrichment

   Notes / Gotchas
   --------------
   - reference_wrestler_rankings_list contains WEEKLY snapshots (week_0, week_1, ...)
   - If you join rankings across multiple weeks, you MUST choose which week "wins"
     (typically latest week) to avoid non-deterministic updates.
   - GROUP BY wrestler_name alone can merge different wrestlers that share a name.
     Prefer (wrestler_name, school) when possible.
============================================================================ */

/* ----------------------------------------------------------------------------
   0) Select schema / DB
---------------------------------------------------------------------------- */
USE wrestling_stats;

/* ----------------------------------------------------------------------------
   1) Quick sanity checks: rankings list table
      - Spot-check rows
      - Confirm row count
---------------------------------------------------------------------------- */
SELECT * FROM reference_wrestler_rankings_list;

SELECT 
	ranking_week, 
    FORMAT(COUNT(*), 0) AS reference_wrestler_rankings_list_row_count 
FROM reference_wrestler_rankings_list
GROUP BY 1 WITH ROLLUP
;

SELECT id, wrestler_name, updated_at_mtn
FROM reference_wrestler_rankings_list
WHERE 1 = 1 AND (wrestler_name LIKE "%Baden Laiminger%" OR wrestler_name LIKE "%Braden Laiminger%")
; -- example LIKE match check

/* ----------------------------------------------------------------------------
   2) Quick sanity checks: wrestler list scrape table
      - Spot-check rows
      - Confirm row count
---------------------------------------------------------------------------- */
SELECT * FROM wrestler_list_scrape_data LIMIT 10;

SELECT 
    track_wrestling_category,
	wrestling_season,
	FORMAT(COUNT(*), 0) AS wrestler_list_scrape_data_row_count 
FROM wrestler_list_scrape_data
GROUP BY 1, 2 WITH ROLLUP
ORDER BY 1
;
/* ----------------------------------------------------------------------------
   3) Spot-check known name/team issues (examples)
      - Useful when troubleshooting mismatches in joins
---------------------------------------------------------------------------- */
SELECT name, team
FROM wrestler_list_scrape_data
WHERE name LIKE "%Elijah Baumgardner%"; -- Elijah Baumgartner vs Elijah Baumgardner (typo case)

SELECT name, team, wrestling_season, track_wrestling_category, grade
FROM wrestler_list_scrape_data
WHERE name = "Cade Hirstine"; -- example exact match check

SELECT name, team, wrestling_season, track_wrestling_category, grade
FROM wrestler_list_scrape_data
WHERE name LIKE "%Aaron Garcia%"; -- example LIKE match check

SELECT name, team, wrestling_season, track_wrestling_category, grade
FROM wrestler_list_scrape_data
WHERE name LIKE "%Braden Laiminger%"; -- example LIKE match check
-- WHERE name LIKE "%Baden Laiminger%"; -- example LIKE match check

/* ----------------------------------------------------------------------------
   4) Diagnostic: union-style join (week 0 + week 1) aggregated by wrestler_name
      - Shows how often the same wrestler_name appears across different weeks/files
      - WHY "488/503/564" happens:
          week0_total + week1_total - overlap = union_total
      - This query is grouped by wrestler_name only (collisions possible)
---------------------------------------------------------------------------- */
SELECT
    r.wrestler_name,

    -- raw, unsorted (kept for reference)
    GROUP_CONCAT(r.school) AS schools_unsorted,

    -- sorted by week number to compare evolution across weeks
    GROUP_CONCAT(r.school      ORDER BY r.ranking_week_number SEPARATOR ', ') AS schools_by_week,
    GROUP_CONCAT(r.`rank`      ORDER BY r.ranking_week_number SEPARATOR ', ') AS ranks_by_week,
    GROUP_CONCAT(r.weight_lbs  ORDER BY r.ranking_week_number SEPARATOR ', ') AS weights_by_week,
    GROUP_CONCAT(r.source_file ORDER BY r.ranking_week_number SEPARATOR ', ') AS source_files_by_week,

    MIN(l.wrestling_season) AS min_wrestling_season,
    MIN(l.track_wrestling_category) AS min_track_wrestling_category,
    MIN(l.name) AS min_matched_name,
    MIN(l.wrestler_id) AS min_matched_wrestler_id,
    MIN(l.team) AS min_matched_team,

    -- match flags derived from whether the JOIN found anything
    CASE WHEN MIN(l.name) IS NULL THEN 0 ELSE 1 END AS is_name_match,

    -- team check: compare ranking school vs list team prefix
    CASE
        WHEN MIN(l.name) IS NULL THEN NULL
        WHEN MIN(r.school) NOT LIKE MIN(l.team) THEN 1
        ELSE 0
    END AS is_team_match,

    -- number of ranking rows included in this group (can be >1 because multiple weeks)
    COUNT(*) AS ranking_rows_in_group,

    -- these window metrics reflect grouped results (not raw rows)
    SUM(CASE WHEN MIN(l.name) IS NOT NULL THEN 1 ELSE 0 END) OVER () AS matched_groups,
    SUM(CASE WHEN MIN(l.name) IS NULL THEN 1 ELSE 0 END) OVER () AS unmatched_groups,
    COUNT(*) OVER () AS total_groups
FROM reference_wrestler_rankings_list AS r
LEFT JOIN wrestler_list_scrape_data AS l ON r.wrestler_name = l.name
    -- AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1) -- team prefix only (removes ", CO")
	-- AND l.wrestling_season = '2025-26'
	-- AND l.track_wrestling_category = 'High School Boys'
WHERE 1 = 1
	-- AND r.ranking_week_number IN (0)
    -- AND r.wrestler_name = "Konner Horton"
    -- AND r.wrestler_name = "Baden Laiminger" -- should be spelled Braden not Baden
GROUP BY r.wrestler_name
-- HAVING is_name_match = 0
ORDER BY r.wrestler_name;

/* ----------------------------------------------------------------------------
   5a) Rankings vs Scrape coverage (weeks 0/1/2)
   - Reference totals (no join)
   - Matched totals (your join)
   - Missing = in reference but not matched
   - IMPORTANT: currently uses wrestler_name only (same caveat as before)
---------------------------------------------------------------------------- */

WITH
-- 1) Reference universe (no join)
ref_matches AS (
  SELECT DISTINCT
    r.ranking_week_number,
    r.wrestler_name
  FROM reference_wrestler_rankings_list r
  WHERE r.ranking_week_number IN (0,1,2)
),

ref_per_name AS (
  SELECT
    wrestler_name,
    MAX(ranking_week_number = 0) AS has_week0,
    MAX(ranking_week_number = 1) AS has_week1,
    MAX(ranking_week_number = 2) AS has_week2
  FROM ref_matches
  GROUP BY wrestler_name
),

-- 2) Matched universe (your join logic)
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

matched_per_name AS (
  SELECT
    wrestler_name,
    MAX(ranking_week_number = 0) AS has_week0,
    MAX(ranking_week_number = 1) AS has_week1,
    MAX(ranking_week_number = 2) AS has_week2
  FROM matched_matches
  GROUP BY wrestler_name
),

-- 3) Missing universe: names in reference but not matched (anti-join on name)
missing_per_name AS (
  SELECT r.*
  FROM ref_per_name r
  LEFT JOIN matched_per_name m
    ON m.wrestler_name = r.wrestler_name
  WHERE m.wrestler_name IS NULL
)

SELECT
  /* ---------------------------
     Reference totals
  --------------------------- */
  (SELECT SUM(has_week0=1) FROM ref_per_name) AS ref_week0_total,
  (SELECT SUM(has_week1=1) FROM ref_per_name) AS ref_week1_total,
  (SELECT SUM(has_week2=1) FROM ref_per_name) AS ref_week2_total,
  (SELECT COUNT(*)        FROM ref_per_name) AS ref_union_total,

  /* ---------------------------
     Matched totals
  --------------------------- */
  (SELECT SUM(has_week0=1) FROM matched_per_name) AS matched_week0_total,
  (SELECT SUM(has_week1=1) FROM matched_per_name) AS matched_week1_total,
  (SELECT SUM(has_week2=1) FROM matched_per_name) AS matched_week2_total,
  (SELECT COUNT(*)        FROM matched_per_name) AS matched_union_total,

  /* ---------------------------
     Missing totals (ref minus matched)
  --------------------------- */
  (SELECT SUM(has_week0=1) FROM missing_per_name) AS missing_week0_total,
  (SELECT SUM(has_week1=1) FROM missing_per_name) AS missing_week1_total,
  (SELECT SUM(has_week2=1) FROM missing_per_name) AS missing_week2_total,
  (SELECT COUNT(*)        FROM missing_per_name) AS missing_union_total,

  /* ---------------------------
     Coverage %
  --------------------------- */
  ROUND(
    100 * (SELECT COUNT(*) FROM matched_per_name) /
          NULLIF((SELECT COUNT(*) FROM ref_per_name), 0),
    2
  ) AS pct_union_matched
;

/* ----------------------------------------------------------------------------
   5b) Week overlap math (week0_only, week1_only, both, union)
      - This reproduces the "564 union" concept deterministically.
      - IMPORTANT: This uses wrestler_name only. Consider (wrestler_name, school).
---------------------------------------------------------------------------- */
WITH matches AS (
  SELECT DISTINCT
    r.ranking_week_number,
    r.wrestler_name
  FROM reference_wrestler_rankings_list r
    JOIN wrestler_list_scrape_data l ON r.wrestler_name = l.name
      AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1)
      AND l.wrestling_season = '2025-26'
      AND l.track_wrestling_category = 'High School Boys'
  WHERE r.ranking_week_number IN (0,1,2)
),
per_name AS (
  SELECT
    wrestler_name,
    MAX(ranking_week_number = 0) AS has_week0,
    MAX(ranking_week_number = 1) AS has_week1,
    MAX(ranking_week_number = 2) AS has_week2
  FROM matches
  GROUP BY wrestler_name
)
SELECT
  -- per-week totals
  SUM(has_week0 = 1) AS week0_total,
  SUM(has_week1 = 1) AS week1_total,
  SUM(has_week2 = 1) AS week2_total,

  -- exclusives + overlap
  SUM(has_week0 = 1 AND has_week1 = 1 AND has_week2 = 1) AS all_weeks,	
  SUM(has_week0 = 1 AND has_week1 = 0 AND has_week2 = 0) AS week0_only,
  SUM(has_week0 = 0 AND has_week1 = 1 AND has_week2 = 0) AS week1_only,
  SUM(has_week0 = 0 AND has_week1 = 0 AND has_week2 = 1) AS week2_only,
  
  SUM(has_week0 = 1 AND has_week1 = 1 AND has_week2 = 0) AS week0_week1_only,
  SUM(has_week0 = 1 AND has_week1 = 0 AND has_week2 = 1) AS week0_week2_only,
  SUM(has_week0 = 0 AND has_week1 = 1 AND has_week2 = 1) AS week1_week2_only,

  -- union
  COUNT(*) AS union_total
FROM per_name;

/* ----------------------------------------------------------------------------
   6) PREVIEW join (no update): what OnTheMat fields would be set on the list table?
      - This is a preview query to validate join logic before any UPDATE.
      - WARNING: If you do not select a single "winning week", results can be ambiguous.
      - The subquery below uses MIN(rank) / MIN(weight) across ALL weeks (not preferred).
---------------------------------------------------------------------------- */

WITH preview AS (
  SELECT
    l.id,
    l.name,
    l.team,
    l.wrestling_season,
    l.track_wrestling_category,

    r.onthemat_rank AS preview_onthemat_rank,
    r.weight_lbs    AS preview_onthemat_weight_lbs,

    CASE
      WHEN r.wrestler_name IS NULL THEN 0 ELSE 1
    END AS preview_is_name_match,

    CASE
      WHEN r.wrestler_name IS NULL THEN NULL
      WHEN r.school NOT LIKE SUBSTRING_INDEX(l.team, ',', 1) THEN 1
      ELSE 0
    END AS preview_is_team_match

  FROM wrestler_list_scrape_data l
  LEFT JOIN (
    SELECT
      wrestler_name,
      school,
      MIN(`rank`)     AS onthemat_rank,
      MIN(weight_lbs) AS weight_lbs
    FROM reference_wrestler_rankings_list
    GROUP BY wrestler_name, school
  ) r
    ON r.wrestler_name = l.name
   AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1)

  WHERE l.wrestling_season = '2025-26'
    AND l.track_wrestling_category = 'High School Boys'
)

SELECT
  p.*,

  /* totals repeated on every row */
  COUNT(*) OVER () AS total_rows,

  SUM(p.preview_is_name_match = 1) OVER () AS total_name_match_1,
  SUM(p.preview_is_name_match = 0) OVER () AS total_name_match_0,

  SUM(p.preview_is_team_match = 0) OVER () AS total_team_match_0,
  SUM(p.preview_is_team_match = 1) OVER () AS total_team_match_1,
  SUM(p.preview_is_team_match IS NULL) OVER () AS total_team_match_null

FROM preview p
ORDER BY p.name
-- LIMIT 100
;

/* ----------------------------------------------------------------------------
   7) Post-update coverage check (list table)
      - How many rows have onthemat fields populated?
      - Run after your UPDATE step.
---------------------------------------------------------------------------- */
SELECT
  COUNT(*) AS total,
  SUM(onthemat_rank IS NOT NULL) AS with_rank,
  SUM(onthemat_weight_lbs IS NOT NULL) AS with_weight
FROM wrestler_list_scrape_data
WHERE 1 = 1
	AND wrestling_season = '2025-26'
	AND track_wrestling_category = 'High School Boys';

/* ----------------------------------------------------------------------------
   8) Identify duplicates / collisions in list table among matched rows
      - Useful to identify cases where the same name appears multiple times
        (different wrestler_id or different teams).
---------------------------------------------------------------------------- */
SELECT
  name,
  GROUP_CONCAT(wrestler_id) AS wrestler_ids,
  GROUP_CONCAT(team) AS teams,
  COUNT(*) AS row_count
FROM wrestler_list_scrape_data
WHERE 1 = 1 
	AND wrestling_season = '2025-26'
	AND track_wrestling_category = 'High School Boys'
	AND onthemat_rank IS NOT NULL
GROUP BY name
ORDER BY row_count DESC
;

/* ----------------------------------------------------------------------------
   9) Targeted spot checks for specific names
---------------------------------------------------------------------------- */
SELECT *
FROM wrestler_list_scrape_data
WHERE 1 = 1
	AND wrestling_season = '2025-26'
	AND track_wrestling_category = 'High School Boys'
	AND name IN ('Noah Garcia-Salazar', 'Ryder Martyn', 'traysen Rosane')
ORDER BY name
;

/* ----------------------------------------------------------------------------
   10) Quick sanity checks: match history metrics table
---------------------------------------------------------------------------- */
SELECT 
	*
FROM wrestler_match_history_metrics_data
LIMIT 10
;

/* ----------------------------------------------------------------------------
   11) Targeted spot checks: metrics for specific names
---------------------------------------------------------------------------- */
SELECT *
FROM wrestler_match_history_metrics_data
WHERE 1 = 1
	AND wrestling_season = '2025-26'
	AND track_wrestling_category = 'High School Boys'
	AND wrestler_name IN ('Noah Garcia-Salazar', 'Ryder Martyn', 'traysen Rosane')
ORDER BY wrestler_name;

/* ----------------------------------------------------------------------------
   12) Validate list enrichment is visible via wrestler_id join into metrics
      - Joins match history metrics to the list table by wrestler_id
      - Filters to only those with a successful OnTheMat name match
---------------------------------------------------------------------------- */
SELECT
  h.wrestling_season,
  h.track_wrestling_category,
  h.wrestler_id AS wrestler_id_metrics,

  l.wrestler_id AS wrestler_id_list,
  l.onthemat_is_name_match,
  l.onthemat_name,
  l.onthemat_is_team_match,
  l.onthemat_team,
  l.onthemat_rank,
  l.onthemat_weight_lbs,
  l.onthemat_rankings_source_file

  -- optional rollups:
  -- FORMAT(COUNT(DISTINCT h.wrestler_id), 0) AS count_distinct_wrestler_id,
  -- FORMAT(COUNT(*), 0) AS row_count

FROM wrestler_match_history_metrics_data AS h
LEFT JOIN wrestler_list_scrape_data AS l
  ON h.wrestler_id = l.wrestler_id
WHERE 1 = 1
	AND h.wrestling_season = '2025-26'
	AND h.track_wrestling_category = 'High School Boys'
	AND l.onthemat_is_name_match = 1
ORDER BY h.wrestling_season, h.track_wrestling_category, h.wrestler_id
;

SELECT 
	wrestler_name,
    school,
    FORMAT(COUNT(*), 0)
FROM reference_wrestler_rankings_list
GROUP BY 1, 2
ORDER BY 1, 2
;

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
;
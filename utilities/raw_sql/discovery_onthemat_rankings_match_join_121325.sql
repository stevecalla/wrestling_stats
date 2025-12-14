USE wrestling_stats;

SELECT * FROM reference_wrestler_rankings_list LIMIT 10;
SELECT FORMAT(COUNT(*), 0), (668 * 2) AS math_calc FROM reference_wrestler_rankings_list LIMIT 10;

SELECT * FROM wrestler_list_scrape_data LIMIT 10;
SELECT FORMAT(COUNT(*), 0) FROM wrestler_list_scrape_data LIMIT 10;
SELECT name, team FROM wrestler_list_scrape_data WHERE name LIKE "%Elijah Baumgardner%"; -- Elijah Baumgartner vs Elijah Baumgardner
SELECT name, team, wrestling_season, track_wrestling_category, grade FROM wrestler_list_scrape_data WHERE name = "Cade Hirstine"; -- 'Cade Hirstine' vs Cade Hirstine, Grandview, CO
SELECT name, team, wrestling_season, track_wrestling_category, grade FROM wrestler_list_scrape_data WHERE name LIKE "%Aaron Garcia%"; -- 'Cade Hirstine' vs Cade Hirstine, Grandview, CO

SELECT
	r.wrestler_name,
    r.school,
    r.rank,
    r.weight_lbs,
    MIN(l.wrestling_season),
    MIN(l.track_wrestling_category),
    MIN(l.name),
    MIN(l.wrestler_id),
    MIN(l.team),
    CASE 
        WHEN l.name IS NULL THEN 0
        ELSE 1
    END AS is_name_match,
    CASE
        WHEN MIN(l.name) IS NULL THEN NULL
        WHEN r.school NOT LIKE MIN(l.team) THEN 1
        ELSE 0
    END AS is_team_match,
    COUNT(*),
    SUM(CASE WHEN MIN(l.name) IS NOT NULL THEN 1 ELSE 0 END) OVER () AS matched_rows,
    SUM(CASE WHEN MIN(l.name) IS NULL THEN 1 ELSE 0 END) OVER () AS unmatched_rows,
    COUNT(*) OVER () AS total_row_count
FROM reference_wrestler_rankings_list AS r
	LEFT JOIN wrestler_list_scrape_data AS l ON r.wrestler_name = l.name
		AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1) -- removed the ", CO" from team name for comparsion in step 2
		AND l.wrestling_season = '2025-26'
		AND l.track_wrestling_category = 'High School Boys'
-- WHERE r.wrestler_name = 'Cade Hirstine'
GROUP BY 1, 2, 3, 4
HAVING is_name_match = 1
ORDER BY 1
;

SELECT
  l.id,
  l.name,
  l.team,
  l.wrestling_season,
  l.track_wrestling_category,

  r.onthemat_rank       AS preview_onthemat_rank,
  r.weight_lbs          AS preview_onthemat_weight_lbs,

  CASE WHEN r.wrestler_name IS NULL THEN 0 ELSE 1 END AS preview_is_name_match,
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
    MIN(`rank`) AS onthemat_rank,
    MIN(weight_lbs) AS weight_lbs
  FROM reference_wrestler_rankings_list
  GROUP BY wrestler_name, school
) r
  ON r.wrestler_name = l.name
  AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1)

WHERE l.wrestling_season = '2025-26'
  AND l.track_wrestling_category = 'High School Boys'
ORDER BY l.name
LIMIT 100;

SELECT
  COUNT(*) AS total,
  SUM(onthemat_rank IS NOT NULL) AS with_rank,
  SUM(onthemat_weight_lbs IS NOT NULL) AS with_weight
FROM wrestler_list_scrape_data
WHERE wrestling_season = '2025-26'
  AND track_wrestling_category = 'High School Boys'
;
  
  
SELECT
  name,
  GROUP_CONCAT(wrestler_id),
  GROUP_CONCAT(team),
  COUNT(*)
FROM wrestler_list_scrape_data
WHERE wrestling_season = '2025-26'
  AND track_wrestling_category = 'High School Boys'
  AND onthemat_rank IS NOT NULL
GROUP BY 1
;

SELECT
  *
FROM wrestler_list_scrape_data
WHERE wrestling_season = '2025-26'
  AND track_wrestling_category = 'High School Boys'
  AND name IN ('Noah Garcia-Salazar', 'Ryder Martyn', 'traysen Rosane')
GROUP BY 1
;

SELECT * FROM wrestler_match_history_metrics_data LIMIT 10;

SELECT
  *
FROM wrestler_match_history_metrics_data
WHERE wrestling_season = '2025-26'
  AND track_wrestling_category = 'High School Boys'
  AND wrestler_name IN ('Noah Garcia-Salazar', 'Ryder Martyn', 'traysen Rosane')
GROUP BY 1
;

SELECT
	r.wrestler_name,
    r.school,
    r.rank,
    r.weight_lbs,
	MIN(h.wrestling_season),
    MIN(h.track_wrestling_category),
    MIN(h.wrestler_name),
	MIN(h.wrestler_id),
	MIN(h.wrestler_team),
    CASE 
        WHEN h.wrestler_name IS NULL THEN 0
        ELSE 1
    END AS is_name_match,
    CASE
        WHEN MIN(h.wrestler_name) IS NULL THEN NULL
        WHEN r.school NOT LIKE MIN(h.wrestler_team) THEN 1
        ELSE 0
    END AS is_team_match,
    COUNT(*),
    SUM(CASE WHEN MIN(h.wrestler_name) IS NOT NULL THEN 1 ELSE 0 END) OVER () AS matched_rows,
    SUM(CASE WHEN MIN(h.wrestler_name) IS NULL THEN 1 ELSE 0 END) OVER () AS unmatched_rows,
    COUNT(*) OVER () AS total_row_count
FROM reference_wrestler_rankings_list AS r
	LEFT JOIN wrestler_match_history_metrics_data AS h ON r.wrestler_name = h.wrestler_name
		AND r.school LIKE SUBSTRING_INDEX(h.wrestler_team, ',', 1) -- removed the ", CO" from team name for comparsion in step 2
		AND h.wrestling_season = '2025-26'
		AND h.track_wrestling_category = 'High School Boys'
GROUP BY 1, 2, 3, 4
HAVING is_name_match = 1
ORDER BY 1
;

-- ///////////////
SELECT * FROM wrestler_list_scrape_data LIMIT 10;
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
--   FORMAT(COUNT(DISTINCT h.wrestler_id), 0) AS count_distinct_wrestler_id,
--   FORMAT(COUNT(*), 0)
FROM wrestler_match_history_metrics_data AS h
LEFT JOIN wrestler_list_scrape_data AS l
  ON h.wrestler_id = l.wrestler_id
WHERE h.wrestling_season = '2025-26'
  AND h.track_wrestling_category = 'High School Boys'
  AND l.onthemat_is_name_match = 1
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
ORDER BY 1
;

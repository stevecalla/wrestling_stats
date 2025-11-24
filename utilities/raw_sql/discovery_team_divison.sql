SELECT event, FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data GROUP BY 1 ORDER BY 1;
SELECT event, FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data WHERE event LIKE "%CHSAA%" GROUP BY 1 ORDER BY 1;

SELECT 
	wrestler_team, 
    wrestler_team_id, 
    MIN(weight_category), 
    event,
    CASE
		WHEN event LIKE "%1A%" THEN "1A"
		WHEN event LIKE "%2A%" THEN "2A"
		WHEN event LIKE "%3A%" THEN "3A"
		WHEN event LIKE "%4A%" THEN "4A"
		WHEN event LIKE "%5A%" THEN "5A"
        ELSE "unknown"
	END AS team_division,
    TRIM(SUBSTRING_INDEX(event, 'A', -1)) AS team_region,
    FORMAT(COUNT(*), 0) 
FROM wrestler_match_history_metrics_data 
WHERE event LIKE "%CHSAA%" AND event LIKE "%Region%" 
GROUP BY 1, 2, 4 
ORDER BY 1
;

SELECT * FROM wrestler_match_history_metrics_data WHERE event LIKE "%2025 CHSAA State Championships%" AND wrestler_team_id IN (1596842147, '1596839147') ORDER BY 1;
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestler_id IN (30622279132, 29827969132) ORDER BY wrestler_id, match_order;

SELECT 
	wrestling_season,
    track_wrestling_category,
	event, 
    wrestler_team, 
    wrestler_team_id,
    -- weight_category, 
    TRIM(SUBSTRING_INDEX(weight_category, '-', -1)) AS team_division,
    COUNT(DISTINCT wrestler_id) AS count_wrestlers_unique,
    FORMAT(COUNT(*), 0) AS count_records
FROM wrestler_match_history_metrics_data 
WHERE 1 = 1
	AND event LIKE "%2025 CHSAA State Championships%"
    AND wrestler_team_id IN (1596842147, '1596839147')
GROUP BY 1, 2, 3, 4, 5, 6
HAVING 1 = 1
    AND RIGHT(team_division, 1) = "A"
ORDER BY team_division, wrestler_team, count_wrestlers_unique DESC
-- LIMIT 10
;

DROP TABLE wrestler_team_division_reference;
-- CREATE TABLE wrestler_team_division_reference
WITH division AS (
SELECT 
	wrestling_season,
    track_wrestling_category,
	event, 
    wrestler_team, 
    wrestler_team_id,
    -- TRIM(SUBSTRING_INDEX(weight_category, '-', -1)) AS team_division,
    CASE
		WHEN event LIKE "%1A%" THEN "1A"
		WHEN event LIKE "%2A%" THEN "2A"
		WHEN event LIKE "%3A%" THEN "3A"
		WHEN event LIKE "%4A%" THEN "4A"
		WHEN event LIKE "%5A%" THEN "5A"
        ELSE "unknown"
	END AS team_division,
    TRIM(SUBSTRING_INDEX(event, 'A', -1)) AS team_region,
    COUNT(DISTINCT wrestler_id) AS count_wrestlers_unique,
    FORMAT(COUNT(*), 0) AS count_records
FROM wrestler_match_history_metrics_data 
WHERE 1 = 1
	AND event LIKE "%CHSAA%" AND event LIKE "%Region%"
	-- AND event LIKE "%2025 CHSAA State Championships%"
    -- AND weight_category NOT LIKE "%G%" -- odd records for 
GROUP BY 1, 2, 3, 4, 5, 6
HAVING 1 = 1
    AND RIGHT(team_division, 1) = "A"
ORDER BY team_division, wrestler_team, count_wrestlers_unique DESC
-- LIMIT 10
)
SELECT
	l.wrestling_season,
	l.track_wrestling_category,
	l.team AS wrestling_team,
    l.team_id AS wrestling_team_id,
	-- d.wrestler_team,
    -- d.wrestler_team_id,
    CASE WHEN d.event IS NULL THEN "no_regional_event" ELSE d.event END AS event,
    CASE WHEN d.event IS NULL THEN "unknown" ELSE d.event END AS team_division,
    CASE WHEN d.event IS NULL THEN "unknown" ELSE d.event END AS team_region
    
FROM wrestler_list_scrape_data AS l
	LEFT JOIN division AS D on d.wrestler_team_id = l.team_id
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1
;

SELECT * FROM wrestler_team_division_reference;
SELECT * FROM wrestler_team_division_reference WHERE wrestler_team_id = 1596622147;
SELECT * FROM wrestler_team_division_reference WHERE team_division = "unknown";
SELECT wrestler_team, COUNT(*) AS count FROM wrestler_team_division_reference GROUP BY 1 HAVING count >= 1 ORDER BY 1;
SELECT team, COUNT(*) AS count FROM wrestler_list_scrape_data GROUP BY team ORDER BY count DESC;

SELECT event, FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data GROUP BY 1 ORDER BY 1;
SELECT * FROM wrestler_match_history_metrics_data WHERE event LIKE "%CHSAA%" ORDER BY 1;

-- **************************
-- discovery query
-- **************************
SELECT 
	id,
    wrestling_season,
    track_wrestling_category,
	wrestler_name, 
    wrestler_id,
    outcome,
    event, 
    round, 
    weight_category, 
    CASE 
        WHEN wrestling_season LIKE "2024-25" THEN "state_qualifier_2024"
        ELSE "other"
    END AS is_state_tournament_qualifier,
    CASE
		WHEN wrestling_season LIKE "2024-25" and round LIKE "%1st Place Match%" AND outcome LIKE "W" THEN "1st_state_place_2025"
		WHEN wrestling_season LIKE "2024-25" and round LIKE "%1st Place Match%" AND outcome LIKE "L" THEN "2nd_state_place_2025"
		WHEN wrestling_season LIKE "2024-25" and round LIKE "%3rd Place Match%" AND outcome LIKE "W" THEN "3rd_state_place_2025"
		WHEN wrestling_season LIKE "2024-25" and round LIKE "%3rd Place Match%" AND outcome LIKE "L" THEN "4th_state_place_2025"
		WHEN wrestling_season LIKE "2024-25" and round LIKE "%5th Place Match%" AND outcome LIKE "W" THEN "5th_state_place_2025"
		WHEN wrestling_season LIKE "2024-25" and round LIKE "%5th Place Match%" AND outcome LIKE "L" THEN "6th_state_place_2025"
        WHEN wrestling_season LIKE "2024-25" THEN "other_2025"
        ELSE "tbd"
	END AS "state_tournment_place",
    raw_details, 
    winner_name, 
    wrestler_grade
FROM wrestler_match_history_metrics_data 
WHERE 1 = 1
	AND event LIKE "%State Championships%" 
    -- AND raw_details LIKE "%1st Place Match%" 
    -- AND raw_details LIKE "%Place Match%" 
    AND weight_category LIKE "%A%"
    -- AND (weight_category LIKE "%106-3A%" OR weight_category LIKE "%175-4A%")
    -- AND outcome = "U" -- "W", "L"
    -- AND wrestler_grade <> "HS Senior"
ORDER BY weight_category, round, winner_name, outcome DESC
;

-- **************************
-- cte to create reference of state qualifying wrestlers
-- **************************
-- DROP TABLE wrestler_state_qualifier_and_place_reference;
-- CREATE TABLE wrestler_state_qualifier_and_place_reference
WITH determine_state_qualifier_and_place AS (
    SELECT 
        id,
        wrestling_season,
        track_wrestling_category,
        wrestler_name, 
        wrestler_id,
        wrestler_first_name,
        wrestler_last_name,
        wrestler_grade,
        record,
        wins_all_run,
        losses_all_run,
        ties_all_run,
        total_matches,
        total_matches_win_pct,
        match_order,
        outcome,
        event, 
        round, 
        weight_category, 
        winner_name,
        CASE 
            WHEN wrestling_season = '2024-25' THEN 'state_qualifier_2025'
            ELSE 'other'
        END AS is_state_tournament_qualifier,
        CASE
            WHEN wrestling_season = '2024-25' AND round LIKE '%1st Place Match%' AND outcome = 'W' THEN '1st_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%1st Place Match%' AND outcome = 'L' THEN '2nd_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%3rd Place Match%' AND outcome = 'W' THEN '3rd_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%3rd Place Match%' AND outcome = 'L' THEN '4th_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%5th Place Match%' AND outcome = 'W' THEN '5th_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%5th Place Match%' AND outcome = 'L' THEN '6th_state_place_2025'
            WHEN wrestling_season = '2024-25' THEN 'other_2025'
            ELSE 'tbd'
        END AS state_tournament_place,
        raw_details
    FROM wrestler_match_history_metrics_data 
    WHERE 1 = 1
        AND event LIKE '%State Championships%' 
        AND weight_category LIKE '%A%'
        AND wrestler_last_name LIKE "%Le%"
),    
    -- For each wrestler, find their *final* state match,
    -- preferring placement matches if they exist
    max_match AS (
        SELECT
            wrestling_season,
            track_wrestling_category,
            wrestler_id,

            COALESCE(
            -- Prefer 1st Place Match if it exists
            MAX(CASE WHEN round LIKE '%1st Place Match%' THEN match_order END),
            -- Otherwise 3rd Place Match
            MAX(CASE WHEN round LIKE '%3rd Place Match%' THEN match_order END),
            -- Otherwise 5th Place Match
            MAX(CASE WHEN round LIKE '%5th Place Match%' THEN match_order END),
            -- Otherwise just use the highest match_order
            MAX(match_order)
            ) AS max_match_order,

            COUNT(DISTINCT wrestler_id) AS count_distinct,
            COUNT(*)         AS count_matches
        FROM determine_state_qualifier_and_place
        GROUP BY
            wrestling_season,
            track_wrestling_category,
            wrestler_id
    ),

    -- Keep only that final (max match_order) row per wrestler
    per_wrestler AS (
        SELECT
            d.*,
            m.count_distinct,
            m.count_matches
        FROM determine_state_qualifier_and_place d
        JOIN max_match m
        ON  d.wrestling_season       = m.wrestling_season
        AND d.track_wrestling_category = m.track_wrestling_category
        AND d.wrestler_id            = m.wrestler_id
        AND d.match_order            = m.max_match_order
    )

    SELECT
        p.wrestling_season,
        p.track_wrestling_category,

        p.wrestler_id,
        p.wrestler_name,
        p.wrestler_first_name,
        p.wrestler_last_name,
        p.wrestler_grade,
        p.weight_category,
        p.winner_name,

        p.record,
        p.wins_all_run,
        p.losses_all_run,
        p.ties_all_run,
        p.total_matches,
        p.total_matches_win_pct,

        -- final state match info (based on max match_order)
        p.event,
        p.round,
        p.outcome,

        p.is_state_tournament_qualifier,
        p.state_tournament_place            AS state_tournament_place_label,

        p.raw_details,

        -- how many state matches this wrestler had (for sanity/debug)
        p.count_distinct                AS count_distinct,
        p.count_matches                 AS count_matches

    FROM per_wrestler p
    WHERE 1 = 1
        -- AND state_tournament_place LIKE "%1st_state_place_2025%"
        -- AND (state_tournament_place LIKE "%2nd_state_place_2025%" OR state_tournament_place LIKE "%1st_state_place_2025%")
        -- AND wrestler_grade <> "HS Senior"
        -- AND wrestler_last_name LIKE "%Kenyon%"
    ORDER BY
        p.weight_category,
        p.winner_name,
        p.wrestler_last_name,
        p.wrestler_first_name
;

-- **************************
-- validate record for new table
-- **************************
SELECT * FROM wrestler_state_qualifier_and_place_reference LIMIT 10;
SELECT MAX(created_at_mtn), MAX(updated_at_mtn) FROM wrestler_state_qualifier_and_place_reference LIMIT 10;
SELECT * FROM wrestler_state_qualifier_and_place_reference WHERE 1 = 1 AND (state_tournament_place LIKE "%3rd_state_place_2025%" OR state_tournament_place LIKE "%4th_state_place_2025%") ORDER BY weight_category, winner_name LIMIT 100;
SELECT winner_name, COUNT(*) AS count FROM wrestler_state_qualifier_and_place_reference WHERE 1 = 1 AND (state_tournament_place LIKE "%3rd_state_place_2025%" OR state_tournament_place LIKE "%4th_state_place_2025%") GROUP BY 1 ORDER BY count, winner_name LIMIT 100;
SELECT state_tournament_place, COUNT(*) AS count FROM wrestler_state_qualifier_and_place_reference GROUP BY 1 WITH ROLLUP ORDER BY 1;

-- **************************
-- counts by place
-- **************************
SELECT state_tournament_place, COUNT(*) AS count FROM wrestler_state_qualifier_and_place_reference WHERE wrestler_grade <> "HS Senior" GROUP BY 1 WITH ROLLUP ORDER BY 1;

-- **************************
-- counts by place by grade
-- **************************
SELECT
    state_tournament_place,
    SUM(CASE WHEN wrestler_grade = 'HS Freshman'  THEN 1 ELSE 0 END) AS freshman,
    SUM(CASE WHEN wrestler_grade = 'HS Sophomore' THEN 1 ELSE 0 END) AS sophomore,
    SUM(CASE WHEN wrestler_grade = 'HS Junior'    THEN 1 ELSE 0 END) AS junior,
    SUM(CASE WHEN wrestler_grade = 'HS Senior'    THEN 1 ELSE 0 END) AS senior,
    COUNT(*) AS total
FROM wrestler_state_qualifier_and_place_reference
GROUP BY state_tournament_place WITH ROLLUP
ORDER BY state_tournament_place
;

-- **************************
-- check append to match history table
-- **************************
SELECT * FROM wrestler_match_history_metrics_data LIMIT 10;
-- check the counts
SELECT
    wrestler_state_tournament_place,

    COUNT(DISTINCT CASE WHEN wrestler_grade = 'HS Freshman'  THEN wrestler_id END) AS freshman,
    COUNT(DISTINCT CASE WHEN wrestler_grade = 'HS Sophomore' THEN wrestler_id END) AS sophomore,
    COUNT(DISTINCT CASE WHEN wrestler_grade = 'HS Junior'    THEN wrestler_id END) AS junior,
    COUNT(DISTINCT CASE WHEN wrestler_grade = 'HS Senior'    THEN wrestler_id END) AS senior,

    COUNT(DISTINCT wrestler_id) AS total

FROM wrestler_match_history_metrics_data
WHERE wrestler_state_tournament_place IS NOT NULL
GROUP BY wrestler_state_tournament_place WITH ROLLUP
ORDER BY wrestler_state_tournament_place
;

SELECT
    opponent_state_tournament_place,

    COUNT(DISTINCT CASE WHEN opponent_grade = 'HS Freshman'  THEN opponent_id END) AS freshman,
    COUNT(DISTINCT CASE WHEN opponent_grade = 'HS Sophomore' THEN opponent_id END) AS sophomore,
    COUNT(DISTINCT CASE WHEN opponent_grade = 'HS Junior'    THEN opponent_id END) AS junior,
    COUNT(DISTINCT CASE WHEN opponent_grade = 'HS Senior'    THEN opponent_id END) AS senior,

    COUNT(DISTINCT opponent_id) AS total

FROM wrestler_match_history_metrics_data
WHERE opponent_state_tournament_place IS NOT NULL
GROUP BY opponent_state_tournament_place WITH ROLLUP
ORDER BY opponent_state_tournament_place
;

-- ALTER TABLE wrestler_match_history_metrics_data
--   DROP COLUMN wrestler_is_state_tournament_qualifier,
--   DROP COLUMN wrestler_state_tournament_place,
--   DROP COLUMN opponent_is_state_tournament_qualifier,
--   DROP COLUMN opponent_state_tournament_place
-- ;

-- **************************
-- check append to wrestler list table
-- **************************
SELECT * FROM wrestler_list_scrape_data LIMIT 10;
SELECT * FROM wrestler_list_scrape_data WHERE wrestler_state_tournament_place IS NOT NULL LIMIT 10;
-- check the counts
SELECT
    wrestler_state_tournament_place,

    COUNT(DISTINCT CASE WHEN grade = 'HS Freshman'  THEN wrestler_id END) AS freshman,
    COUNT(DISTINCT CASE WHEN grade = 'HS Sophomore' THEN wrestler_id END) AS sophomore,
    COUNT(DISTINCT CASE WHEN grade = 'HS Junior'    THEN wrestler_id END) AS junior,
    COUNT(DISTINCT CASE WHEN grade = 'HS Senior'    THEN wrestler_id END) AS senior,

    COUNT(DISTINCT wrestler_id) AS total

FROM wrestler_list_scrape_data
WHERE wrestler_state_tournament_place IS NOT NULL
GROUP BY wrestler_state_tournament_place WITH ROLLUP
ORDER BY wrestler_state_tournament_place
;

-- ALTER TABLE wrestler_list_scrape_data
--   DROP COLUMN wrestler_is_state_tournament_qualifier,
--   DROP COLUMN wrestler_state_tournament_place
-- ;


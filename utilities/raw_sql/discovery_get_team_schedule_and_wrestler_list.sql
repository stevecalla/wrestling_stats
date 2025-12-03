SELECT * FROM wrestler_team_
division_reference WHERE wrestling_season LIKE "2025-26" AND track_wrestling_category LIKE "High School Boys" LIMIT 10;

SELECT * FROM team_schedule_scrape_data WHERE wrestling_season LIKE "2025-26" AND track_wrestling_category LIKE "High School Boys" LIMIT 10;
SELECT * FROM team_schedule_scrape_data WHERE wrestling_season LIKE "2025-26" AND track_wrestling_category LIKE "High School Boys" AND team_id IS NULL LIMIT 10;
SELECT FORMAT(COUNT(*), 0) FROM team_schedule_scrape_data WHERE wrestling_season LIKE "2025-26" AND track_wrestling_category LIKE "High School Boys" AND team_id IS NULL;
SELECT team_name_raw, team_id, track_wrestling_category, FORMAT(COUNT(DISTINCT team_name_raw), 0) AS count FROM team_schedule_scrape_data GROUP BY team_name_raw, 2, 3 ORDER BY 3, 1 ASC;
SELECT team_name_raw, team_id, track_wrestling_category, event_name, FORMAT(COUNT(DISTINCT team_name_raw), 0) AS count FROM team_schedule_scrape_data GROUP BY team_name_raw, 2, 3, 4 ORDER BY 3, 1 ASC;

SELECT 
	-- *
	start_date,
    team_name_raw,
    team_id, 
    
    event_name
FROM team_schedule_scrape_data
WHERE wrestling_season = '2025-26'
  AND track_wrestling_category = 'High School Boys'
  -- AND track_wrestling_category = 'High School Girls'
  AND start_date IN (
        CURDATE()               -- today
        -- DATE_SUB(CURDATE(), INTERVAL 1 DAY)  -- yesterday
        -- , DATE_ADD(CURDATE(), INTERVAL 1 DAY)  -- tomorrow
      )
ORDER BY start_date, event_name
-- LIMIT 20
;

SELECT * FROM wrestler_list_scrape_data LIMIT 10;
SELECT wrestling_season, track_wrestling_category, team, team_id FROM wrestler_list_scrape_data LIMIT 10;


-- retrieves events from yesterday & today
WITH recent_events AS (
  SELECT 
      ts.start_date,
      ts.team_name_raw,
      ts.team_id,
      ts.event_name,
      ts.wrestling_season,
      ts.track_wrestling_category
  FROM team_schedule_scrape_data ts
  WHERE 1 = 1
	AND ts.wrestling_season = '2025-26'
    AND ts.track_wrestling_category = 'High School Boys'
    -- AND ts.track_wrestling_category = 'High School Girls'
    AND ts.start_date IN (
          CURDATE(),						          -- today
          DATE_SUB(CURDATE(), INTERVAL 1 DAY)  		  -- yesterday
          -- , DATE_ADD(CURDATE(), INTERVAL 1 DAY)    -- tomorrow
        )
)

SELECT 
    -- re.start_date,
    -- re.team_name_raw,
    -- re.team_id        AS event_team_id,
    -- re.event_name,

    -- w.wrestling_season,
    -- w.track_wrestling_category,
    -- w.wrestler_id,
    -- w.name            AS wrestler_name,
    -- w.team            AS wrestler_team_name,
    -- w.team_id         AS wrestler_team_id,
    -- w.name_link

    w.id,
    w.name_link

FROM recent_events re

LEFT JOIN wrestler_list_scrape_data w
  ON w.wrestling_season         = re.wrestling_season
     AND w.track_wrestling_category = re.track_wrestling_category
     AND (
          -- 1) primary: match on team_id when present
          (re.team_id IS NOT NULL AND w.team_id = re.team_id)
          -- 2) fallback: match on team name when event.team_id is NULL
          OR (re.team_id IS NULL AND w.team = re.team_name_raw)
         )
WHERE 1 = 1
	AND w.name_link IS NOT NULL AND w.name_link <> ''
ORDER BY
    re.start_date,
    re.event_name,
    
    w.name
;

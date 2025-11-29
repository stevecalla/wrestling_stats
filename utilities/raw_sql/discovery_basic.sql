USE wrestling_stats;
-- DROP TABLE `wrestler_list_scrape_data`;
-- DROP TABLE `wrestler_match_history_scrape_data`;

-- ============================
-- wrestler list
-- ============================
SELECT * FROM wrestler_list_scrape_data ORDER BY id ASC LIMIT 60;
SELECT MAX(updated_at_mtn) FROM wrestler_list_scrape_data LIMIT 60;
SELECT DATE_FORMAT(created_at_mtn, '%Y-%m-%d'), DATE_FORMAT(updated_at_mtn, '%Y-%m-%d'), FORMAT(COUNT(*), 0) FROM wrestler_list_scrape_data  GROUP BY 1, 2 WITH ROLLUP;
SELECT wrestling_season, track_wrestling_category, FORMAT(COUNT(*), 0) FROM wrestler_list_scrape_data  GROUP BY 1, 2 WITH ROLLUP;

SELECT * FROM wrestler_list_scrape_data WHERE wrestling_season LIKE '2025-26' ORDER BY id ASC LIMIT 60;
SELECT last_name, name, team, COUNT(*) FROM wrestler_list_scrape_data WHERE wrestling_season LIKE '2025-26' GROUP BY 1, 2, 3 ORDER BY last_name ASC;

SELECT * FROM wrestler_list_scrape_data  WHERE DATE_FORMAT(updated_at_mtn, '%Y-%m-%d') = '2025-11-18';
SELECT "query count records" AS query_label, FORMAT(COUNT(DISTINCT wrestler_id), 0), FORMAT(COUNT(*), 0) FROM wrestler_list_scrape_data; -- COUNT RECORDS
SELECT "query duplicate id check" AS query_label, wrestler_id, FORMAT(COUNT(*), 0) AS COUNT FROM wrestler_list_scrape_data GROUP BY 1, 2 HAVING COUNT > 1; -- CHECK FOR DUPLICATES

SELECT * FROM wrestler_list_scrape_data WHERE gender IN ("F") ORDER BY name; -- CHECK FOR GIRLS
SELECT * FROM wrestler_list_scrape_data WHERE grade LIKE "%Senior%" ORDER BY id; -- CHECK FOR GIRLS
SELECT * FROM wrestler_list_scrape_data WHERE wrestler_id IN ("30579778132");
-- ============================
-- wrestler match history
-- ============================
SELECT * FROM wrestler_match_history_scrape_data LIMIT 10;
SELECT MAX(updated_at_mtn), FORMAT(COUNT(*), 0) FROM wrestler_match_history_scrape_data GROUP BY 1 LIMIT 10;
SELECT DATE_FORMAT(updated_at_mtn, '%Y-%m-%d'), FORMAT(COUNT(*), 0) FROM wrestler_match_history_scrape_data GROUP BY 1 WITH ROLLUP;
SELECT * FROM wrestler_match_history_scrape_data WHERE updated_at_mtn = MAX(updated_at_mtn) LIMIT 10;

SELECT "query count records" AS query_label, FORMAT(COUNT(DISTINCT wrestler_id), 0), FORMAT(COUNT(*), 0) FROM wrestler_match_history_scrape_data; -- COUNT RECORDS
SELECT "query duplicate id check" AS query_label, wrestler_id, FORMAT(COUNT(*), 0) AS COUNT FROM wrestler_match_history_scrape_data GROUP BY 1, 2 HAVING COUNT > 1; -- CHECK FOR DUPLICATES

SELECT * FROM wrestler_match_history_scrape_data WHERE wrestler_id IN ('29790065132') ORDER BY id, start_date;
SELECT * FROM wrestler_match_history_scrape_data WHERE id IN (189, 239, 399);
SELECT wrestler, wrestler_school, opponent, opponent_school, opponent_id, raw_details FROM wrestler_match_history_scrape_data history;

-- Normalize to compare “logical” duplicates regardless of NULL/unknown/case/space
WITH normalized AS (
  SELECT
    id,
    wrestler_id,
    COALESCE(DATE_FORMAT(start_date, '%Y-%m-%d'), '0000-00-00') AS n_start_date,
    LOWER(TRIM(COALESCE(NULLIF(event, 'unknown'), '')))  AS n_event,
    -- LOWER(TRIM(COALESCE(NULLIF(round, 'unknown'), '')))  AS n_round,
    LOWER(TRIM(COALESCE(NULLIF(opponent_id, 'unknown'), ''))) AS n_opponent_id,
    LOWER(TRIM(COALESCE(NULLIF(raw_details, 'unknown'), ''))) AS n_raw_details
  FROM wrestler_match_history_scrape_data
)
SELECT 
  wrestler_id, n_start_date, n_event, n_opponent_id, n_raw_details,
  COUNT(*) AS ct
FROM normalized
GROUP BY 1,2,3,4,5
HAVING ct > 1
ORDER BY ct DESC, wrestler_id
LIMIT 50;

-- ============================
-- wrestler match history
-- ============================
SELECT * FROM wrestler_list_scrape_data_2024_2025_boys_backup LIMIT 10;
SELECT "query count records" AS query_label, FORMAT(COUNT(DISTINCT wrestler_id), 0), FORMAT(COUNT(*), 0) FROM wrestler_list_scrape_data_2024_2025_boys_backup; -- COUNT RECORDS

-- ============================
-- wrestler match history
-- ============================
SELECT * FROM wrestler_match_history_2024_2025_boys_all ORDER BY id DESC LIMIT 10;
SELECT MAX(updated_at_mtn) FROM wrestler_match_history_2024_2025_boys_all LIMIT 10;
SELECT "query count records" AS query_label, FORMAT(COUNT(DISTINCT wrestler_id), 0), FORMAT(COUNT(*), 0) FROM wrestler_match_history_2024_2025_boys_all; -- COUNT RECORDS

-- ============================
-- add match_order to wrestler_match_history_2024_2025_boys_all
-- ============================
ALTER TABLE wrestler_match_history_2024_2025_boys_all
  ADD COLUMN match_order INT UNSIGNED NULL
  AFTER round;
  
UPDATE wrestler_match_history_2024_2025_boys_all t
JOIN (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY wrestler_id
      ORDER BY id
    ) AS match_order
  FROM wrestler_match_history_2024_2025_boys_all
) AS x
  USING (id)
SET t.match_order = x.match_order;

-- ============================
-- wrestler match history
-- ============================
SELECT * FROM wrestler_match_history_metrics_data ORDER BY id DESC LIMIT 10;
SELECT MAX(updated_at_mtn) FROM wrestler_match_history_metrics_data LIMIT 10;
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestler_last_name LIKE "%salazar%" AND wrestler_first_name LIKE "%matthew%" ORDER BY match_order LIMIT 100;
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestler_id = '30213719132' ORDER BY match_order;
SELECT "query count records" AS query_label, FORMAT(COUNT(DISTINCT wrestler_id), 0), FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data; -- COUNT RECORDS






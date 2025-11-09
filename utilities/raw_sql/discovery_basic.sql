USE wrestling_stats;
-- DROP TABLE `wrestler_list_scrape_data`;
-- DROP TABLE `wrestler_match_history_scrape_data`;

SELECT * FROM wrestler_list_scrape_data ORDER BY id LIMIT 60;
SELECT "query count records" AS query_label, FORMAT(COUNT(DISTINCT wrestler_id), 0), FORMAT(COUNT(*), 0) FROM wrestler_list_scrape_data; -- COUNT RECORDS
SELECT "query duplicate id check" AS query_label, wrestler_id, FORMAT(COUNT(*), 0) AS COUNT FROM wrestler_list_scrape_data GROUP BY 1, 2 HAVING COUNT > 1; -- CHECK FOR DUPLICATES
SELECT * FROM wrestler_list_scrape_data WHERE gender IN ("F") ORDER BY name; -- CHECK FOR GIRLS
SELECT * FROM wrestler_list_scrape_data WHERE wrestler_id IN ("30579778132");

SELECT * FROM wrestler_match_history_scrape_data LIMIT 10;
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
    LOWER(TRIM(COALESCE(NULLIF(round, 'unknown'), '')))  AS n_round,
    LOWER(TRIM(COALESCE(NULLIF(opponent, 'unknown'), ''))) AS n_opponent,
    LOWER(TRIM(COALESCE(NULLIF(raw_details, 'unknown'), ''))) AS n_raw_details
  FROM wrestler_match_history_scrape_data
)
SELECT 
  wrestler_id, n_start_date, n_event, n_round, n_opponent, n_raw_details,
  COUNT(*) AS ct
FROM normalized
GROUP BY 1,2,3,4,5,6
HAVING ct > 1
ORDER BY ct DESC, wrestler_id
LIMIT 50;



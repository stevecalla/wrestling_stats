USE wrestling_stats;
-- DROP TABLE wrestler_match_history_scrape_tasks;

SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_scrape_tasks LIMIT 10;
SELECT
  task_set_id,
  SUM(status='Done')   AS done_count,
  SUM(status='Locked') AS locked_count,
  SUM(status='Failed') AS failed_count,
  SUM(status='Pending') AS pending_count,
  COUNT(*) AS total_count,
  MIN(updated_at_mtn) AS min_updated_at_mtn,
  MAX(updated_at_mtn) AS max_updated_at_mtn,
    -- duration between min/max in HH:MM:SS
    SEC_TO_TIME(
        TIMESTAMPDIFF(
            SECOND,
            MIN(updated_at_mtn),
            MAX(updated_at_mtn)
        )
    ) AS duration_hh_mm_ss
FROM wrestler_match_history_scrape_tasks
GROUP BY task_set_id WITH ROLLUP
ORDER BY 1 DESC;

SELECT
  task_set_id,
  CASE
  WHEN locked_by IS NOT NULL
  THEN SUBSTRING_INDEX(locked_by, 'port=', -1)
  ELSE NULL
END AS locked_port,
SUBSTRING_INDEX(
  SUBSTRING_INDEX(locked_by, 'pid=', -1),
  '|',
  1
) AS locked_pid,
  SUM(status='Done')   AS done_count,
  SUM(status='Locked') AS locked_count,
  SUM(status='Failed') AS failed_count,
  SUM(status='Pending') AS pending_count,
  COUNT(*) AS total_count,
     -- duration between min/max in HH:MM:SS
    SEC_TO_TIME(
        TIMESTAMPDIFF(
            SECOND,
            MIN(updated_at_mtn),
            
            MAX(updated_at_mtn)
        )
    ) AS duration_hh_mm_ss, 
  locked_by,
  MIN(updated_at_mtn) AS min_updated_at_mtn,
  MAX(updated_at_mtn) AS max_updated_at_mtn,
  MAX(updated_at_utc) AS max_updated_at_utc
FROM wrestler_match_history_scrape_tasks
GROUP BY task_set_id, locked_by WITH ROLLUP
ORDER BY 1, 2 ASC;

SELECT * FROM wrestler_match_history_scrape_tasks LIMIT 10;
SELECT * FROM wrestler_match_history_scrape_tasks WHERE last_error IS NOT NULL LIMIT 10;
SELECT DISTINCT last_error, COUNT(*) FROM wrestler_match_history_scrape_tasks WHERE last_error IS NOT NULL GROUP BY 1 LIMIT 10;
SELECT * FROM wrestler_match_history_scrape_tasks WHERE updated_at_mtn = '2026-01-10 13:26:25' LIMIT 10;

SELECT * FROM wrestler_match_history_scrape_data WHERE wrestler_id = 35021874132;
SELECT * FROM wrestler_match_history_scrape_data WHERE wrestler_id = 35021875132;

-- SELECT
--   @@global.time_zone  AS global_tz,
--   @@session.time_zone AS session_tz,
--   NOW()               AS now_now,
--   CURRENT_TIMESTAMP() AS now_current_ts,
--   UTC_TIMESTAMP()     AS now_utc;
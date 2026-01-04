USE wrestling_stats;
-- DROP TABLE wrestler_match_history_scrape_tasks;

SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_scrape_tasks;
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
  locked_by,
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
GROUP BY task_set_id, locked_by WITH ROLLUP
ORDER BY 1, 2 ASC;

SELECT * FROM wrestler_match_history_scrape_tasks WHERE last_error IS NOT NULL LIMIT 10;

SELECT * FROM wrestler_match_history_scrape_data WHERE wrestler_id = 35021874132;
SELECT * FROM wrestler_match_history_scrape_data WHERE wrestler_id = 35021875132;

-- SELECT
--   @@global.time_zone  AS global_tz,
--   @@session.time_zone AS session_tz,
--   NOW()               AS now_now,
--   CURRENT_TIMESTAMP() AS now_current_ts,
--   UTC_TIMESTAMP()     AS now_utc;

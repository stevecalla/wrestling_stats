USE wrestling_stats;
show tables;
SELECT * FROM wrestler_match_history_wrestler_ids_data;
SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_wrestler_ids_data;

SELECT * FROM wrestler_match_history_scrape_data;
SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_scrape_data;
SELECT * FROM wrestler_match_history_scrape_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Boys";
SELECT * FROM wrestler_match_history_scrape_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Girls";
SELECT id, COUNT(*) AS count FROM wrestler_match_history_scrape_data GROUP BY 1 HAVING count > 1 ORDER BY 1;

SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data;
SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Boys";
SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Girls";

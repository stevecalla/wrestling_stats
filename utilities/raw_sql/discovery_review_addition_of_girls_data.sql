DROP TABLE IF EXISTS test_wrestler_table_win;
CREATE TABLE test_wrestler_table_win LIKE `reference_team_alias_map`;
INSERT INTO test_wrestler_table_win SELECT * FROM `reference_team_alias_map`;

SELECT * FROM test_wrestler_table_win;

SELECT * FROM test_wrestler_table_mac;

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

SELECT "1_" AS query_lable, r.* FROM reference_team_alias_map AS r;
SELECT "2_" AS query_lable, r.* FROM reference_wrestler_2026_state_qualifier_flags AS r;
SELECT "3_" AS query_lable, r.* FROM reference_wrestler_2026_team_division_flags AS r;
SELECT "4_" AS query_lable, r.* FROM reference_wrestler_cross_season_summary AS r;
SELECT "5_" AS query_lable, r.* FROM wrestler_list_scrape_data AS r;
SELECT "6_" AS query_label, r.* FROM wrestler_list_scrape_data_2024_2025_boys_backup AS r;
SELECT "7_" AS query_label, r.* FROM wrestler_match_history_2024_2025_boys_all AS r;
SELECT "8_" AS query_label, r.* FROM wrestler_match_history_metrics_data AS r;
SELECT "9_" AS query_label, r.* FROM wrestler_match_history_scrape_data AS r;
SELECT "10_" AS query_label, r.* FROM wrestler_match_history_wrestler_ids_data AS r;
SELECT "11_" AS query_label, r.* FROM wrestler_state_qualifier_and_place_reference AS r;
SELECT "12_" AS query_label, r.* FROM wrestler_team_division_reference AS r;


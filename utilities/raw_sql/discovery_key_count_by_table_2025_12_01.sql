USE wrestling_stats;
show tables;
-- SELECT * FROM wrestler_match_history_wrestler_ids_data;
-- SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_wrestler_ids_data;

-- SELECT * FROM wrestler_match_history_scrape_data;
-- SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_scrape_data;
-- SELECT * FROM wrestler_match_history_scrape_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Boys";
-- SELECT * FROM wrestler_match_history_scrape_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Girls";
-- SELECT id, COUNT(*) AS count FROM wrestler_match_history_scrape_data GROUP BY 1 HAVING count > 1 ORDER BY 1;

-- SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data;
-- SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Boys";
-- SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data WHERE wrestling_season LIKE "2024-25" AND track_wrestling_category LIKE "High School Girls";

SELECT "1_team_alias_map" AS query_label, r.* FROM reference_team_alias_map AS r LIMIT 10;
SELECT "2_state_flags" AS query_label, r.* FROM reference_wrestler_2026_state_qualifier_flags AS r LIMIT 10;
SELECT "3_team_flags" AS query_label, r.* FROM reference_wrestler_2026_team_division_flags AS r LIMIT 10;
SELECT "4_cross_season_summary" AS query_label, r.* FROM reference_wrestler_cross_season_summary AS r LIMIT 10;
SELECT "5_list_scape" AS query_label, r.* FROM wrestler_list_scrape_data AS r LIMIT 10;
SELECT "6_2025_list_backup" AS query_label, r.* FROM wrestler_list_scrape_data_2024_2025_boys_backup AS r LIMIT 10;
SELECT "7_2025_boys_match_history_backup" AS query_label, r.* FROM wrestler_match_history_2024_2025_boys_all AS r LIMIT 10;
SELECT "8_match_metrics" AS query_label, r.* FROM wrestler_match_history_metrics_data AS r WHERE wrestling_season LIKE "2025-26" AND track_wrestling_category LIKE "High School Boys" LIMIT 10;
SELECT "9_match_scrape" AS query_label, r.* FROM wrestler_match_history_scrape_data AS r WHERE wrestling_season LIKE "2025-26" AND track_wrestling_category LIKE "High School Boys" LIMIT 150;
SELECT "10_wrestler_ids" AS query_label, r.* FROM wrestler_match_history_wrestler_ids_data AS r  WHERE wrestling_season LIKE "2025-26" AND track_wrestling_category LIKE "High School Boys" LIMIT 10000;
SELECT "11_state_qualifier_reference" AS query_label, r.* FROM wrestler_state_qualifier_and_place_reference AS r LIMIT 10;
SELECT "12_team_reference" AS query_label, r.* FROM wrestler_team_division_reference AS r LIMIT 10;
SELECT "13_team_schedule_scrape" AS query_label, r.* FROM team_schedule_scrape_data AS r LIMIT 10;
SELECT '14_reference_wrestler_rankings_list' AS query_label, r.* FROM reference_wrestler_rankings_list AS r LIMIT 10;

SELECT '1_team_alias_map' AS table_name, FORMAT(COUNT(0), 0) AS formatted_count FROM reference_team_alias_map
UNION ALL SELECT '2_state_flags', FORMAT(COUNT(0), 0) FROM reference_wrestler_2026_state_qualifier_flags
UNION ALL SELECT '3_team_flags', FORMAT(COUNT(0), 0) FROM reference_wrestler_2026_team_division_flags
UNION ALL SELECT '4_cross_season_summary', FORMAT(COUNT(0), 0) FROM reference_wrestler_cross_season_summary
UNION ALL SELECT '5_list_scrape', FORMAT(COUNT(0), 0) FROM wrestler_list_scrape_data
UNION ALL SELECT '6_2025_list_backup', FORMAT(COUNT(0), 0) FROM wrestler_list_scrape_data_2024_2025_boys_backup
UNION ALL SELECT '7_2025_boys_match_history_backup', FORMAT(COUNT(0), 0) FROM wrestler_match_history_2024_2025_boys_all
UNION ALL SELECT '8_match_metrics', FORMAT(COUNT(0), 0) FROM wrestler_match_history_metrics_data
UNION ALL SELECT '9_match_scrape', FORMAT(COUNT(0), 0) FROM wrestler_match_history_scrape_data
UNION ALL SELECT '10_wrestler_ids', FORMAT(COUNT(0), 0) FROM wrestler_match_history_wrestler_ids_data
UNION ALL SELECT '11_state_qualifier_reference', FORMAT(COUNT(0), 0) FROM wrestler_state_qualifier_and_place_reference
UNION ALL SELECT '12_team_reference', FORMAT(COUNT(0), 0) FROM wrestler_team_division_reference
UNION ALL SELECT "13_team_schedule_scrape" AS query_label, FORMAT(COUNT(0), 0) FROM team_schedule_scrape_data
UNION ALL SELECT '14_reference_wrestler_rankings_list' AS query_label, FORMAT(COUNT(0), 0) FROM reference_wrestler_rankings_list
LIMIT 20;

SELECT '1_team_alias_map' AS table_name,
  "" AS hs_boys_2024_25,
  "" AS hs_boys_2025_26,
  "" AS hs_girls_2024_25,
  "" AS hs_girls_2025_26,
  FORMAT(COUNT(*), 0) AS count_total,
  "no categories" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM reference_team_alias_map
UNION ALL
SELECT '2_state_flags' AS table_name,
  "" AS hs_boys_2024_25,
  "" AS hs_boys_2025_26,
  "" AS hs_girls_2024_25,
  "" AS hs_girls_2025_26,
  FORMAT(COUNT(*), 0) AS count_total,
  "no categories" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM reference_wrestler_2026_state_qualifier_flags
UNION ALL
SELECT '3_team_flags' AS table_name,
  "" AS hs_boys_2024_25,
  "" AS hs_boys_2025_26,
  "" AS hs_girls_2024_25,
  "" AS hs_girls_2025_26,
  FORMAT(COUNT(*), 0) AS count_total,
  "no categories" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM reference_wrestler_2026_team_division_flags
UNION ALL
SELECT '4_cross_season_summary' AS table_name,
  FORMAT(SUM(CASE WHEN seasons_present LIKE '2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2024_25,
  FORMAT(SUM(CASE WHEN seasons_present LIKE '2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2025_26,
  FORMAT(SUM(CASE WHEN seasons_present LIKE '2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2024_25,
  FORMAT(SUM(CASE WHEN seasons_present LIKE '2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2025_26,
  FORMAT(COUNT(*), 0) AS count_total,
  "using seasons_present" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM reference_wrestler_cross_season_summary
UNION ALL
SELECT '5_list_scrape' AS table_name,
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2024_25,
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2025_26,
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2024_25,
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2025_26,
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_list_scrape_data
UNION ALL
SELECT '6_2025_list_backup' AS table_name,
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2024_25,
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2025_26,
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2024_25,
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2025_26,
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_list_scrape_data_2024_2025_boys_backup
UNION ALL
SELECT '7_2025_boys_match_history_backup' AS table_name,
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2024_25,
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0) AS hs_boys_2025_26,
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2024_25,
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0) AS hs_girls_2025_26,
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_match_history_2024_2025_boys_all
UNION ALL
SELECT '8_match_metrics',
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_match_history_metrics_data
UNION ALL
SELECT '9_match_scrape',
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_match_history_scrape_data
UNION ALL
SELECT '10_wrestler_ids',
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_match_history_wrestler_ids_data
UNION ALL
SELECT '11_state_qualifier_reference',
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_state_qualifier_and_place_reference
UNION ALL
SELECT '12_team_reference',
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM wrestler_team_division_reference
UNION ALL
SELECT '13_team_schedule_scrape',
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM team_schedule_scrape_data
UNION ALL
SELECT '14_reference_wrestler_rankings_list',
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Boys'  THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2024-25' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(SUM(CASE WHEN wrestling_season='2025-26' AND track_wrestling_category='High School Girls' THEN 1 ELSE 0 END),0),
  FORMAT(COUNT(*), 0) AS count_total,
  "" AS note,
  now() AS created_at_mtn,
  utc_timestamp() AS created_at_utc
FROM reference_wrestler_rankings_list
;




SELECT 
	wrestling_season,
    track_wrestling_category,
    governing_body,
    wrestler_id
FROM wrestling_stats.wrestler_list_scrape_data
WHERE 1 = 1
	AND governing_body = "Colorado High School Activities Association"
	AND wrestling_season = "2024-25"
    AND track_wrestling_category = "High School Boys"
ORDER BY wrestler_id
LIMIT 10
;

SELECT * FROM wrestler_match_history_wrestler_ids_data LIMIT 10;
SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_wrestler_ids_data;

SELECT * FROM wrestler_match_history_metrics_data LIMIT 10;
SELECT FORMAT(COUNT(*), 0) FROM wrestler_match_history_metrics_data;

SELECT * FROM wrestler_list_scrape_data WHERE wrestler_id = 30058988132 LIMIT 10; -- 'Von Yoshimura'
SELECT * FROM wrestler_match_history_metrics_data WHERE wrestler_id = '30058988132'; -- 'Von Yoshimura'

SELECT opponent_team, opponent_team_id FROM wrestler_match_history_metrics_data WHERE id IN (117450, 23331, 113863, 56663); -- aj opponent team name

SELECT score_details, raw_details FROM wrestler_match_history_metrics_data WHERE id IN (48530, 19373, 134027, 114358); -- score detail = team name
SELECT score_details, raw_details FROM wrestler_match_history_metrics_data WHERE score_details = "vs. no winner info";
SELECT score_details, raw_details FROM wrestler_match_history_metrics_data WHERE raw_details LIKE "% vs. %";

SELECT * FROM wrestler_match_history_metrics_data WHERE score_details = "Bye";
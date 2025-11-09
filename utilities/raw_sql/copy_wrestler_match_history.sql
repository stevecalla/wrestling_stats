-- 1️⃣ Create a new table with identical structure (includes indexes + AUTO_INCREMENT)
CREATE TABLE wrestler_match_history_2024_2025_boys_all LIKE wrestler_match_history;

-- 2️⃣ Copy all rows into it
INSERT INTO wrestler_match_history_2024_2025_boys_all SELECT * FROM wrestler_match_history;

SHOW TABLES LIKE 'wrestler_match_history%';
SHOW CREATE TABLE wrestler_match_history_copy;
SELECT COUNT(*) AS row_count_copy FROM wrestler_match_history_copy;
SELECT COUNT(*) AS row_count_original FROM wrestler_match_history;


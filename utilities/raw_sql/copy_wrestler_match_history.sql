-- ================================
-- CREATE BACKUP OF WRESTLER MATCH HISTORY TABLE
-- =================================
-- 1️ Create a new table with identical structure (includes indexes + AUTO_INCREMENT)
-- CREATE TABLE wrestler_match_history_2024_2025_boys_all LIKE wrestler_match_history_scrape_data;

-- -- 2️ Copy all rows into it
-- INSERT INTO wrestler_match_history_2024_2025_boys_all SELECT * FROM wrestler_match_history_scrape_data;

-- SHOW TABLES LIKE 'wrestler_match_history%';
-- SHOW CREATE TABLE wrestler_match_history_2024_2025_boys_all;
-- SELECT COUNT(*) AS row_count_copy FROM wrestler_match_history_2024_2025_boys_all;
-- SELECT COUNT(*) AS row_count_original FROM wrestler_match_history_scrape_data;

-- ================================
-- CREATE BACKUP OF WRESTLER LIST TABLE
-- =================================
-- 1️ Create a new table with identical structure (includes indexes + AUTO_INCREMENT)
CREATE TABLE wrestler_list_2024_2025_boys_all LIKE wrestler_list;

-- 2️ Copy all rows into it
INSERT INTO wrestler_list_2024_2025_boys_all SELECT * FROM wrestler_list;

SHOW TABLES LIKE 'wrestler_list%';
SHOW CREATE TABLE wrestler_list_2024_2025_boys_all;
SELECT COUNT(*) AS row_count_copy FROM wrestler_list_2024_2025_boys_all;
SELECT COUNT(*) AS row_count_original FROM wrestler_list;


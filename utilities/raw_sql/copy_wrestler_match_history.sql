-- ================================
-- CREATE BACKUP OF WRESTLER MATCH HISTORY TABLE
-- =================================
-- 1️ Create a new table with identical structure (includes indexes + AUTO_INCREMENT)
-- 2️ Copy all rows into it
CREATE TABLE wrestler_match_history_2024_2025_boys_all LIKE wrestler_match_history_scrape_data;
INSERT INTO wrestler_match_history_2024_2025_boys_all SELECT * FROM wrestler_match_history_scrape_data;
-- ================================
-- TRANSFER BACKUP TO WRESTLER LIST TABLE
-- =================================
TRUNCATE TABLE wrestler_match_history_scrape_data;
INSERT INTO wrestler_match_history_scrape_data (
      id,
      wrestling_season,
      track_wrestling_category,
      page_url,
      wrestler_id,
      wrestler,
      start_date,
      end_date,
      event,
      weight_category,
      match_order,
      opponent_id,
      raw_details,
      created_at_mtn,
      created_at_utc,
      updated_at_mtn,
      updated_at_utc
)
SELECT
      id,
      wrestling_season,
      track_wrestling_category,
      page_url,
      wrestler_id,
      wrestler,
      start_date,
      end_date,
      event,
      weight_category,
      match_order,
      opponent_id,
      raw_details,
      created_at_mtn,
      created_at_utc,
      updated_at_mtn,
      updated_at_utc
FROM wrestler_match_history_2024_2025_boys_all;

-- ================================
-- OTHER STUFF
-- ================================
-- SHOW TABLES LIKE 'wrestler_match_history%';
-- SHOW CREATE TABLE wrestler_match_history_2024_2025_boys_all;
-- SELECT COUNT(*) AS row_count_copy FROM wrestler_match_history_2024_2025_boys_all;
-- SELECT COUNT(*) AS row_count_original FROM wrestler_match_history_scrape_data;

-- ================================
-- CREATE BACKUP OF WRESTLER LIST TABLE
-- =================================
-- 1️ Create a new table with identical structure (includes indexes + AUTO_INCREMENT)
-- 2️ Copy all rows into it
CREATE TABLE wrestler_list_scrape_data_2024_2025_boys_backup LIKE wrestler_list_scrape_data;
INSERT INTO wrestler_list_scrape_data_2024_2025_boys_backup SELECT * FROM wrestler_list_scrape_data;
-- ================================
-- TRANSFER BACKUP TO WRESTLER LIST TABLE
-- =================================
TRUNCATE TABLE wrestler_list_scrape_data;
INSERT INTO wrestler_list_scrape_data SELECT * FROM wrestler_list_2024_2025_boys_all;
-- ================================
-- OTHER STUFF
-- ================================
SHOW TABLES LIKE 'wrestler_list%';
SHOW CREATE TABLE wrestler_list_2024_2025_boys_all;
SELECT COUNT(*) AS row_count_copy FROM wrestler_list_2024_2025_boys_all;
SELECT COUNT(*) AS row_count_original FROM wrestler_list;


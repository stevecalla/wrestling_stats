ALTER TABLE wrestler_match_history
  ADD INDEX idx_wmh_wrestler_season (wrestler_id, wrestling_season),
  ADD INDEX idx_wmh_season_cat (wrestling_season, track_wrestling_category, wrestler_id),
  ADD INDEX idx_wmh_id (id);
  
ALTER TABLE wrestler_match_history
  ADD INDEX idx_wmh_date (start_date);

CALL wrestling_stats.compute_wrestler_rankings(
  '2024-11-01',  -- start (inclusive)
  '2025-04-01',  -- end (exclusive)
  FALSE,         -- Varsity-only
  TRUE,          -- persist per-match table
  24.0,          -- base K
  0.5            -- late-season boost
);

SELECT * FROM wrestler_rankings_final ORDER BY final_score DESC LIMIT 25;

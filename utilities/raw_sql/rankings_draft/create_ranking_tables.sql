-- Per-match ratings (optional persist)
CREATE TABLE IF NOT EXISTS wrestler_ratings_per_match (
  rn                BIGINT NOT NULL,
  match_id          BIGINT NOT NULL,
  wrestler_id       BIGINT UNSIGNED NOT NULL,
  opponent_id       BIGINT UNSIGNED NOT NULL,
  start_date        DATE NULL,
  round             VARCHAR(128) NULL,
  event             VARCHAR(255) NULL,
  weight_category   VARCHAR(64)  NULL,
  outcome           TINYINT NOT NULL,      -- 1 win, 0 loss
  k_used            DECIMAL(8,3) NOT NULL,
  r_a_before        DECIMAL(8,3) NOT NULL,
  r_b_before        DECIMAL(8,3) NOT NULL,
  r_a_after         DECIMAL(8,3) NOT NULL,
  r_b_after         DECIMAL(8,3) NOT NULL,
  PRIMARY KEY (rn),
  KEY (wrestler_id),
  KEY (opponent_id),
  KEY (start_date)
);

-- Final rankings, one row per wrestler_id
CREATE TABLE IF NOT EXISTS wrestler_rankings_final (
  wrestler_id            BIGINT UNSIGNED PRIMARY KEY,
  matches                INT NOT NULL,
  final_elo              DECIMAL(8,3) NOT NULL,
  elo_first              DECIMAL(8,3) NOT NULL,
  elo_last               DECIMAL(8,3) NOT NULL,
  delta_elo              DECIMAL(8,3) NOT NULL,
  sos_avg_opponent_pre   DECIMAL(8,3) NULL,
  improvement_slope      DOUBLE NULL,
  final_score            DOUBLE NULL,
  updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                          ON UPDATE CURRENT_TIMESTAMP
);

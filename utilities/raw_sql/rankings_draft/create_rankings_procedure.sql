CREATE PROCEDURE `new_procedure` ()
BEGIN

-- ===============================================
-- ELO-style wrestler rankings (Varsity-only default)
-- ===============================================
DECLARE v_done BOOL DEFAULT FALSE;
DECLARE v_rn BIGINT;
DECLARE v_match_id BIGINT;
DECLARE v_wrestler_id BIGINT UNSIGNED;
DECLARE v_opponent_id BIGINT UNSIGNED;
DECLARE v_start_date DATE;
DECLARE v_round VARCHAR(128);
DECLARE v_event VARCHAR(255);
DECLARE v_weight_category VARCHAR(64);
DECLARE v_outcome TINYINT;
DECLARE v_k_used DECIMAL(8,3);

DECLARE v_ra DECIMAL(10,4);
DECLARE v_rb DECIMAL(10,4);
DECLARE v_exp_a DECIMAL(12,8);
DECLARE v_ra_new DECIMAL(10,4);
DECLARE v_rb_new DECIMAL(10,4);

DECLARE cur CURSOR FOR
  SELECT rn, match_id, wrestler_id, opponent_id, start_date, round, event, weight_category, outcome, k_used
  FROM tmp_matches
  ORDER BY rn;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

-- 1) Clean & order matches
DROP TEMPORARY TABLE IF EXISTS tmp_matches;
CREATE TEMPORARY TABLE tmp_matches AS
SELECT
    v.match_id,
    v.wrestler_id,
    v.opponent_id,
    v.start_date,
    v.round,
    v.event,
    v.weight_category,
    v.outcome,
    ROW_NUMBER() OVER (ORDER BY v.start_date, v.match_id) AS rn,
    -- K-factor (late-season boost + round importance)
    (p_base_k * (1.0 + p_late_boost * PERCENT_RANK() OVER (ORDER BY v.start_date))) *
    (CASE
       WHEN v.round LIKE '%Final%'   THEN 1.25
       WHEN v.round LIKE '%Semi%'    THEN 1.15
       WHEN v.round LIKE '%Quarter%' THEN 1.10
       WHEN v.round LIKE '%Consol%'  THEN 1.05
       ELSE 1.0
     END) AS k_used
FROM (
    SELECT
      m.id AS match_id,
      m.wrestler_id,
      m.opponent_id,
      m.start_date, m.round, m.event, m.weight_category,
      CASE
        WHEN m.result IS NULL THEN NULL
        WHEN LOWER(m.result) LIKE '%bye%' THEN NULL
        WHEN LOWER(m.result) LIKE '%win%' OR m.result = 'W' THEN 1
        WHEN LOWER(m.result) LIKE '%loss%' OR m.result = 'L' THEN 0
        ELSE NULL
      END AS outcome
    FROM wrestler_match_history m
    WHERE
      m.wrestler_id IS NOT NULL
      AND m.opponent_id IS NOT NULL
      AND m.result IS NOT NULL
      AND LOWER(m.result) NOT LIKE '%bye%'
      AND (p_season_start IS NULL OR m.start_date >= p_season_start)
      AND (p_season_end IS NULL OR m.start_date < p_season_end)
      AND (
        p_include_jv = TRUE
        OR (
          (m.level IS NOT NULL AND LOWER(m.level) LIKE '%varsity%')
          OR (
            m.level IS NULL
            AND (m.round IS NULL OR LOWER(m.round)  NOT LIKE '%junior varsity%')
            AND (m.event IS NULL OR LOWER(m.event)  NOT LIKE '%junior varsity%')
          )
        )
      )
  ) AS v
WHERE v.outcome IN (0,1);

CREATE INDEX ix_tmp_matches_rn ON tmp_matches(rn);

-- 2) Ratings map (current Elo)
DROP TEMPORARY TABLE IF EXISTS tmp_ratings;
CREATE TEMPORARY TABLE tmp_ratings (
  wrestler_id BIGINT UNSIGNED PRIMARY KEY,
  rating      DECIMAL(8,3) NOT NULL
) ENGINE=Memory;

INSERT IGNORE INTO tmp_ratings (wrestler_id, rating)
SELECT wrestler_id, 1500.0 FROM (
  SELECT wrestler_id FROM tmp_matches
  UNION
  SELECT opponent_id FROM tmp_matches
) u;

-- 3) Per-match output
DROP TEMPORARY TABLE IF EXISTS tmp_per_match;
CREATE TEMPORARY TABLE tmp_per_match (
  rn                BIGINT NOT NULL,
  match_id          BIGINT NOT NULL,
  wrestler_id       BIGINT UNSIGNED NOT NULL,
  opponent_id       BIGINT UNSIGNED NOT NULL,
  start_date        DATE NULL,
  round             VARCHAR(128) NULL,
  event             VARCHAR(255) NULL,
  weight_category   VARCHAR(64)  NULL,
  outcome           TINYINT NOT NULL,
  k_used            DECIMAL(8,3) NOT NULL,
  r_a_before        DECIMAL(8,3) NOT NULL,
  r_b_before        DECIMAL(8,3) NOT NULL,
  r_a_after         DECIMAL(8,3) NOT NULL,
  r_b_after         DECIMAL(8,3) NOT NULL,
  ts                BIGINT NULL,
  PRIMARY KEY (rn),
  KEY (wrestler_id),
  KEY (opponent_id)
) ENGINE=Memory;

-- 4) Cursor to walk matches chronologically
OPEN cur;
read_loop: LOOP
  FETCH cur INTO v_rn, v_match_id, v_wrestler_id, v_opponent_id, v_start_date, v_round, v_event, v_weight_category, v_outcome, v_k_used;
  IF v_done THEN
    LEAVE read_loop;
  END IF;

  SELECT rating INTO v_ra FROM tmp_ratings WHERE wrestler_id = v_wrestler_id;
  IF v_ra IS NULL THEN SET v_ra = 1500.0; INSERT IGNORE INTO tmp_ratings VALUES (v_wrestler_id, v_ra); END IF;

  SELECT rating INTO v_rb FROM tmp_ratings WHERE wrestler_id = v_opponent_id;
  IF v_rb IS NULL THEN SET v_rb = 1500.0; INSERT IGNORE INTO tmp_ratings VALUES (v_opponent_id, v_rb); END IF;

  SET v_exp_a = 1.0 / (1.0 + POW(10.0, (v_rb - v_ra)/400.0));

  SET v_ra_new = v_ra + v_k_used * (v_outcome - v_exp_a);
  SET v_rb_new = v_rb + v_k_used * ((1.0 - v_outcome) - (1.0 - v_exp_a));

  INSERT INTO tmp_ratings (wrestler_id, rating) VALUES (v_wrestler_id, v_ra_new)
    ON DUPLICATE KEY UPDATE rating = VALUES(rating);
  INSERT INTO tmp_ratings (wrestler_id, rating) VALUES (v_opponent_id, v_rb_new)
    ON DUPLICATE KEY UPDATE rating = VALUES(rating);

  INSERT INTO tmp_per_match
    (rn, match_id, wrestler_id, opponent_id, start_date, round, event, weight_category,
     outcome, k_used, r_a_before, r_b_before, r_a_after, r_b_after, ts)
  VALUES
    (v_rn, v_match_id, v_wrestler_id, v_opponent_id, v_start_date, v_round, v_event, v_weight_category,
     v_outcome, v_k_used, v_ra, v_rb, v_ra_new, v_rb_new, UNIX_TIMESTAMP(v_start_date));
END LOOP;
CLOSE cur;

-- 5) Optional: persist per-match
IF p_save_per_match THEN
  TRUNCATE TABLE wrestler_ratings_per_match;
  INSERT INTO wrestler_ratings_per_match
  SELECT rn, match_id, wrestler_id, opponent_id, start_date, round, event, weight_category,
         outcome, k_used, r_a_before, r_b_before, r_a_after, r_b_after
  FROM tmp_per_match
  ORDER BY rn;
END IF;

-- 6) Aggregate: first/last, delta, SoS, improvement slope
DROP TEMPORARY TABLE IF EXISTS tmp_bounds;
CREATE TEMPORARY TABLE tmp_bounds AS
SELECT
  wrestler_id,
  MIN(rn) AS first_rn,
  MAX(rn) AS last_rn
FROM tmp_per_match
GROUP BY wrestler_id;

DROP TEMPORARY TABLE IF EXISTS tmp_sums;
CREATE TEMPORARY TABLE tmp_sums AS
SELECT
  wrestler_id,
  COUNT(*)           AS n,
  SUM(ts)            AS sx,
  SUM(r_a_after)     AS sy,
  SUM(ts * r_a_after) AS sxy,
  SUM(ts * ts)       AS sxx,
  AVG(r_b_before)    AS sos_avg_opponent_pre
FROM tmp_per_match
GROUP BY wrestler_id;

REPLACE INTO wrestler_rankings_final
SELECT
  p.wrestler_id,
  COUNT(*) AS matches,
  MAX(CASE WHEN p.rn = b.last_rn  THEN p.r_a_after  END) AS final_elo,
  MIN(CASE WHEN p.rn = b.first_rn THEN p.r_a_before END) AS elo_first,
  MAX(CASE WHEN p.rn = b.last_rn  THEN p.r_a_after  END) AS elo_last,
  (MAX(CASE WHEN p.rn = b.last_rn  THEN p.r_a_after  END)
  - MIN(CASE WHEN p.rn = b.first_rn THEN p.r_a_before END)) AS delta_elo,
  s.sos_avg_opponent_pre,
  CASE
    WHEN (s.n * s.sxx - s.sx * s.sx) = 0 THEN NULL
    ELSE (s.n * s.sxy - s.sx * s.sy) / (s.n * s.sxx - s.sx * s.sx)
  END AS improvement_slope,
  NULL AS final_score,
  CURRENT_TIMESTAMP
FROM tmp_per_match p
JOIN tmp_bounds b ON b.wrestler_id = p.wrestler_id
JOIN tmp_sums   s ON s.wrestler_id = p.wrestler_id
GROUP BY p.wrestler_id;

-- 7) Composite score (final Elo + improvement + SoS)
UPDATE wrestler_rankings_final x
JOIN (
  SELECT
    AVG(delta_elo) AS mu_imp,  STDDEV_POP(delta_elo) AS sd_imp,
    AVG(sos_avg_opponent_pre) AS mu_sos, STDDEV_POP(sos_avg_opponent_pre) AS sd_sos
  FROM wrestler_rankings_final
) s ON 1=1
SET x.final_score =
    (0.7 * x.final_elo)
  + (0.2 * CASE WHEN s.sd_imp = 0 THEN 0 ELSE (x.delta_elo - s.mu_imp) / s.sd_imp END)
  + (0.1 * CASE WHEN s.sd_sos = 0 THEN 0 ELSE (x.sos_avg_opponent_pre - s.mu_sos) / s.sd_sos END);

END

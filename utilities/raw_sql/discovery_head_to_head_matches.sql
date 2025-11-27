WITH determine_state_qualifier_and_place AS (
    SELECT 
        id,
        wrestling_season,
        track_wrestling_category,
        wrestler_name, 
        wrestler_id,
        wrestler_first_name,
        wrestler_last_name,
        wrestler_grade,
        record,
        wins_all_run,
        losses_all_run,
        ties_all_run,
        total_matches,
        total_matches_win_pct,
        match_order,
        outcome,
        event, 
        round, 
        weight_category, 
        winner_name,
        CASE 
            WHEN wrestling_season = '2024-25' THEN 'state_qualifier_2024'
            ELSE 'other'
        END AS is_state_tournament_qualifier,
        CASE
            WHEN wrestling_season = '2024-25' AND round LIKE '%1st Place Match%' AND outcome = 'W' THEN '1st_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%1st Place Match%' AND outcome = 'L' THEN '2nd_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%3rd Place Match%' AND outcome = 'W' THEN '3rd_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%3rd Place Match%' AND outcome = 'L' THEN '4th_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%5th Place Match%' AND outcome = 'W' THEN '5th_state_place_2025'
            WHEN wrestling_season = '2024-25' AND round LIKE '%5th Place Match%' AND outcome = 'L' THEN '6th_state_place_2025'
            WHEN wrestling_season = '2024-25' THEN 'other_2025'
            ELSE 'tbd'
        END AS state_tournament_place,
        raw_details
    FROM wrestler_match_history_metrics_data 
    WHERE 1 = 1
        AND event LIKE '%State Championships%' 
        AND weight_category LIKE '%A%'
),

-- For each wrestler, find their *last* state match by match_order
max_match AS (
    SELECT
        wrestling_season,
        track_wrestling_category,
        wrestler_id,
        MAX(match_order) AS max_match_order,
        COUNT(*)         AS match_count
    FROM determine_state_qualifier_and_place
    GROUP BY
        wrestling_season,
        track_wrestling_category,
        wrestler_id
),

-- Keep only that final (max match_order) row per wrestler (their final state result)
per_wrestler AS (
    SELECT
        d.*,
        m.match_count
    FROM determine_state_qualifier_and_place d
    JOIN max_match m
      ON  d.wrestling_season         = m.wrestling_season
      AND d.track_wrestling_category = m.track_wrestling_category
      AND d.wrestler_id              = m.wrestler_id
      AND d.match_order              = m.max_match_order
),

-- Just the 1st and 2nd place finishers per weight
finalists AS (
    SELECT
        p.*
    FROM per_wrestler p
    WHERE p.state_tournament_place IN ('1st_state_place_2025', '2nd_state_place_2025')
),

-- Pair up 1st and 2nd place per weight
finalist_pairs AS (
    SELECT
        f1.wrestling_season,
        f1.track_wrestling_category,
        f1.weight_category,

        -- 1st place
        f1.wrestler_id          AS first_place_id,
        f1.wrestler_name        AS first_place_name,
        f1.wrestler_first_name  AS first_place_first_name,
        f1.wrestler_last_name   AS first_place_last_name,
        f1.wrestler_grade       AS first_place_grade,
        f1.record               AS first_place_record,
        f1.total_matches        AS first_place_total_matches,
        f1.total_matches_win_pct AS first_place_win_pct,

        -- 2nd place
        f2.wrestler_id          AS second_place_id,
        f2.wrestler_name        AS second_place_name,
        f2.wrestler_first_name  AS second_place_first_name,
        f2.wrestler_last_name   AS second_place_last_name,
        f2.wrestler_grade       AS second_place_grade,
        f2.record               AS second_place_record,
        f2.total_matches        AS second_place_total_matches,
        f2.total_matches_win_pct AS second_place_win_pct
    FROM finalists f1
    JOIN finalists f2
      ON  f1.wrestling_season         = f2.wrestling_season
      AND f1.track_wrestling_category = f2.track_wrestling_category
      AND f1.weight_category          = f2.weight_category
      AND f1.state_tournament_place   = '1st_state_place_2025'
      AND f2.state_tournament_place   = '2nd_state_place_2025'
),

-- Look for ALL prior head-to-head matches between these two,
-- *excluding* the State 1st Place Match final
prior_head_to_head AS (
    SELECT
        fp.wrestling_season,
        fp.track_wrestling_category,
        fp.weight_category,
        fp.first_place_id,
        fp.second_place_id,

        COUNT(*) AS prior_meet_count,

        SUM(
            CASE 
                WHEN h.wrestler_id = fp.first_place_id AND h.outcome = 'W' THEN 1
                ELSE 0
            END
        ) AS first_place_prior_wins,

        SUM(
            CASE 
                WHEN h.wrestler_id = fp.second_place_id AND h.outcome = 'W' THEN 1
                ELSE 0
            END
        ) AS second_place_prior_wins

    FROM finalist_pairs fp
    JOIN wrestler_match_history_metrics_data h
      ON  h.wrestling_season         = fp.wrestling_season
      AND h.track_wrestling_category = fp.track_wrestling_category
      AND (
            (h.wrestler_id = fp.first_place_id  AND h.opponent_id = fp.second_place_id)
         OR (h.wrestler_id = fp.second_place_id AND h.opponent_id = fp.first_place_id)
          )
      -- Exclude the State final itself
      AND NOT (
            h.event LIKE '%State Championships%'
        AND h.round LIKE '%1st Place Match%'
      )
    GROUP BY
        fp.wrestling_season,
        fp.track_wrestling_category,
        fp.weight_category,
        fp.first_place_id,
        fp.second_place_id
)

SELECT
    fp.wrestling_season,
    fp.track_wrestling_category,
    fp.weight_category,

    -- 1st place info
    fp.first_place_id,
    fp.first_place_name,
    fp.first_place_first_name,
    fp.first_place_last_name,
    fp.first_place_grade,
    fp.first_place_record,
    fp.first_place_total_matches,
    fp.first_place_win_pct,

    -- 2nd place info
    fp.second_place_id,
    fp.second_place_name,
    fp.second_place_first_name,
    fp.second_place_last_name,
    fp.second_place_grade,
    fp.second_place_record,
    fp.second_place_total_matches,
    fp.second_place_win_pct,

    -- prior head-to-head summary
    COALESCE(ph.prior_meet_count, 0)        AS prior_meet_count,
    COALESCE(ph.first_place_prior_wins, 0)  AS first_place_prior_wins,
    COALESCE(ph.second_place_prior_wins, 0) AS second_place_prior_wins,

    CASE
        WHEN ph.prior_meet_count IS NULL OR ph.prior_meet_count = 0
            THEN 'no_prior_meeting'
        WHEN ph.first_place_prior_wins > ph.second_place_prior_wins
            THEN 'first_place_led_series'
        WHEN ph.second_place_prior_wins > ph.first_place_prior_wins
            THEN 'second_place_led_series'
        ELSE 'series_tied'
    END AS prior_series_summary

FROM finalist_pairs fp
LEFT JOIN prior_head_to_head ph
  ON  ph.wrestling_season         = fp.wrestling_season
  AND ph.track_wrestling_category = fp.track_wrestling_category
  AND ph.weight_category          = fp.weight_category
  AND ph.first_place_id           = fp.first_place_id
  AND ph.second_place_id          = fp.second_place_id

ORDER BY
    fp.track_wrestling_category,
    fp.weight_category,
    fp.first_place_last_name,
    fp.first_place_first_name
;

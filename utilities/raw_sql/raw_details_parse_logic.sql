USE wrestling_stats;

WITH base AS (
    SELECT
        h.id,
        h.wrestling_season,
        h.track_wrestling_category,

        h.wrestler_id,
        h.wrestler        AS wrestler_name,

        h.opponent_id,

        h.event,
        h.start_date,
        h.end_date,

        h.weight_category,

        -- normalize once
        TRIM(REPLACE(REPLACE(h.raw_details, '\r', ' '), '\n', ' '))             AS raw_details,
        LOWER(TRIM(REPLACE(REPLACE(h.raw_details, '\r', ' '), '\n', ' ')))      AS lower_raw
        
    FROM wrestler_match_history h
    -- WHERE h.wrestler_id IN (29790065132, 30579778132)
    -- ORDER BY here forces a sort; remove it in a CTE for speed; step 4 partions by wrestler id over the id to ensure correct order for record calc
    -- ORDER BY h.id, h.start_date
)
-- SELECT * FROM base;

/* -------------------------
    Step 2: cheap parse + flags
-------------------------- */
, step_2_match_detail AS (
    SELECT
    b.id,
    b.wrestling_season,
    b.track_wrestling_category,
    b.wrestler_id,
    b.wrestler_name,
    b.event,
    b.start_date,
    b.raw_details,

    /* ROUND (simple: text before first ' - ' unless looks like a name '(') */
    CASE
        WHEN INSTR(b.raw_details, ' - ') = 0 THEN NULL
        WHEN INSTR(TRIM(SUBSTRING_INDEX(b.raw_details, ' - ', 1)), '(') > 0 THEN NULL
        ELSE TRIM(SUBSTRING_INDEX(b.raw_details, ' - ', 1))
    END AS round,

    /* RESULT (simple) */
    CASE
        WHEN REGEXP_LIKE(b.lower_raw, '\\breceived a bye\\b') THEN 'bye'
        WHEN INSTR(b.lower_raw, ' over ') > 0 THEN 'over'
        ELSE NULL
    END AS result,

    /* SCORE DETAILS (paren strip, with bye) */
    CASE
        WHEN REGEXP_LIKE(b.lower_raw, 'received a bye') THEN 'Bye'
        ELSE TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(b.raw_details, '\\([^)]*\\)$'), '[()]', ''))
    END AS score_details,

    /* Precompute positions and slices for outcome W/L */
    INSTR(b.lower_raw, ' over ') AS pos_over,
    CASE
        WHEN INSTR(b.lower_raw, ' over ') > 0
        THEN SUBSTRING(b.lower_raw, 1, INSTR(b.lower_raw, ' over ') - 1)
        ELSE NULL
    END AS before_over,
    CASE
        WHEN INSTR(b.lower_raw, ' over ') > 0
        THEN SUBSTRING(b.lower_raw, INSTR(b.lower_raw, ' over ') + 6)
        ELSE NULL
    END AS after_over,

    /* Name checks (lower once) */
    LOWER(b.wrestler_name) AS me_name_lower,
    CASE
        WHEN INSTR(b.lower_raw, ' over ') > 0 AND LOWER(b.wrestler_name) <> ''
        THEN (INSTR(
                CASE WHEN INSTR(b.lower_raw, ' over ') > 0
                    THEN SUBSTRING(b.lower_raw, 1, INSTR(b.lower_raw, ' over ') - 1)
                    ELSE ''
                END, LOWER(b.wrestler_name)
            ) > 0)
        ELSE 0
    END AS me_before,
    CASE
        WHEN INSTR(b.lower_raw, ' over ') > 0 AND LOWER(b.wrestler_name) <> ''
        THEN (INSTR(
                CASE WHEN INSTR(b.lower_raw, ' over ') > 0
                    THEN SUBSTRING(b.lower_raw, INSTR(b.lower_raw, ' over ') + 6)
                    ELSE ''
                END, LOWER(b.wrestler_name)
            ) > 0)
        ELSE 0
    END AS me_after,

    /* Cheap boolean flags (prefer LIKE/INSTR to REGEXP where possible) */
    (REGEXP_LIKE(b.lower_raw, '\\breceived a bye\\b') OR INSTR(b.lower_raw,' bye')>0) AS is_bye,
    (INSTR(b.lower_raw,'exhibition')>0)                                               AS is_exhibition,
    (INSTR(b.lower_raw,' tie')>0 OR INSTR(b.lower_raw,' draw')>0)                     AS is_tie,
    REGEXP_LIKE(b.raw_details, '\\bover\\s+unknown\\s*\\(\\s*for\\.\\s*\\)', 'i')     AS has_unknown_forfeit,
    (
        INSTR(b.lower_raw,' forfeit')>0 OR
        INSTR(b.lower_raw,' for.')>0    OR
        INSTR(b.lower_raw,' fft')>0     OR
        REGEXP_LIKE(b.lower_raw, '\\bmff\\b') OR
        REGEXP_LIKE(b.lower_raw, '\\bmed(?:ical)?\\s*for(?:feit)?\\b') OR
        REGEXP_LIKE(b.lower_raw, '\\binj\\.?\\s*def\\.?\\b') OR
        INSTR(b.lower_raw,'injury default')>0 OR
        REGEXP_LIKE(b.lower_raw, '\\b(dq|disqualification)\\b')
    ) AS has_forfeit_family
    FROM base b
),

/* -------------------------
    Step 3: outcome / counts / varsity
-------------------------- */
step_3_outcome_detail AS (
    SELECT
    m.*,

    /* outcome (W/L/bye/T/U) */
    CASE
        WHEN m.is_bye THEN 'bye'
        WHEN m.is_exhibition THEN 'U'
        WHEN m.is_tie THEN 'T'
        WHEN m.has_unknown_forfeit THEN 'W'
        WHEN m.has_forfeit_family THEN
        CASE
            WHEN m.pos_over > 0 THEN
            CASE
                WHEN m.me_before = 1 AND m.me_after = 0 THEN 'W'
                WHEN m.me_before = 0 AND m.me_after = 1 THEN 'L'
                ELSE 'U'
            END
            ELSE 'U'
        END
        WHEN m.pos_over > 0 THEN
        CASE
            WHEN m.me_before = 1 AND m.me_after = 0 THEN 'W'
            WHEN m.me_before = 0 AND m.me_after = 1 THEN 'L'
            ELSE 'U'
        END
        ELSE 'U'
    END AS outcome,

    /* counts_in_record */
    CASE
        WHEN m.is_bye OR m.is_exhibition THEN 0
        ELSE 1
    END AS counts_in_record,

    /* is_varsity (simple prefix test on round) */
    CASE WHEN REGEXP_LIKE(COALESCE(m.round,''), '^varsity', 'i') THEN 1 ELSE 0 END AS is_varsity
    FROM step_2_match_detail m
)

/* -------------------------
Step 4: running record per match (W/L/T) + varsity-only running record
-------------------------- */

-- , step_4_running_record AS (
--     SELECT
--         m.*,

--         /* running ALL-matches record (counts_in_record = 1) */
--         SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'W' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wins_all_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'L' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS losses_all_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'T' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ties_all_run,

--         /* running VARSITY-only record */
--         SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'W' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wins_var_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'L' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS losses_var_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'T' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ties_var_run
--     FROM step_3_outcome_detail m
-- ),

-- , step_4_running_record AS (
--     SELECT
--         m.*,

--         /* running ALL-matches record (counts_in_record = 1) */
--         SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'W' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wins_all_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'L' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS losses_all_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'T' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ties_all_run,

--         /* running VARSITY-only record */
--         SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'W' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wins_var_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'L' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS losses_var_run,

--         SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'T' THEN 1 ELSE 0 END)
--         OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
--                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ties_var_run
--     FROM step_3_outcome_detail m
-- )

, step_4_running_record AS (
  SELECT
    m.*,

    /* running ALL-matches record (counts_in_record = 1) */
    SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'W' THEN 1 ELSE 0 END)
      OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wins_all_run,

    SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'L' THEN 1 ELSE 0 END)
      OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS losses_all_run,

    SUM(CASE WHEN m.counts_in_record = 1 AND m.outcome = 'T' THEN 1 ELSE 0 END)
      OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ties_all_run,

    /* total matches (for record running total) */
    SUM(CASE WHEN m.counts_in_record = 1 THEN 1 ELSE 0 END)
      OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_matches,

    /* running VARSITY-only record */
    SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'W' THEN 1 ELSE 0 END)
      OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wins_var_run,

    SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'L' THEN 1 ELSE 0 END)
      OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS losses_var_run,

    SUM(CASE WHEN m.counts_in_record = 1 AND m.is_varsity = 1 AND m.outcome = 'T' THEN 1 ELSE 0 END)
      OVER (PARTITION BY m.wrestling_season, m.wrestler_id ORDER BY m.id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ties_var_run

  FROM step_3_outcome_detail m
)

/* -------------------------
Step 5: format strings like JS
-------------------------- */
, step_5_format AS (
    SELECT
        r.*,
        CONCAT(COALESCE(r.wins_all_run,0), '-', COALESCE(r.losses_all_run,0), '-', COALESCE(r.ties_all_run,0), ' W-L-T')               AS record,
        CONCAT(COALESCE(r.wins_var_run,0), '-', COALESCE(r.losses_var_run,0), '-', COALESCE(r.ties_var_run,0), ' W-L-T (Varsity)')     AS record_varsity
    FROM step_4_running_record r
)

-- after step_5_format (and before your final SELECT)
, step_6_opponent_name AS (
    SELECT
        s5.*,
        h.opponent_id,
        COALESCE(
        o.name,
        CASE
            WHEN s5.result = 'bye' OR h.opponent_id IS NULL OR INSTR(h.raw_details, ' over ') = 0 THEN NULL
            WHEN INSTR(LOWER(SUBSTRING_INDEX(h.raw_details, ' over ', 1)), LOWER(l.name)) > 0
            THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING(h.raw_details, INSTR(h.raw_details, ' over ') + 6), ' (', 1), ' - ', -1))
            WHEN INSTR(LOWER(SUBSTRING(h.raw_details, INSTR(h.raw_details, ' over ') + 6)), LOWER(l.name)) > 0
            THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(h.raw_details, ' over ', 1), ' (', 1), ' - ', -1))
            ELSE NULL
        END
        ) AS opponent_name
    FROM step_5_format s5
        LEFT JOIN wrestler_match_history h ON h.id = s5.id
        LEFT JOIN wrestler_list_scrape_data l ON l.wrestler_id = s5.wrestler_id
        LEFT JOIN wrestler_list_scrape_data o ON o.wrestler_id = h.opponent_id
)

, step_7_opponent_team AS (
    SELECT
        s6.*,
        COALESCE(
        o.team,
        CASE
            WHEN s6.result = 'bye' OR s6.opponent_id IS NULL OR INSTR(h.raw_details,' over ') = 0 THEN NULL
            /* our wrestler on LEFT → opponent is RIGHT: grab text inside parentheses on right side */
            WHEN INSTR(LOWER(SUBSTRING_INDEX(h.raw_details,' over ',1)), LOWER(l.name)) > 0
            THEN TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(SUBSTRING(h.raw_details, INSTR(h.raw_details,' over ')+6), '\\([^)]*\\)'),'[()]',''))
            /* our wrestler on RIGHT → opponent is LEFT: grab text inside parentheses on left side */
            WHEN INSTR(LOWER(SUBSTRING(h.raw_details, INSTR(h.raw_details,' over ')+6)), LOWER(l.name)) > 0
            THEN TRIM(REGEXP_REPLACE(REGEXP_SUBSTR(SUBSTRING_INDEX(h.raw_details,' over ',1), '\\([^)]*\\)'),'[()]',''))
            ELSE NULL
        END
        ) AS opponent_team
    FROM step_6_opponent_name s6
    LEFT JOIN wrestler_match_history h ON h.id = s6.id
    LEFT JOIN wrestler_list_scrape_data l ON l.wrestler_id = s6.wrestler_id
    LEFT JOIN wrestler_list_scrape_data o ON o.wrestler_id = s6.opponent_id
)

-- 1) Clean display names (strip trailing "(...)" and suffixes)
, step_8_clean_names AS (
    SELECT
        s7.*,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(s7.wrestler_name, '\\s*\\([^)]*\\)\\s*$', ''), '\\s*,?\\s*(Jr\\.?|Sr\\.?|II|III|IV|V|VI)\\s*$', '')) AS w_clean,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(s7.opponent_name, '\\s*\\([^)]*\\)\\s*$', ''), '\\s*,?\\s*(Jr\\.?|Sr\\.?|II|III|IV|V|VI)\\s*$', '')) AS o_clean

        -- TRIM(REGEXP_REPLACE(s7.wrestler_name, '\\s*\\([^)]*\\)\\s*$', '')) AS w_clean,
        -- TRIM(REGEXP_REPLACE(s7.opponent_name, '\\s*\\([^)]*\\)\\s*$', '')) AS o_clean

    FROM step_7_opponent_team s7
)

-- 2) Detect multi-word last names (particles), then derive first/last
, step_8_names_split AS (
    SELECT
        x.*,

        /* final wrestler names */
        TRIM(SUBSTRING(x.w_clean, 1,
            CHAR_LENGTH(x.w_clean) - CHAR_LENGTH(x.w_last) - CASE WHEN CHAR_LENGTH(x.w_clean) > CHAR_LENGTH(x.w_last) THEN 1 ELSE 0 END
        )) AS wrestler_first_name,
        x.w_last AS wrestler_last_name,

        /* final opponent names */
        TRIM(SUBSTRING(x.o_clean, 1,
            CHAR_LENGTH(x.o_clean) - CHAR_LENGTH(x.o_last) - CASE WHEN CHAR_LENGTH(x.o_clean) > CHAR_LENGTH(x.o_last) THEN 1 ELSE 0 END
        )) AS opponent_first_name,
        x.o_last AS opponent_last_name

    FROM (
        SELECT
        c.*,

        /* wrestler last name with particles */
        CASE
            WHEN REGEXP_LIKE(SUBSTRING_INDEX(c.w_clean,' ', -3), '(?i)^(de la|van der|von der)\\s+\\S+$')
            THEN SUBSTRING_INDEX(c.w_clean,' ', -3)
            WHEN REGEXP_LIKE(SUBSTRING_INDEX(c.w_clean,' ', -2), '(?i)^(de|da|del|der|di|du|van|von|la|le|st\\.?|san)\\s+\\S+$')
            THEN SUBSTRING_INDEX(c.w_clean,' ', -2)
            ELSE SUBSTRING_INDEX(c.w_clean,' ', -1)
        END AS w_last,

        /* opponent last name with particles */
        CASE
            WHEN REGEXP_LIKE(SUBSTRING_INDEX(c.o_clean,' ', -3), '(?i)^(de la|van der|von der)\\s+\\S+$')
            THEN SUBSTRING_INDEX(c.o_clean,' ', -3)
            WHEN REGEXP_LIKE(SUBSTRING_INDEX(c.o_clean,' ', -2), '(?i)^(de|da|del|der|di|du|van|von|la|le|st\\.?|san)\\s+\\S+$')
            THEN SUBSTRING_INDEX(c.o_clean,' ', -2)
            ELSE SUBSTRING_INDEX(c.o_clean,' ', -1)
        END AS o_last

        FROM step_8_clean_names c
    ) AS x
)

, step_9_winner AS (
  SELECT
    s8.*,
    CASE
      WHEN s8.outcome = 'W'  THEN s8.wrestler_id
      WHEN s8.outcome = 'L'  THEN s8.opponent_id
      ELSE NULL                          -- bye / T / U
    END AS winner_id,
    CASE
      WHEN s8.outcome = 'W'  THEN s8.wrestler_name
      WHEN s8.outcome = 'L'  THEN s8.opponent_name
      WHEN s8.outcome = 'bye' THEN 'Bye'
      ELSE NULL
    END AS winner_name
  FROM step_8_names_split s8
)

-- final select uses the CTE result
SELECT
  s9.id,
  s9.wrestling_season,
  s9.track_wrestling_category,
  l.governing_body,

  s9.wrestler_id,
  l.name                    AS wrestler_name,
  s9.wrestler_first_name,
  s9.wrestler_last_name,
  l.gender                  AS wrestler_gender,

  l.team        AS wrestler_team,
  l.team_id     AS wrestler_team_id,
  l.grade       AS wrestler_grade,
  l.level       AS wrestler_level,

  h.event, h.start_date, h.end_date, h.weight_category,

  s9.opponent_id,
  s9.opponent_name, 
  s9.opponent_first_name,
  s9.opponent_last_name,
  s9.opponent_team, 

  s9.winner_id,
  s9.winner_name,

  s9.round, s9.is_varsity,
  s9.result, s9.score_details, s9.outcome, s9.counts_in_record,
  s9.wins_all_run, s9.losses_all_run, s9.ties_all_run,
  s9.total_matches,
  ROUND(
    CASE WHEN s9.total_matches > 0
        THEN s9.wins_all_run / s9.total_matches * 100
        ELSE NULL END, 1
  ) AS total_matches_win_pct,

  s9.wins_var_run, s9.losses_var_run, s9.ties_var_run,
  
  s9.record, s9.record_varsity,
  s9.raw_details,
  h.page_url, h.created_at_mtn, h.created_at_utc, h.updated_at_mtn, h.updated_at_utc

FROM step_9_winner s9
    LEFT JOIN wrestler_list_scrape_data l ON l.wrestler_id = s9.wrestler_id
    LEFT JOIN wrestler_match_history h ON h.id = s9.id

ORDER BY s9.wrestler_id, s9.id
;

/* ---------------------------------------------------------------------------
QUERY A — “BEST MATCH PER LIST ROW” (ACTIONABLE / COPY-PASTE FIXES)

Purpose:
- Generates candidate matches between:
  - wrestler_list_scrape_data (your scraped list) and
  - reference_wrestler_rankings_list (OnTheMat rankings)
- Produces exactly ONE “best” candidate per list row (list_id) using:
  - weight match
  - school similarity (both-direction LIKE)
  - name heuristics (first token hit, last token hit, soundex)
  - a simple score_0_to_5
  - ROW_NUMBER() ranking to pick the top candidate

When to use:
- When you want a deterministic, “winner-take-all” suggestion for each unmatched list wrestler.
- When you want an easy copy/paste mapping into NAME_FIXES:
  - name_fix_array outputs like: ['Rankings Name', 'List Name']

Notes:
- This is the “action” query: it selects one recommended mapping per list_id.
- Safer for automation than the debug query because it forces a single choice.

--------------------------------------------------------------------------- */
WITH
r AS (
  SELECT
    r.wrestler_name AS ranking_wrestler_name,
    r.school        AS ranking_school,
    r.weight_lbs    AS ranking_weight_lbs,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(r.wrestler_name,'[^A-Za-z0-9 ]',' '),'\\s+',' '))) AS ranking_name_norm,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(r.school,'[^A-Za-z0-9 ]',' '),'\\s+',' ')))       AS ranking_school_norm
  FROM reference_wrestler_rankings_list r
  WHERE r.wrestling_season = '2025-26'
    AND r.track_wrestling_category = 'High School Boys'
),
l AS (
  SELECT
    l.id AS list_id,
    l.name AS list_wrestler_name,
    SUBSTRING_INDEX(l.team, ',', 1) AS list_school,
    l.weight_class AS list_weight_lbs,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(l.name,'[^A-Za-z0-9 ]',' '),'\\s+',' '))) AS list_name_norm,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(SUBSTRING_INDEX(l.team, ',', 1),'[^A-Za-z0-9 ]',' '),'\\s+',' '))) AS list_school_norm
  FROM wrestler_list_scrape_data l
  WHERE l.wrestling_season = '2025-26'
    AND l.track_wrestling_category = 'High School Boys'
    AND (l.onthemat_is_name_match = 0 OR l.onthemat_is_name_match IS NULL)
),
candidates AS (
  SELECT
    l.list_id,
    l.list_wrestler_name,
    l.list_school,
    l.list_weight_lbs,

    r.ranking_wrestler_name,
    r.ranking_school,
    r.ranking_weight_lbs,

    l.list_name_norm,
    r.ranking_name_norm,

    (
      (LOCATE(SUBSTRING_INDEX(l.list_name_norm,' ',1),   r.ranking_name_norm) > 0) +
      (LOCATE(SUBSTRING_INDEX(l.list_name_norm,' ', -1), r.ranking_name_norm) > 0) +
      (SOUNDEX(l.list_name_norm) = SOUNDEX(r.ranking_name_norm)) +
      (r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')) +
      (l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%'))
    ) AS match_score_0_to_5
  FROM l
  JOIN r
    ON r.ranking_weight_lbs = l.list_weight_lbs
   AND (
        r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')
     OR l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%')
   )
  WHERE
    (SOUNDEX(l.list_name_norm) = SOUNDEX(r.ranking_name_norm)
     OR LOCATE(SUBSTRING_INDEX(l.list_name_norm,' ', -1), r.ranking_name_norm) > 0)
),
ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.list_id
      ORDER BY c.match_score_0_to_5 DESC, c.ranking_wrestler_name
    ) AS rn
  FROM candidates c
)
SELECT
  list_id,
  list_wrestler_name,
  list_school,
  list_weight_lbs,

  ranking_wrestler_name,
  ranking_school,
  ranking_weight_lbs,

  match_score_0_to_5,

  -- JS-style array literal you can copy/paste into NAME_FIXES:
  CONCAT(
    "['",
    REPLACE(ranking_wrestler_name, "'", "\\'"),
    "', '",
    REPLACE(list_wrestler_name, "'", "\\'"),
    "']"
  ) AS name_fix_array,

  SUBSTRING_INDEX(list_name_norm,' ',1)   AS list_first_name_norm,
  SUBSTRING_INDEX(list_name_norm,' ', -1) AS list_last_name_norm,
  SUBSTRING_INDEX(ranking_name_norm,' ',1)   AS ranking_first_name_norm,
  SUBSTRING_INDEX(ranking_name_norm,' ', -1) AS ranking_last_name_norm,

  CHAR_LENGTH(SUBSTRING_INDEX(list_name_norm,' ', -1))
    - CHAR_LENGTH(SUBSTRING_INDEX(
        SUBSTRING_INDEX(list_name_norm,' ', -1),
        SUBSTRING_INDEX(ranking_name_norm,' ', -1),
        1
      )) AS last_name_common_prefix_len,

  CONCAT(
    CASE
      WHEN SUBSTRING_INDEX(list_name_norm,' ',1) = SUBSTRING_INDEX(ranking_name_norm,' ',1)
        THEN 'first=OK'
      ELSE CONCAT('first:', SUBSTRING_INDEX(list_name_norm,' ',1), '→', SUBSTRING_INDEX(ranking_name_norm,' ',1))
    END,
    ' | ',
    CASE
      WHEN SUBSTRING_INDEX(list_name_norm,' ', -1) = SUBSTRING_INDEX(ranking_name_norm,' ', -1)
        THEN 'last=OK'
      ELSE CONCAT('last:', SUBSTRING_INDEX(list_name_norm,' ', -1), '→', SUBSTRING_INDEX(ranking_name_norm,' ', -1))
    END
  ) AS name_diff_hint
FROM ranked
WHERE rn = 1
ORDER BY match_score_0_to_5 DESC, list_id;


/* ---------------------------------------------------------------------------
QUERY B — “SHOW ME ALL CANDIDATES” (DEBUG / TUNING)

Purpose:
- Similar matching logic (weight + school + name heuristics) but returns
  ALL candidate ranking rows for each list_id (no “winner” selection).

When to use:
- Debugging or tuning matching heuristics:
  - See if multiple candidates exist for the same list_id (common with shared last names).
  - Inspect which sub-signals are firing:
      soundex_match / first_name_hit / last_name_hit
  - Adjust thresholds before generating final NAME_FIXES.

Notes:
- This is the “exploration” query: it does NOT enforce a single best match.
- You must manually decide which candidate is correct if there are multiple
  high-scoring rows for the same list_id.

--------------------------------------------------------------------------- */
WITH r AS (
  SELECT
    wrestler_name,
    school,
    weight_lbs,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(wrestler_name, '[^A-Za-z0-9 ]', ' '), '\\s+', ' '))) AS r_name_norm,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(school,       '[^A-Za-z0-9 ]', ' '), '\\s+', ' '))) AS r_school_norm
  FROM reference_wrestler_rankings_list
  WHERE wrestling_season = '2025-26'
    AND track_wrestling_category = 'High School Boys'
),
l AS (
  SELECT
    id AS list_id,
    name AS list_name,
    SUBSTRING_INDEX(team, ',', 1) AS list_school,
    weight_class AS weight_lbs,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name, '[^A-Za-z0-9 ]', ' '), '\\s+', ' '))) AS l_name_norm,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(SUBSTRING_INDEX(team, ',', 1), '[^A-Za-z0-9 ]', ' '), '\\s+', ' '))) AS l_school_norm
  FROM wrestler_list_scrape_data
  WHERE wrestling_season = '2025-26'
    AND track_wrestling_category = 'High School Boys'
    AND (onthemat_is_name_match = 0 OR onthemat_is_name_match IS NULL)
)
SELECT
  l.list_id,
  l.list_name,
  l.list_school,
  l.weight_lbs,
  r.wrestler_name AS candidate_name,
  r.school AS candidate_school,

  (SOUNDEX(l.l_name_norm) = SOUNDEX(r.r_name_norm)) AS soundex_match,
  (LOCATE(SUBSTRING_INDEX(l.l_name_norm,' ',1),  r.r_name_norm) > 0) AS first_name_hit,
  (LOCATE(SUBSTRING_INDEX(l.l_name_norm,' ', -1), r.r_name_norm) > 0) AS last_name_hit,

  (
    (LOCATE(SUBSTRING_INDEX(l.l_name_norm,' ',1),   r.r_name_norm) > 0) +
    (LOCATE(SUBSTRING_INDEX(l.l_name_norm,' ', -1), r.r_name_norm) > 0) +
    (SOUNDEX(l.l_name_norm) = SOUNDEX(r.r_name_norm)) +
    (r.r_school_norm LIKE CONCAT('%', l.l_school_norm, '%')) +
    (l.l_school_norm LIKE CONCAT('%', r.r_school_norm, '%'))
  ) AS score_0_to_5

FROM l
JOIN r
  ON r.weight_lbs = l.weight_lbs
 AND (
      r.r_school_norm LIKE CONCAT('%', l.l_school_norm, '%')
   OR l.l_school_norm LIKE CONCAT('%', r.r_school_norm, '%')
 )
WHERE
  (SOUNDEX(l.l_name_norm) = SOUNDEX(r.r_name_norm)
   OR LOCATE(SUBSTRING_INDEX(l.l_name_norm,' ', -1), r.r_name_norm) > 0)
ORDER BY
  l.list_id,
  score_0_to_5 DESC,
  candidate_name;

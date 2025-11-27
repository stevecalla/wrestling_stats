// C:\Users\calla\development\projects\wrestling_stats\utilities\raw_sql\discovery_team_division.sql
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import { get_pool } from "../../mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  const sql = `
    -- DROP TABLE wrestler_state_qualifier_and_place_reference;

    CREATE TABLE IF NOT EXISTS wrestler_state_qualifier_and_place_reference (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

        wrestling_season         VARCHAR(32)  NOT NULL,
        track_wrestling_category VARCHAR(32)  NOT NULL,

        wrestler_id               BIGINT UNSIGNED,
        wrestler_name             VARCHAR(255),
        wrestler_first_name       VARCHAR(255),
        wrestler_last_name        VARCHAR(255),
        wrestler_grade            VARCHAR(64)   NULL,
        weight_category           VARCHAR(64)   NULL,

        record                    VARCHAR(64),
        wins_all_run              INT, 
        losses_all_run            INT, 
        ties_all_run              INT,
        total_matches             INT,
        total_matches_win_pct     FLOAT,
        winner_name               VARCHAR(255),

        event                     VARCHAR(255)  NULL,
        round                     VARCHAR(128)  NULL,
        outcome                   CHAR(10)      NULL,        -- W/L/T/U

        is_state_tournament_qualifier       VARCHAR(50)  NULL,
        state_tournament_place              VARCHAR(50)  NULL,

        raw_details               TEXT,

        -- how many state matches this wrestler had (for sanity/debug)
        count_distinct            INT,
        count_matches             INT,

        -- timestamps
        created_at_mtn           DATETIME     NOT NULL,
        created_at_utc           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

        updated_at_mtn           DATETIME     NOT NULL,
        updated_at_utc           DATETIME     NOT NULL,

        -- KEY / INDEXES
        UNIQUE KEY idx_alpha (wrestling_season, track_wrestling_category, wrestler_id), -- TODO:
        INDEX ix_team_id (wrestler_id),
        PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;
  await pool.query(sql);
  _ensured = true;
}

async function upsert_state_qualifier_reference() {
  // ✅ Always ensure table exists
  await ensure_table();

  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const created_at_mtn = now_mtn;
  const created_at_utc = now_utc;
  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  let inserted = 0;
  let updated = 0;

  // wrestling_stats\utilities\raw_sql\discovery_team_2024_25_champs.sql
  const sql = `
    INSERT INTO wrestler_state_qualifier_and_place_reference (
        wrestling_season,
        track_wrestling_category,

        wrestler_id,
        wrestler_name,
        wrestler_first_name,
        wrestler_last_name,
        wrestler_grade,
        weight_category,

        record,
        wins_all_run,
        losses_all_run,
        ties_all_run,
        total_matches,
        total_matches_win_pct,
        winner_name,

        event,
        round,
        outcome,

        is_state_tournament_qualifier,
        state_tournament_place,

        raw_details,

        count_distinct,
        count_matches,

        created_at_mtn,
        created_at_utc,
        updated_at_mtn,
        updated_at_utc
    )
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
                WHEN wrestling_season = '2024-25' THEN 'state_qualifier_2025'
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

    -- For each wrestler, find their *final* state match,
    -- preferring placement matches if they exist
    max_match AS (
        SELECT
            wrestling_season,
            track_wrestling_category,
            wrestler_id,

            COALESCE(
              -- Prefer 1st Place Match if it exists
              MAX(CASE WHEN round LIKE '%1st Place Match%' THEN match_order END),
              -- Otherwise 3rd Place Match
              MAX(CASE WHEN round LIKE '%3rd Place Match%' THEN match_order END),
              -- Otherwise 5th Place Match
              MAX(CASE WHEN round LIKE '%5th Place Match%' THEN match_order END),
              -- Otherwise just use the highest match_order
              MAX(match_order)
            ) AS max_match_order,

            COUNT(DISTINCT wrestler_id) AS count_distinct,
            COUNT(*)         AS count_matches
        FROM determine_state_qualifier_and_place
        GROUP BY
            wrestling_season,
            track_wrestling_category,
            wrestler_id
    ),

    -- Keep only that final (max match_order) row per wrestler
    per_wrestler AS (
        SELECT
            d.*,
            m.count_distinct,
            m.count_matches
        FROM determine_state_qualifier_and_place d
        JOIN max_match m
          ON  d.wrestling_season         = m.wrestling_season
          AND d.track_wrestling_category = m.track_wrestling_category
          AND d.wrestler_id              = m.wrestler_id
          AND d.match_order              = m.max_match_order
    )

    SELECT
        p.wrestling_season,
        p.track_wrestling_category,

        p.wrestler_id,
        p.wrestler_name,
        p.wrestler_first_name,
        p.wrestler_last_name,
        p.wrestler_grade,
        p.weight_category,
        
        p.record,
        p.wins_all_run,
        p.losses_all_run,
        p.ties_all_run,
        p.total_matches,
        p.total_matches_win_pct,
        p.winner_name,

        p.event,
        p.round,
        p.outcome,

        p.is_state_tournament_qualifier,
        p.state_tournament_place,

        p.raw_details,

        p.count_distinct,
        p.count_matches,

        ?  AS created_at_mtn,
        ?  AS created_at_utc,
        ?  AS updated_at_mtn,
        ?  AS updated_at_utc
    FROM per_wrestler p
    -- no ORDER BY needed inside INSERT

    ON DUPLICATE KEY UPDATE

      -- Only bump updated_* if any tracked column actually changed (NULL-safe)
      updated_at_mtn =
        CASE
          WHEN NOT (
            wrestler_state_qualifier_and_place_reference.wrestling_season         <=> VALUES(wrestling_season) AND
            wrestler_state_qualifier_and_place_reference.track_wrestling_category <=> VALUES(track_wrestling_category) AND
            wrestler_state_qualifier_and_place_reference.wrestler_id              <=> VALUES(wrestler_id) AND
            wrestler_state_qualifier_and_place_reference.wrestler_name            <=> VALUES(wrestler_name) AND
            wrestler_state_qualifier_and_place_reference.wrestler_first_name      <=> VALUES(wrestler_first_name) AND
            wrestler_state_qualifier_and_place_reference.wrestler_last_name       <=> VALUES(wrestler_last_name) AND
            wrestler_state_qualifier_and_place_reference.wrestler_grade           <=> VALUES(wrestler_grade) AND
            wrestler_state_qualifier_and_place_reference.weight_category          <=> VALUES(weight_category) AND
            wrestler_state_qualifier_and_place_reference.record                   <=> VALUES(record) AND
            wrestler_state_qualifier_and_place_reference.wins_all_run             <=> VALUES(wins_all_run) AND
            wrestler_state_qualifier_and_place_reference.losses_all_run           <=> VALUES(losses_all_run) AND
            wrestler_state_qualifier_and_place_reference.ties_all_run             <=> VALUES(ties_all_run) AND
            wrestler_state_qualifier_and_place_reference.total_matches            <=> VALUES(total_matches) AND
            wrestler_state_qualifier_and_place_reference.total_matches_win_pct    <=> VALUES(total_matches_win_pct) AND
            wrestler_state_qualifier_and_place_reference.winner_name              <=> VALUES(winner_name) AND
            wrestler_state_qualifier_and_place_reference.event                    <=> VALUES(event) AND
            wrestler_state_qualifier_and_place_reference.round                    <=> VALUES(round) AND
            wrestler_state_qualifier_and_place_reference.outcome                  <=> VALUES(outcome) AND
            wrestler_state_qualifier_and_place_reference.is_state_tournament_qualifier <=> VALUES(is_state_tournament_qualifier) AND
            wrestler_state_qualifier_and_place_reference.state_tournament_place   <=> VALUES(state_tournament_place) AND
            wrestler_state_qualifier_and_place_reference.raw_details              <=> VALUES(raw_details) AND
            wrestler_state_qualifier_and_place_reference.count_distinct           <=> VALUES(count_distinct) AND
            wrestler_state_qualifier_and_place_reference.count_matches            <=> VALUES(count_matches)
          )
          THEN VALUES(updated_at_mtn)
          ELSE wrestler_state_qualifier_and_place_reference.updated_at_mtn
        END,

      updated_at_utc =
        CASE
          WHEN NOT (
            wrestler_state_qualifier_and_place_reference.wrestling_season         <=> VALUES(wrestling_season) AND
            wrestler_state_qualifier_and_place_reference.track_wrestling_category <=> VALUES(track_wrestling_category) AND
            wrestler_state_qualifier_and_place_reference.wrestler_id              <=> VALUES(wrestler_id) AND
            wrestler_state_qualifier_and_place_reference.wrestler_name            <=> VALUES(wrestler_name) AND
            wrestler_state_qualifier_and_place_reference.wrestler_first_name      <=> VALUES(wrestler_first_name) AND
            wrestler_state_qualifier_and_place_reference.wrestler_last_name       <=> VALUES(wrestler_last_name) AND
            wrestler_state_qualifier_and_place_reference.wrestler_grade           <=> VALUES(wrestler_grade) AND
            wrestler_state_qualifier_and_place_reference.weight_category          <=> VALUES(weight_category) AND
            wrestler_state_qualifier_and_place_reference.record                   <=> VALUES(record) AND
            wrestler_state_qualifier_and_place_reference.wins_all_run             <=> VALUES(wins_all_run) AND
            wrestler_state_qualifier_and_place_reference.losses_all_run           <=> VALUES(losses_all_run) AND
            wrestler_state_qualifier_and_place_reference.ties_all_run             <=> VALUES(ties_all_run) AND
            wrestler_state_qualifier_and_place_reference.total_matches            <=> VALUES(total_matches) AND
            wrestler_state_qualifier_and_place_reference.total_matches_win_pct    <=> VALUES(total_matches_win_pct) AND
            wrestler_state_qualifier_and_place_reference.winner_name              <=> VALUES(winner_name) AND
            wrestler_state_qualifier_and_place_reference.event                    <=> VALUES(event) AND
            wrestler_state_qualifier_and_place_reference.round                    <=> VALUES(round) AND
            wrestler_state_qualifier_and_place_reference.outcome                  <=> VALUES(outcome) AND
            wrestler_state_qualifier_and_place_reference.is_state_tournament_qualifier <=> VALUES(is_state_tournament_qualifier) AND
            wrestler_state_qualifier_and_place_reference.state_tournament_place   <=> VALUES(state_tournament_place) AND
            wrestler_state_qualifier_and_place_reference.raw_details              <=> VALUES(raw_details) AND
            wrestler_state_qualifier_and_place_reference.count_distinct           <=> VALUES(count_distinct) AND
            wrestler_state_qualifier_and_place_reference.count_matches            <=> VALUES(count_matches)
          )
          THEN CURRENT_TIMESTAMP
          ELSE wrestler_state_qualifier_and_place_reference.updated_at_utc
        END,

      -- If unique key hits (season, category, wrestler_id), update these fields:
      wrestler_state_qualifier_and_place_reference.wrestling_season         = VALUES(wrestling_season),
      wrestler_state_qualifier_and_place_reference.track_wrestling_category = VALUES(track_wrestling_category),
      wrestler_state_qualifier_and_place_reference.wrestler_id              = VALUES(wrestler_id),
      wrestler_state_qualifier_and_place_reference.wrestler_name            = VALUES(wrestler_name),
      wrestler_state_qualifier_and_place_reference.wrestler_first_name      = VALUES(wrestler_first_name),
      wrestler_state_qualifier_and_place_reference.wrestler_last_name       = VALUES(wrestler_last_name),
      wrestler_state_qualifier_and_place_reference.wrestler_grade           = VALUES(wrestler_grade),
      wrestler_state_qualifier_and_place_reference.weight_category          = VALUES(weight_category),
      wrestler_state_qualifier_and_place_reference.record                   = VALUES(record),
      wrestler_state_qualifier_and_place_reference.wins_all_run             = VALUES(wins_all_run),
      wrestler_state_qualifier_and_place_reference.losses_all_run           = VALUES(losses_all_run),
      wrestler_state_qualifier_and_place_reference.ties_all_run             = VALUES(ties_all_run),
      wrestler_state_qualifier_and_place_reference.total_matches            = VALUES(total_matches),
      wrestler_state_qualifier_and_place_reference.total_matches_win_pct    = VALUES(total_matches_win_pct),
      wrestler_state_qualifier_and_place_reference.winner_name              = VALUES(winner_name),
      wrestler_state_qualifier_and_place_reference.event                    = VALUES(event),
      wrestler_state_qualifier_and_place_reference.round                    = VALUES(round),
      wrestler_state_qualifier_and_place_reference.outcome                  = VALUES(outcome),
      wrestler_state_qualifier_and_place_reference.is_state_tournament_qualifier = VALUES(is_state_tournament_qualifier),
      wrestler_state_qualifier_and_place_reference.state_tournament_place   = VALUES(state_tournament_place),
      wrestler_state_qualifier_and_place_reference.raw_details              = VALUES(raw_details),
      wrestler_state_qualifier_and_place_reference.count_distinct           = VALUES(count_distinct),
      wrestler_state_qualifier_and_place_reference.count_matches            = VALUES(count_matches)
  `;

  const params = [
    created_at_mtn,
    created_at_utc,
    updated_at_mtn,
    updated_at_utc,
  ];

  const [res] = await pool.query(sql, params);

  const affected = Number(res.affectedRows || 0);
  inserted += affected;

  return { inserted, updated };
}

// For your standalone script usage:
// upsert_state_qualifier_reference().then(r => {
//   console.log("upsert_state_qualifier_reference:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("upsert_state_qualifier_reference error:", err);
//   process.exit(1);
// });


// upsert_wrestler_team_info();

export { upsert_state_qualifier_reference };

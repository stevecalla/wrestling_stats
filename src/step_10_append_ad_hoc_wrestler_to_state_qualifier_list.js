// src/step_4_create_wrestler_match_history_metrics.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

// QUERIES
// included below

// These are the wrestlers you want to manually append to the state qualifier list
const updates = [
  { wrestler_id: 29790065132, wrestling_season: "2024-25", track_wrestling_category: "High School Boys" },
];

async function step_10_append_ad_hoc_wrestler_to_state_qualifier_list() {
  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  for (const row of updates) {
    const { wrestler_id, wrestling_season, track_wrestling_category } = row;

    // Insert the single row with the MAX(match_order) for this wrestler/season/category
    await pool.query(
      `
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
      SELECT
        m.wrestling_season,
        m.track_wrestling_category,

        m.wrestler_id,
        m.wrestler_name,
        m.wrestler_first_name,
        m.wrestler_last_name,
        m.wrestler_grade,
        m.weight_category,

        m.record,
        m.wins_all_run,
        m.losses_all_run,
        m.ties_all_run,
        m.total_matches,
        m.total_matches_win_pct,
        m.winner_name,

        m.event,
        m.round,
        m.outcome,

        NULL AS is_state_tournament_qualifier,
        NULL AS state_tournament_place,

        m.raw_details,

        1 AS count_distinct,
        1 AS count_matches,

        m.created_at_mtn,
        m.created_at_utc,
        m.updated_at_mtn,
        m.updated_at_utc

      FROM wrestler_match_history_metrics_data AS m

      WHERE m.wrestler_id = ?
        AND m.wrestling_season = ?
        AND m.track_wrestling_category = ?

      ORDER BY m.match_order DESC
      LIMIT 1
      
      ON DUPLICATE KEY UPDATE
        wrestler_name             = VALUES(wrestler_name),
        wrestler_first_name       = VALUES(wrestler_first_name),
        wrestler_last_name        = VALUES(wrestler_last_name),
        wrestler_grade            = VALUES(wrestler_grade),
        weight_category           = VALUES(weight_category),

        record                    = VALUES(record),
        wins_all_run              = VALUES(wins_all_run),
        losses_all_run            = VALUES(losses_all_run),
        ties_all_run              = VALUES(ties_all_run),
        total_matches             = VALUES(total_matches),
        total_matches_win_pct     = VALUES(total_matches_win_pct),
        winner_name               = VALUES(winner_name),

        event                     = VALUES(event),
        round                     = VALUES(round),
        outcome                   = VALUES(outcome),

        raw_details               = VALUES(raw_details),

        count_distinct            = VALUES(count_distinct),
        count_matches             = VALUES(count_matches),

        updated_at_mtn            = VALUES(updated_at_mtn),
        updated_at_utc            = VALUES(updated_at_utc);
      `,
      [wrestler_id, wrestling_season, track_wrestling_category]
    );

    console.log(
      `Inserted/updated ad-hoc wrestler ${wrestler_id} for ${wrestling_season} / ${track_wrestling_category}`
    );
  }

  console.log("Ad-hoc wrestler state qualifier list updates complete");
}

// await step_10_append_ad_hoc_wrestler_to_state_qualifier_list();

export { step_10_append_ad_hoc_wrestler_to_state_qualifier_list };


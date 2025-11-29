// C:\Users\calla\development\projects\wrestling_stats\utilities\raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql

import { get_pool } from "../../mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../date_time_tools/get_mountain_time_offset_hours.js";

async function step_6_apply_2025_flags_to_2026_match_metrics() {
  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  let reset_rows     = 0;
  let updated_w_state = 0;
  let updated_o_state = 0;
  let updated_w_team  = 0;
  let updated_o_team  = 0;

  // 0) Optional: reset 2025-26 metrics fields to NULL
  const reset_sql = `
    UPDATE wrestler_match_history_metrics_data
    SET
        wrestler_team_division = NULL,
        wrestler_team_region = NULL,
        wrestler_is_state_tournament_qualifier = NULL,
        wrestler_state_tournament_place = NULL,
        opponent_team_division = NULL,
        opponent_team_region = NULL,
        opponent_is_state_tournament_qualifier = NULL,
        opponent_state_tournament_place = NULL
    WHERE wrestling_season = '2025-26'
      AND (
            wrestler_team_division IS NOT NULL
         OR wrestler_team_region IS NOT NULL
         OR wrestler_is_state_tournament_qualifier IS NOT NULL
         OR wrestler_state_tournament_place IS NOT NULL
         OR opponent_team_division IS NOT NULL
         OR opponent_team_region IS NOT NULL
         OR opponent_is_state_tournament_qualifier IS NOT NULL
         OR opponent_state_tournament_place IS NOT NULL
      );
  `;
  const [resReset] = await pool.query(reset_sql);
  reset_rows = Number(resReset.affectedRows || 0);

  // 1) Wrestler side: state qualifier/place
  const update_w_state_sql = `
    UPDATE wrestler_match_history_metrics_data m
    JOIN reference_wrestler_2026_state_qualifier_flags r
          ON m.wrestler_id = r.wrestler_id_2026
    SET
        m.wrestler_is_state_tournament_qualifier = r.wrestler_is_state_tournament_qualifier_2025,
        m.wrestler_state_tournament_place        = r.wrestler_state_tournament_place_2025,
        m.updated_at_mtn = ?,
        m.updated_at_utc = ?
    WHERE m.wrestling_season = '2025-26';
  `;
  const [resWState] = await pool.query(update_w_state_sql, [
    updated_at_mtn,
    updated_at_utc,
  ]);
  updated_w_state = Number(resWState.affectedRows || 0);

  // 2) Opponent side: state qualifier/place
  const update_o_state_sql = `
    UPDATE wrestler_match_history_metrics_data m
    JOIN reference_wrestler_2026_state_qualifier_flags r
          ON m.opponent_id = r.wrestler_id_2026
    SET
        m.opponent_is_state_tournament_qualifier = r.wrestler_is_state_tournament_qualifier_2025,
        m.opponent_state_tournament_place        = r.wrestler_state_tournament_place_2025,
        m.updated_at_mtn = ?,
        m.updated_at_utc = ?
    WHERE m.wrestling_season = '2025-26';
  `;
  const [resOState] = await pool.query(update_o_state_sql, [
    updated_at_mtn,
    updated_at_utc,
  ]);
  updated_o_state = Number(resOState.affectedRows || 0);

  // 3) Wrestler side: team division/region
  const update_w_team_sql = `
    UPDATE wrestler_match_history_metrics_data m
    JOIN reference_wrestler_2026_team_division_flags r
          ON m.wrestler_team_id = r.team_id_2026
    SET
        m.wrestler_team_division = r.team_division_2025,
        m.wrestler_team_region   = r.team_region_2025,
        m.updated_at_mtn = ?,
        m.updated_at_utc = ?
    WHERE m.wrestling_season = '2025-26';
  `;
  const [resWTeam] = await pool.query(update_w_team_sql, [
    updated_at_mtn,
    updated_at_utc,
  ]);
  updated_w_team = Number(resWTeam.affectedRows || 0);

  // 4) Opponent side: team division/region
  const update_o_team_sql = `
    UPDATE wrestler_match_history_metrics_data m
    JOIN reference_wrestler_2026_team_division_flags r
          ON m.opponent_team_id = r.team_id_2026
    SET
        m.opponent_team_division = r.team_division_2025,
        m.opponent_team_region   = r.team_region_2025,
        m.updated_at_mtn = ?,
        m.updated_at_utc = ?
    WHERE m.wrestling_season = '2025-26';
  `;
  const [resOTeam] = await pool.query(update_o_team_sql, [
    updated_at_mtn,
    updated_at_utc,
  ]);
  updated_o_team = Number(resOTeam.affectedRows || 0);

  return {
    reset_rows,
    updated_w_state,
    updated_o_state,
    updated_w_team,
    updated_o_team,
  };
}

// For your standalone script usage:
// step_6_apply_2025_flags_to_2026_match_metrics().then(r => {
//   console.log("step_6_apply_2025_flags_to_2026_match_metrics:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("step_6_apply_2025_flags_to_2026_match_metrics error:", err);
//   process.exit(1);
// });

export { step_6_apply_2025_flags_to_2026_match_metrics };

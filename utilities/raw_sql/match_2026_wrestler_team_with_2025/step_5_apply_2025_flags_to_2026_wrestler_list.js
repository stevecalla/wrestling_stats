// C:\Users\calla\development\projects\wrestling_stats\utilities\raw_sql\match_2026_wrestler_team_with_2025\match_2026_wrestler_team_with_2025.sql

import { get_pool } from "../../mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../date_time_tools/get_mountain_time_offset_hours.js";

async function step_5_apply_2025_flags_to_2026_wrestler_list() {
  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  let updated_state = 0;
  let updated_team  = 0;

  // 1) Push 2025 state qualifier/place onto 2026 wrestlers
  const update_state_sql = `
    UPDATE wrestler_list_scrape_data w
        JOIN reference_wrestler_2026_state_qualifier_flags r ON w.wrestler_id = r.wrestler_id_2026
    SET
        w.wrestler_is_state_tournament_qualifier = r.wrestler_is_state_tournament_qualifier_2025,
        w.wrestler_state_tournament_place        = r.wrestler_state_tournament_place_2025,
        w.updated_at_mtn = ?,
        w.updated_at_utc = ?
    WHERE w.wrestling_season = '2025-26';
  `;

  const [resState] = await pool.query(update_state_sql, [
    updated_at_mtn,
    updated_at_utc,
  ]);
  updated_state = Number(resState.affectedRows || 0);

  // 2) Push 2025 team division/region onto 2026 wrestlers
  const update_team_sql = `
    UPDATE wrestler_list_scrape_data w
        JOIN reference_wrestler_2026_team_division_flags r ON w.team_id = r.team_id_2026
    SET
        w.team_division = r.team_division_2025,
        w.team_region   = r.team_region_2025,
        w.updated_at_mtn = ?,
        w.updated_at_utc = ?
    WHERE w.wrestling_season = '2025-26';
  `;

  const [resTeam] = await pool.query(update_team_sql, [
    updated_at_mtn,
    updated_at_utc,
  ]);
  updated_team = Number(resTeam.affectedRows || 0);

  return {
    updated_state,
    updated_team,
  };
}

// For your standalone script usage:
// step_5_apply_2025_flags_to_2026_wrestler_list().then(r => {
//   console.log("step_5_apply_2025_flags_to_2026_wrestler_list:", r);
//   process.exit(0);
// }).catch(err => {
//   console.error("step_5_apply_2025_flags_to_2026_wrestler_list error:", err);
//   process.exit(1);
// });

export { step_5_apply_2025_flags_to_2026_wrestler_list };

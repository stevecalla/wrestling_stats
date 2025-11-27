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

const updates = [
  {wrestler_team_id: 1596622147, wrestling_season: "2024-25", track_wrestling_category: "High School Boys", team_division: "unknown", team_region: "unknown"},
];

async function step_6_append_team_division_updates() {
    
  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  for (const team of updates) {
    await pool.query(
      `
      UPDATE wrestler_team_division_reference
      SET 
        team_division = ?, 
        team_region = ?,
        updated_at_mtn = ?,
        updated_at_utc = ?

      WHERE wrestler_team_id = ?
        AND wrestling_season = ?
        AND track_wrestling_category = ?
      `,
      [
        // order must match the order in the update query
        team.team_division,
        team.team_region,
        updated_at_mtn,
        updated_at_utc,
        team.wrestler_team_id,
        team.wrestling_season,
        team.track_wrestling_category
      ]
    );
  }

  console.log("team division/region updates complete");
}

// await step_6_append_team_division_updates();

export { step_6_append_team_division_updates };

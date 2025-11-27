// src/step_11_append_state_qualifier_to_match_metrics.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

async function ensure_team_columns(pool) {
  const alters = [
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN wrestler_is_state_tournament_qualifier VARCHAR(50) NULL AFTER raw_details
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN wrestler_state_tournament_place VARCHAR(50) NULL AFTER wrestler_is_state_tournament_qualifier
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN opponent_is_state_tournament_qualifier VARCHAR(50) NULL AFTER wrestler_state_tournament_place
    `,
    `
      ALTER TABLE wrestler_match_history_metrics_data
        ADD COLUMN opponent_state_tournament_place VARCHAR(50) NULL AFTER opponent_is_state_tournament_qualifier
    `,
  ];

  for (const sql of alters) {
    try {
      await pool.query(sql);
    } catch (err) {
      // Ignore "duplicate column" errors; rethrow anything else
      if (err.code === "ER_DUP_FIELDNAME") {
        continue;
      }
      throw err;
    }
  }
}

async function step_11_append_state_qualifier_to_match_metrics() {
  const pool = await get_pool();

  // 1) Ensure columns exist (safe to run multiple times)
  await ensure_team_columns(pool);

  // 2) Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  // 3) Update metric rows from the reference table
  const sql = `
    UPDATE wrestler_match_history_metrics_data m

    -- JOIN for wrestler
    LEFT JOIN wrestler_state_qualifier_and_place_reference r_w ON  r_w.wrestler_id         = m.wrestler_id
      AND r_w.wrestling_season         = m.wrestling_season
      AND r_w.track_wrestling_category = m.track_wrestling_category

    -- JOIN for opponent
    LEFT JOIN wrestler_state_qualifier_and_place_reference r_o ON  r_o.wrestler_id         = m.opponent_id
      AND r_o.wrestling_season         = m.wrestling_season
      AND r_o.track_wrestling_category = m.track_wrestling_category

    SET
      -- wrestler team fields
      m.wrestler_is_state_tournament_qualifier = r_w.is_state_tournament_qualifier,
      m.wrestler_state_tournament_place   = r_w.state_tournament_place,

      -- opponent team fields
      m.opponent_is_state_tournament_qualifier = r_o.is_state_tournament_qualifier,
      m.opponent_state_tournament_place   = r_o.state_tournament_place,

      -- timestamps
      m.updated_at_mtn         = ?,
      m.updated_at_utc         = ?
  `;

  const [result] = await pool.query(sql, [updated_at_mtn, updated_at_utc]);

  console.log(
    "state qualifier & place updates complete 🔗",
    "affectedRows =", result.affectedRows,
    "changedRows =", result.changedRows
  );

  console.log("state qualifier & place updates complete 🔗");
}

// await step_11_append_state_qualifier_to_match_metrics();

export { step_11_append_state_qualifier_to_match_metrics };

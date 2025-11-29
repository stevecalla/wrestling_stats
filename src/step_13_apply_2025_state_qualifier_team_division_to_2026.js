// src\step_13_apply_2025_state_qualifier_team_division_to_2026.js

// import path from "path";
// import { fileURLToPath } from "url";

// import dotenv from "dotenv";
// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);
// dotenv.config({ path: path.resolve(__dirname, "../.env") });

// QUERIES
import { step_1_create_reference_team_alias_map } from "../utilities/raw_sql/match_2026_wrestler_team_with_2025/step_1_query_create_reference_team_alias_map.js";
import { step_2_create_reference_wrestler_cross_season_summary } from "../utilities/raw_sql/match_2026_wrestler_team_with_2025/step_2_query_create_reference_wrestler_cross_season_summary.js";
import { step_3_create_reference_wrestler_2026_state_qualifier_flags } from "../utilities/raw_sql/match_2026_wrestler_team_with_2025/step_3_query_create_reference_wrestler_2026_state_qualifier_flags.js";
import { step_4_create_reference_wrestler_2026_team_division_flags } from "../utilities/raw_sql/match_2026_wrestler_team_with_2025/step_4_query_create_reference_wrestler_2026_team_division_flags.js";
import { step_5_apply_2025_flags_to_2026_wrestler_list } from "../utilities/raw_sql/match_2026_wrestler_team_with_2025/step_5_apply_2025_flags_to_2026_wrestler_list.js";
import { step_6_apply_2025_flags_to_2026_match_metrics } from "../utilities/raw_sql/match_2026_wrestler_team_with_2025/step_6_apply_2025_flags_to_2026_match_metrics.js";

async function step_13_apply_2025_state_qualifier_team_division_to_2026() {

  // 1) CREATE REFERENCE TABLE TO BE ABLE TO MATCH WRESTLERS WITH TEAM NAMES THAT CHANGED VS PRIOR SEASON
  await step_1_create_reference_team_alias_map();
  console.log("✅ step_1_create_reference_team_alias_map complete");

  // 2) MATCH 2025 & 2026 WRESTLERS & USE ...team_alias_map TABLE TO ADDRESS CHANGES IN TEAM NAMES SEASON TO SEASON
  await step_2_create_reference_wrestler_cross_season_summary();
  console.log("✅ step_2_create_reference_wrestler_cross_season_summary complete");

  // 3) CREATE 2026 WRESTLER ID WITH 2025 STATE QUALIFIER & PLACE INFO
  await step_3_create_reference_wrestler_2026_state_qualifier_flags();
  console.log("✅ step_3_create_reference_wrestler_2026_state_qualifier_flags complete");

  // 4) CREATE 2026 TEAM ID WITH 2025 TEAM DIVISION & REGION INFO
  await step_4_create_reference_wrestler_2026_team_division_flags();
  console.log("✅ step_4_create_reference_wrestler_2026_team_division_flags complete");

  // 5) APPLY 2025 STATE QUALIFIER & TEAM DIVISION FLAGS TO 2026 WRESTLERS LIST TABLE
  await step_5_apply_2025_flags_to_2026_wrestler_list();
  console.log("✅ step_5_apply_2025_flags_to_2026_wrestler_list complete");

  // 6) APPLY 2025 STATE QUALIFIER & TEAM DIVISION FLAGS TO 2026 MATCH HISTORY METRICS DATA TABLE
  await step_6_apply_2025_flags_to_2026_match_metrics();
  console.log("✅ step_6_apply_2025_flags_to_2026_match_metrics complete")

  console.log("✅ apply 2025 state qualifier team division to 2026 complete 🔗");
}

// await step_13_apply_2025_state_qualifier_team_division_to_2026();

export { step_13_apply_2025_state_qualifier_team_division_to_2026 };

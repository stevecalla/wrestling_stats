// src\step_5_team_division_data.js

// QUERIES
import { upsert_wrestler_team_info } from "../utilities/raw_sql/create_drop_table_queries/query_create_team_division_region.js";

async function step_5_create_team_division_data() {
    try {

        const result = await upsert_wrestler_team_info();
        console.log("upsert_wrestler_team_info result:", result);
        
        // process.exit(0);

    } catch (err) {
        console.error("upsert_wrestler_team_info error:", err);

        // process.exit(1);
    }
}

export { step_5_create_team_division_data };

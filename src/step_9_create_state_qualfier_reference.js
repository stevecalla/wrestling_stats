// src\step_9_create_state_qualfier_reference.js

// QUERIES
import { upsert_state_qualifier_reference } from "../utilities/raw_sql/create_drop_table_queries/query_create_state_qualifier_reference.js";

async function step_9_create_state_qualfier_reference() {
    try {

        const result = await upsert_state_qualifier_reference();
        console.log("upsert_state_qualifier_reference result:", result);
        
        // process.exit(0);

    } catch (err) {
        console.error("upsert_state_qualifier_reference error:", err);

        // process.exit(1);
    }
}

export { step_9_create_state_qualfier_reference };

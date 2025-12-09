// src/step_4_create_wrestler_match_history_metrics.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";

// QUERIES
import { query_create_wrestler_ids_table } from "../utilities/raw_sql/create_drop_table_queries/query_create_wrestler_ids_table.js";
import { step_1_query_wrestler_ids_data } from "../utilities/raw_sql/wrestler_match_history/step_1_get_wrestler_ids_data.js";

import { query_create_wrestler_match_history_metrics_table } from "../utilities/raw_sql/create_drop_table_queries/query_create_wrestler_match_history_metrics_table.js";
import { step_2_create_wrestler_history_match_metrics_data } from "../utilities/raw_sql/wrestler_match_history/step_2_create_wrestler_history_match_metrics_data.js";

// TRANSFER FUNCTION
import { execute_transfer_data_between_tables } from "../utilities/mysql/transfer_local_data_between_local_tables/1_transfer_data_between_local_tables.js";

async function step_4_create_wrestler_match_history_data(config) {
    const { governing_body, wrestling_season, track_wrestling_category } = config;

    let batch_size = 500;
    let table_name = "";
    let create_table_query = "";
    let get_data_query = "";
    const is_not_test = true;
    let result = "";
    let count = 0;

    // VARIABLES
    let QUERY_OPTIONS = {
        governing_body,
        wrestling_season,
        track_wrestling_category,
        is_create_table: true,
        drop_if_exists: false, // 👈 DO NOT drop metrics table
        insert_ignore: true,   // 👈 we’ll use to ignore duplicates in flush_batch function for metrics table
    };

    // Step 1: Create and populate the wrestler IDs table
    // IDs table: drop_if_exists: true; insert_ignore: false (or omitted)
    // Table rebuilt every run; no long-term dedupe needed.
    // NOTE: only run is_not_test === true
    if (is_not_test) {
        batch_size = 2000;
        table_name = `wrestler_match_history_wrestler_ids_data`;
        create_table_query = await query_create_wrestler_ids_table(table_name);
        get_data_query = step_1_query_wrestler_ids_data; // pass function forward

        QUERY_OPTIONS = {
            ...QUERY_OPTIONS,       // keep existing keys
            drop_if_exists: true,   // 👈 scratch table, always rebuild
            insert_ignore: false,   // doesn’t really matter here b/c step_1_query_wrestler_ids_data uses DISTINCT; used in step #2 below
        };

        // CREATE TABLE & GET / TRANSFER DATA
        const { result, row_count } = await execute_transfer_data_between_tables(batch_size, table_name, create_table_query, get_data_query, QUERY_OPTIONS);

        count = row_count;
        console.log(result, row_count);
    }

    // Step 2: Loop in batches and insert into the base table
    // Metrics table: drop_if_exists: false; insert_ignore: true; PRIMARY KEY (id) ensures each match is unique.
    // Running: girls 24–25, boys 24–25, boys 25–26, girls 25–25, adds rows for each group.
    if (is_not_test) {
        const pool = await get_pool();
        const conn = await pool.getConnection();
        try {
            console.log(
                `[METRICS] Deleting existing rows for ${wrestling_season} / ${track_wrestling_category} / ${governing_body}`
            );
            await conn.execute(
                `
        DELETE FROM wrestler_match_history_metrics_data
        WHERE wrestling_season = ?
            AND track_wrestling_category = ?
            AND governing_body = ?
        `,
                [wrestling_season, track_wrestling_category, governing_body]
            );
        } finally {
            conn.release();
        }
    }

    // Step 3: Loop in batches and insert into the base table
    // Metrics table: drop_if_exists: false; insert_ignore: true; PRIMARY KEY (id) ensures each match is unique.
    // Running: girls 24–25, boys 24–25, boys 25–26, girls 25–25, adds rows for each group.
    // If you rerun a group or reprocess overlapping Wrestlers/IDs, any existing id rows are ignored, so no duplicates.
    batch_size = 500;
    table_name = 'wrestler_match_history_metrics_data';
    create_table_query = await query_create_wrestler_match_history_metrics_table(table_name);
    get_data_query = step_2_create_wrestler_history_match_metrics_data;

    const LIMIT_SIZE = 1000;
    count = is_not_test ? count : 1; // NOTE: only run loop once is_not_test = false

    // 🔧 RESET FLAGS FOR METRICS TABLE HERE SINCE STEP 1 MODIFIED
    QUERY_OPTIONS = {
        ...QUERY_OPTIONS,
        is_create_table: true,   // OK to always true, CREATE TABLE IF NOT EXISTS is a no-op if it exists
        drop_if_exists: false,   // ✅ NEVER drop metrics table
        insert_ignore: true,     // ✅ skip duplicate ids in metrics
    };

    let total_inserted = 0;

    for (let offset = 0; offset < count; offset += LIMIT_SIZE) {
        QUERY_OPTIONS = {
            ...QUERY_OPTIONS,
            limit_size: LIMIT_SIZE,
            offset_size: offset,
        };

        const { result: batch_result, row_count: batch_count } =
            await execute_transfer_data_between_tables(
                batch_size,
                table_name,
                create_table_query,
                get_data_query,
                QUERY_OPTIONS
            );

        console.log(
            `[METRICS] offset=${offset} limit=${LIMIT_SIZE} -> inserted ${batch_count} rows`
        );
        total_inserted += batch_count;
    }

    console.log(
        `[METRICS] total inserted into ${table_name} for ${wrestling_season} / ${track_wrestling_category}: ${total_inserted}`
    );

    return { result: 'ok', row_count: total_inserted };
}

// execute_create_recognition_base_data().catch(err => {
//     console.error('Stream failed:', err);
//     process.exit(1);
// });

export {
    step_4_create_wrestler_match_history_data,
}

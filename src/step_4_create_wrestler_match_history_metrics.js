// src/step_4_create_wrestler_match_history_metrics.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

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
    };

    // Step 1: Create and populate the wrestler IDs table
    // NOTE: only run is_not_test === true
    if (is_not_test) {
        batch_size = 2000;
        table_name = `wrestler_match_history_wrestler_ids_data`;
        create_table_query = await query_create_wrestler_ids_table(table_name);
        get_data_query = step_1_query_wrestler_ids_data; // pass function forward

        // CREATE TABLE & GET / TRANSFER DATA
        const { result, row_count } = await execute_transfer_data_between_tables(batch_size, table_name, create_table_query, get_data_query, QUERY_OPTIONS);

        count = row_count;
        console.log(result, row_count);
    }

    // Step 2: Loop in batches and insert into the base table
    batch_size = 500;
    table_name = 'wrestler_match_history_metrics_data';
    create_table_query = await query_create_wrestler_match_history_metrics_table(table_name);
    get_data_query = step_2_create_wrestler_history_match_metrics_data;

    // await execute_transfer_data_between_tables(batch_size, table_name, create_table_query, get_data_query, QUERY_OPTIONS);

    const LIMIT_SIZE = 1000;
    count = is_not_test ? count : 1; // NOTE: only run loop once is_not_test = false

    for (let offset = 0; offset < count; offset += LIMIT_SIZE) {

        let is_create_table = offset === 0 ? true : false;

        QUERY_OPTIONS = {
            ...QUERY_OPTIONS,
            limit_size: LIMIT_SIZE,
            offset_size: offset,
            is_create_table: is_create_table,
        };

        result = await execute_transfer_data_between_tables(batch_size, table_name, create_table_query, get_data_query, QUERY_OPTIONS);
    }

    return result;
}

// execute_create_recognition_base_data().catch(err => {
//     console.error('Stream failed:', err);
//     process.exit(1);
// });

export {
    step_4_create_wrestler_match_history_data,
}

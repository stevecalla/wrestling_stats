import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { determine_os_path } from "../../directory_tools/determine_os_path.js";
import { create_directory } from "../../directory_tools/create_directory.js";

import { get_pool, get_pool_stream } from "../../mysql/mysql_pool.js";

import { stream_query_to_csv } from "../../streaming/stream_query_to_csv.js";

import { get_current_date_for_file_naming } from "../../date_time_tools/get_current_datetime.js";
import { start_timer, stop_timer } from "../../stop_watch_timer/timer.js";
import { trigger_garbage_collection } from "../../garbage_collection/trigger_garbage_collection.js";

// STEP #1 - DELETE ARCHIVED FILES
async function delete_archived_files(directory_name) {
    console.log('Deleting files from archive');

    // Create the "archive" directory if it doesn't exist
    const directory_path = await create_directory(`${directory_name}_archive`);

    // List all files in the directory
    const files = fs.readdirSync(directory_path);
    console.log(files);

    // Iterate through each file
    files?.forEach((file) => {
        if (file.endsWith('.csv')) {
            // Construct the full file path
            const filePath = `${directory_path}/${file}`;
            console.log(filePath);

            try {
                // Delete the file
                fs.unlinkSync(filePath);
                console.log(`File ${filePath} deleted successfully.`);
            } catch (deleteErr) {
                console.error(`Error deleting file ${filePath}:`, deleteErr);
            }
        }
    });
}

// STEP #2 - MOVE FILES TO ARCHIVE
async function move_files_to_archive(directory_name) {
    console.log('Moving files to archive');

    const os_path = await determine_os_path();

    try {
        // List all files in the directory
        // const source_path = `${os_path}\${directory_name}`;
        const source_path = path.join(os_path, directory_name);
        await create_directory(directory_name);
        
        const files = fs.readdirSync(source_path);
        console.log(files);

        // Create the "archive" directory if it doesn't exist
        const destinationPath = await create_directory(`${directory_name}_archive`);
        console.log(destinationPath);

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${source_path}/${file}`;
                const destinationFilePath = `${destinationPath}/${file}`;

                try {
                    // Move the file to the "archive" directory
                    fs.renameSync(sourceFilePath, destinationFilePath);
                    console.log(`Archived ${file}`);;
                } catch (archiveErr) {
                    console.error(`Error moving file ${file} to archive:`, archiveErr);
                }
            }
        }

    } catch (readErr) {
        console.error('Error reading files:', readErr);
    }
}

// MAIN FUNCTION TO EXECUTE THE PROCESS
async function execute_retrieve_data(options, iteration) {
    const startTime = performance.now();

    const retrieval_batch_size = 50000;
    let offset = 0;
    let batchCounter = 0;
    let rowsReturned = 0;
    
    let pool = "";
    const { get_data } = options;
    const { pool_name, file_name, directory_name, query } = get_data[iteration];

    try {
        // STEP #1: DELETE PRIOR FILES
        await delete_archived_files(directory_name);

        // STEP #2 - MOVE FILES TO ARCHIVE
        await move_files_to_archive(directory_name);

        // STEP 3 PULL SQL DATA FROM BOOKING, KEY METRICS & PACING METRICS TABLES
        console.log(`\nSTEP 3: PULL SQL DATA`);

        start_timer(`0_get_data`);

        // pool = await createLocalDBConnection(pool_name);
        pool = await get_pool_stream();
    
        do {
            const sql = typeof query === 'function' ? await query(retrieval_batch_size, offset) : query;
            console.log(`sql query = `, sql);

            // Create export directory if needed
            const directory_path = await create_directory(directory_name);

            const timestamp = get_current_date_for_file_naming();
            const filePath = path.join(
                directory_path,
                `results_${timestamp}_${file_name}_offset_${offset}_batch_${batchCounter + 1}.csv`
            );

            console.log(`🚀 Exporting: ${filePath}`);
            const before = performance.now();

            await stream_query_to_csv(pool, sql, filePath);

            const after = performance.now();
            console.log(`⏱️  Elapsed Time: ${((after - before) / 1000).toFixed(2)} sec`);

            // Estimate whether data was returned by checking file size
            const stats = fs.statSync(filePath);
            rowsReturned = stats.size > 100 ? retrieval_batch_size : 0; // crude check

            // const isEmpty = stats.size === 0; // or 0 if you want *truly empty* only
            const isEmpty = stats.size < 100; // or < 100 if you want header-only files removed

            if (isEmpty) {
                console.log(`🧹 File ${path.basename(filePath)} is empty (${stats.size} bytes) — deleting.`);
                try {
                    fs.unlinkSync(filePath);
                } catch (err) {
                    console.warn(`⚠️ Could not delete empty file: ${err.message}`);
                }
                rowsReturned = 0;
            } else {
                rowsReturned = retrieval_batch_size;
            }

            // offset += retrieval_batch_size;
            batchCounter++;

            await trigger_garbage_collection();

        } while (batchCounter < 1);  //testing
        // } while (rowsReturned > 0);

        stop_timer(`0_get_data`);

        console.log('All queries executed successfully.');

    } catch (error) {
        console.error('Error:', error);
        stop_timer(`0_get_data`);

    } finally {
        // await pool.end();
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`✅ Total Elapsed Time: ${((elapsedTime) / 1000).toFixed(2)} sec`);

        await trigger_garbage_collection();

        stop_timer(`0_get_data`);

        return elapsedTime;
    }
}

// Run the main function
// execute_retrieve_data();

export { execute_retrieve_data };
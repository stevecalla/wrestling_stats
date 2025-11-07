import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import { get_pool } from "../mysql/mysql_pool.js";
import { trigger_garbage_collection } from "../garbage_collection/trigger_garbage_collection.js";

// const { local_usat_sales_db_config } = require('../../utilities/config');
// const { create_local_db_connection } = require('../../utilities/connectionLocalDB');
// const { streamQueryToCsv } = require('../../utilities/stream_query_to_csv');

// const { determine_os_path } = require('../../utilities/determine_os_path');
// const { create_directory } = require('../../utilities/createDirectory');
import { determine_os_path } from "../directory_tools/determine_os_path.js";
import { create_directory } from "../directory_tools/create_directory.js";

import { get_current_datetime_for_file_naming } from "../date_time_tools/get_current_datetime.js";

// STEP #1 - DELETE ARCHIVED FILES
async function delete_archived_files(directory_name_archive) {
    console.log('Deleting files from archive');

    // Create the "archive" directory if it doesn't exist
    const directory_path = await create_directory(directory_name_archive);

    // List all files in the directory
    const files = fs.readdirSync(directory_path);
    console.log(files);

    const logPath = await determine_os_path();

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
async function move_files_to_archive(directory_name, directory_name_archive) {
    console.log('Moving files to archive');

    const os_path = await determine_os_path();

    try {
        // List all files in the directory
        await create_directory(directory_name);
        const sourcePath = `${os_path}${directory_name}`;
        const files = fs.readdirSync(sourcePath);
        console.log(files);

        // Create the "archive" directory if it doesn't exist
        const destinationPath = await create_directory(directory_name_archive);
        console.log(destinationPath);

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${sourcePath}/${file}`;
                const destinationFilePath = `${destinationPath}/${file}`;

                try {
                    // Move the file to the "archive" directory
                    fs.renameSync(sourceFilePath, destinationFilePath);
                    console.log(`Archived ${file}`);
                } catch (archiveErr) {
                    console.error(`Error moving file ${file} to archive:`, archiveErr);
                }
            }
        }

    } catch (readErr) {
        console.error('Error reading files:', readErr);
    }
}

// QUERIES & STREAMS DATA DIRECTLY TO CSV VS HOLDING IN MEMORY
// async function execute_retrieve_data(options) {
async function execute_retrieve_data(options, datasetId, bucketName, schema, directoryName) {

    const startTime = performance.now();

    // const pool = await create_local_db_connection(await local_usat_sales_db_config());
    const pool = await get_pool();
    
    const directory_name = directoryName ?? `/bigquery`;
    const directory_name_archive = `${directory_name}_archive`;

    const retrieval_batch_size = 100000;

    console.log(options, directory_name, directory_name_archive);

    let offset = 0;
    let batchCounter = 0;
    // let rowsReturned = 0;

    try {
        await delete_archived_files(directory_name_archive);
        await move_files_to_archive(directory_name, directory_name_archive);

        const { fileName, query } = options[0];

    //     do {
    //         const sql = typeof query === 'function' ? await query(retrieval_batch_size, offset) : query;

    //         // console.log(sql);

    //         // Create export directory if needed
    //         const dirPath = await create_directory(directory_name);
    //         const timestamp = getCurrentDateTimeForFileNaming();
    //         const filePath = path.join(
    //             dirPath,
    //             `results_${timestamp}_${fileName}_offset_${offset}_batch_${batchCounter + 1}.csv`
    //         );

    //         console.log(`🚀 Exporting: ${filePath}`);
    //         const before = performance.now();

    //         await streamQueryToCsv(pool, sql, filePath);

    //         const after = performance.now();
    //         console.log(`⏱️  Elapsed Time: ${((after - before) / 1000).toFixed(2)} sec`);

    //         // Estimate whether data was returned by checking file size
    //         const stats = fs.statSync(filePath);
    //         rowsReturned = stats.size > 100 ? retrieval_batch_size : 0; // crude check

    //         offset += retrieval_batch_size;
    //         batchCounter++;

    //         await triggerGarbageCollection();

    //     // } while (batchCounter < 1);  //testing
    //     } while (rowsReturned > 0);

    } catch (err) {
        console.error('🔥 Error in data retrieval:', err);
    } finally {
        await pool.end();
        const endTime = performance.now();
        console.log(`✅ Total Elapsed Time: ${((endTime - startTime) / 1000).toFixed(2)} sec`);

        await trigger_garbage_collection();
    }
}

// Run the main function
execute_retrieve_data();

export { execute_retrieve_data };
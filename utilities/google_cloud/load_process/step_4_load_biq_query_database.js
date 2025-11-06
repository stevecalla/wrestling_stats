import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import { promises as fs } from "fs";   // async fs (e.g., await fs.readFile)
import path from "path";
import { fileURLToPath } from "url";

// SET GOOGLE CLOUD CREDENTIALS
// SET .env FOR SERVICE KEY PATH BASED ON OPERATING ENVIRONMENT
import os from "os";
const platform = os.platform();
let GOOGLE_APPLICATION_CREDENTIALS = ""; // absolute path to JSON key
if (platform === 'win32') GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_WINDOWS;
else if (platform === 'darwin') GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_MAC;
else GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_LINUX;

// Import the Google Cloud client libraries
import { BigQuery } from "@google-cloud/bigquery";
import { Storage } from "@google-cloud/storage";

import { determine_os_path } from "../../directory_tools/determine_os_path.js";
const TREAT_PLAIN_CSV_AS_GZIP = false; // If you upload with `gzip: true` but keep ".csv" filenames, set this true:

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT_ID_WRESTLING;
const BUCKET_NAME = process.env.GOOGLE_CLOUD_STORAGE_BUCKET_WRESTLING;
const DATASET_ID = process.env.GOOGLE_BIGQUERY_DATABASE_NAME;

import { booking_schema } from "../schemas/schema_booking_data.js";

async function execute_load_bigquery_database(options, iteration) {
    const start_time = performance.now();

    // const { csv_folder_name } = options;
    const { table_ids, directory_name } = options.get_data[iteration];

    // Instantiate clients
    const bigqueryClient = new BigQuery({ keyFilename: GOOGLE_APPLICATION_CREDENTIALS, projectId: PROJECT_ID });
    const storageClient = new Storage({ projectId: PROJECT_ID, keyFilename: GOOGLE_APPLICATION_CREDENTIALS });

    const os_path = await determine_os_path();
    const directory = path.join(os_path, directory_name);
    const files = (await fs.readdir(directory)).filter(f => f.endsWith('.csv'));

    // ========================
    // usage:
    // console.log('✅ CSVs (non-empty):', files);
    console.log('files length =', files.length);
    // console.log(files);
    if (!files.length) {
        console.log('No CSV files found. Skipping load.');
        return '0.00';
    }

    // 🔸 MINIMAL CHANGE #1: map local .csv -> remote .csv.gz (since uploads used gzip: true)
    const remote_names = files.map(f => f.replace(/\.csv$/i, '.csv.gz'));

    // Build Storage File objects (use remote names)
    const file_objs = remote_names.map(name => storageClient.bucket(BUCKET_NAME).file(name));
    const uris = remote_names.map(f => `gs://${BUCKET_NAME}/${f}`);

    console.log('URIs being passed to BQ:', uris);
    // console.log(file_objs);

    // Metadata upgrade: compression + quoted newlines + quote
    const base = {
        sourceFormat: 'CSV',
        location: 'US',
        writeDisposition: 'WRITE_APPEND',
        compression: 'GZIP',                 // <— important when you upload gz
        allowQuotedNewlines: true,   // CSVs with quoted \n won’t fail
        quote: '"',
        fieldDelimiter: ',',    // be explicit
        encoding: 'UTF-8',
        columnNameCharacterMap: 'V2',   // 👈 allow BigQuery to normalize header names
    };

    const metadata =
        (table_ids[0] === 'booking_data')
            ? { ...base, skipLeadingRows: 1, schema: { fields: booking_schema } } // explicit schema → ok to skip header
            : { ...base, autodetect: true }; // autodetect → DO NOT set skipLeadingRows

    // console.log(metadata);

    try {
        // Kick off a single load job for all files
        const [job] = await bigqueryClient
            .dataset(DATASET_ID)
            .table(table_ids[0])
            .load(file_objs, metadata);

        console.log(`Job ${job.id} started for ${file_objs.length} files...`);

        // Parse job.id which looks like "<project>:<location>.<jobId>"
        let projectId, location, jobId;
        const m = typeof job.id === 'string' && job.id.match(/^([^:]+):([^.]+)\.(.+)$/);
        if (m) {
            [, projectId, location, jobId] = m;
        } else {
            // Fallback: if format differs, assume US and use whole id as jobId
            jobId = job.id;
            location = metadata.location || 'US';
        }

        // Create a fresh job handle and poll until DONE
        const poll_job = bigqueryClient.job(jobId, { location });
        let meta;
        while (true) {
            [meta] = await poll_job.getMetadata();
            const state = meta?.status?.state;
            if (state === 'DONE') break;
            await sleep(2000);
        }

        const errors = meta?.status?.errors;
        const error_result = meta?.status?.errorResult;
        if (errors?.length || error_result) {
            console.error('Load job errors:', errors || error_result);
            throw new Error('BigQuery load job failed');
        }

        console.log('Load status:', meta?.status?.state);
        console.log('Input files:', meta?.statistics?.load?.inputFiles);
        console.log('Output rows:', meta?.statistics?.load?.outputRows);
        console.log('Output bytes:', meta?.statistics?.load?.outputBytes);
        console.log(`Job ${jobId} completed.`);

        const elapsed_time = ((performance.now() - start_time) / 1000).toFixed(2);
        console.log(`STEP #4: Elapsed time: ${elapsed_time}\n`);
        return elapsed_time;

    } catch (err) {
        console.error('Error in BigQuery load:', err);
        throw err;
    }

}

// execute_load_big_querydatabase();

export { execute_load_bigquery_database };



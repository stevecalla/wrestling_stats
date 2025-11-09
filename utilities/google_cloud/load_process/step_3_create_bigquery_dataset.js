import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import path from "path";
import { fileURLToPath } from "url";

// Import the Google Cloud client libraries
import { BigQuery } from "@google-cloud/bigquery";

// SET GOOGLE CLOUD CREDENTIALS
// SET .env FOR SERVICE KEY PATH BASED ON OPERATING ENVIRONMENT
import os from "os";
const platform = os.platform();
let GOOGLE_APPLICATION_CREDENTIALS = ""; // absolute path to JSON key
if (platform === 'win32') GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_WINDOWS;
else if (platform === 'darwin') GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_MAC;
else GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_LINUX;

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT_ID_WRESTLING;
const DATASET_ID = process.env.GOOGLE_BIGQUERY_DATABASE_NAME;

async function execute_create_bigquery_dataset(options, iteration) {
  const table_id = options.get_data[iteration].table_ids[0];

  try {
    const startTime = performance.now();

    // Instantiate client
    const bigqueryClient = new BigQuery({ keyFilename: GOOGLE_APPLICATION_CREDENTIALS, projectId: PROJECT_ID });

    // Ensure dataset exists (explicitly set location = US on first create)
    const datasetRef = bigqueryClient.dataset(DATASET_ID);
    const [datasetExists] = await datasetRef.exists();

    if (!datasetExists) {
      const [dataset] = await bigqueryClient.createDataset(DATASET_ID, { location: 'US' });
      console.log(`Dataset ${dataset.id} created (location=US).`);
    } else {
      console.log(`Dataset ${DATASET_ID} already exists.`);
    }

    // Create/replace tables
    const tableOptions = { location: 'US' };

    const tableRef = datasetRef.table(table_id);

    const [tableExists] = await tableRef.exists();
    if (tableExists) {
      await tableRef.delete({ force: true });
      console.log(`Deleted existing table ${table_id}.`);
      // }

      const [table] = await datasetRef.createTable(table_id, tableOptions);
      console.log(`Table ${table.id} created.`);
    }

    const elapsedTime = ((performance.now() - startTime) / 1_000).toFixed(2);
    return elapsedTime;

  } catch (error) {
    console.error('Error:', error);
    throw error;
  }
}

// execute_create_bigquery_dataset();

export { execute_create_bigquery_dataset };

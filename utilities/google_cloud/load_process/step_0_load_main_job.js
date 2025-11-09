import { get_current_date_time } from "../../date_time_tools/get_current_datetime.js";

import { execute_retrieve_data } from "./step_1_retrieve_data_process.js";
import { execute_upload_csv_to_cloud } from "./step_2_upload_csv_to_cloud.js";
import { execute_create_bigquery_dataset } from "./step_3_create_bigquery_dataset.js";
import { execute_load_bigquery_database } from "./step_4_load_biq_query_database.js";

// // WRESTER DATA
import { wrestler_list_query } from "../queries/query_wrestler_list.js";
import { wrestler_match_history_query } from "../queries/query_wrestler_match_history.js";

const run_step_1 = true;  // execute_retrieve_data
const run_step_2 = true;  // execute_upload_csv_to_cloud
const run_step_3 = true;  // execute_create_bigquery_dataset
const run_step_4 = true;  // execute_load_bigquery_database

const directory_prefix = 'wrestling';

const data_options = {
  wrestler: {
    // data_label: "matches",
    // csv_folder_name: "bigquery",
    // table_ids: ["wrestler_list", "wrestler_match_history"],
    get_data: [
      // {
      //     file_name: `wrestler_list`,
      //     directory_name: `${directory_prefix}_list`,
      //     query: wrestler_list_query,
      //     table_ids: ["wrestler_list"],
      // },
      {
          file_name: `wrestler_match_history`,
          directory_name: `${directory_prefix}_match_history_data`,
          query: wrestler_match_history_query,
          table_ids: ["wrestler_match_history"],
      },
    ],
  },
};

async function execute_steps(stepFunctions, options, iteration) {

  for (let i = 0; i < stepFunctions.length; i++) {

    const stepFunction = stepFunctions[i];

    if (stepFunction) {
      const stepName = `STEP #${i + 1}`;
      console.log(`\n*************** STARTING ${stepName} ***************\n`);

      try { // Add try/catch within the loop for individual step error handling
        const getResults = await stepFunction(options, iteration);
        const message = getResults ? `${stepName} executed successfully. Elapsed Time: ${getResults}` : `${stepName} executed successfully.`; // Modified message
        console.log(message);

      } catch (error) {
        console.error(`Error executing ${stepName}:`, error);

        // Decide whether to continue or break the loop here.
          // For example, to stop on the first error:
          // To continue despite errors in individual steps:
        break;
        // continue;
      }

      console.log('\n*************** END OF', stepName, '**************\n');
    } else {
      console.log(`Skipped STEP #${i + 1} due to toggle set to false.`);
    }
  }
}

async function execute_load_data_to_bigquery(data) {
  const startTime = performance.now();
  console.log(`\n\nPROGRAM START TIME = ${get_current_date_time()}`);

  try {
    const { get_data } = data_options[data];

    for (let i = 0; i < get_data.length; i++) {
      
      const stepFunctions = [
        run_step_1 ? execute_retrieve_data : null,
        run_step_2 ? execute_upload_csv_to_cloud : null,
        run_step_3 ? execute_create_bigquery_dataset : null,
        run_step_4 ? execute_load_bigquery_database : null,
      ];
  
      const iteration = i;
      await execute_steps(stepFunctions, data_options[data], iteration); // Call the new function

    }


  } catch (error) {
    console.error('Error in main process:', error); // More specific message
    return;
  }

  const endTime = performance.now();
  const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2);

  console.log(`\nPROGRAM END TIME: ${get_current_date_time()}; ELASPED TIME: ${elapsedTime} sec\n`);

  return elapsedTime;
}

// execute_load_data_to_bigquery("wrestler");

export { execute_load_data_to_bigquery }
// IMPORTS
import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import { step_1_run_alpha_wrestler_list } from "./src/step_1_get_wrestler_list.js";
import { step_2_write_wrestler_match_url_array } from "./src/step_2_create_wrestler_match_url_array.js";
// import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";

import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history_copy.js";

import { step_9_close_chrome_dev } from "./src/step_9_close_chrome_developer.js";

async function main() {
  const is_test = true;
  const loop_start = 0; // to set the for loop start index in step 3; used 0, 109, and 458

  console.log(`\n\x1b[33m➕ === STARTING MAIN PROGRAM (is_test = ${is_test}) === \x1b[0m\n`);

  const ALPHA_WRESTLER_LIST_LIMIT = is_test ? 5 : 30; // number of wrestler pages to test
  const MATCHES_PAGE_LIMIT = is_test ? 10 : 2000; // number of wrestler pages to test

  const URL = "https://www.trackwrestling.com/";
  const WRESTLING_SEASON = "2024-25"; // season to scrape
  // const WRESTLING_SEASON = "2025-26"; // season to scrape
  // --- LOOP: one alpha step → write file → progress → compute next prefix
  const adjusted_season = WRESTLING_SEASON.replace("-", "_");
  let file_name = "";
  let folder_name = "";

  // step 0: launch Chrome Developer with CDP
  const { browser, page, context } = await step_0_launch_chrome_developer(URL);

  // step 1: get the wrestler list and their match URLs
  // console.log(`\n=== STARTING SCRAPE for ${ALPHA_WRESTLER_LIST_LIMIT} WRESTLERS for SEASON ${WRESTLING_SEASON} ===\n`);
  file_name = `wrestlers_alpha_${adjusted_season}.csv`;
  folder_name = "input";
  // await step_1_run_alpha_wrestler_list(ALPHA_WRESTLER_LIST_LIMIT, WRESTLING_SEASON, page, browser, folder_name, file_name);

  // NOTE: STEP #2 WRITE WRESTLER MATCH URL ARRAY
  console.log(`\n=== STARTING STEP #2 TO WRITE WRESTLER MATCH URL ARRAY ===\n`);
  const DIR = "/Users/stevecalla/wrestling/data_tracker_wrestling";
  folder_name = "input";
  const url_file_name = `wrestler_match_urls_${adjusted_season}.js`;
  // await step_2_write_wrestler_match_url_array(DIR, folder_name, url_file_name, file_name );

  // NOTE: STEP #3 GET WRESTLER MATCH HISTORY
  // console.log(`\n=== STARTING SCRAPE FOR ${WRESTLING_SEASON} WRESTLERS MATCH HISTORY SEASON. PAGE LIMIT = ${MATCHES_PAGE_LIMIT} ===\n`);
  file_name = `tw_matches_full_${adjusted_season}.csv`;
  folder_name = "output";
  await step_3_get_wrestler_match_history(MATCHES_PAGE_LIMIT, WRESTLING_SEASON, page, browser, context, folder_name, file_name, loop_start);

  // step #9 CLOSE BROWSER AND EXIT
  // await step_9_close_chrome_dev(browser, context);
}


await main();

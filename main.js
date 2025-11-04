import path from "path";

import { determine_os_path } from "./utilities/directory_tools/determine_os_path.js";
import { create_directory } from "./utilities/directory_tools/create_directory.js";
import { color_text } from "./utilities/console_logs/console_colors.js";
import { sleep_with_countdown } from "./utilities/time_out_tools/sleep.js";

// IMPORTS
import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import { step_1_run_alpha_wrestler_list } from "./src/step_1_get_wrestler_list.js";
import { step_2_write_wrestler_match_url_array } from "./src/step_2_create_wrestler_match_url_array.js";
import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";

import { step_9_close_chrome_dev } from "./src/step_9_close_chrome_developer.js";

async function main() {
  // Initialize variables
  console.log(color_text(`➕ === STARTING MAIN PROGRAM ===`, "red"));

  let is_test = "";

  const URL_HOME_PAGE = "https://www.trackwrestling.com/";
  const URL_LOGIN_PAGE = "https://www.trackwrestling.com/seasons/index.jsp";
  const WRESTLING_SEASON = "2024-25"; // season to scrape
  // const WRESTLING_SEASON = "2025-26"; // season to scrape
  // --- LOOP: one alpha step → write file → progress → compute next prefix
  const adjusted_season = WRESTLING_SEASON.replace("-", "_");

  const DIRECTORY = determine_os_path();
  let folder_name = "";
  let folder_path = "";
  let file_name = "";
  let file_path = "";

  // BEGIN STEPS
  // STEP 0: LAUNCH CHROME DEVELOPER WITH CDP
  const { browser, page, context } = await step_0_launch_chrome_developer(URL_HOME_PAGE);

  // STEP 1: GET THE WRESTLER LIST WITH MATCH HISTORY URLS
  is_test = false;
  const ALPHA_WRESTLER_LIST_LIMIT = is_test ? 10 : 30; // number of wrestlers to retrieve in step 1
  console.log(color_text(`➕ ===  is_test = ${is_test} ===`, "red"));
  folder_name = "input";
  folder_path = await create_directory(folder_name);
  const wrestler_list_file_name = `wrestlers_alpha_${adjusted_season}.csv`;
  const wrestler_list_file_path = path.join(folder_path, wrestler_list_file_name);
  
  console.log(color_text(`\n=== STARTING SCRAPE for ${ALPHA_WRESTLER_LIST_LIMIT} WRESTLERS for SEASON ${WRESTLING_SEASON} ===\n`, "red"));
  // await step_1_run_alpha_wrestler_list(URL_LOGIN_PAGE, ALPHA_WRESTLER_LIST_LIMIT, WRESTLING_SEASON, page, browser, wrestler_list_file_path);

  // // STEP #2: WRITE WRESTLER MATCH URL_HOME_PAGE ARRAY
  // // NOTE: DONT NEED THIS STEP IF STEP #3 STREAMS DIRECTLY FROM THE CSV CREATED IN STEP 1
  // console.log(color_text(`\n=== STARTING STEP #2 TO WRITE WRESTLER MATCH URL_HOME_PAGE ARRAY ===\n`, "red"));

  // folder_name = "input";
  // folder_path = await create_directory(folder_name);
  // const url_file_name = `wrestler_match_urls_${adjusted_season}.js`;
  // file_path = path.join(folder_path, file_name);
  // let url_file_path = path.join(folder_path, url_file_name);

  // await step_2_write_wrestler_match_url_array(file_path, url_file_path, url_file_name);

  // STEP #3: GET WRESTLER MATCH HISTORY
  is_test = false;
  const MATCHES_PAGE_LIMIT = is_test ? 10 : 2000; // number of wrestler match history pages to retrieve in step 3
  const loop_start = 0; // to set the for loop start index in step 3; used 0, 109, and 458
  folder_name = "output";
  folder_path = await create_directory(folder_name);
  file_name = `tw_matches_full_${adjusted_season}.csv`;
  file_path = path.join(folder_path, file_name);

  console.log(color_text(`\n=== STARTING SCRAPE FOR ${WRESTLING_SEASON} WRESTLERS MATCH HISTORY SEASON. PAGE LIMIT = ${MATCHES_PAGE_LIMIT} ===\n`, "red"));
  await step_3_get_wrestler_match_history(URL_HOME_PAGE, URL_LOGIN_PAGE, MATCHES_PAGE_LIMIT, loop_start, WRESTLING_SEASON, page, browser, context, file_path, wrestler_list_file_path);

  // step #9 CLOSE BROWSER AND EXIT
  // await sleep_with_countdown(3000);
  // console.log("✅ Done waiting!");
  // await step_9_close_chrome_dev(browser, context);
}

await main();

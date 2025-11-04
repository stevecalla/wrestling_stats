import path from "path";

import { determine_os_path } from "./utilities/directory_tools/determine_os_path.js";
import { create_directory } from "./utilities/directory_tools/create_directory.js";
import { color_text } from "./utilities/console_logs/console_colors.js";
import { sleep_with_countdown } from "./utilities/time_out_tools/sleep.js";

// IMPORTS
import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import { step_1_run_alpha_wrestler_list } from "./src/step_1_get_wrestler_list.js";

import { step_2_write_wrestler_match_url_array } from "./src/step_2_create_wrestler_match_url_array.js";

// import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";

import { step_9_close_chrome_dev } from "./src/step_9_close_chrome_developer.js";

async function main() {
  // Initialize variables
  const is_test = false;
  const loop_start = 0; // to set the for loop start index in step 3; used 0, 109, and 458

  console.log(color_text(`➕ === STARTING MAIN PROGRAM (is_test = ${is_test}) ===`, "red"));

  const ALPHA_WRESTLER_LIST_LIMIT = is_test ? 10 : 30; // number of wrestlers to retrieve in step 1
  const MATCHES_PAGE_LIMIT = is_test ? 100 : 2000; // number of wrestler match history pages to retrieve in step 3

  const URL = "https://www.trackwrestling.com/";
  // const WRESTLING_SEASON = "2024-25"; // season to scrape
  const WRESTLING_SEASON = "2025-26"; // season to scrape
  // --- LOOP: one alpha step → write file → progress → compute next prefix
  const adjusted_season = WRESTLING_SEASON.replace("-", "_");

  const DIRECTORY = determine_os_path();
  let folder_name = "";
  let folder_path = "";
  let file_name = "";
  let file_path = "";

  // BEGIN STEPS
  // STEP 0: LAUNCH CHROME DEVELOPER WITH CDP
  const { browser, page, context } = await step_0_launch_chrome_developer(URL);

  // STEP 1: GET THE WRESTLER LIST & MATCH URLS
  console.log(color_text(`\n=== STARTING SCRAPE for ${ALPHA_WRESTLER_LIST_LIMIT} WRESTLERS for SEASON ${WRESTLING_SEASON} ===\n`, "red"));

  folder_name = "input";
  folder_path = await create_directory(folder_name);
  file_name = `wrestlers_alpha_${adjusted_season}.csv`;
  file_path = path.join(folder_path, file_name);

  await step_1_run_alpha_wrestler_list(ALPHA_WRESTLER_LIST_LIMIT, WRESTLING_SEASON, page, browser, file_path);

  // STEP #2: WRITE WRESTLER MATCH URL ARRAY
  console.log(color_text(`\n=== STARTING STEP #2 TO WRITE WRESTLER MATCH URL ARRAY ===\n`, "red"));

  // const DIR = "/Users/stevecalla/wrestling/data_tracker_wrestling";
  folder_name = "input";
  folder_path = await create_directory(folder_name);
  folder_path = await create_directory(folder_name);

  const url_file_name = `wrestler_match_urls_${adjusted_season}.js`;

  file_path = path.join(folder_path, file_name);
  let url_file_path = path.join(folder_path, url_file_name);

  await step_2_write_wrestler_match_url_array(file_path, url_file_path, url_file_name);

  // // STEP #3: GET WRESTLER MATCH HISTORY
  // console.log(color_text(`\n=== STARTING SCRAPE FOR ${WRESTLING_SEASON} WRESTLERS MATCH HISTORY SEASON. PAGE LIMIT = ${MATCHES_PAGE_LIMIT} ===\n`, "red"));
  // file_name = `tw_matches_full_${adjusted_season}.csv`;
  // folder_name = "output";
  // // await step_3_get_wrestler_match_history(MATCHES_PAGE_LIMIT, WRESTLING_SEASON, page, browser, context, folder_name, file_name, loop_start);

  // step #9 CLOSE BROWSER AND EXIT
  // await sleep_with_countdown(3000);
  // console.log("✅ Done waiting!");
  // await step_9_close_chrome_dev(browser, context);
}

await main();

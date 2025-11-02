// IMPORTS
import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import { step_1_run_alpha_wrestler_list } from "./src/step_1_get_wrestler_list.js";
// import { step_2_write_wrestler_match_url_array } from "./src/step_2_create_wrestler_match_url_array.js";
// import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";

async function main() {
  const URL = "https://www.trackwrestling.com/";
  const ALPHA_WRESTLER_LIST_LIMIT = 300; // number of wrestler pages to test
  const MATCHES_PAGE_LIMIT = 3; // number of wrestler pages to test
  const WRESTLING_SEASON = "2025-26" || "2024-25"; // season to scrape

  // step 0: launch Chrome Developer with CDP
  const { browser, page } = await step_0_launch_chrome_developer(URL);

  // step 1: get the wrestler list and their match URLs
  console.log(`\n=== STARTING SCRAPE for ${ALPHA_WRESTLER_LIST_LIMIT} WRESTLERS for SEASON ${WRESTLING_SEASON} ===\n`);
  await step_1_run_alpha_wrestler_list(ALPHA_WRESTLER_LIST_LIMIT, WRESTLING_SEASON, page, browser);

  // await step_2_write_wrestler_match_url_array();

  // NOTE: STEP #3 GET WRESTLER MATCH HISTORY
  // console.log(`\n=== STARTING SCRAPE for ${MATCHES_PAGE_LIMIT} WRESTLERS for SEASON ${WRESTLING_SEASON} ===\n`);
  // await step_3_get_wrestler_match_history(MATCHES_PAGE_LIMIT, WRESTLING_SEASON, page, browser);
}

await main();

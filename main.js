import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import {step_2_write_wrestler_match_url_array} from "./src/step_2_create_wrestler_match_url_array.js";
import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";


// step 0: launch Chrome Developer with CDP
const URL = "https://www.trackwrestling.com/";
// const URL = "https://www.trackwrestling.com/seasons/index.jsp";
const { browser, page } = await step_0_launch_chrome_developer(URL);

// step 1: get the wrestler list and their match URLs

// await step_2_write_wrestler_match_url_array();

// NOTE: STEP #3 GET WRESTLER MATCH HISTORY
const TEST_PAGE_NO = 3; // number of wrestler pages to test
const WRESTLING_SEASON = "2024-25"; // season to scrape
// const WRESTLING_SEASON = "2025-26"; // season to scrape
console.log(`\n=== STARTING SCRAPE for ${TEST_PAGE_NO} WRESTLERS for SEASON ${WRESTLING_SEASON} ===\n`);

await step_3_get_wrestler_match_history(TEST_PAGE_NO, WRESTLING_SEASON, page, browser);


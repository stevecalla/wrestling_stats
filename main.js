import { step_0_launch_chrome_developer } from "./src/step_0_launch_chrome_developer.js";
import {step_2_write_wrestler_match_url_array} from "./src/step_2_create_wrestler_match_url_array.js";
import { step_3_get_wrestler_match_history } from "./src/step_3_get_wrestler_match_history.js";


const { browser, trackwrestling_page } = await step_0_launch_chrome_developer();

// step 1: get the wrestler list and their match URLs

// await step_2_write_wrestler_match_url_array();

// NOTE: STEP #3 GET WRESTLER MATCH HISTORY
const TEST_PAGE_NO = 3; // number of wrestler pages to test
const WRESTLING_SEASON = "2024-2025"; // season to scrape
console.log(`\n=== STARTING SCRAPE for ${TEST_PAGE_NO} WRESTLERS for SEASON ${WRESTLING_SEASON} ===\n`);

await step_3_get_wrestler_match_history(TEST_PAGE_NO, browser, trackwrestling_page, WRESTLING_SEASON);


import { execute_scrape_track_wrestling } from "../../main.js";
import { log_with_time_stamp } from "../../utilities/date_time_tools/get_current_datetime.js";

async function run_girls_all_wrestlers_2024_25() {
    log_with_time_stamp('Hello - RUN UPDATE LEADS JOB');
    log_with_time_stamp('Starting update process...');

    try {

        const hs_girls_all_2025 = {
            track_wrestling_category: "High School Girls",
            wrestling_season: "2024-25",
            gender: "F",
            use_scheduled_events_iterator_query: false,
            use_wrestler_list_iterator_query: true,
            // sql_where_filter_state_qualifier: "AND wrestler_is_state_tournament_qualifier IS NOT NULL",
            // sql_team_id_list: "AND team_id IN (764192150, 839403150)",
            // sql_wrestler_id_list: "AND wrestler_id IN (35527236132, 35671717132)",
        };

        await execute_scrape_track_wrestling(hs_girls_all_2025);
        
    } catch (error) {

        log_with_time_stamp(`Error with request: ${error.message}`, 'error');
    }
}

run_girls_all_wrestlers_2024_25();

// export { run_girls_all_wrestlers_2024_25 };

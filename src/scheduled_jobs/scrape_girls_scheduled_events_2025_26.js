import { execute_scrape_track_wrestling } from "../../main.js";
import { log_with_time_stamp } from "../../utilities/date_time_tools/get_current_datetime.js";

async function run_girls_scheduled_events_2025_26() {
    log_with_time_stamp('Hello - RUN UPDATE GIRLS EVENTS');
    log_with_time_stamp('Starting update process...');

    try {

        const hs_girls_events_2026 = {
            track_wrestling_category: "High School Girls",
            wrestling_season: "2025-26",
            gender: "F",
            use_scheduled_events_iterator_query: true,
            use_wrestler_list_iterator_query: false,
            // sql_where_filter_state_qualifier: "AND wrestler_is_state_tournament_qualifier IS NOT NULL",
            // sql_where_filter_onthemat_ranking_list: "AND onthemat_is_name_match = 1",
            // sql_team_id_list: "AND team_id IN (764192150, 839403150)",
            // sql_wrestler_id_list: "AND wrestler_id IN (35527236132, 35671717132)",
        };

        await execute_scrape_track_wrestling(hs_girls_events_2026);
        
    } catch (error) {

        log_with_time_stamp(`Error with request: ${error.message}`, 'error');
    }
}

run_girls_scheduled_events_2025_26();

// export { run_girls_scheduled_events_2025_26 };

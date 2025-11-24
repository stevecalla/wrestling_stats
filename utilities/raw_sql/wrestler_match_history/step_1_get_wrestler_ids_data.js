function step_1_query_wrestler_ids_data(created_at_mtn, created_at_utc, QUERY_OPTIONS) {
    return `
        SELECT DISTINCT
            governing_body,
            track_wrestling_category,
            wrestling_season,
            wrestler_id,
            
            -- CREATED AT DATES
            '${created_at_mtn}' AS created_at_mtn,
            '${created_at_utc}' AS created_at_utc

        FROM wrestling_stats.wrestler_list_scrape_data
        WHERE 1 = 1
          AND governing_body = "${QUERY_OPTIONS.governing_body}"
          AND track_wrestling_category = "${QUERY_OPTIONS.track_wrestling_category}"
          AND wrestling_season = "${QUERY_OPTIONS.wrestling_season}"
          
        ORDER BY wrestler_id
        -- LIMIT 10
        ;
      `
    ;
  }
  

export { step_1_query_wrestler_ids_data };
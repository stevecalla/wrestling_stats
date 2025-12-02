async function team_schedule_scrape_data(batch_size = 10, offset = 0) {
    return `
        SELECT 
            *
        FROM wrestling_stats.team_schedule_scrape_data
        LIMIT ${batch_size} OFFSET ${offset}
        -- LIMIT 1 OFFSET 1
        ;
    `;
}

export { team_schedule_scrape_data };
async function wrestler_match_history_scrape_tasks(batch_size = 10, offset = 0) {
    return `
        SELECT 
            *
        FROM wrestler_match_history_scrape_tasks
        LIMIT ${batch_size} OFFSET ${offset}
        -- LIMIT 1 OFFSET 1
        ;
    `;
}

export { wrestler_match_history_scrape_tasks };
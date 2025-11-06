// wrestling_stats\utilities\google_cloud\queries\query_wrestler_match_history.js

async function wrestler_match_history_query(batch_size = 10, offset = 0) {
    return `
        SELECT 
            *
        FROM wrestling_stats.wrestler_match_history
        LIMIT ${batch_size} OFFSET ${offset}
        -- LIMIT 1 OFFSET 1
        ;
`;
}

export { wrestler_match_history_query };
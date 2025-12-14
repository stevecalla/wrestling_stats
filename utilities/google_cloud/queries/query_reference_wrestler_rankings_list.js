async function reference_wrestler_rankings_list(batch_size = 10, offset = 0) {
    return `
        SELECT 
            *
        FROM wrestling_stats.reference_wrestler_rankings_list
        LIMIT ${batch_size} OFFSET ${offset}
        -- LIMIT 1 OFFSET 1
        ;
    `;
}

export { reference_wrestler_rankings_list };
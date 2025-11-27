async function wrestler_state_qualifier_and_place_reference(batch_size = 10, offset = 0) {
    return `
        SELECT 
            *
        FROM wrestling_stats.wrestler_state_qualifier_and_place_reference
        LIMIT ${batch_size} OFFSET ${offset}
        -- LIMIT 1 OFFSET 1
        ;
    `;
}

export { wrestler_state_qualifier_and_place_reference };
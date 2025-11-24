async function wrestler_team_division_reference(batch_size = 10, offset = 0) {
    return `
        SELECT 
            *
        FROM wrestling_stats.wrestler_team_division_reference
        LIMIT ${batch_size} OFFSET ${offset}
        -- LIMIT 1 OFFSET 1
        ;
    `;
}

export { wrestler_team_division_reference };
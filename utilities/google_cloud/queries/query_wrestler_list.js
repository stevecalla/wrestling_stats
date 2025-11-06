// \utilities\google_cloud\queries\query_wrestler_list.js

async function wrestler_list_query(batch_size = 10, offset = 0) {
    return `
        SELECT 
            id,

            -- SELECTION CRITERIA
            season,
            last_name_prefix,
            grade,
            level,
            governing_body,

            -- WRESTLER INFO
            wrestler_id,
            name,
            first_name,
            last_name,
            team,
            team_id,
            weight_class,
            gender,

            record_text,
            wins,
            losses,
            matches,
            win_pct,

            -- WEBSITE URL / LINK DETAILS
            name_link,
            team_link,
            page_url,

            -- timestamps
            DATE_FORMAT(created_at_mtn, '%Y-%m-%d %H:%i:%s') AS created_at_mtn,
            DATE_FORMAT(created_at_utc, '%Y-%m-%d %H:%i:%s') AS created_at_utc,
            DATE_FORMAT(updated_at_mtn, '%Y-%m-%d %H:%i:%s') AS updated_at_mtn,
            DATE_FORMAT(updated_at_utc, '%Y-%m-%d %H:%i:%s') AS updated_at_utc

        FROM wrestling_stats.wrestler_list
        LIMIT ${batch_size} OFFSET ${offset}
        -- LIMIT 1 OFFSET 1
        ;
`;
}

export { wrestler_list_query };
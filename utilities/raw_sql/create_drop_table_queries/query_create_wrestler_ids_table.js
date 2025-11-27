const id_fields = `
    wrestling_season   VARCHAR(32)  NOT NULL,
    track_wrestling_category VARCHAR(32) NOT NULL,
    governing_body     VARCHAR(100),
    wrestler_id        BIGINT UNSIGNED,
`;

const created_at_dates = `
    -- CREATED AT DATES
    created_at_mtn DATETIME,
    created_at_utc DATETIME,
`;

const index_fields = `
    PRIMARY KEY (wrestling_season, track_wrestling_category, wrestler_id)
`;

async function query_create_wrestler_ids_table(table_name) {
  const query = `
    CREATE TABLE IF NOT EXISTS ${table_name} (
      ${id_fields}
      ${created_at_dates}
      ${index_fields}
    );
  `;

  return query;
}

export {
    query_create_wrestler_ids_table,
}
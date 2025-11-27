const id_fields = `
  id BIGINT UNSIGNED,
  
  wrestling_season          VARCHAR(32)  NOT NULL,
  track_wrestling_category  VARCHAR(32) NOT NULL,
  governing_body            VARCHAR(100),
  wrestler_id               BIGINT UNSIGNED,

  wrestler_name             VARCHAR(255),
  wrestler_first_name       VARCHAR(255),
  wrestler_last_name        VARCHAR(255),
  wrestler_gender           VARCHAR(32),

  wrestler_team             VARCHAR(255) NULL,
  wrestler_team_id          BIGINT UNSIGNED NULL,
  wrestler_grade            VARCHAR(64)  NULL,
  wrestler_level            VARCHAR(64)  NULL,

  event                     VARCHAR(255)  NULL,
  start_date                DATE          NULL,
  end_date                  DATE          NULL,
  weight_category           VARCHAR(64)   NULL,
  
  match_order               INT UNSIGNED NULL,
  opponent_id               BIGINT UNSIGNED NULL,
  
  opponent_name             VARCHAR(255),
  opponent_first_name       VARCHAR(255),
  opponent_last_name        VARCHAR(255),
  opponent_gender           VARCHAR(32),

  opponent_team             VARCHAR(255) NULL,
  opponent_team_id       BIGINT UNSIGNED NULL,
  opponent_grade         VARCHAR(64)  NULL,
  opponent_level         VARCHAR(64)  NULL,

  winner_id                 BIGINT UNSIGNED,
  winner_name               VARCHAR(255),

  round                     VARCHAR(128)  NULL,

  is_varsity                BOOLEAN,
  result                    VARCHAR(64)   NULL,
  score_details             VARCHAR(255)  NULL,
  outcome                   CHAR(10)       NULL,        -- W/L/T/U

  counts_in_record          TINYINT(1),

  wins_all_run              INT, 
  losses_all_run            INT, 
  ties_all_run              INT,
  total_matches             INT,

  total_matches_win_pct     FLOAT,

  wins_var_run              INT, 
  losses_var_run            INT, 
  ties_var_run              INT,
  
  record                    VARCHAR(64),
  record_varsity            VARCHAR(64),

  raw_details               TEXT,

  -- WEBSITE URL / LINK DETAILS
  name_link                 TEXT,
  team_link                 TEXT,
  page_url                  TEXT,
  

  -- NEW COLUMNS you can filter on
  max_match_order           INT,
  is_final_match_by_order   INT,
  is_final_match_state      INT,
  final_match_order         INT,
  is_final_match            INT,

  -- TIMESTAMPS
  created_at_mtn            DATETIME,
  created_at_utc            DATETIME,
  updated_at_mtn            DATETIME,
  updated_at_utc            DATETIME,
`;

const index_fields = `
  -- PRIMARY KEY (id),

  -- Core access pattern: season + category + wrestler + date
  KEY idx_season_category_wrestler_date (
    wrestling_season,
    track_wrestling_category,
    wrestler_id,
    start_date
  ),

  -- For pulling a wrestler’s match sequence within a season
  KEY idx_season_wrestler_match_order (
    wrestling_season,
    wrestler_id,
    match_order
  ),

  -- Opponent-centric lookups (e.g., "all matches vs X in a season")
  KEY idx_season_opponent (
    wrestling_season,
    opponent_id
  ),

  -- Winner-centric lookups (e.g., winning streaks, stats by winner)
  KEY idx_season_winner (
    wrestling_season,
    winner_id
  ),

  -- Helpful for time-window queries, incremental loads, etc.
  KEY idx_created_at_mtn (created_at_mtn),

  -- If you ever need to find rows by source URL
  KEY idx_page_url (page_url(255))
`;

async function query_create_wrestler_match_history_metrics_table(table_name) {
  const query = `
    CREATE TABLE IF NOT EXISTS ${table_name} (
      ${id_fields}
      ${index_fields}
    );
  `;

  return query;
}

export { query_create_wrestler_match_history_metrics_table };
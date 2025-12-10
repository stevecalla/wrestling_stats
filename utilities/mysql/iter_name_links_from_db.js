// utilities/mysql/iter_name_links_from_db.js
import { get_pool } from "./mysql_pool.js";

// NOTE: THE QUERIERS BELOW ARE FOR WRESTLER PAGES BASED ON A VARIETY OF CRITERIA BUT MOSTLY A DIRECT PULL OF WRESTLERS BASED ON CATEGORY (High School Boys, High School Girls), seasson, gender, state qualifier, team id, wrestler id
/** Fast COUNT(*) with a simple filter to avoid NULL/blank links */
export async function count_rows_in_db_wrestler_links(
    wrestling_season, 
    track_wrestling_category,
    gender, 
    sql_where_filter_state_qualifier,
    sql_team_id_list,
    sql_wrestler_id_list,
 ) {
  const pool = await get_pool();
  const [rows] = await pool.query(
    `SELECT 
      COUNT(*) AS cnt 
    FROM wrestler_list_scrape_data 
    WHERE 1 = 1
      AND name_link IS NOT NULL AND name_link <> ''
      AND track_wrestling_category = "${track_wrestling_category}"
      AND wrestling_season = "${wrestling_season}"
      AND gender = "${gender}"
      ${sql_where_filter_state_qualifier}
      ${sql_team_id_list}
      ${sql_wrestler_id_list}
    `
  );
  return Number(rows?.[0]?.cnt || 0);
}

/**
 * Async generator: yields { i, url } without loading everything into memory.
 * Uses LIMIT/OFFSET in small batches for stable, low-memory iteration.
 */
export async function* iter_name_links_from_db({
  start_at = 0,
  limit = Infinity,           // same semantic as CSV version
  batch_size = 500,           // tune as desired
  wrestling_season,
  track_wrestling_category,
  gender,
  sql_where_filter_state_qualifier,
  sql_team_id_list,
  sql_wrestler_id_list,
} = {}) {
  const pool = await get_pool();

  let yielded = 0;
  let offset = start_at;

  const link_query = `
      SELECT 
        id, 
        name_link
      FROM wrestler_list_scrape_data
      WHERE 1 = 1
        AND name_link IS NOT NULL AND name_link <> ''
        AND track_wrestling_category = "${track_wrestling_category}"
        AND wrestling_season = "${wrestling_season}"
        AND gender = "${gender}"
        ${sql_where_filter_state_qualifier}
        ${sql_team_id_list}
        ${sql_wrestler_id_list}
      ORDER BY id
      LIMIT ? OFFSET ?
  `;
  
  console.log('log inside iter_name_links_from_db', link_query);
  
  // console.log("sql where filter", sql_where_filter_state_qualifier);

  while (yielded < limit) {
    const to_fetch = Math.min(batch_size, limit - yielded);
    const [rows] = await pool.query(link_query, [to_fetch, offset]);

    if (!rows.length) break;

    for (const row of rows) {
      const url = row.name_link;
      const i = yielded + 1; // 1-based like your CSV logs
      yield { i, url };
      yielded += 1;
      if (yielded >= limit) break;
    }

    offset += rows.length;
  }
}

// NOTE: THE QUERIES BELOW ARE FOR WRESTLER PAGE LINKS BASED ON THE EVENTS HAPPENING TODAY & YESTERDAY
// SOURCE: utilities\raw_sql\discovery_get_team_schedule_and_wrestler_list.sql
/** Fast COUNT(*) with a simple filter to avoid NULL/blank links */
export async function count_name_links_based_on_event_schedule(
    wrestling_season, 
    track_wrestling_category,
    // gender, 
    // sql_where_filter_state_qualifier,
    // sql_team_id_list,
    // sql_wrestler_id_list,
 ) {
  const pool = await get_pool();
  const [rows] = await pool.query(
    `
      -- retrieves events from yesterday & today
      WITH recent_events AS (
        SELECT 
            ts.start_date,
            ts.team_name_raw,
            ts.team_id,
            ts.event_name,
            ts.wrestling_season,
            ts.track_wrestling_category
        FROM team_schedule_scrape_data ts
        WHERE ts.wrestling_season = "${wrestling_season}"
          AND ts.track_wrestling_category = "${track_wrestling_category}"
          AND team_name_raw LIKE "%, CO%"
          AND ts.start_date IN (
                -- CURDATE(),                                -- today
                DATE_SUB(CURDATE(), INTERVAL 1 DAY)       -- yesterday
                -- "2025-12-02"
                -- "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05", "2025-12-06"
              )
      )
      SELECT 
  
        COUNT(DISTINCT w.id) as cnt

      FROM recent_events re

      LEFT JOIN wrestler_list_scrape_data w
        ON w.wrestling_season             = re.wrestling_season
          AND w.track_wrestling_category  = re.track_wrestling_category
          AND (
                -- 1) primary: match on team_id when present
                (re.team_id IS NOT NULL AND w.team_id = re.team_id)
                -- 2) fallback: match on team name when event.team_id is NULL
                OR (re.team_id IS NULL AND w.team = re.team_name_raw)
              )
      WHERE 1 = 1
        AND w.name_link IS NOT NULL AND w.name_link <> ''
      -- GROUP BY 1, 2
      -- ORDER BY 1, 2
      ;

    `
  );
  return Number(rows?.[0]?.cnt || 0);
}

export async function* iter_name_links_based_on_event_schedule({
  start_at = 0,
  limit = Infinity,           // same semantic as CSV version
  batch_size = 500,           // tune as desired
  wrestling_season,
  track_wrestling_category,
  // gender,
  // sql_where_filter_state_qualifier,
  // sql_team_id_list,
  // sql_wrestler_id_list,
} = {}) {
  const pool = await get_pool();

  let yielded = 0;
  let offset = start_at;

  const link_query = `
      -- retrieves events from yesterday & today
      WITH recent_events AS (
        SELECT 
            ts.start_date,
            ts.team_name_raw,
            ts.team_id,
            ts.event_name,
            ts.wrestling_season,
            ts.track_wrestling_category
        FROM team_schedule_scrape_data ts
        WHERE ts.wrestling_season = "${wrestling_season}"
          AND ts.track_wrestling_category = "${track_wrestling_category}"
          AND team_name_raw LIKE "%, CO%"
          AND ts.start_date IN (
                -- CURDATE(),                                -- today
                DATE_SUB(CURDATE(), INTERVAL 1 DAY)       -- yesterday
                -- "2025-12-02"
                -- "2025-12-02", "2025-12-03", "2025-12-04", "2025-12-05", "2025-12-06"
              )
      )
      SELECT DISTINCT

        w.id,
        w.name_link

      FROM recent_events re

      LEFT JOIN wrestler_list_scrape_data w ON w.wrestling_season = re.wrestling_season
          AND w.track_wrestling_category  = re.track_wrestling_category
          AND (
                -- 1) primary: match on team_id when present
                (re.team_id IS NOT NULL AND w.team_id = re.team_id)
                -- 2) fallback: match on team name when event.team_id is NULL
                OR (re.team_id IS NULL AND w.team = re.team_name_raw)
              )
      WHERE 1 = 1
        AND w.name_link IS NOT NULL AND w.name_link <> ''
      GROUP BY 1, 2
      ORDER BY 1, 2
      LIMIT ? OFFSET ?
      ;
  `;

  console.log('log inside iter_name_links_from_db', link_query);

  while (yielded < limit) {
    const to_fetch = Math.min(batch_size, limit - yielded);
    const [rows] = await pool.query(link_query, [to_fetch, offset]);

    if (!rows.length) break;

    for (const row of rows) {
      const url = row.name_link;
      const i = yielded + 1; // 1-based like your CSV logs
      yield { i, url };
      yielded += 1;
      if (yielded >= limit) break;
    }

    offset += rows.length;
  }
}

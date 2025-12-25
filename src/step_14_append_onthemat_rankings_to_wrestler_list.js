// src/step_14_append_onthemat_rankings_to_wrestler_list.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";
import { run_parse_wrestler_docx_v2 } from "../utilities/on_the_mat_parsers/parse_wrestler_list_from_docx_v2.js";

// -------------------------------------------------
// Auto-generate NAME_FIXES from list ↔ rankings candidates
// -------------------------------------------------
async function build_name_fixes_from_candidates(pool, {
  wrestling_season = "2025-26",
  track_wrestling_category = "High School Boys",
  min_score = 4,          // 0..5, recommend 4 or 5
  limit = 5000
} = {}) {

  // NOTE: MySQL CTEs must be declared in ONE WITH clause (no nested WITH).
  const candidates_sql = `
    WITH
    r AS (
      SELECT
        r.wrestler_name AS ranking_wrestler_name,
        r.school        AS ranking_school,
        r.weight_lbs    AS ranking_weight_lbs,
        LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(r.wrestler_name,'[^A-Za-z0-9 ]',' '),'\\\\s+',' '))) AS ranking_name_norm,
        LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(r.school,'[^A-Za-z0-9 ]',' '),'\\\\s+',' ')))       AS ranking_school_norm
      FROM reference_wrestler_rankings_list r
      WHERE r.wrestling_season = ?
        AND r.track_wrestling_category = ?
    ),
    l AS (
      SELECT
        l.id AS list_id,
        l.name AS list_wrestler_name,
        SUBSTRING_INDEX(l.team, ',', 1) AS list_school,
        l.weight_class AS list_weight_lbs,
        LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(l.name,'[^A-Za-z0-9 ]',' '),'\\\\s+',' '))) AS list_name_norm,
        LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(SUBSTRING_INDEX(l.team, ',', 1),'[^A-Za-z0-9 ]',' '),'\\\\s+',' '))) AS list_school_norm
      FROM wrestler_list_scrape_data l
      WHERE l.wrestling_season = ?
        AND l.track_wrestling_category = ?
        AND (l.onthemat_is_name_match = 0 OR l.onthemat_is_name_match IS NULL)
    ),
    candidates AS (
      SELECT
        l.list_id,
        l.list_wrestler_name,
        l.list_school,
        l.list_weight_lbs,

        r.ranking_wrestler_name,
        r.ranking_school,
        r.ranking_weight_lbs,

        l.list_name_norm,
        r.ranking_name_norm,

        (
          (LOCATE(SUBSTRING_INDEX(l.list_name_norm,' ',1),   r.ranking_name_norm) > 0) +
          (LOCATE(SUBSTRING_INDEX(l.list_name_norm,' ', -1), r.ranking_name_norm) > 0) +
          (SOUNDEX(l.list_name_norm) = SOUNDEX(r.ranking_name_norm)) +
          (r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')) +
          (l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%'))
        ) AS match_score_0_to_5

      FROM l
        JOIN r ON 
          -- relaxed join to exclude weight b/c weight changes
          r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')
                    OR l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%')

          -- 	  r.ranking_weight_lbs = l.list_weight_lbs
          --       AND (
          --             r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')
          --             OR l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%')
          --       )
      WHERE
        (SOUNDEX(l.list_name_norm) = SOUNDEX(r.ranking_name_norm)
         OR LOCATE(SUBSTRING_INDEX(l.list_name_norm,' ', -1), r.ranking_name_norm) > 0)
    ),
    ranked AS (
      SELECT
        c.*,
        ROW_NUMBER() OVER (
          PARTITION BY c.list_id
          ORDER BY c.match_score_0_to_5 DESC, c.ranking_wrestler_name
        ) AS rn
      FROM candidates c
    )
    SELECT
      list_id,
      list_wrestler_name,
      list_school,
      list_weight_lbs,

      ranking_wrestler_name,
      ranking_school,
      ranking_weight_lbs,

      match_score_0_to_5
    FROM ranked
    WHERE rn = 1
      AND match_score_0_to_5 >= ?
    ORDER BY match_score_0_to_5 DESC, list_id
    LIMIT ?
  `;

  const [rows] = await pool.query(candidates_sql, [
    wrestling_season,
    track_wrestling_category,
    wrestling_season,
    track_wrestling_category,
    min_score,
    limit
  ]);

  // Build [["Good Name", "Bad Name"], ...] where "bad" is rankings, "good" is list
  // Deduplicate by (from→to). Also avoid no-op mappings.
  const seen = new Set();
  const fixes = [];

  for (const row of rows) {
    const to   = String(row.list_wrestler_name || "").trim();    // Good name
    const from = String(row.ranking_wrestler_name || "").trim(); // Bad name
    if (!from || !to) continue;
    if (from === to) continue;

    const key = `${from}→${to}`;
    if (seen.has(key)) continue;
    seen.add(key);

    fixes.push([to, from]);
  }

  return fixes;
}

async function update_rankings_list_with_fixed_name(pool) {
    // VERSION 1 NAME FIXES
    // const [res] = await pool.query(`
    //   UPDATE reference_wrestler_rankings_list
    //     SET wrestler_name = 'Braden Laiminger'
    //     -- WHERE id = 792
    //     WHERE wrestler_name = 'Baden Laiminger'
    //       AND wrestling_season = '2025-26'
    //       AND track_wrestling_category = 'High School Boys'
    // `);
    // console.log("test update:", { affectedRows: res.affectedRows, changedRows: res.changedRows });

    // VERSION 2 NAME FIXES
    // const NAME_FIXES = [
    //   ["Baden Laiminger", "Braden Laiminger"],
    //   ["Elijah Baumgartner", "Elijah Baumgardner"],
    //   ['Talen Phillips', 'Taten Phillips'],
    //   ['Teagan  Young', 'Teagan Young'],
    // ];

    // for (const [from, to] of NAME_FIXES) {
    //   const [res] = await pool.query(
    //     `
    //       UPDATE reference_wrestler_rankings_list
    //         SET wrestler_name = ?
    //         WHERE wrestler_name = ?
    //           AND wrestling_season = '2025-26'
    //           AND track_wrestling_category = 'High School Boys'
    //     `,
    //     [to, from]
    //   );

    //   console.log(`name fix "${from}" → "${to}"`, {
    //     affectedRows: res.affectedRows,
    //     changedRows: res.changedRows
    //   });
    // }
    // const [result] = await pool.query(update_sql, [updated_at_mtn, updated_at_utc]);
    // update_result = result;

    // VERSION 3 NAME FIXES
    // Auto-suggest name fixes for unmatched list rows (score-based)
    const NAME_FIXES_AUTO = await build_name_fixes_from_candidates(pool, {
      wrestling_season: "2025-26",
      track_wrestling_category: "High School Boys",
      min_score: 4,    // try 5 if you want ultra-strict
      limit: 5000
    });

    console.log("NAME_FIXES_AUTO (from candidates) =", {
      count: NAME_FIXES_AUTO.length,
      sample: NAME_FIXES_AUTO.slice(0, 10)
    });

    const NAME_FIXES_MANUAL_AND_AUTO = [
      ...NAME_FIXES_AUTO, // ✅ merge in auto fixes

      // MANUAL FIXES
      // ['list name', 'on the mat ranking name'], template

      ['Alberto Cedillo', 'Albert Cedillo'],
      ['Andreas Bowman', 'AJ Bowman'],
      ['Angelo Garcia', 'Anthony Garcia'],
      ['Benjamin Gomez', 'Ben Gomez'],
      ['Cayetano Cushing', 'Caye Cushing'],
      ['Charles Rider', 'Charlie Rider'],
      ['Christopher Alvarez', 'Chris Alvarez'],
      ['Derick Sievertson', 'Derrick Sievertsen'],
      ['Edward Hallberg', 'EJ Hallberg'],
      ['Joseph Gamez', 'Joe Gamez'],
      ['Kamden Kenney', 'Kamden Kenny'],
      ['Manuel Mota', 'Manny Mota'],
      ['Mason Hill', 'Nathan Hill'],
      ['Maximus Mcnett', 'Max Mcnett'],
      ['Nate Hill', 'Nathan Hill'],
      ['Nicholas Penfold', 'Nick Penfold'],
      ['Robert Mitchell', 'Bobby Mitchell'],
      ['Santiino Garcia-Sanchez', 'Santino Sanchez'],
      ['Taten Phillips', 'Talen Phillips'],
      ['Zachary Stevens', 'Zach Stevens'],

      ['Tony Griego', 'Anthony Griego'],
      ['Cameron Benavidez', 'Cam Benavidez'],
      ['Elijah Pulford', 'Eli Pulford'],
      ['Grayson Luxner', 'Gray Luxner'],
      ['Mace Harris', 'Mason Harris'],
      ['Xavier Jacquez', 'Zavier Jacquez'],
      	
      ['Jeremiah Waldschmidt', 'Jerry Waldschmidt'],
      ['Kol Herskind', 'Kai Herskind'],
      ['Braden Laiminger', 'Baden Laiminger'],

      ['Rene Ornelas Arreola', 'Rene Ornelas Ameola'],
      ['Xaiyven Calma-Viloria', 'Xaiyven Calma-Vitoria'],
    ];

    // UPDATE reference_wrestler_rankings_list with FIXED NAME
    for (const [to, from] of NAME_FIXES_MANUAL_AND_AUTO) {
      const [res] = await pool.query(
        `
          UPDATE reference_wrestler_rankings_list
            SET wrestler_name = ?
            WHERE 1 = 1
              AND wrestler_name = ?
              AND wrestling_season = '2025-26'
              AND track_wrestling_category = 'High School Boys'
        `,
        [to, from]
      );

      console.log(`name fix "${from}" → "${to}"`, {
        affectedRows: res.affectedRows,
        changedRows: res.changedRows
      });
    }
}

async function build_team_fixes_from_candidates(pool, {
  wrestling_season = "2025-26",
  track_wrestling_category = "High School Boys",
  candidate_team_count = 1, // 0 or > 1 either don't have a team in the wrestler list or have more than 1 similar name
  limit = 5000
} = {}) {

  // NOTE: MySQL CTEs must be declared in ONE WITH clause (no nested WITH).
  const candidates_sql = `
    WITH
      r AS (
        SELECT DISTINCT
          school AS ranking_school,
          LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(school,'[^A-Za-z0-9 ]',' '),'\\s+',' '))) AS ranking_school_norm
        FROM reference_wrestler_rankings_list
        WHERE wrestling_season = ?
          AND track_wrestling_category = ?
      ),
      l AS (
        SELECT DISTINCT
          team AS list_team,
          SUBSTRING_INDEX(team, ',', 1) AS list_school,
          LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(SUBSTRING_INDEX(team, ',', 1),'[^A-Za-z0-9 ]',' '),'\\s+',' '))) AS list_school_norm
        FROM wrestler_list_scrape_data
        WHERE wrestling_season = ?
          AND track_wrestling_category = ?
      )
      SELECT
        r.ranking_school,
        GROUP_CONCAT(DISTINCT l.list_team ORDER BY l.list_team SEPARATOR ' , ') AS list_school,
        
          -- JS-style array literal you can copy/paste into NAME_FIXES:
        CONCAT(
          "['",
          REPLACE(GROUP_CONCAT(DISTINCT l.list_team ORDER BY l.list_team SEPARATOR ' , '), "'", "\\'"),
          "', '",
          REPLACE(r.ranking_school, "'", "\\'"),
          "']"
        ) AS name_fix_array,
        
        COUNT(DISTINCT l.list_team) AS candidate_team_count
      FROM r
      LEFT JOIN l
        ON l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%')
        OR r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')
      WHERE 1 = 1
      GROUP BY r.ranking_school
      HAVING 1 = 1
        AND candidate_team_count = ?
      ORDER BY candidate_team_count DESC, r.ranking_school
    ;
  `;

  const [rows] = await pool.query(candidates_sql, [
    wrestling_season,
    track_wrestling_category,
    wrestling_season,
    track_wrestling_category,
    candidate_team_count,
    limit
  ]);

  // Build [["Good Name", "Bad Name"], ...] where "bad" is rankings, "good" is list
  // Deduplicate by (from→to). Also avoid no-op mappings.
  const seen = new Set();
  const fixes = [];

  for (const row of rows) {
    const to   = String(row.list_school || "").trim();    // Good name
    const from = String(row.ranking_school || "").trim(); // Bad name
    if (!from || !to) continue;
    if (from === to) continue;

    const key = `${from}→${to}`;
    if (seen.has(key)) continue;
    seen.add(key);

    fixes.push([to, from]);
  }

  return fixes;
}

async function update_rankings_school_with_fixed_school(pool) {

    // Auto-suggest team fixes for unmatched list rows (score-based)
    const NAME_FIXES_AUTO = await build_team_fixes_from_candidates(pool, {
      wrestling_season: "2025-26",
      track_wrestling_category: "High School Boys",
      candidate_team_count: 1, // 0 or > 1 either don't have a team in the wrestler list or have more than 1 similar name
      limit: 5000
    });

    console.log("TEAM_FIXES_AUTO (from candidates) =", {
      count: NAME_FIXES_AUTO.length,
      sample: NAME_FIXES_AUTO.slice(0, 10)
    });

    const NAME_FIXES_MANUAL_AND_AUTO = [
      ...NAME_FIXES_AUTO, // ✅ merge in auto fixes
      
      // ['list team', 'on the mat ranking team'], template

      // NO MATCH B/C OF "fr" AT END
      ['Roosevelt High School, CO', 'Roosevelt, Fr'],
      ['Roosevelt High School, CO', 'Roosevelt Fr'],
      ['Roosevelt High School, CO', 'Roosevelt F'],

      // MULTIPLE POSSIBLE MATCHES; I NARROWED DOWN TO THE MOST LIKELY
      ['Arvada West, CO', 'Arvada West'],
      ['The Classical Academy, CO', 'Classical Academy'],
      ['Eagle Valley, CO', 'Eagle Valley'],
      ['Falcon, CO', 'Falcon'],
      ['Golden, CO', 'Golden'],
      ['Grand Valley, CO', 'Grand Valley'],
      ['Highland, CO', 'Highland'],
      ['Jefferson, CO', 'Jefferson'],
      ['Lewis-Palmer, CO', 'Lewis Palmer'],
      ['Palmer, CO', 'Palmer'],
      ['Palmer Ridge, CO', 'Palmer Ridge'],
      ['Platte Valley, CO', 'Platte Valley'],
      ['Ralston Valley, CO', 'Ralston Valley'],
      ['Skyline Falcons, CO', 'Skyline Falcons'],
      ['Thomas Jefferson, CO', 'Thomas Jefferson'],
      ['Thompson Valley, CO', 'Thompson Valley'],
      ['Valley, CO', 'Valley'],
      ['Walsenburg, CO', 'Walsenburg'],
      ['Walsenburg, CO', 'Walsenburg Jr/Sr High School'],
    ];

    // UPDATE reference_wrestler_rankings_list with FIXED TEAM
    for (const [to, from] of NAME_FIXES_MANUAL_AND_AUTO) {
      const [res] = await pool.query(
        `
          UPDATE reference_wrestler_rankings_list
            SET school = SUBSTRING_INDEX(?, ',', 1)
            WHERE 1 = 1
              AND school = ?
              AND wrestling_season = '2025-26'
              AND track_wrestling_category = 'High School Boys'
        `,
        [to, from]
      );

      console.log(`name fix "${from}" → "${to}"`, {
        affectedRows: res.affectedRows,
        changedRows: res.changedRows
      });
    }
}

async function update_team_with_team_transfer(pool) {
    const NAME_FIXES_MANUAL_AND_AUTO = [
      // ['name','old ranked team','new ranked team based on track wrestler school'], template

      ['Elijah Hernandez', 'Grand Junction' , 'Central-GJ, CO'],
      ['Micah Bautista', 'Grand Junction' , 'Central-GJ, CO'],
      ['Tristan Valdez', 'Grand Junction' , 'Central-GJ, CO'],
    ];

    // UPDATE reference_wrestler_rankings_list with FIXED TEAM
    for (const [name, from, to] of NAME_FIXES_MANUAL_AND_AUTO) {
      const [res] = await pool.query(
        `
          UPDATE reference_wrestler_rankings_list
            SET school = SUBSTRING_INDEX(?, ',', 1)
            WHERE 1 = 1
              AND wrestler_name = ?
              AND school = ?
              AND wrestling_season = '2025-26'
              AND track_wrestling_category = 'High School Boys'
        `,
        [to, name, from]
      );

      console.log(`name fix "${from}" → "${to}"`, {
        affectedRows: res.affectedRows,
        changedRows: res.changedRows
      });
    }
}

/* -------------------------------------------------
   Ensure OnTheMat columns exist on LIST table
--------------------------------------------------*/
async function drop_onthemat_columns_from_wrestler_list(pool) {
  // const pool = await get_pool();

  const drop_sql = `
    ALTER TABLE wrestler_list_scrape_data
      DROP COLUMN onthemat_is_name_match,
      DROP COLUMN onthemat_name,
      DROP COLUMN onthemat_is_team_match,
      DROP COLUMN onthemat_team,
      DROP COLUMN onthemat_rankings_source_file,
      DROP COLUMN onthemat_rank,
      DROP COLUMN onthemat_weight_lbs
  `;

  await pool.query(drop_sql);

  console.log("OnTheMat columns dropped from wrestler_list_scrape_data 🧹");
}

async function ensure_onthemat_columns(pool) {
  const alters = [
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_is_name_match TINYINT NULL AFTER wrestler_state_tournament_place
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_name TEXT NULL AFTER onthemat_is_name_match
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_is_team_match TINYINT NULL AFTER onthemat_name
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_team TEXT NULL AFTER onthemat_is_team_match
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_rank INT NULL AFTER onthemat_team
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_weight_lbs INT NULL AFTER onthemat_rank
    `,
    `
      ALTER TABLE wrestler_list_scrape_data
        ADD COLUMN onthemat_rankings_source_file VARCHAR(50) NULL AFTER onthemat_weight_lbs
    `,
  ];

  for (const sql of alters) {
    try {
      await pool.query(sql);
    } catch (err) {
      if (err.code === "ER_DUP_FIELDNAME") continue;
      throw err;
    }
  }
}

async function get_sql_to_update_wrestler_list_with_onthemat_rankings() {
  // -------------------------------------------------
  // 3) UPDATE LIST TABLE from OnTheMat rankings
  //    (transaction managed in JS; no multi-statement query)
  // -------------------------------------------------
  const update_sql = `
    UPDATE wrestler_list_scrape_data l
      LEFT JOIN (
        SELECT
          wrestler_name,
          school,

          -- pick the latest week we have for this wrestler/school
          MAX(ranking_week_number) AS latest_ranking_week_number,

          -- carry the source_file corresponding to the latest week
          SUBSTRING_INDEX(
            GROUP_CONCAT(source_file ORDER BY ranking_week_number DESC SEPARATOR '||'),
            '||',
            1
          ) AS source_file,

          -- rank/weight for the latest week (deterministic)
          SUBSTRING_INDEX(
            GROUP_CONCAT(\`rank\` ORDER BY ranking_week_number DESC, \`rank\` ASC SEPARATOR '||'),
            '||',
            1
          ) AS onthemat_rank,

          SUBSTRING_INDEX(
            GROUP_CONCAT(weight_lbs ORDER BY ranking_week_number DESC SEPARATOR '||'),
            '||',
            1
          ) AS weight_lbs

        FROM reference_wrestler_rankings_list
        WHERE 1 = 1
          -- If you want to restrict the join to specific snapshots (recommended while validating week_0 vs week_1):
          -- AND ranking_week_number IN (0, 1)

          -- Default / production behavior:
          -- Leave this commented out to include ALL ranking weeks.
        GROUP BY wrestler_name, school
      ) r
        ON r.wrestler_name = l.name
          AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1)

    SET
      l.onthemat_is_name_match =
        CASE WHEN r.wrestler_name IS NULL THEN 0 ELSE 1 END,

      l.onthemat_name = r.wrestler_name,

      l.onthemat_is_team_match =
        CASE
          WHEN r.wrestler_name IS NULL THEN NULL
          WHEN r.school LIKE SUBSTRING_INDEX(l.team, ',', 1) THEN 1
          ELSE 0
        END,

      l.onthemat_team = r.school,

      l.onthemat_rankings_source_file = r.source_file,

      l.onthemat_rank = r.onthemat_rank,
      l.onthemat_weight_lbs = r.weight_lbs,

      l.updated_at_mtn = ?,
      l.updated_at_utc = ?

    WHERE l.wrestling_season = '2025-26'
      AND l.track_wrestling_category = 'High School Boys';
  `;

  return update_sql;
}

async function step_14_append_onthemat_rankings_to_wrestler_list() {
  // CREATE TIMESTAMPS
  const now_utc = new Date();
  const offset = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + offset * 3600 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

  // GET POOL
  const pool = await get_pool();

  // CREATE VARIABLES
  let update_result;
  let rollback_or_commit = "COMMIT"; // let rollback_or_commit = "ROLLBACK"; // change to "COMMIT" when ready

  try {
    await pool.query("START TRANSACTION");
    
    // 0) WHEN NECESSARY DROP THE ONTHEMAT RANKNGS LIST (TO START WITH CLEAN DATASET)
    await pool.query("DROP TABLE `wrestling_stats`.`reference_wrestler_rankings_list`");

    // 1) WHEN NECESSARY CREATE OR RECREATE THE ONTHEMAT RANKINGS LIST
    await run_parse_wrestler_docx_v2();

    // 2) WHEN NECESSARY REMOVE ONTHEMAT WRESTLER COLUMNS (TO START WITH CLEAN DATASET)
    await drop_onthemat_columns_from_wrestler_list(pool);

    // 3) APPEND OR ENSURE ONTHEMAT COLUMNS EXIST
    await ensure_onthemat_columns(pool);

    // 4) MODIFY WRESTLER NAMES IN ONTHEMAT RANKINGS TABLE (BASED ON NICKNAMES OR MISPELLINGS)
    await update_rankings_list_with_fixed_name(pool);
    await update_rankings_school_with_fixed_school(pool);
    await update_team_with_team_transfer(pool);

    // 5) UPDATE WRESTLER LIST WITH ONTHEMAT RANKINGS INFO
    const update_sql = await get_sql_to_update_wrestler_list_with_onthemat_rankings();
    const [result] = await pool.query(update_sql, [updated_at_mtn, updated_at_utc]);
    update_result = result;

    // Optional: show how many rows were updated inside the transaction
    // (ROW_COUNT() pertains to the last statement on this connection)
    const [[row_count]] = await pool.query("SELECT ROW_COUNT() AS rows_updated");
    console.log("rows_updated (ROW_COUNT) =", row_count?.rows_updated);

    await pool.query(rollback_or_commit);
  } catch (err) {
    try {
      await pool.query("ROLLBACK");
    } catch (rollback_err) {
      console.error("rollback failed:", rollback_err?.message || rollback_err);
    }
    throw err;
  }

  // console.log(
  //   "OnTheMat → wrestler_list_scrape_data updates complete 🔗",
  //   "affectedRows =", update_result?.affectedRows,
  //   "changedRows =", update_result?.changedRows,
  //   "txn =", rollback_or_commit
  // );

  // // -------------------------------------------------
  // // 4) Summary (LIST table perspective)
  // // -------------------------------------------------
  // const summary_sql = `
  //   SELECT
  //     SUM(CASE WHEN onthemat_is_name_match = 1 THEN 1 ELSE 0 END) AS matched_rows,
  //     SUM(CASE WHEN onthemat_is_name_match = 0 THEN 1 ELSE 0 END) AS unmatched_rows,
  //     COUNT(*) AS total_rows,
  //     SUM(CASE WHEN onthemat_rank IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_rank,
  //     SUM(CASE WHEN onthemat_weight_lbs IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_weight
  //   FROM wrestler_list_scrape_data
  //   WHERE wrestling_season = '2025-26'
  //     AND track_wrestling_category = 'High School Boys'
  // `;

  // const [rows] = await pool.query(summary_sql);
  // console.log("OnTheMat match summary 📊", rows[0]);
}

// await step_14_append_onthemat_rankings_to_wrestler_list();
// step_14_drop_onthemat_columns_from_wrestler_list();

export { step_14_append_onthemat_rankings_to_wrestler_list };

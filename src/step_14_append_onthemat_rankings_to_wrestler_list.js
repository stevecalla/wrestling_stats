// src/step_14_append_onthemat_rankings_to_wrestler_list.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import { get_pool } from "../utilities/mysql/mysql_pool.js";
import { get_mountain_time_offset_hours } from "../utilities/date_time_tools/get_mountain_time_offset_hours.js";

// -------------------------------------------------
// Auto-generate NAME_FIXES from list↔rankings candidates
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
      JOIN r
        ON r.ranking_weight_lbs = l.list_weight_lbs
       AND (
            r.ranking_school_norm LIKE CONCAT('%', l.list_school_norm, '%')
         OR l.list_school_norm LIKE CONCAT('%', r.ranking_school_norm, '%')
       )
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

  // Build [["Bad Name", "Good Name"], ...] where "bad" is rankings, "good" is list
  // Deduplicate by (from→to). Also avoid no-op mappings.
  const seen = new Set();
  const fixes = [];

  for (const row of rows) {
    const from = String(row.ranking_wrestler_name || "").trim();
    const to   = String(row.list_wrestler_name || "").trim();
    if (!from || !to) continue;
    if (from === to) continue;

    const key = `${from}→${to}`;
    if (seen.has(key)) continue;
    seen.add(key);

    fixes.push([from, to]);
  }

  return fixes;
}

/* -------------------------------------------------
   Ensure OnTheMat columns exist on LIST table
--------------------------------------------------*/
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

async function step_14_append_onthemat_rankings_to_wrestler_list() {
  const pool = await get_pool();

  const [[whoami]] = await pool.query(`
    SELECT 
        DATABASE() AS db,
        @@hostname AS mysql_hostname,
        @@port AS mysql_port,
        USER() AS mysql_user,
        CURRENT_USER() AS mysql_current_user
    `
  );
  console.log("DB identity:", whoami);

  // 1) Ensure columns exist
  await ensure_onthemat_columns(pool);

  // 2) Timestamps
  const now_utc = new Date();
  const offset = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + offset * 3600 * 1000);

  const updated_at_mtn = now_mtn;
  const updated_at_utc = now_utc;

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
        -- AND r.school LIKE SUBSTRING_INDEX(l.team, ',', 1)

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

  // NOTE:
  // - For a "test run", we ROLLBACK at the end.
  // - When you are ready to apply changes, switch to COMMIT.

  let update_result;
  // let rollback_or_commit = "ROLLBACK"; // change to "COMMIT" when ready
  let rollback_or_commit = "COMMIT"; // change to "COMMIT" when ready

  try {
    await pool.query("START TRANSACTION");

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

    const NAME_FIXES = [
      ["Baden Laiminger", "Braden Laiminger"],
      ["Elijah Baumgartner", "Elijah Baumgardner"],
      ["Talen Phillips", "Taten Phillips"],
      ["Teagan  Young", "Teagan Young"],
      ['Derrick Sievertsen', 'Derick Sievertson'],
      ['Nick Penfold', 'Nicholas Penfold'],
      ['EJ Hallberg', 'Edward Hallberg'],
      ['Ben Gomez', 'Benjamin Gomez'],
      ['Bobby Mitchell', 'Robert Mitchell'],
      ['Zach Stevens', 'Zachary Stevens'],
      ['Max Mcnett', 'Maximus Mcnett'],
      ['Kamden Kenny', 'Kamden Kenney'],
      ['Joe Gamez', 'Joseph Gamez'],
      ['Albert Cedillo', 'Alberto Cedillo'],
      ['Caye Cushing', 'Cayetano Cushing'],
      ['AJ Bowman', 'Andreas Bowman'],
      ['Chris Alvarez', 'Christopher Alvarez'],
      ['Charlie Rider', 'Charles Rider'],
      ['Santino Sanchez', 'Santiino Garcia-Sanchez'],
      ['James Danielson', 'JD Danielson'],
      ['Manny Mota', 'Manuel Mota'],
      ['Kai Herskind', 'Kol Herskind'],

      ...NAME_FIXES_AUTO, // ✅ merge in auto fixes
    ];

    for (const [from, to] of NAME_FIXES) {
      const [res] = await pool.query(
        `
          UPDATE reference_wrestler_rankings_list
            SET wrestler_name = ?
            WHERE wrestler_name = ?
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

async function step_14_drop_onthemat_columns_from_wrestler_list() {
  const pool = await get_pool();

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

// await step_14_append_onthemat_rankings_to_wrestler_list();
// step_14_drop_onthemat_columns_from_wrestler_list();

export { step_14_append_onthemat_rankings_to_wrestler_list };

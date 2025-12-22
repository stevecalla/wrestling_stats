// src/step_3_get_wrestler_match_history_parallel_scrape/step_1_create_scrape_tasks_table.js
// Creates the wrestler_match_history_scrape_tasks table if it doesn't exist.
//
// Notes:
// - Safe to run multiple times
// - v2: supports parallel workers + overlapping jobs via task_set_id
// - v3: explicit UTC + MTN timestamps (created / updated)

import { get_pool } from "../../utilities/mysql/mysql_pool.js";

async function main() {
  const pool = await get_pool();

  const sql = `
    CREATE TABLE IF NOT EXISTS wrestler_match_history_scrape_tasks (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      -- ✅ isolates separate scheduled jobs / scopes / runs
      task_set_id VARCHAR(64) NOT NULL,
      job_type VARCHAR(32) NOT NULL DEFAULT 'list',

      wrestling_season VARCHAR(32) NOT NULL,
      track_wrestling_category VARCHAR(64) NOT NULL,
      gender VARCHAR(2) NOT NULL,

      wrestler_id BIGINT UNSIGNED NOT NULL,
      name_link VARCHAR(512) NULL,

      status ENUM('PENDING','LOCKED','DONE','FAILED') NOT NULL DEFAULT 'PENDING',

      attempt_count INT NOT NULL DEFAULT 0,

      locked_by VARCHAR(128) NULL,
      locked_at_utc DATETIME NULL,

      last_error TEXT NULL,

      -- ✅ human / local timestamps (derived at write time)
      created_at_mtn DATETIME NOT NULL,
      updated_at_mtn DATETIME NOT NULL,

      -- ✅ canonical timestamps (always UTC)
      created_at_utc DATETIME NOT NULL,
      updated_at_utc DATETIME NOT NULL,

      PRIMARY KEY (id),

      -- ✅ uniqueness scoped to task_set_id so overlapping jobs do not collide
      UNIQUE KEY uk_task (
        task_set_id,
        wrestling_season,
        track_wrestling_category,
        gender,
        wrestler_id
      ),

      -- worker / cleanup performance
      KEY idx_scope (
        task_set_id,
        wrestling_season,
        track_wrestling_category,
        gender,
        job_type
      ),
      KEY idx_status (task_set_id, status),
      KEY idx_locked (locked_at_utc),
      KEY idx_created_utc (created_at_utc),
      KEY idx_updated_utc (updated_at_utc)
    ) ENGINE=InnoDB;
  `;

  try {
    console.log(
      "🛠️ Creating table wrestler_match_history_scrape_tasks (v3, UTC+MTN timestamps) (if not exists)..."
    );
    await pool.query(sql);
    console.log("✅ Table ready: wrestler_match_history_scrape_tasks (v3)");
  } catch (err) {
    console.error("❌ Failed to create table:", err?.message || err);
    throw err;
  }
}

/* -------------------------------------------------
   EXPORT
--------------------------------------------------*/
export { main as step_1_create_scrape_tasks_table };

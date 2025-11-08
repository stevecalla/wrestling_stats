// src/db/upsert_wrestler_match_history.js
import { get_pool } from "./mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  const sql = `
    CREATE TABLE IF NOT EXISTS wrestler_match_history (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      wrestling_season VARCHAR(32)  NOT NULL,
      track_wrestling_category VARCHAR(32) NOT NULL,
      gender          VARCHAR(32)  NOT NULL,
      page_url        VARCHAR(1024) NULL,
      wrestler_id     BIGINT UNSIGNED NOT NULL,
      wrestler        VARCHAR(255)   NOT NULL,
      first_name      VARCHAR(255)   NULL,
      last_name       VARCHAR(255)   NULL,
      wrestler_school VARCHAR(255)  NULL,

      start_date      DATE          NULL,
      end_date        DATE          NULL,

      event           VARCHAR(255)  NULL,
      weight_category VARCHAR(64)   NULL,
      round           VARCHAR(128)  NULL,
      
      opponent        VARCHAR(255)  NULL,
      opponent_first_name VARCHAR(255) NULL,
      opponent_last_name  VARCHAR(255) NULL,
      opponent_id     BIGINT UNSIGNED NULL,
      opponent_school VARCHAR(255)  NULL,

      result          VARCHAR(64)   NULL,
      score_details   VARCHAR(255)  NULL,
      winner_name     VARCHAR(255)  NULL,
      outcome         VARCHAR(8)    NULL,        -- CHANGED (was CHAR(1)) now supports 'W','L','T','U','bye'
      counts_in_record TINYINT(1)   NULL,        -- CHANGED new: whether it impacted W-L-T (1/0)
      record          VARCHAR(64)   NULL,        -- "12-3-0 W-L-T"
      record_varsity  VARCHAR(64)   NULL,        -- CHANGED new: varsity-only split
      raw_details     TEXT          NULL,

      -- Timestamps:
      -- created_* are immutable (insert only).
      -- updated_* change on any update.
      created_at_mtn  DATETIME      NOT NULL,
      created_at_utc  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

      updated_at_mtn  DATETIME      NOT NULL,
      updated_at_utc  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

      UNIQUE KEY uk_match_sig (
        wrestler_id,
        start_date,
        event(120),
        round(60),
        opponent(120)
      ),
      KEY idx_wrestler_id_start (wrestler_id, start_date),
      PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;
  await pool.query(sql);
  _ensured = true;
}

// Minimal MM/DD/YYYY → YYYY-MM-DD (or return null if malformed)
function to_mysql_date(mdy) {
  if (!mdy) return null;
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(mdy);
  if (!m) return null;
  const [, mm, dd, yyyy] = m;
  return `${yyyy}-${String(mm).padStart(2, "0")}-${String(dd).padStart(2, "0")}`;
}

/**
 * Upsert an array of match rows from step 3 (one wrestler page).
 * - created_at_* set only on insert (immutable)
 * - updated_at_* always refreshed to "now" on update
 * - uses Mountain Time offset function for *_mtn columns
 *
 * @param {Array<object>} rows rows returned by extractor_source()
 */
export async function upsert_wrestler_match_history(rows, meta) {
  if (!rows?.length) return { inserted: 0, updated: 0 };

  await ensure_table();
  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via your offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  // For inserts:
  const created_at_utc = now_utc;
  const created_at_mtn = now_mtn;

  // For updates (and also initial insert's updated_*):
  const updated_at_utc = now_utc;
  const updated_at_mtn = now_mtn;
  
  // shape inbound → DB columns
  const wrestling_season = meta?.wrestling_season || "unknown";
  const track_wrestling_category = meta?.track_wrestling_category || "unknown";
  const gender = meta?.gender || "unknown";

  // Insert columns (include both created_* and updated_*; the ON DUPLICATE block
  // will avoid touching created_* but will refresh updated_*).
  const cols = [
    "wrestling_season", "track_wrestling_category", "gender", "page_url",
    "wrestler_id", "wrestler", "first_name", "last_name", "wrestler_school",
    "start_date", "end_date",
    "event", "weight_category", "round",
    "opponent", "opponent_first_name", "opponent_last_name", "opponent_id", "opponent_school",
    "result", "score_details", "winner_name", "outcome",
    "counts_in_record",                    // CHANGED new col
    "record", "record_varsity",            // CHANGED add varsity split
    "raw_details",
    "created_at_mtn", "created_at_utc",
    "updated_at_mtn", "updated_at_utc"
  ];

  const chunk_size = 500;
  let inserted = 0, updated = 0;

  for (let i = 0; i < rows.length; i += chunk_size) {
    const slice = rows.slice(i, i + chunk_size);

    const shaped = slice.map(r => ({
      wrestling_season,
      track_wrestling_category,
      gender,
      page_url: r.page_url ?? null,
      wrestler_id: Number(r.wrestler_id) || 0,
      wrestler: r.wrestler ?? "",
      first_name: r.first_name ?? null, 
      last_name: r.last_name ?? null,   
      wrestler_school: r.wrestler_school ?? null,

      start_date: to_mysql_date(r.start_date),
      end_date: to_mysql_date(r.end_date),

      event: r.event ?? null,
      weight_category: r.weight_category ?? null,
      round: r.round ?? null,
      opponent: r.opponent ?? null,
      opponent_first_name: r.opponent_first_name ?? null,
      opponent_last_name: r.opponent_last_name ?? null,  
      opponent_id: r.opponent_id ? Number(r.opponent_id) : null,
      opponent_school: r.opponent_school ?? null,

      result: r.result ?? null,
      score_details: r.score_details ?? null,
      winner_name: r.winner_name ?? null,
      outcome: r.outcome ?? null,                        // can now be 'bye'
      counts_in_record: (r.counts_in_record ?? null),    // CHANGED new field (expects true/false → 1/0 if you prefer)
      record: r.record ?? null,
      record_varsity: r.record_varsity ?? null,          // CHANGED new field
      raw_details: r.raw_details ?? null,

      // timestamps for the INSERT attempt
      created_at_mtn,
      created_at_utc,
      updated_at_mtn,
      updated_at_utc
    }));

    const placeholders = shaped
      .map((_, idx) => `(${cols.map(c => `:v${idx}_${c}`).join(",")})`)
      .join(",");

    const params = {};
    shaped.forEach((v, idx) => {
      for (const c of cols) params[`v${idx}_${c}`] = v[c];
    });

    const sql = `
      INSERT INTO wrestler_match_history (${cols.join(",")})
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE
        wrestling_season      = VALUES(wrestling_season),
        track_wrestling_category = VALUES(track_wrestling_category),
        gender             = VALUES(gender),
        page_url           = VALUES(page_url),
        wrestler           = VALUES(wrestler),
        first_name         = VALUES(first_name), 
        last_name          = VALUES(last_name),  
        wrestler_school    = VALUES(wrestler_school),
        end_date           = VALUES(end_date),
        event              = VALUES(event),
        weight_category    = VALUES(weight_category),
        round              = VALUES(round),
        opponent           = VALUES(opponent),
        opponent_first_name= VALUES(opponent_first_name),
        opponent_last_name = VALUES(opponent_last_name), 
        opponent_id        = VALUES(opponent_id),
        opponent_school    = VALUES(opponent_school),
        result             = VALUES(result),
        score_details      = VALUES(score_details),
        winner_name        = VALUES(winner_name),
        outcome            = VALUES(outcome),           -- CHANGED widened type supports 'bye'
        counts_in_record   = VALUES(counts_in_record),  -- CHANGED new field
        record             = VALUES(record),
        record_varsity     = VALUES(record_varsity),    -- CHANGED new field
        raw_details        = VALUES(raw_details),
        -- do NOT touch created_* on update:
        updated_at_mtn     = VALUES(updated_at_mtn),
        updated_at_utc     = CURRENT_TIMESTAMP
    `;

    const [res] = await pool.query({ sql, values: params });

    // Heuristic counts for ON DUPLICATE:
    const affected = Number(res.affectedRows || 0);
    const _updated = Math.max(0, affected - slice.length);
    const _inserted = slice.length - _updated;

    inserted += _inserted;
    updated += _updated;
  }

  return { inserted, updated };
}

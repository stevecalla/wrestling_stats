// utilities/mysql/upsert_team_schedule.js
import { get_pool } from "./mysql_pool.js";
import { get_mountain_time_offset_hours } from "../date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

// Minimal MM/DD/YYYY → YYYY-MM-DD (or return null if malformed)
function to_mysql_date(mdy) {
  if (!mdy) return null;
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(mdy);
  if (!m) return null;
  const [, mm, dd, yyyy] = m;
  return `${yyyy}-${String(mm).padStart(2, "0")}-${String(dd).padStart(2, "0")}`;
}

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  const sql = `
    CREATE TABLE IF NOT EXISTS team_schedule_scrape_data (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      wrestling_season         VARCHAR(32)  NOT NULL,
      track_wrestling_category VARCHAR(32)  NOT NULL,
      gender                   VARCHAR(8)   NULL,

      grid_page_index          INT UNSIGNED NULL,

      date_raw                 VARCHAR(32)  NULL,
      start_date               DATE         NULL,
      end_date                 DATE         NULL,

      event_name               VARCHAR(255) NULL,
      event_js                 VARCHAR(512) NULL,

      team_name_raw            VARCHAR(255) NULL,
      team_role                VARCHAR(32)  NULL,
      team_index               INT UNSIGNED NULL,
      team_id                  BIGINT UNSIGNED NULL,

      row_index_in_span        INT UNSIGNED NULL,
      search_span_label        VARCHAR(64)  NULL,
      row_index_global         INT UNSIGNED NULL,

      created_at_mtn           DATETIME     NOT NULL,
      created_at_utc           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

      updated_at_mtn           DATETIME     NOT NULL,
      updated_at_utc           DATETIME     NOT NULL,

      -- UNIQUE signature for one row of a team in an event
      UNIQUE KEY uk_team_schedule_sig (
        wrestling_season,
        track_wrestling_category,
        gender,
        start_date,
        event_name(150),
        team_name_raw(150),
        team_index
      ),

      KEY idx_team_schedule_span  (wrestling_season, track_wrestling_category, start_date),
      KEY idx_team_schedule_team  (wrestling_season, team_id),
      KEY idx_team_schedule_event (wrestling_season, start_date, event_name(150)),

      PRIMARY KEY (id)
    ) ENGINE=InnoDB
      DEFAULT CHARSET=utf8mb4
      COLLATE=utf8mb4_0900_ai_ci;
  `;
  await pool.query(sql);
  _ensured = true;
}

/**
 * Upsert an array of team-schedule rows from step 2.
 * - created_at_* set only on insert (immutable)
 * - updated_at_* only refreshed when the existing row’s data differs
 * @param {Array<object>} rows rows from step_2 enriched_rows
 * @param {object} meta { wrestling_season, track_wrestling_category, gender }
 */
export async function upsert_team_schedule(rows, meta) {
  if (!rows?.length) return { inserted: 0, updated: 0 };

  await ensure_table();
  const pool = await get_pool();

  // Batch timestamps (UTC → MTN)
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
  const gender = meta?.gender || null;

  // Insert columns (include both created_* and updated_*; the ON DUPLICATE block
  // will avoid touching created_* but will refresh updated_*).
  const cols = [
    "wrestling_season",
    "track_wrestling_category",
    "gender",

    "grid_page_index",

    "date_raw",
    "start_date",
    "end_date",

    "event_name",
    "event_js",

    "team_name_raw",
    "team_role",
    "team_index",
    "team_id",

    "row_index_in_span",
    "search_span_label",
    "row_index_global",

    "created_at_mtn",
    "created_at_utc",
    "updated_at_mtn",
    "updated_at_utc",
  ];

  const chunk_size = 500;
  let inserted = 0;
  let updated = 0;

  for (let i = 0; i < rows.length; i += chunk_size) {
    const slice = rows.slice(i, i + chunk_size);

    const shaped = slice.map((r) => ({
      wrestling_season,
      track_wrestling_category,
      gender,

      grid_page_index:
        typeof r.grid_page_index === "number" ? r.grid_page_index : null,

      date_raw: r.date_raw ?? null,
      start_date: to_mysql_date(r.start_date),
      end_date: to_mysql_date(r.end_date),

      event_name: r.event_name ?? null,
      event_js: r.event_js ?? null,

      team_name_raw: r.team_name_raw ?? null,
      team_role: r.team_role ?? null,
      team_index:
        typeof r.team_index === "number"
          ? r.team_index
          : r.team_index != null
          ? Number(r.team_index)
          : null,
      team_id: r.team_id != null ? Number(r.team_id) : null,

      row_index_in_span:
        typeof r.row_index_in_span === "number"
          ? r.row_index_in_span
          : r.row_index_in_span != null
          ? Number(r.row_index_in_span)
          : null,
      search_span_label: r.search_span_label ?? null,
      row_index_global:
        typeof r.row_index_global === "number"
          ? r.row_index_global
          : r.row_index_global != null
          ? Number(r.row_index_global)
          : null,

      created_at_mtn,
      created_at_utc,
      updated_at_mtn,
      updated_at_utc,
    }));

    const placeholders = shaped
      .map(
        (_, idx) =>
          `(${cols.map((c) => `:v${idx}_${c}`).join(",")})`
      )
      .join(",");

    const params = {};
    shaped.forEach((v, idx) => {
      for (const c of cols) params[`v${idx}_${c}`] = v[c];
    });

    const sql = `
      INSERT INTO team_schedule_scrape_data (${cols.join(",")})
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE

        -- Only bump updated_* if something actually changed
        updated_at_mtn = 
          CASE
            WHEN NOT (
              wrestling_season         <=> VALUES(wrestling_season) AND
              track_wrestling_category <=> VALUES(track_wrestling_category) AND
              gender                   <=> VALUES(gender) AND
              grid_page_index          <=> VALUES(grid_page_index) AND
              date_raw                 <=> VALUES(date_raw) AND
              start_date               <=> VALUES(start_date) AND
              end_date                 <=> VALUES(end_date) AND
              event_name               <=> VALUES(event_name) AND
              event_js                 <=> VALUES(event_js) AND
              team_name_raw            <=> VALUES(team_name_raw) AND
              team_role                <=> VALUES(team_role) AND
              team_index               <=> VALUES(team_index) AND
              team_id                  <=> VALUES(team_id) AND
              row_index_in_span        <=> VALUES(row_index_in_span) AND
              search_span_label        <=> VALUES(search_span_label) AND
              row_index_global         <=> VALUES(row_index_global)
            )
            THEN VALUES(updated_at_mtn)
            ELSE updated_at_mtn
          END,

        updated_at_utc = 
          CASE
            WHEN NOT (
              wrestling_season         <=> VALUES(wrestling_season) AND
              track_wrestling_category <=> VALUES(track_wrestling_category) AND
              gender                   <=> VALUES(gender) AND
              grid_page_index          <=> VALUES(grid_page_index) AND
              date_raw                 <=> VALUES(date_raw) AND
              start_date               <=> VALUES(start_date) AND
              end_date                 <=> VALUES(end_date) AND
              event_name               <=> VALUES(event_name) AND
              event_js                 <=> VALUES(event_js) AND
              team_name_raw            <=> VALUES(team_name_raw) AND
              team_role                <=> VALUES(team_role) AND
              team_index               <=> VALUES(team_index) AND
              team_id                  <=> VALUES(team_id) AND
              row_index_in_span        <=> VALUES(row_index_in_span) AND
              search_span_label        <=> VALUES(search_span_label) AND
              row_index_global         <=> VALUES(row_index_global)
            )
            THEN CURRENT_TIMESTAMP
            ELSE updated_at_utc
          END,

        wrestling_season         = VALUES(wrestling_season),
        track_wrestling_category = VALUES(track_wrestling_category),
        gender                   = VALUES(gender),
        grid_page_index          = VALUES(grid_page_index),

        date_raw                 = VALUES(date_raw),
        start_date               = VALUES(start_date),
        end_date                 = VALUES(end_date),

        event_name               = VALUES(event_name),
        event_js                 = VALUES(event_js),

        team_name_raw            = VALUES(team_name_raw),
        team_role                = VALUES(team_role),
        team_index               = VALUES(team_index),
        team_id                  = VALUES(team_id),

        row_index_in_span        = VALUES(row_index_in_span),
        search_span_label        = VALUES(search_span_label),
        row_index_global         = VALUES(row_index_global)
    `;

    const [res] = await pool.query({ sql, values: params });

    const affected = Number(res.affectedRows || 0);
    const _updated = Math.max(0, affected - slice.length);
    const _inserted = slice.length - _updated;

    inserted += _inserted;
    updated += _updated;
  }

  return { inserted, updated };
}

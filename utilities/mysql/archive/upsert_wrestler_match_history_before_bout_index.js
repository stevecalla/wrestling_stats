  // src/db/upsert_wrestler_match_history_before_bout_index.js
  import { get_pool } from "./mysql_pool.js";
  import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

  let _ensured = false;

  function normalize_raw_details(s) {
    return (s || "")
      .normalize("NFKD")                   // strip accents
      .replace(/[\u0300-\u036f]/g, "")     // remove diacritics
      .replace(/\s+/g, " ")                // collapse whitespace
      .trim()
      .toLowerCase();                      // optional: make case-insensitive
  }

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
      CREATE TABLE IF NOT EXISTS wrestler_match_history_scrape_data (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

        wrestling_season VARCHAR(32)  NOT NULL,
        track_wrestling_category VARCHAR(32) NOT NULL,
        page_url        VARCHAR(1024) NULL,

        wrestler_id     BIGINT UNSIGNED NOT NULL,
        wrestler        VARCHAR(255)   NOT NULL,

        start_date      DATE          NULL,
        end_date        DATE          NULL,

        event           VARCHAR(255)  NULL,
        weight_category VARCHAR(64)   NULL,

        match_order     INT UNSIGNED NULL,   -- 👈 NEW
        opponent_id     BIGINT UNSIGNED NULL,

        raw_details     TEXT          NOT NULL,

        -- Timestamps:
        -- created_* are immutable (insert only).
        -- updated_* change on any update.
        created_at_mtn  DATETIME      NOT NULL,
        created_at_utc  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

        updated_at_mtn  DATETIME      NOT NULL,
        updated_at_utc  DATETIME      NOT NULL,          -- *** CHANGED: removed ON UPDATE CURRENT_TIMESTAMP

        UNIQUE KEY uk_match_sig (
          wrestler_id,
          start_date,
          event(120),
          weight_category,
          raw_details(255)
        ),
        KEY idx_wrestler_id_start (wrestler_id, start_date),
        KEY idx_wmh_wrestler_season (wrestler_id, wrestling_season),
        KEY idx_wmh_season_cat (wrestling_season, track_wrestling_category, wrestler_id),
        KEY idx_wmh_date (start_date),
        
        /* INDEXES for running totals and season-based queries */
        KEY idx_wmh_season_wrestler_id (wrestling_season, wrestler_id, id),
        KEY idx_wmh_season_wrestler_start (wrestling_season, wrestler_id, start_date),

        PRIMARY KEY (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    `;
    await pool.query(sql);
    _ensured = true;
  }

  /**
   * Upsert an array of match rows from step 3 (one wrestler page).
   * - created_at_* set only on insert (immutable)
   * - updated_at_* only refreshed when the existing row’s data differs from the incoming row
   * - uses Mountain Time offset function for *_mtn columns
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
      "wrestling_season",
      "track_wrestling_category", 
      "page_url",

      "wrestler_id", 
      "wrestler", 

      "start_date",
      "end_date",

      "event", 
      "weight_category", 

      "match_order", 
      "opponent_id",

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
        page_url: r.page_url ?? null,
        wrestler_id: Number(r.wrestler_id) || 0,
        wrestler: r.wrestler ?? "",

        start_date: to_mysql_date(r.start_date),
        end_date: to_mysql_date(r.end_date),

        event: r.event ?? null,
        weight_category: r.weight_category ?? null,

        match_order: typeof r.match_order === "number" ? r.match_order : null,  // 👈 NEW
        opponent_id: r.opponent_id ? Number(r.opponent_id) : null,
        
        // raw_details: normalize_raw_details(r.raw_details),
        raw_details: r.raw_details,

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
        INSERT INTO wrestler_match_history_scrape_data (${cols.join(",")})
        VALUES ${placeholders}
        ON DUPLICATE KEY UPDATE

          -- do NOT touch created_* on update:
          -- Only bump updated_* if any tracked column actually changed (NULL-safe)
          -- updated_at_* must be listed first here / at the top of ths insert to detect the change
          updated_at_mtn = 
            CASE
              WHEN NOT (
                wrestling_season       <=> VALUES(wrestling_season) AND
                track_wrestling_category <=> VALUES(track_wrestling_category) AND
                wrestler               <=> VALUES(wrestler) AND
                start_date             <=> VALUES(start_date) AND
                end_date               <=> VALUES(end_date) AND
                event                  <=> VALUES(event) AND
                weight_category        <=> VALUES(weight_category) AND
                match_order            <=> VALUES(match_order) AND   -- 👈 include
                opponent_id            <=> VALUES(opponent_id) AND
                raw_details            <=> VALUES(raw_details)
              )
              THEN VALUES(updated_at_mtn)
              ELSE updated_at_mtn
            END,

          updated_at_utc = 
            CASE
              WHEN NOT (
                wrestling_season       <=> VALUES(wrestling_season) AND
                track_wrestling_category <=> VALUES(track_wrestling_category) AND
                wrestler               <=> VALUES(wrestler) AND
                start_date             <=> VALUES(start_date) AND
                end_date               <=> VALUES(end_date) AND
                event                  <=> VALUES(event) AND
                weight_category        <=> VALUES(weight_category) AND
                match_order            <=> VALUES(match_order) AND   -- 👈 include
                opponent_id            <=> VALUES(opponent_id) AND
                raw_details            <=> VALUES(raw_details)
              )
              THEN CURRENT_TIMESTAMP
              ELSE updated_at_utc
            END,
        
          wrestling_season            = VALUES(wrestling_season),
          track_wrestling_category    = VALUES(track_wrestling_category),
          page_url                    = VALUES(page_url),
          
          wrestler           = VALUES(wrestler),

          start_date         = VALUES(start_date),
          end_date           = VALUES(end_date),

          event              = VALUES(event),
          weight_category    = VALUES(weight_category),
          
          match_order        = VALUES(match_order),   -- 👈 NEW
          opponent_id        = VALUES(opponent_id),

          raw_details        = VALUES(raw_details)
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

// utilities/mysql/upsert_wrestlers_list.js
import { get_pool } from "./mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  const sql = `
    CREATE TABLE IF NOT EXISTS wrestler_list_scrape_data (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      -- SELECTION CRITERIA
      wrestling_season   VARCHAR(32)  NOT NULL,
      track_wrestling_category VARCHAR(32) NOT NULL,
      last_name_prefix   VARCHAR(8)   NOT NULL,
      grade              VARCHAR(64)  NULL,
      level              VARCHAR(64)  NULL,
      governing_body     VARCHAR(100) NULL,

      -- WRESTLER INFO
      wrestler_id        BIGINT UNSIGNED NULL,
      name               VARCHAR(255) NOT NULL,
      first_name         VARCHAR(255) NULL,
      last_name          VARCHAR(255) NULL,
      team               VARCHAR(255) NULL,
      team_id            BIGINT UNSIGNED NULL,
      weight_class       VARCHAR(64)  NULL,
      gender             VARCHAR(32)  NULL,

      record_text        VARCHAR(64)  NULL,
      wins               INT          NULL,
      losses             INT          NULL,
      matches            INT          NULL,
      win_pct            DECIMAL(5,3) NULL,

      -- WEBSITE URL / LINK DETAILS
      name_link          TEXT         NULL,
      team_link          TEXT         NULL,
      page_url           TEXT         NULL,

      -- timestamps
      created_at_mtn     DATETIME     NOT NULL,
      created_at_utc     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,

      updated_at_mtn     DATETIME     NOT NULL,
      updated_at_utc     DATETIME      NOT NULL,          -- *** CHANGED: removed ON UPDATE CURRENT_TIMESTAMP

      -- Hybrid uniqueness:
      UNIQUE KEY uk_wrestler (wrestling_season, wrestler_id),

      -- UNIQUE KEY uk_alpha    (wrestling_season, last_name_prefix, grade, level, name, team),
      KEY idx_alpha (wrestling_season, last_name_prefix, grade, level, name, team),

      INDEX ix_wrestler_id (wrestler_id),
      INDEX ix_team_id     (team_id),
      PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
  `;
  await pool.query(sql);
  _ensured = true;
}

/**
 * Upsert an array of rows from step 1.
 * - created_at_* are immutable (set on insert only)
 * - updated_at_* only refreshed when the existing row’s data differs from the incoming row
 * @param {Array<object>} rows one batch from a single letter+grade
 * @param {object} meta { wrestling_season, track_wrestling_category, last_name_prefix, grade, level, governing_body }
 */
export async function upsert_wrestlers_list(rows, meta) {
  if (!rows?.length) return { inserted: 0, updated: 0 };

  await ensure_table();
  const pool = await get_pool();

  // Batch timestamps (UTC → MTN via offset fn)
  const now_utc = new Date();
  const mtn_offset_hours = get_mountain_time_offset_hours(now_utc);
  const now_mtn = new Date(now_utc.getTime() + mtn_offset_hours * 60 * 60 * 1000);

  // Insert-side (immutable created_*), and update-side (updated_*)
  const created_at_utc = now_utc;
  const created_at_mtn = now_mtn;

  // For updates (and also initial insert's updated_*):
  const updated_at_utc = now_utc;
  const updated_at_mtn = now_mtn;

  // shape inbound → DB columns
  const wrestling_season = meta?.wrestling_season || "unknown";
  const track_wrestling_category = meta?.track_wrestling_category || "unknown";
  const last_name_prefix = meta?.last_name_prefix ?? meta?.prefix ?? ""; // fallback if caller still sends 'prefix'
  const grade = meta?.grade || null;
  const level = meta?.level || null;
  const governing_body = meta?.governing_body || null;

  const chunk_size = 500;
  let inserted = 0;
  let updated = 0;

  for (let i = 0; i < rows.length; i += chunk_size) {
    const slice = rows.slice(i, i + chunk_size);

    const values = slice.map((r) => ({
      wrestling_season,
      track_wrestling_category,
      last_name_prefix,
      grade,
      level,
      governing_body,

      name: r.name ?? "",
      first_name: r.first_name ?? null,
      last_name: r.last_name ?? null,
      name_link: r.name_link ?? null,
      team: r.team ?? null,
      team_link: r.team_link ?? null,

      wrestler_id: Number.isFinite(+r.wrestler_id) ? +r.wrestler_id : null,
      team_id: Number.isFinite(+r.team_id) ? +r.team_id : null,

      weight_class: r.weight_class ?? null,
      gender: r.gender ?? null,

      record_text: r.record ?? null,
      wins: Number.isFinite(+r.wins) ? +r.wins : null,
      losses: Number.isFinite(+r.losses) ? +r.losses : null,
      matches: Number.isFinite(+r.matches)
        ? +r.matches
        : (Number.isFinite(+r.wins) && Number.isFinite(+r.losses)
            ? +r.wins + +r.losses
            : null),
      win_pct: Number.isFinite(+r.win_pct)
        ? +r.win_pct
        : (Number.isFinite(+r.wins) && Number.isFinite(+r.losses) && +r.wins + +r.losses > 0
            ? Number((+r.wins / (+r.wins + +r.losses)).toFixed(3))
            : null),

      // timestamps for INSERT attempt
      created_at_mtn,
      created_at_utc,
      updated_at_mtn,
      updated_at_utc,

      page_url: r.page_url ?? null,
    }));

    const cols = [
      "wrestling_season",
      "track_wrestling_category",
      "last_name_prefix",
      "grade",
      "level",
      "governing_body",
      "wrestler_id",
      "name",
      "first_name",
      "last_name",
      "name_link",
      "team",
      "team_link",
      "team_id",
      "weight_class",
      "gender",
      "record_text",
      "wins",
      "losses",
      "matches",
      "win_pct",
      "created_at_mtn",
      "created_at_utc",
      "updated_at_mtn",
      "updated_at_utc",
      "page_url",
    ];

    const placeholders = values
      .map((_, idx) => `(${cols.map((c) => `:v${idx}_${c}`).join(",")})`)
      .join(",");

    const params = {};
    values.forEach((v, idx) => {
      for (const c of cols) params[`v${idx}_${c}`] = v[c];
    });

    const sql = `
      INSERT INTO wrestler_list_scrape_data (${cols.join(",")})
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE

        -- do NOT touch created_* on update:
        -- Only bump updated_* if any tracked column actually changed (NULL-safe)
        -- updated_at_* must be listed first here / at the top of ths insert to detect the change
        updated_at_mtn =
          CASE
            WHEN NOT (
              wrestling_season         <=> VALUES(wrestling_season) AND
              track_wrestling_category <=> VALUES(track_wrestling_category) AND
              wrestler_id              <=> VALUES(wrestler_id) AND
              team_id                  <=> VALUES(team_id) AND
              name                     <=> VALUES(name) AND       -- ✅ NEW (required)
              team                     <=> VALUES(team) AND       -- ✅ NEW (required)
              weight_class             <=> VALUES(weight_class) AND
              gender                   <=> VALUES(gender) AND
              level                    <=> VALUES(level) AND
              governing_body           <=> VALUES(governing_body) AND
              record_text              <=> VALUES(record_text) AND
              wins                     <=> VALUES(wins) AND
              losses                   <=> VALUES(losses) AND
              matches                  <=> VALUES(matches) AND
              win_pct                  <=> VALUES(win_pct) AND
              first_name               <=> VALUES(first_name) AND
              last_name                <=> VALUES(last_name)
            )
            THEN VALUES(updated_at_mtn)
            ELSE updated_at_mtn
          END,

        updated_at_utc =
          CASE
            WHEN NOT (
              wrestling_season         <=> VALUES(wrestling_season) AND
              track_wrestling_category <=> VALUES(track_wrestling_category) AND
              wrestler_id              <=> VALUES(wrestler_id) AND
              team_id                  <=> VALUES(team_id) AND
              name                     <=> VALUES(name) AND       -- ✅ NEW (required)
              team                     <=> VALUES(team) AND       -- ✅ NEW (required)
              weight_class             <=> VALUES(weight_class) AND
              gender                   <=> VALUES(gender) AND
              level                    <=> VALUES(level) AND
              governing_body           <=> VALUES(governing_body) AND
              record_text              <=> VALUES(record_text) AND
              wins                     <=> VALUES(wins) AND
              losses                   <=> VALUES(losses) AND
              matches                  <=> VALUES(matches) AND
              win_pct                  <=> VALUES(win_pct) AND
              first_name               <=> VALUES(first_name) AND
              last_name                <=> VALUES(last_name)
            )
            THEN CURRENT_TIMESTAMP
            ELSE updated_at_utc
          END,

        -- If either unique key hits (by wrestler_id or composite), update these fields:
        wrestling_season          = VALUES(wrestling_season),
        track_wrestling_category  = VALUES(track_wrestling_category),

        -- ✅ NEW (required): update name/team, but don't overwrite good values with blank/null
        name = CASE
          WHEN VALUES(name) IS NOT NULL AND VALUES(name) <> '' THEN VALUES(name)
          ELSE name
        END,
        team = CASE
          WHEN VALUES(team) IS NOT NULL AND VALUES(team) <> '' THEN VALUES(team)
          ELSE team
        END,

        name_link                 = VALUES(name_link),
        team_link                 = VALUES(team_link),
        wrestler_id               = VALUES(wrestler_id),
        team_id                   = VALUES(team_id),
        weight_class              = VALUES(weight_class),
        gender                    = VALUES(gender),
        level                     = VALUES(level),
        governing_body            = VALUES(governing_body),
        record_text               = VALUES(record_text),
        wins                      = VALUES(wins),
        losses                    = VALUES(losses),
        matches                   = VALUES(matches),
        win_pct                   = VALUES(win_pct),
        page_url                  = VALUES(page_url),
        first_name                = VALUES(first_name),
        last_name                 = VALUES(last_name)
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

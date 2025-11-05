// utilities/mysql/upsert_wrestlers_list.js
import { get_pool } from "./mysql_pool.js";
import { get_mountain_time_offset_hours } from "../../utilities/date_time_tools/get_mountain_time_offset_hours.js";

let _ensured = false;

async function ensure_table() {
  if (_ensured) return;
  const pool = await get_pool();

  const sql = `
    CREATE TABLE IF NOT EXISTS wrestler_list (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

      -- SELECTION CRITERIA
      season           VARCHAR(32)  NOT NULL,
      prefix           VARCHAR(8)   NOT NULL,
      grade            VARCHAR(64)  NULL,
      level            VARCHAR(64)  NULL,
      governing_body   VARCHAR(100) NULL,

      -- WRESTLER INFO
      wrestler_id      BIGINT UNSIGNED NULL,
      name             VARCHAR(255) NOT NULL,
      team             VARCHAR(255) NULL,
      team_id          BIGINT UNSIGNED NULL,
      weight_class     VARCHAR(64)  NULL,
      gender           VARCHAR(32)  NULL,

      record_text      VARCHAR(64)  NULL,
      wins             INT          NULL,
      losses           INT          NULL,
      matches          INT          NULL,
      win_pct          DECIMAL(5,3) NULL,

      -- WEBSITE URL / LINK DETAILS
      name_link        TEXT         NULL,
      team_link        TEXT         NULL,
      page_url         TEXT         NULL,

      -- timestamps
      created_at_mtn   DATETIME     NOT NULL,
      created_at_utc   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at_mtn   DATETIME     NOT NULL,
      updated_at_utc   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

      -- Hybrid uniqueness:
      UNIQUE KEY uk_wrestler (season, wrestler_id),
      UNIQUE KEY uk_alpha    (season, prefix, grade, level, name, team),

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
 * - updated_at_* are refreshed on update (and set on initial insert)
 * @param {Array<object>} rows one batch from a single letter+grade
 * @param {object} meta { season, prefix, grade, level, governing_body }
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
  const updated_at_utc = now_utc;
  const updated_at_mtn = now_mtn;

  // shape inbound → DB columns
  const season = meta?.season || "unknown";
  const prefix = meta?.prefix || "";
  const grade = meta?.grade || null;
  const level = meta?.level || null;
  const governing_body = meta?.governing_body || null;

  const chunk_size = 500;
  let inserted = 0;
  let updated = 0;

  for (let i = 0; i < rows.length; i += chunk_size) {
    const slice = rows.slice(i, i + chunk_size);

    const values = slice.map(r => ({
      season,
      prefix,
      grade,
      level,
      governing_body,

      name: r.name ?? "",
      name_link: r.name_link ?? null,
      team: r.team ?? null,
      team_link: r.team_link ?? null,

      wrestler_id: Number.isFinite(+r.wrestler_id) ? +r.wrestler_id : null,
      team_id:     Number.isFinite(+r.team_id)     ? +r.team_id     : null,

      weight_class: r.weight_class ?? null,
      gender: r.gender ?? null,

      record_text: r.record ?? null,
      wins: Number.isFinite(+r.wins) ? +r.wins : null,
      losses: Number.isFinite(+r.losses) ? +r.losses : null,
      matches: Number.isFinite(+r.matches)
        ? +r.matches
        : (Number.isFinite(+r.wins) && Number.isFinite(+r.losses)
            ? (+r.wins + +r.losses)
            : null),
      win_pct: Number.isFinite(+r.win_pct)
        ? +r.win_pct
        : ((Number.isFinite(+r.wins) && Number.isFinite(+r.losses) && (+r.wins + +r.losses) > 0)
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
      "season", "prefix", "grade", "level", "governing_body",
      "wrestler_id", "name", "name_link", "team", "team_link", "team_id",
      "weight_class", "gender",
      "record_text", "wins", "losses", "matches", "win_pct",
      "created_at_mtn", "created_at_utc",
      "updated_at_mtn", "updated_at_utc",
      "page_url"
    ];

    const placeholders = values
      .map((_, idx) => `(${cols.map(c => `:v${idx}_${c}`).join(",")})`)
      .join(",");

    const params = {};
    values.forEach((v, idx) => {
      for (const c of cols) params[`v${idx}_${c}`] = v[c];
    });

    const sql = `
      INSERT INTO wrestler_list (${cols.join(",")})
      VALUES ${placeholders}
      ON DUPLICATE KEY UPDATE
        -- If either unique key hits (by wrestler_id or composite), update these fields:
        name_link       = VALUES(name_link),
        team_link       = VALUES(team_link),
        wrestler_id     = VALUES(wrestler_id),
        team_id         = VALUES(team_id),
        weight_class    = VALUES(weight_class),
        gender          = VALUES(gender),
        level           = VALUES(level),
        governing_body  = VALUES(governing_body),
        record_text     = VALUES(record_text),
        wins            = VALUES(wins),
        losses          = VALUES(losses),
        matches         = VALUES(matches),
        win_pct         = VALUES(win_pct),
        page_url        = VALUES(page_url),
        -- keep created_* immutable; refresh only updated_*:
        updated_at_mtn  = VALUES(updated_at_mtn),
        updated_at_utc  = CURRENT_TIMESTAMP
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

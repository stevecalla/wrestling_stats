// utilities/mysql/iter_name_links_from_db.js
import { get_pool } from "./mysql_pool.js";

/** Fast COUNT(*) with a simple filter to avoid NULL/blank links */
export async function count_rows_in_db_wrestler_links() {
  const pool = await get_pool();
  const [rows] = await pool.query(
    "SELECT COUNT(*) AS cnt FROM wrestler_list WHERE name_link IS NOT NULL AND name_link <> ''"
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
} = {}) {
  const pool = await get_pool();

  let yielded = 0;
  let offset = start_at;

  while (yielded < limit) {
    const to_fetch = Math.min(batch_size, limit - yielded);
    const [rows] = await pool.query(
      `
        SELECT id, name_link
        FROM wrestler_list
        WHERE name_link IS NOT NULL AND name_link <> ''
        ORDER BY id
        LIMIT ? OFFSET ?
      `,
      [to_fetch, offset]
    );

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

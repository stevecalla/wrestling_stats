// src/step_3_get_wrestler_match_history.js (ESM, snake_case)
import fs from "fs";
import { parse } from "fast-csv";

/* ------------------------------------------
   csv streaming (minimal)
-------------------------------------------*/
/**
 * async generator that yields { i, url } from CSV's name_link column
 * @param {string} csv_path
 * @param {{start_at?: number, limit?: number}} opts
 */
async function* iter_name_links_from_csv(csv_path, { start_at = 0, limit = Infinity } = {}) {
  if (!fs.existsSync(csv_path)) throw new Error(`CSV not found: ${csv_path}`);

  let i = -1;         // data row index (excludes header)
  let emitted = 0;

  const stream = fs.createReadStream(csv_path).pipe(
    parse({ headers: true, trim: true, ignoreEmpty: true })
  );

  for await (const row of stream) {
    i += 1;
    if (i < start_at) continue;

    // accept case-variants just in case
    const url = row.name_link ?? row["name_link"] ?? row["NAME_LINK"];
    if (!url) continue;

    yield { i, url };

    emitted += 1;
    if (emitted >= limit) break;
  }
}

export { iter_name_links_from_csv };
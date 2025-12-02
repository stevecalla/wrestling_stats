// utilities\mysql\transfer_local_data_between_local_tables\1_transfer_data_between_local_tables.js
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import { get_pool, get_pool_stream } from "../mysql_pool.js";
import { start_timer, stop_timer } from "../../stop_watch_timer/timer.js";

function get_created_at_mtn() {
  return `
    SELECT 
      CASE 
        WHEN UTC_TIMESTAMP() >= DATE_ADD(
                DATE_ADD(CONCAT(YEAR(UTC_TIMESTAMP()), '-03-01'),
                    INTERVAL ((7 - DAYOFWEEK(CONCAT(YEAR(UTC_TIMESTAMP()), '-03-01')) + 1) % 7 + 7) DAY),
                INTERVAL 2 HOUR)
        AND UTC_TIMESTAMP() < DATE_ADD(
                DATE_ADD(CONCAT(YEAR(UTC_TIMESTAMP()), '-11-01'),
                    INTERVAL ((7 - DAYOFWEEK(CONCAT(YEAR(UTC_TIMESTAMP()), '-11-01')) + 1) % 7) DAY),
                INTERVAL 2 HOUR)
        THEN DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL -6 HOUR), '%Y-%m-%d %H:%i:%s')
        ELSE DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL -7 HOUR), '%Y-%m-%d %H:%i:%s')
        END AS created_at_mtn
    ;
  `;
}

function get_created_at_utc() {
  return `
    SELECT 
      DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%d %H:%i:%s') AS created_at_utc
    ;
  `;
}

// schema.js
async function create_target_table(dst, TABLE_NAME, TABLE_STRUCTURE, options = {}) {
  const { drop_if_exists = true } = options;

  if (drop_if_exists) {
    await dst.execute(`DROP TABLE IF EXISTS \`${TABLE_NAME}\``);
  }
  
  // your CREATE TABLE ... already uses IF NOT EXISTS
  await dst.execute(TABLE_STRUCTURE);
}

// Flushes one batch of rows into the target table via a single multi-row INSERT
async function flush_batch(dst, TABLE_NAME, rows, options = {}) {
  if (!rows || rows.length === 0) return;

  const cols    = Object.keys(rows[0]);
  const colList = cols.map(c => `\`${c}\``).join(',');

  // Build "(?,?,?),(?,?,?),…" with rows.length tuples
  const placeholders = rows
    .map(() => `(${cols.map(_ => '?').join(',')})`)
    .join(',');

  const verb = options.insert_ignore ? 'INSERT IGNORE' : 'INSERT';
  const sql  = `${verb} INTO \`${TABLE_NAME}\` (${colList}) VALUES ${placeholders}`;

  // Flatten all row values into one big array
  const values = [];
  for (const row of rows) {
    for (const col of cols) {
      const value = row[col];
      if (value === undefined) {
        console.error(`[ERROR] Undefined value found in column "${col}" for row:`, row);
        throw new Error(`Undefined value detected in column "${col}"`);
      }
      values.push(value);
    }
  }

  await dst.execute(sql, values);
}

async function execute_transfer_data_between_tables(BATCH_SIZE, TABLE_NAME, CREATE_TABLE_QUERY, GET_DATA_QUERY, QUERY_OPTIONS) {  
  const src = get_pool_stream();  // non‑promise, for .stream()
  const promisePool = await get_pool();        // this is PromisePool
  const dst = await promisePool.getConnection(); // promise API, for transaction + execute()

  start_timer('timer');
  let result = 'Transfer Failed'; // default unless successful
  let row_count = 0;

  // GET CREATED AT MTN TO PASS ALONG
  let [rows] = await src.promise().query(get_created_at_mtn());
  const created_at_mtn = rows[0]?.created_at_mtn;

  // GET CREATED AT UTC TO PASS ALONG
  [rows] = await src.promise().query(get_created_at_utc());
  const created_at_utc = rows[0]?.created_at_utc;

  const drop_if_exists = QUERY_OPTIONS?.drop_if_exists ?? true;
  const insert_ignore = QUERY_OPTIONS?.insert_ignore ?? false;

  try {
    await dst.beginTransaction(); // 1) Start transaction

    if (QUERY_OPTIONS?.is_create_table) // Only drop / create table the first time the function runs
      await create_target_table(dst, TABLE_NAME, CREATE_TABLE_QUERY, { drop_if_exists }); // 2) Create target table

    // Build the actual SQL string from the generator function
    const sql = GET_DATA_QUERY(created_at_mtn, created_at_utc, QUERY_OPTIONS);

    // console.log("\n===== ACTUAL SQL SENT TO MYSQL =====\n");
    // console.log(sql);
    // console.log("\n====================================\n");

    const stream = src
      .query(sql)
      .stream(); // 3) Stream from source

    let buffer = [];
    let totalRows = 0;
    let batchCount = 0;

    for await (const row of stream) {
      buffer.push(row);
      totalRows++;

      // Check for undefined values
      for (const [key, value] of Object.entries(row)) {
        if (value === undefined) {
          console.warn(`[Warning] Undefined value detected in field "${key}"`, row);
        }
      }

      if (buffer.length >= BATCH_SIZE) {
        await flush_batch(dst, TABLE_NAME, buffer, { insert_ignore });
        batchCount++;
        console.log(`[INFO] Flushed batch #${batchCount} (${batchCount * BATCH_SIZE} rows)...`);
        buffer = [];
      }
    }

    // 4) Flush leftover rows
    if (buffer.length) {
      await flush_batch(dst, TABLE_NAME, buffer, { insert_ignore });
      batchCount++;
      console.log(`[INFO] Flushed final batch #${batchCount} (${totalRows} total rows).`);
    }

    if (QUERY_OPTIONS?.is_create_table) {
      // Log a small sample from the temp table (e.g., first 5 rows)
      const [sampleRows] = await src.promise().query(`
        SELECT 
          * 
        FROM ${TABLE_NAME}
        ORDER BY 1
        LIMIT 5
      `);

      const sampleRowsLimited = sampleRows.map(row => {
        const limited = {};
        const keys = Object.keys(row).slice(0, 5); // get first 5 column names
        keys.forEach(key => limited[key] = row[key]);
        return limited;
      });

      console.log('SAMPLE OF FIRST FIVE ROWS & FIRST FIVE COLUMNS ONLY')
      console.table(sampleRowsLimited);

    }

    // Step 2: Count number of rows in rev_recognition_base_profile_ids_data
    // const src_v2 = await get_pool_stream();
    TABLE_NAME = `wrestler_match_history_wrestler_ids_data`;
    let [[{ count }]] = await src.promise().query(`SELECT COUNT(*) AS count FROM ${TABLE_NAME}`);
    row_count = count;

    console.log('*********************************');
    console.log('wrestler id table length = ', row_count);

    await dst.commit(); // 5) Commit transaction
    result = 'Transfer Successful';
    // console.log(`[SUCCESS] Transfer complete: ${totalRows} total rows in ${batchCount} batches.`);

  } catch (err) {
    await dst.rollback(); // Roll back if failure
    console.error('[ERROR] Transfer failed, rolled back transaction:', err);
    throw err;

  } finally {

    // await src.promise().query(`DROP TABLE IF EXISTS rev_recognition_base_profile_ids_data`);

    // src.end();

    dst.release();      // release connection back to pool
    // await promisePool.end();   // close pool

    stop_timer('timer');
  }

  return { result, row_count };
}

// execute_transfer_data_between_tables().catch(err => {
//     console.error('Stream failed:', err);
//     process.exit(1);
// });

export {
  execute_transfer_data_between_tables,
}


import fs from "fs";
import path from "path";
import fastcsv from "fast-csv";
import { Transform } from "stream";

/**
 * Stream query results directly to CSV (mysql2 non-promise pool required)
 * @param {Pool} pool - mysql2 **non-promise** pool (for .stream())
 * @param {string|object} query - SQL string or mysql2 query options { sql, values }
 * @param {string} filePath - output CSV path
 * @param {string} fileFlags - 'w' to overwrite, 'a' to append
 * @param {number} highWaterMark - rows pulled per chunk from MySQL
 */
async function stream_query_to_csv(pool, query, filePath, fileFlags = "w", highWaterMark = 1000) {
  return new Promise((resolve, reject) => {
    try {
      // 0) Ensure directory exists
      fs.mkdirSync(path.dirname(filePath), { recursive: true });

      // 1) Determine header/BOM behavior for append mode
      const appending = fileFlags.startsWith("a") && fs.existsSync(filePath);

      // 2) Create write stream
      const writeStream = fs.createWriteStream(filePath, { flags: fileFlags });

      // 3) CSV stream (keep your original settings; suppress headers/BOM when appending)
      const csvStream = fastcsv.format({
        headers: !appending,              // only write headers when not appending
        writeHeaders: !appending,
        writeBOM: !appending,             // Excel-friendly but only once
        quoteColumns: true,
        quote: '"',
        escape: '"',
        includeEndRowDelimiter: true,
        alwaysQuote: true,
      });

      // 4) MySQL query stream (requires mysql2 non-promise pool)
      const queryStream = pool.query(query).stream({ highWaterMark });

      let rows_count = 0;
      let charCount = 0;
      let byteCount = 0;

      // Counts Unicode code points and bytes flowing to disk
      const charCounter = new Transform({
        decodeStrings: false, // chunk will be a string
        transform(chunk, enc, cb) {
          charCount += [...chunk].length;                 // code points
          byteCount += Buffer.byteLength(chunk, "utf8");  // bytes
          this.push(chunk);
          cb();
        },
      });

      // 5) Wire up backpressure: pause MySQL if CSV buffer is saturated
      queryStream
        .on("data", (row) => {
          rows_count += 1;
          const ok = csvStream.write(row);
          if (!ok) {
            queryStream.pause();
            csvStream.once("drain", () => queryStream.resume());
          }
        })
        .on("end", () => csvStream.end())
        .on("error", (err) => {
          csvStream.destroy(err);
        });

      // 6) Pipe CSV -> counter -> file, and finalize
      csvStream
        .pipe(charCounter)
        .pipe(writeStream)
        .on("finish", async () => {
          // authoritative size on disk
          let size = byteCount;
          try {
            const { size: statSize } = await fs.promises.stat(filePath);
            size = statSize;
          } catch {}
          console.log(
            `\n\nWrote #1: ${rows_count} rows_count to ${filePath} (${size.toLocaleString()} bytes) ${charCount.toLocaleString()} characters`
          );
          resolve({ filePath, rows_count, sizeBytes: size, charCount });
        })
        .on("error", (err) => {
          // ensure upstream stopped
          queryStream.destroy(err);
          reject(err);
        });
    } catch (err) {
      reject(err);
    }
  });
}

export { stream_query_to_csv };

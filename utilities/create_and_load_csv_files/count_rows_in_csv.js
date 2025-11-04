import fs from "fs";
import { parse } from "fast-csv";

async function count_rows_in_csv(csv_path) {
  return new Promise((resolve, reject) => {
    let count = 0;
    fs.createReadStream(csv_path)
      .pipe(parse({ headers: true }))
      .on("error", reject)
      .on("data", () => count++)
      .on("end", () => resolve(count));
  });
}

export { count_rows_in_csv };
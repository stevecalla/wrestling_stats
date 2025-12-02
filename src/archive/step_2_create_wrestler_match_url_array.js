// import fs from "fs";
// import path from "path";
// import { parse } from "csv-parse/sync";

// /**
//  * Reads the CSV and extracts Name_link column
//  */
// function extract_wrestler_match_link(INPUT_CSV_FILE_PATH) {
//   if (!fs.existsSync(INPUT_CSV_FILE_PATH)) {
//     throw new Error(`File not found: ${INPUT_CSV_FILE_PATH}`);
//   }

//   const csvText = fs.readFileSync(INPUT_CSV_FILE_PATH, "utf8");
//   const records = parse(csvText, {
//     columns: true,
//     skip_empty_lines: true,
//     trim: true,
//   });

//   const first = records[0] || {};
//   const nameCol = Object.keys(first).find(
//     (k) => k.toLowerCase() === "name_link"
//   );

//   if (!nameCol) throw new Error(`No "Name_link" column found in ${INPUT_CSV_FILE_PATH}`);

//   return records
//     .map((r) => r[nameCol])
//     .filter((v) => v && v.trim().length > 0);
// }

// /**
//  * Writes array file with export const
//  */
// async function main(file_path, url_file_path, url_file_name) {

//   const INPUT_CSV_FILE_PATH = file_path;
//   const OUTPUT_FILE_PATH = url_file_path;

//   const url_list = extract_wrestler_match_link(INPUT_CSV_FILE_PATH);

//   // create the JS file name adjusting the wrestling season for 2024-25
//   const variable_name = url_file_name.replace(".js", "");

//   const fileContent = `// Auto-generated from wrestlers_2024.csv
// export const ${variable_name} = ${JSON.stringify(url_list, null, 2)};
// `;

//   fs.writeFileSync(OUTPUT_FILE_PATH, fileContent, "utf8");

//   console.log(`✅ Step 2: Wrote ${url_list.length} URLs → ${OUTPUT_FILE_PATH}`);
// }

// export { main as step_2_write_wrestler_match_url_array };

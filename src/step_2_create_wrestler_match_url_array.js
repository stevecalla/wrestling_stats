import fs from "fs";
import path from "path";
import { parse } from "csv-parse/sync";

/**
 * Reads the CSV and extracts Name_link column
 */
function extract_name_links(csvPath) {
  if (!fs.existsSync(csvPath)) {
    throw new Error(`File not found: ${csvPath}`);
  }

  const csvText = fs.readFileSync(csvPath, "utf8");
  const records = parse(csvText, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  const first = records[0] || {};
  const nameCol = Object.keys(first).find(
    (k) => k.toLowerCase() === "name_link"
  );

  if (!nameCol) throw new Error(`No "Name_link" column found in ${csvPath}`);

  return records
    .map((r) => r[nameCol])
    .filter((v) => v && v.trim().length > 0);
}

/**
 * Writes array file with export const
 */
async function step_2_write_wrestler_match_url_array(DIR, folder_name, url_file_name, file_name) {

  fs.mkdirSync(path.join(DIR, folder_name), { recursive: true });

  const INPUT_CSV = path.join(DIR, folder_name, file_name);
  const OUTPUT_JS = path.join(DIR, folder_name, url_file_name);

  const urls = extract_name_links(INPUT_CSV);

  // create the JS file name adjusting the wrestling season for 2024-25
  const js_var_name = url_file_name.replace(".js", "");
  const fileContent = `// Auto-generated from wrestlers_2024.csv
export const ${js_var_name} = ${JSON.stringify(urls, null, 2)};
`;
  fs.writeFileSync(OUTPUT_JS, fileContent, "utf8");
  console.log(`✅ Step 2: Wrote ${urls.length} URLs → ${OUTPUT_JS}`);
}

export { step_2_write_wrestler_match_url_array };

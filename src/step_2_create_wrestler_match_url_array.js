import fs from "fs";
import { parse } from "csv-parse/sync";

const INPUT_CSV = "/Users/stevecalla/wrestling/data/input/wrestlers_2024.csv";
const OUTPUT_JS = "/Users/stevecalla/wrestling/data/input/urls_wrestlers.js";

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
async function step_2_write_wrestler_match_url_array(outPath = OUTPUT_JS) {

  const urls = extract_name_links(INPUT_CSV);

  const fileContent = `// Auto-generated from wrestlers_2024.csv
export const URL_WRESTLERS = ${JSON.stringify(urls, null, 2)};
`;
  fs.writeFileSync(outPath, fileContent, "utf8");
  console.log(`✅ Step 2: Wrote ${urls.length} URLs → ${outPath}`);
}

// this code runs only when the file is executed directly
if (import.meta.url === process.argv[1] || import.meta.url === `file://${process.argv[1]}`) {
  try {
    await step_2_write_wrestler_match_url_array(OUTPUT_JS);

  } catch (err) {
    console.error("❌ Error:", err.message);
  }
}

export { step_2_write_wrestler_match_url_array };

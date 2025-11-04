import fs from "fs";

function to_csv_file(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const esc = v => {
    if (v == null) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [
    headers.join(","),
    ...rows.map(r => headers.map(h => esc(r[h])).join(",")),
  ];
  return lines.join("\n"); // <-- no extra newline at the end
}

async function save_to_csv_file(all_rows, iteration_index, headers_written, file_path) {
  console.log(`📝 Saving to CSV file: ${file_path}`);

  // Skip empty batches until we get data
  if (!Array.isArray(all_rows) || all_rows.length === 0) {
    console.log(`ℹ️ Iteration ${iteration_index}: no data, waiting for next batch...`);
    headers_written = headers_written; // this was returning undefined so assigned it to itself
    return headers_written;
  }

  // Build CSV normally
  const csvFull = to_csv_file(all_rows);
  const lines = csvFull.split(/\r?\n/).filter(Boolean);
  const headerLine = lines.shift();  // first line
  const rowsOnly = lines.join("\n");

  // If we haven’t written headers yet, start fresh file
  if (!headers_written) {
    fs.writeFileSync(file_path, headerLine + "\n" + rowsOnly, "utf8");
    console.log(`\n\x1b[32m🧾 Created ${file_path} with headers + ${all_rows.length} rows\x1b[0m`); // green
    headers_written = true;
    return headers_written;
  } else {
    // Append only rows
    fs.appendFileSync(file_path, "\n" + rowsOnly, "utf8");
    console.log(`\n\x1b[33m➕ Appended ${all_rows.length} rows → ${file_path}\x1b[0m`); // yellow
    headers_written = true;
    return headers_written;
  }

}

export { save_to_csv_file };
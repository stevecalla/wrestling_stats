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

async function save_to_csv_file(all_rows, iterationIndex, headersWritten, file_path) {
  console.log(`📝 Saving to CSV file: ${file_path}`);

  // Skip empty batches until we get data
  if (!Array.isArray(all_rows) || all_rows.length === 0) {
    console.log(`ℹ️ Iteration ${iterationIndex}: no data, waiting for next batch...`);
    headersWritten = headersWritten;
    return headersWritten;
  }

  // Build CSV normally
  const csvFull = to_csv_file(all_rows);
  const lines = csvFull.split(/\r?\n/).filter(Boolean);
  const headerLine = lines.shift();  // first line
  const rowsOnly = lines.join("\n");

  // If we haven’t written headers yet, start fresh file
  if (!headersWritten) {
    fs.writeFileSync(file_path, headerLine + "\n" + rowsOnly, "utf8");
    console.log(`\n\x1b[32m🧾 Created ${file_path} with headers + ${all_rows.length} rows\x1b[0m`); // green
    headersWritten = true;
    return headersWritten;
  } else {
    // Append only rows
    fs.appendFileSync(file_path, "\n" + rowsOnly, "utf8");
    console.log(`\n\x1b[33m➕ Appended ${all_rows.length} rows → ${file_path}\x1b[0m`); // yellow
    headersWritten = true;
    return headersWritten;
  }

}

export { save_to_csv_file };
// ================= ALPHA SWEEP (prefix-driven; no pager spoofing) =================
// Start at 'a' → search → scrape 50-per-page → take last row's last-name 2-letter
// prefix as the next search term → repeat until no more results.
// ================================================================================

// ---- tiny utils ----
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function getPager(which = 'top') {
  const id = which === 'top' ? '#dataGridNextPrev_top' : '#dataGridNextPrev_bottom';
  return document.querySelector(id) || document.querySelector('.dataGridNextPrev') || null;
}

function getPagerRangeText(which = 'top') {
  const pager = getPager(which);
  if (!pager) return '';
  const spans = pager.querySelectorAll('span');
  for (const s of spans) {
    const t = (s.textContent || '').trim();
    if (/^\d+\s*-\s*\d+\s+of\s+\d+$/i.test(t)) return t;
  }
  return '';
}

function parseRange(which = 'top') {
  const t = getPagerRangeText(which);
  const m = t.match(/(\d+)\s*-\s*(\d+)\s+of\s+(\d+)/i);
  return m ? { start: +m[1], end: +m[2], total: +m[3], text: t } : null;
}

function hasNextPage(which = 'top') {
  const r = parseRange(which);
  return r ? r.end < r.total : false;
}

function clickNext(which = 'top') {
  const pager = getPager(which);
  if (!pager) return false;
  const nextAnchor =
    pager.querySelector('a .dgNext')?.closest('a') ||
    pager.querySelector('a[href^="javascript:defaultGrid.next"]') ||
    pager.querySelector('a i.icon-arrow_r')?.closest('a');
  if (!nextAnchor) return false;
  nextAnchor.click();
  return true;
}

// rows & change detection
function getGridRows() {
  return document.querySelectorAll('tr.dataGridRow');
}

function lastRowSignature() {
  const rows = getGridRows();
  const last = rows[rows.length - 1];
  if (!last) return '';
  const txt = last.innerText.replace(/\s+/g, ' ').trim();
  const hrefs = Array.from(last.querySelectorAll('a')).map(a => a.href).join('|');
  return `${txt}::${hrefs}`;
}

async function waitForLastRowChange(prevSig, { timeoutMs = 12000, pollMs = 120 } = {}) {
  const start = performance.now();
  while (performance.now() - start < timeoutMs) {
    const now = lastRowSignature();
    if (now && now !== prevSig) return true;
    await sleep(pollMs);
  }
  return false;
}

// ---- keep page size at 50 (if the "Show:" input exists) ----
async function ensurePageSize50(which = 'top') {
  const pager = getPager(which);
  const input = pager?.querySelector('input[type="text"][onchange*="updateLimit"]');
  if (!input) return;
  const current = parseInt(input.value || '0', 10);
  if (current === 50) return;
  const before = lastRowSignature();
  input.value = '50';
  input.dispatchEvent(new Event('change', { bubbles: true }));
  await waitForLastRowChange(before).catch(() => {});
}

// ---- open search, set last-name prefix, run search ----
async function openSearchPanel() {
  const openBtn = document.querySelector('#searchButton, input[type="button"][onclick*="openSearchWrestlers"]');
  if (openBtn) openBtn.click();
  await sleep(500);
}

async function runSearchWithPrefix(prefix) {
  await openSearchPanel();
  const lastNameInput = document.querySelector('#s_lastName');
  if (!lastNameInput) throw new Error('Last Name input #s_lastName not found');
  lastNameInput.focus();
  lastNameInput.value = prefix;
  lastNameInput.dispatchEvent(new Event('input', { bubbles: true }));
  lastNameInput.dispatchEvent(new Event('change', { bubbles: true }));

  const innerSearch = document.querySelector('input[type="button"][onclick*="searchWrestlers"], input.segment-track[value="Search"]');
  if (!innerSearch) throw new Error('Inner Search button not found');
  const before = lastRowSignature();
  innerSearch.click();
  await waitForLastRowChange(before);
  await ensurePageSize50('top');
}

// ---- extract wrestler rows → objects ----
function get_wrestler_table_data() {
  const rows = document.querySelectorAll('tr.dataGridRow');
  const data = Array.from(rows).map((row) => {
    const cols = row.querySelectorAll('td div');
    const getText = (i) => (cols[i]?.textContent || '').trim();
    const getLink = (i) => cols[i]?.querySelector('a')?.href || null;
    return {
      name: getText(2),
      name_link: getLink(2),
      team: getText(3),
      team_link: getLink(3),
      weight_class: getText(4),
      gender: getText(5),
      grade: getText(6),
      record: getText(8),
      has_check_icon: !!cols[1]?.querySelector('.greenIcon'),
      row_index: row.rowIndex
    };
  });
  console.table(data);
  return data;
}

// // ---- CSV helpers ----
// const CSV_COLUMNS = [
//   'name','name_link','team','team_link',
//   'weight_class','gender','grade','record',
//   'has_check_icon','row_index'
// ];

// function csvEscape(v) {
//   if (v == null) return '';
//   const s = String(v);
//   return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
// }

// function toCSVRows(items) {
//   return items.map(obj => CSV_COLUMNS.map(c => csvEscape(obj[c])).join(',')).join('\n') + '\n';
// }

// function headerRow() {
//   return CSV_COLUMNS.map(csvEscape).join(',') + '\n';
// }

// async function createCSVWriter({ fileName = 'wrestlers.csv' } = {}) {
//   const supportsFS = !!window.showSaveFilePicker;
//   if (supportsFS) {
//     const handle = await showSaveFilePicker({
//       suggestedName: fileName,
//       types: [{ description: 'CSV', accept: { 'text/csv': ['.csv'] } }]
//     });
//     const stream = await handle.createWritable();
//     let wroteHeader = false;
//     return {
//       async writeHeader() { if (!wroteHeader) { await stream.write(headerRow()); wroteHeader = true; } },
//       async writeRows(rows) { await stream.write(toCSVRows(rows)); },
//       async close() { await stream.close(); }
//     };
//   } else {
//     let buffer = ''; let wroteHeader = false;
//     return {
//       async writeHeader() { if (!wroteHeader) { buffer += headerRow(); wroteHeader = true; } },
//       async writeRows(rows) { buffer += toCSVRows(rows); },
//       async close() {
//         const blob = new Blob([buffer], { type: 'text/csv;charset=utf-8' });
//         const url = URL.createObjectURL(blob);
//         const a = Object.assign(document.createElement('a'), { href: url, download: fileName });
//         document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
//       }
//     };
//   }
// }

// ---- last-name parsing + next-prefix logic ----
function parseLastNameFromNameCell(nameText) {
  if (!nameText) return '';
  const t = nameText.trim();
  if (t.includes(',')) return t.split(',')[0].trim();      // "Doe, John"
  const parts = t.split(/\s+/);
  if (parts.length > 1) return parts[parts.length - 1];    // "John Doe" → "Doe"
  return t;
}

function twoLetterPrefixFromLastRow() {
  const rows = getGridRows();
  const last = rows[rows.length - 1];
  if (!last) return '';
  const cols = last.querySelectorAll('td div');
  const nameText = (cols[2]?.textContent || '').trim();
  const lastName = parseLastNameFromNameCell(nameText).toLowerCase().replace(/[^a-z]/g, '');
  return lastName.slice(0, 2); // first two letters
}

function bumpTwoLetterPrefix(prefix) {
  // "aa"→"ab", ..."az"→"ba", ..."zz"→null
  if (!/^[a-z]{2}$/.test(prefix)) return null;
  const a = prefix.charCodeAt(0), b = prefix.charCodeAt(1);
  if (b < 122) return String.fromCharCode(a) + String.fromCharCode(b + 1);
  if (a < 122)  return String.fromCharCode(a + 1) + 'a';
  return null;
}
function nextFirstLetter(c) {
  if (!/^[a-z]$/.test(c)) return null;
  return c === 'z' ? null : String.fromCharCode(c.charCodeAt(0) + 1);
}

// ---- scrape all pages for the current search results ----
async function scrapeAllPagesAppend(writer, which = 'top', delayMs = 800) {
  let pagesAdvanced = 0;
  let rowsTotal = 0;

  const firstData = get_wrestler_table_data();
  rowsTotal += firstData.length;
  await writer.writeRows(firstData);
  let r = parseRange(which);
  console.log(`[scrape] page 1 saved ${firstData.length} rows${r ? ` — ${r.text}` : ''}`);

  while (hasNextPage(which)) {
    const before = lastRowSignature();
    const clicked = clickNext(which);
    if (!clicked) { console.warn('[scrape] next button not found; stopping.'); break; }
    const changed = await waitForLastRowChange(before);
    if (!changed) { console.warn('[scrape] no change after next; stopping.'); break; }

    await ensurePageSize50(which);

    const data = get_wrestler_table_data();
    rowsTotal += data.length;
    await writer.writeRows(data);
    pagesAdvanced += 1;
    r = parseRange(which);
    console.log(`[scrape] page ${pagesAdvanced + 1} saved ${data.length} rows${r ? ` — ${r.text}` : ''}`);

    await sleep(delayMs);
  }
  return { pagesAdvanced, rowsTotal };
}

// ---- MAIN: streamlined alpha sweep (no pager text changes) ----
async function runAlphaSweep({
  startPrefix = 'a',
  fileName = 'wrestlers.csv',
  delayMs = 800,
  guardMaxSteps = 5, // safety
} = {}) {
  // const writer = await createCSVWriter({ fileName });
  // await writer.writeHeader();

  let prefix = startPrefix.toLowerCase();
  let steps = 0;

  while (prefix && steps < guardMaxSteps) {
    steps++;
    console.log(`\n[alpha] SEARCH prefix = "${prefix}"`);
    await runSearchWithPrefix(prefix);

    // No results? advance first letter (e.g., a→b)
    if (getGridRows().length === 0) {
      const nf = nextFirstLetter(prefix[0]);
      if (!nf) break;
      prefix = nf;
      continue;
    }

    // Scrape pages for this prefix
    await scrapeAllPagesAppend(writer, 'top', delayMs);

    // Decide next prefix from last row (2 letters of last name)
    const nxt2 = twoLetterPrefixFromLastRow();
    if (nxt2 && (/^[a-z]{2}$/i.test(nxt2))) {
      // If we didn't move forward (e.g., still "aa"), bump it ("aa"→"ab", ... "az"→"ba")
      prefix = (nxt2 > prefix ? nxt2 : bumpTwoLetterPrefix(prefix.length === 1 ? prefix + 'a' : prefix)) || nextFirstLetter(prefix[0]);
    } else {
      // Fallback: advance by first letter
      prefix = nextFirstLetter(prefix[0]);
    }
  }

  await writer.close();
  console.log('[alpha] done.');
}

// ---------------- RUN ----------------
await runAlphaSweep({
  startPrefix: 'a',      // start at top of alphabet
  fileName: 'wrestlers.csv',
  delayMs: 700
});

export { runAlphaSweep };
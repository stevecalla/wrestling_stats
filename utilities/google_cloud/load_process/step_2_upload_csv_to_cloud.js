import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import { promises as fs } from "fs";   // async fs (e.g., await fs.readFile)
import fsSync from "fs";               // sync fs (e.g., fsSync.existsSync)
import path from "path";
import { fileURLToPath } from "url";
import os from "os";
const platform = os.platform();

import { Storage } from "@google-cloud/storage";
import { Transform } from "stream";
import { determine_os_path } from "../../directory_tools/determine_os_path.js";

// SET GOOGLE CLOUD CREDENTIALS
let GOOGLE_APPLICATION_CREDENTIALS = ""; // absolute path to JSON key
if (platform === 'win32') GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_WINDOWS;
else if (platform === 'darwin') GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_MAC;
else GOOGLE_APPLICATION_CREDENTIALS = process.env.GOOGLE_APPLICATION_CREDENTIALS_LINUX;

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT_ID_WRESTLING;
const BUCKET_NAME = process.env.GOOGLE_CLOUD_STORAGE_BUCKET_WRESTLING;

// --- tiny helpers ---
async function is_zero_byte(filePath) {
  try {
    const st = await fs.stat(filePath);
    return st.size === 0;
  } catch {
    return true;
  }
}

async function has_Utf8_bom(filePath) {
  const fd = await fs.open(filePath, 'r');
  try {
    const buf = Buffer.alloc(3);
    const { bytesRead } = await fd.read(buf, 0, 3, 0);
    return bytesRead === 3 && buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf;
  } finally {
    await fd.close();
  }
}

/** create a temp file with BOM stripped (only strips if present on first chunk) */
async function write_no_bom_file(srcPath) {
  const dstPath = srcPath.replace(/\.csv$/i, '.nobom.csv');
  await new Promise((resolve, reject) => {
    let first = true;
    const stripBom = new Transform({
      transform(chunk, _enc, cb) {
        if (first) {
          first = false;
          // remove UTF-8 BOM if present at start of file
          if (chunk.length >= 3 && chunk[0] === 0xef && chunk[1] === 0xbb && chunk[2] === 0xbf) {
            return cb(null, chunk.slice(3));
          }
        }
        cb(null, chunk);
      }
    });
    fsSync.createReadStream(srcPath)
      .pipe(stripBom)
      .pipe(fsSync.createWriteStream(dstPath))
      .on('finish', resolve)
      .on('error', reject);
  });
  return dstPath;
}

async function execute_upload_csv_to_cloud(options, iteration) {
  const { get_data, disableGzip = false } = options || {};
  const { directory_name } = get_data[iteration];

  // --- Preflight ---
  console.log('************* STARTING STEP #2 (UPLOAD) ***************');
  console.log('[preflight] platform:', platform);
  console.log('[preflight] project id:', PROJECT_ID);
  console.log('[preflight] bucket:', BUCKET_NAME);

  // Resolve key path and verify it exists (Windows is most fragile here)
  const keyPath = GOOGLE_APPLICATION_CREDENTIALS ? path.resolve(GOOGLE_APPLICATION_CREDENTIALS) : '';
  console.log('[preflight] GOOGLE_APPLICATION_CREDENTIALS:', keyPath || '(empty)');
  if (!keyPath || !fsSync.existsSync(keyPath)) {
    console.error('❌ Service-account key file is missing or path is wrong. Fix your .env variable for WINDOWS/MAC/LINUX.');
    throw new Error('Missing or invalid GOOGLE_APPLICATION_CREDENTIALS path.');
  }

  const startTime = performance.now();

  // 1) GCS client
  const storageClient = new Storage({
    projectId: PROJECT_ID,
    keyFilename: keyPath,
  });
  const bucket = storageClient.bucket(BUCKET_NAME);

  // 2) Check bucket access early (fast fail on 401/403 or NotFound)
  try {
    const [exists] = await bucket.exists();
    if (!exists) {
      throw new Error(`Bucket ${BUCKET_NAME} does not exist or is not visible to this service account.`);
    }
    // quick permissions probe: list objects (does not require create)
    await bucket.getFiles({ maxResults: 1 });
    console.log('✅ Bucket exists and is listable.');
  } catch (e) {
    console.error('❌ Bucket access check failed:', e.code || '', e.message);
    throw e;
  }

  // 3) Locate CSVs
  const os_path = await determine_os_path();
  const directory = path.join(os_path, directory_name);

  let files = await fs.readdir(directory);
  files = files.filter(f => f.toLowerCase().endsWith('.csv')); // includes .nobom.csv (expected)

  if (!files.length) {
    console.log('No CSV files found in:', directory);
    return;
  }

  console.log('\nFiles to be uploaded:');
  files.forEach(f => console.log('  ' + path.join(directory, f)));

  // 4) Uploads with better error visibility
  const CONCURRENCY = 6;
  let ok = 0, fail = 0;
  const startOverall = performance.now();
  const errors = [];

  for (let i = 0; i < files.length; i += CONCURRENCY) {
    const batch = files.slice(i, i + CONCURRENCY);

    const results = await Promise.allSettled(batch.map(async (file) => {
      const localFilePath = path.join(directory, file);

      // sanity checks
      try {
        const st = await fs.stat(localFilePath);
        if (st.size === 0) {
          console.warn(`🧹 Skipping zero-byte file: ${file}`);
          return;
        }
      } catch (e) {
        throw new Error(`Cannot stat "${file}": ${e.message}`);
      }

      // Strip BOM if present (creates .nobom.csv only when needed)
      let uploadSrc = localFilePath;
      let tempMade = false;
      try {
        if (await has_Utf8_bom(localFilePath)) {
          uploadSrc = await write_no_bom_file(localFilePath);
          tempMade = true;
        }
      } catch (e) {
        throw new Error(`BOM handling failed for "${file}": ${e.message}`);
      }

      const remoteName = file.replace(/\.csv$/i, disableGzip ? '.csv' : '.csv.gz');

      // Upload
      try {
        await bucket.upload(uploadSrc, {
          destination: remoteName,
          gzip: !disableGzip, // turn off with { disableGzip: true } in options for testing
          metadata: disableGzip
            ? { contentType: 'text/csv' }
            : { contentType: 'application/gzip' },
          validation: 'crc32c',
          timeout: 10 * 60 * 1000,
        });
      } catch (e) {
        // surface detailed error
        const code = e.code || e.statusCode || '';
        const msg = e.message || String(e);
        throw new Error(`Upload failed for "${file}" → "${remoteName}": [${code}] ${msg}`);
      } finally {
        if (tempMade) {
          try { await fs.unlink(uploadSrc); } catch { }
        }
      }

      console.log(`✅ Uploaded ${path.basename(uploadSrc)} → gs://${BUCKET_NAME}/${remoteName}`);
    }));

    results.forEach(r => {
      if (r.status === 'fulfilled') ok++;
      else {
        fail++;
        errors.push(r.reason?.message || String(r.reason));
      }
    });
  }

  console.log(
    `\n✅ Upload complete: ${ok}/${files.length} succeeded, ${fail} failed in ${((performance.now() - startOverall) / 1000).toFixed(2)}s`
  );

  if (fail) {
    console.error('---- Detailed upload errors ----');
    errors.forEach((e, idx) => console.error(`#${idx + 1}:`, e));
    throw new Error(`Some uploads failed (${fail}).`);
  }

  console.log(`\nTotal elapsed time: ${((performance.now() - startTime) / 1000).toFixed(2)}s\n`);
}

export { execute_upload_csv_to_cloud };

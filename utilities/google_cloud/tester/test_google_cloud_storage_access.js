import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import { Storage } from "@google-cloud/storage";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";

let keyFile = "";
if (os.platform() === "win32")
  keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS_WINDOWS;
else if (os.platform() === "darwin")
  keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS_MAC;
else
  keyFile = process.env.GOOGLE_APPLICATION_CREDENTIALS_LINUX;

const storage = new Storage({
  projectId: process.env.GOOGLE_CLOUD_PROJECT_ID_WRESTLING,
  keyFilename: keyFile,
});

async function main() {
  const bucketName = process.env.GOOGLE_CLOUD_STORAGE_BUCKET_WRESTLING;
  const [exists] = await storage.bucket(bucketName).exists();
  console.log(`Bucket ${bucketName} exists:`, exists);
}

main().catch(console.error);

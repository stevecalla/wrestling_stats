import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../../.env") });

import { get_pool } from "../../mysql/mysql_pool.js";

async function checkVersion() {
  const pool = await get_pool();
  const [rows] = await pool.query("SELECT VERSION() AS version");
  console.log("MySQL via pool:", rows[0].version);
}

checkVersion().then(() => process.exit());

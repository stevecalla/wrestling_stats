// ESM
import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../.env") });

import mysql from "mysql2/promise";
let _pool;
let _initPromise;

/**
 * Lazily initializes:
 * 1) Connects WITHOUT database
 * 2) CREATE DATABASE IF NOT EXISTS <db>
 * 3) Builds a pool bound to that database
 */
async function get_pool() {
    if (_pool) return _pool;
    if (_initPromise) return _initPromise;

    _initPromise = (async () => {
        const {
            MYSQL_HOST = process.env.MYSQL_HOST,
            MYSQL_PORT = process.env.MYSQL_PORT,
            MYSQL_USER = process.env.MYSQL_USER,
            MYSQL_PASSWORD = process.env.MYSQL_PASSWORD,
            MYSQL_DATABASE = process.env.MYSQL_DATABASE,
            MYSQL_CONNECTION_LIMIT = process.env.MYSQL_CONNECTION_LIMIT,
            MYSQL_TIMEZONE = "Z", // store UTC
        } = process.env;

    // Step 1: ensure DB exists (no database selected yet)
    const admin = await mysql.createConnection({
        host: MYSQL_HOST,
        port: Number(MYSQL_PORT),
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        multipleStatements: true,
        timezone: MYSQL_TIMEZONE,
    });

    await admin.query(
        `CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;`
    );
    await admin.end();

    // Step 2: create the pool bound to that DB
    _pool = mysql.createPool({
        host: MYSQL_HOST,
        port: Number(MYSQL_PORT),
        user: MYSQL_USER,
        password: MYSQL_PASSWORD,
        database: MYSQL_DATABASE,
        waitForConnections: true,
        connectionLimit: Number(MYSQL_CONNECTION_LIMIT),
        queueLimit: 0,
        timezone: MYSQL_TIMEZONE,
        dateStrings: true,
        namedPlaceholders: true,
    });

    return _pool;
}) ();

return _initPromise;
}

export { get_pool }

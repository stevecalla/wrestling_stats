// utilities/mysql/mysql_pool.js

import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });


import mysql from "mysql2";
import mysqlPromise from "mysql2/promise";

let _pool_stream;
let _pool_promise;
let _initPromise;

/**
 * Lazily initializes:
 * 1) Connects WITHOUT database
 * 2) CREATE DATABASE IF NOT EXISTS <db>
 * 3) Builds a pool bound to that database
 */
async function get_pool() {
    if (_pool_promise) return _pool_promise;
    if (_initPromise) return _initPromise;

    _initPromise = (async () => {
        // Step 1: ensure DB exists (no database selected yet)
        const admin = await mysqlPromise.createConnection({
            host: process.env.MYSQL_HOST,
            port: Number(process.env.MYSQL_PORT),
            user: process.env.MYSQL_USER,
            password: process.env.MYSQL_PASSWORD,
            multipleStatements: true,
            timezone: "Z", // store UTC
        });

        await admin.query(
            `CREATE DATABASE IF NOT EXISTS \`${process.env.MYSQL_DATABASE}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;`
        );
        await admin.end();

        // Step 2: create the pool bound to that DB
        _pool_promise = mysqlPromise.createPool({
            host: process.env.MYSQL_HOST,
            port: Number(process.env.MYSQL_PORT),
            user: process.env.MYSQL_USER,
            password: process.env.MYSQL_PASSWORD,
            database: process.env.MYSQL_DATABASE,
            waitForConnections: true,
            connectionLimit: Number(process.env.MYSQL_CONNECTION_LIMIT),
            queueLimit: 0,
            timezone: "Z", // store UTC
            dateStrings: true,
            namedPlaceholders: true,
        });

        return _pool_promise;
    })();

    return _initPromise;
}

// Non-promise pool for streaming
function get_pool_stream() {
    if (_pool_stream) return _pool_stream;
    _pool_stream = mysql.createPool({
        host: process.env.MYSQL_HOST,
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASSWORD,
        database: process.env.MYSQL_DATABASE,
        waitForConnections: true,
        connectionLimit: 10,
        enableKeepAlive: true,
    });
    return _pool_stream;
}

async function close_pools() {
  // Close promise pool
  if (_pool_promise) {
    const pool = await _pool_promise; // handle case where _initPromise is still resolving
    await pool.end();
    _pool_promise = undefined;
    _initPromise  = undefined;
  }

  // Close stream pool
  if (_pool_stream) {
    await _pool_stream.end();
    _pool_stream = undefined;
  }
}

// get_pool();

export { get_pool, get_pool_stream, close_pools };


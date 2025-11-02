// import dotenv from "dotenv";
// const __filename = fileURLToPath(import.meta.url);
// const __dirname = path.dirname(__filename);
// dotenv.config({ path: path.resolve(__dirname, "../.env") });

// // launch_chrome_dev.js (ESM)
// import fs from "fs";
// import path from "path";
// import { fileURLToPath } from "url";
// import { spawn } from "child_process";
// import { chromium } from "playwright";

// const DEVTOOLS_PORT = process.env.DEVTOOLS_PORT?.trim() || "9223"; // use your own port
// const CONNECT_URL = `http://localhost:${DEVTOOLS_PORT}`;
// const LOAD_TIMEOUT_MS = 30000;

// const USER_DATA_DIR =
//   (process.env.STORE_CHROME_DATA && process.env.STORE_CHROME_DATA.trim()) ||
//   path.resolve(process.env.HOME || "~", "chrome-tw-user-data");

// async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

// async function is_chrome_dev_up(url = `${CONNECT_URL}/json/version`) {
//   try {
//     const res = await fetch(url, { cache: "no-store" });
//     return res.ok;
//   } catch {
//     return false;
//   }
// }

// function find_mac_chrome() {
//   const candidates = [
//     "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
//     "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
//     "/Applications/Chromium.app/Contents/MacOS/Chromium",
//   ];
//   for (const p of candidates) {
//     try { if (fs.existsSync(p)) return p; } catch {}
//   }
//   return null;
// }

// async function go_to_url_in_chrome(URL) {
//   const browser = await chromium.connectOverCDP(CONNECT_URL);
//   const context = browser.contexts()[0];
//   if (!context) throw new Error("No contexts from CDP connection.");

//   const page = await context.newPage();
//   page.setDefaultTimeout(LOAD_TIMEOUT_MS);
//   if (URL) {
//     await page.goto(URL, { waitUntil: "domcontentloaded", timeout: LOAD_TIMEOUT_MS });
//     await page.waitForTimeout(800);
//   }
//   return { browser, page };
// }

// async function step_0_launch_chrome_developer(URL) {
//   // Ensure the profile path exists before launch
//   fs.mkdirSync(USER_DATA_DIR, { recursive: true });

//   console.log("[ENV] DEVTOOLS_PORT:", DEVTOOLS_PORT);
//   console.log("[ENV] STORE_CHROME_DATA:", USER_DATA_DIR);

//   // if (await is_chrome_dev_up()) {
//   //   // If you want to FORCE your profile, you should not reuse an already-running Chrome
//   //   console.log(`[CDP] A Chrome is already listening on ${DEVTOOLS_PORT}. Reusing it (profile may NOT be ${USER_DATA_DIR}).`);
//   //   return go_to_url_in_chrome(URL);
//   // }

//   if (process.platform !== "darwin") {
//     console.warn("[CDP] Auto-launch is set up for macOS only. Start Chrome manually with:");
//     console.warn(`  /path/to/Chrome --remote-debugging-port=${DEVTOOLS_PORT} --user-data-dir="${USER_DATA_DIR}" about:blank`);
//     return;
//   }

//   const chromeBin = find_mac_chrome();
//   if (!chromeBin) throw new Error("Could not find Chrome in /Applications.");

//   console.log("[CDP] Launching Chrome with:");
//   console.log(`  --remote-debugging-port=${DEVTOOLS_PORT}`);
//   console.log(`  --user-data-dir=${USER_DATA_DIR}`);

//   const child = spawn(chromeBin, [
//     `--remote-debugging-port=${DEVTOOLS_PORT}`,
//     `--user-data-dir=${USER_DATA_DIR}`,
//     `--no-first-run`,
//     `--no-default-browser-check`,
//     "about:blank",
//   ], { stdio: "ignore", detached: true });
//   child.unref();

//   const deadline = Date.now() + 20000;
//   while (Date.now() < deadline) {
//     if (await is_chrome_dev_up()) {
//       console.log("[CDP] Chrome is now available.");
//       const { browser, page } = await go_to_url_in_chrome(URL);
//       return { browser, page };
//     }
//     await wait(400);
//   }
//   throw new Error("Timed out waiting for Chrome DevTools.");
// }

// // run if invoked directly
// if (import.meta.url === `file://${process.argv[1]}`) {
//   step_0_launch_chrome_developer(process.env.TARGET_URL).catch(err => {
//     console.error(err);
//     process.exit(1);
//   });
// }

// export { step_0_launch_chrome_developer };


import { chromium } from "playwright";

const browser = await chromium.connectOverCDP("http://localhost:9222");
const [context] = browser.contexts();
const page = await context.newPage();
await page.goto("https://www.google.com");
console.log("✅ Connected and opened a page");

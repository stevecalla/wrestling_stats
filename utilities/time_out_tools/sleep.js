async function sleep_with_countdown(ms) {
  const totalSeconds = ms / 1000;
  for (let i = 1; i <= totalSeconds; i++) {
    console.log(`⏳  Waiting ${i}/${totalSeconds} second${totalSeconds > 1 ? "s" : ""}...`);
    await new Promise(r => setTimeout(r, 1000));
  }
}

export { sleep_with_countdown };

// EXECUTION EXAMPLE
// await sleep_with_countdown(3000);
// console.log("✅ Done waiting!");
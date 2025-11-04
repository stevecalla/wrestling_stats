// utilities/console_colors.js
// Simple ESM helper to colorize console text

const COLORS = {
  reset: "\x1b[0m",
  black: "\x1b[30m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
  white: "\x1b[37m",
  brightRed: "\x1b[91m",
  brightGreen: "\x1b[92m",
  brightYellow: "\x1b[93m",
  brightBlue: "\x1b[94m",
  brightMagenta: "\x1b[95m",
  brightCyan: "\x1b[96m",
  brightWhite: "\x1b[97m",
};

/**
 * Wraps text with a given ANSI color code.
 * @param {string} text - The text to colorize
 * @param {string} color - Key from COLORS map
 * @returns {string}
 */
function color_text(text, color = "reset") {
  const code = COLORS[color] || COLORS.reset;
  return `${code}${text}${COLORS.reset}`;
}

export { COLORS, color_text };

// USAGE EXAMPLES
// console.log(color_Text("=== STARTING STEP #2 TO WRITE WRESTLER MATCH URL ARRAY ===", "red"));
// console.log(color_Text("✅ Success!", "green"));
// console.log(color_Text("⚠️ Warning", "yellow"));


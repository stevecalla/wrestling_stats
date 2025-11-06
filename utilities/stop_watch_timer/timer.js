import readline from "readline";

// ANSI escape codes for colors
const BLUE = '\x1b[34m';
const RESET = '\x1b[0m';
const YELLOW = '\x1b[33m';
const RED = '\x1b[31m';

const update_interval = 1000; // Update interval in milliseconds (1000ms = 1 second)
let seconds = 0;
let timer_interval = {}; // Object to hold timer intervals


// Create readline interface
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

// Function to format time as HH:MM:SS
function format_time(totalSeconds) {
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const secs = totalSeconds % 60;
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
}

let position = 10;
async function start_timer_message() {
    // console.clear();
    // readline.cursorTo(process.stdout, 0, position); //positions next console.log 1 line below SSH msg
    process.stdout.write(`${BLUE}\nStarting timer... Press Ctrl+C to stop...`);
    // readline.clearLine(process.stdout, position); // Clear the current line
}

// Function to update timer
async function update_timer(i) {
    await start_timer_message();

    // readline.cursorTo(process.stdout, 0, position + 2 + i); // Move cursor to top-left corner, but 2 lines below starting line
    // readline.clearLine(process.stdout, position_2); // Clear the current line
    
    process.stdout.write(`${RED}Timer: ${YELLOW}${format_time(seconds)}${RESET}...`); // Display the timer in red and yellow
    seconds++; // Increment seconds
}

// Function to stop the timer
function stop_timer(i) {
    if (timer_interval[`timerInterval_${i}`]) { // Check if the timer_interval is set
        console.log('Clearing timer interval...'); // Log to confirm clearInterval is about to be called
        clearInterval(timer_interval[`timerInterval_${i}`]); // Stop the timer interval
        seconds = 0; // resest timer to 0
        console.log('Timer interval cleared.'); // Log to confirm clearInterval has been called
    } else {
        console.log('No timer interval found to clear.'); // Log if timer_interval is not set
    }
    position += 15;

    rl.close(); // Close readline interface
    // process.exit(); // Exit the process
}

// Function to start the timer
function start_timer(i) {
    timer_interval[`timerInterval_${i}`] = setInterval(() => update_timer(i), update_interval); // Set up the timer interval
}

// Export functions
export { start_timer, stop_timer };
// Function to trigger garbage collection
async function trigger_garbage_collection() {
    if (global.gc) {
        global.gc();
        console.log("\nGarbage collection triggered.");
    } else {
        console.warn("Garbage collection is not enabled. Run the script with --expose-gc.");
    }
}

export { trigger_garbage_collection };
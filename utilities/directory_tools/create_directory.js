import fs from "fs";
import path from "path";
import { determine_os_path } from "./determine_os_path.js";

async function create_directory(directoryName = "data") {
    const os_path = await determine_os_path();
    const directory_path = path.join(os_path, directoryName);

    // CHECK IF DIRECTORY EXISTS, IF NOT, CREATE IT
    fs.mkdirSync(directory_path, { recursive: true });

    return directory_path;
}

// create_directory();

export { create_directory };
import os from "os";
import path from "path";

const csv_export_path_linux = `/home/steve-calla/development/wrestling/data`;
const csv_export_path_mac = `/Users/stevecalla/development/wrestling/data`;
const csv_export_path_windows = `C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data/wrestling`;

async function determine_os_user() {
    const os_user_name = os.userInfo().username;
    return os_user_name;
}

async function determine_os_path() {
    // Determine the CSV export path based on the OS
    const isMac = process.platform === 'darwin'; // macOS
    const isLinux = process.platform === 'linux'; // Linux
    const os_path = isMac ? csv_export_path_mac : (isLinux ? csv_export_path_linux : csv_export_path_windows);
    return os_path;
}

// READ UBUNTU SERVER UPDATE LOG
// C:\Users\calla\development\wrestling_stats\cron_jobs\cron_update_ubuntu\ubuntu-update.log
// /home/steve-calla/development/wrestling_stats/cron_jobs/cron_update_ubuntu/ubuntu-update.log
const ubuntu_folder_path = {
    linux: {
        'steve-calla': '/home/steve-calla/development/wrestling_stats/cron_jobs/cron_update_ubuntu',
    },
    mac: '/Users/teamkwsc/development/wrestling_stats/cron_jobs/cron_update_ubuntu',
    windows: 'C:\\Users\\calla\\development\\wrestling_stats\\cron_jobs\\cron_update_ubuntu'
}

async function determine_ubuntu_update_log_file_path(file_name) {
    // Get the current platform
    const platform = process.platform;
    let dir_path = "";

    if (platform === 'darwin') {// macOS
        dir_path = ubuntu_folder_path.mac;
    } else if (platform === 'linux') {
        const username = await determine_os_user();
        dir_path = ubuntu_folder_path.linux[username] || ubuntu_folder_path.linux['usat-server'];
    } else {// Windows
        dir_path = ubuntu_folder_path.windows;
    }

    // Append filename in a cross-platform way
    const file_path = path.normalize(path.join(dir_path, file_name));

    return { file_path, platform };
}

export {
    determine_os_path,
    determine_ubuntu_update_log_file_path,
}

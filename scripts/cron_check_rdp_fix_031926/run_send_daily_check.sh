#!/bin/bash
#!/usr/bin/env bash
set -euo pipefail

# COMMENT OUT ENV IF RUNNING WINDOWS
# /home/steve-calla/developmenet/wrestling/wrestling_stats/.env
ENV_FILE="/home/steve-calla/development/wrestling/wrestling_stats/.env"
if [ -f "$ENV_FILE" ]; then
  # Export only lines that look like KEY=VALUE, ignoring comments/blank lines
  # Handles quotes and spaces after '='; avoids evaluating command substitutions.
  while IFS= read -r line; do
    # skip comments/blank
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
    # Only accept KEY=VALUE where KEY is a valid env name
    if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=(.*)$ ]]; then
      key="${BASH_REMATCH[1]}"
      val="${BASH_REMATCH[2]}"
      # strip optional surrounding quotes
      val="${val#\"}"; val="${val%\"}"
      val="${val#\'}"; val="${val%\'}"
      export "$key=$val"
    fi
  done < "$ENV_FILE"
else
  echo "[ERROR] Env file $ENV_FILE not found" >&2
  exit 1
fi

# Start timer
start_time=$(date +%s)
start_time_readable=$(date +"%I:%M:%S %p")

# Determine the current OS user
current_user=$(whoami)
echo "Current user: $current_user"
echo "Running send ubuntu update log email cron job."

# PATH TO JS FILE
if [ "$current_user" == "steve-calla" ]; then
    # mac
    JS_FILE="/home/steve-calla/development/wrestling/wrestling_stats/scripts/cron_check_rdp_fix_031926/send_daily_check.js"
    NODE_PATH="/home/$current_user/.nvm/versions/node/v18.20.4/bin/node"
elif [ "$current_user" == "tbd" ]; then
    # linux
    JS_FILE="/home/steve-calla/development/wrestling/wrestling_stats/scripts/cron_check_rdp_fix_031926/send_daily_check.js"
    NODE_PATH="/usr/bin/node"
elif [ "$current_user" == "calla" ]; then
    # windows
    # C:\Users\calla\development\usat\sql_programs\utilities\cron_update_ubuntu\get_ubuntu_log.js
    JS_FILE="C:/Users/calla/development/ezhire/programs/scheduled_jobs/cron_check_rdp_fix_031926/send_daily_check.js"
    NODE_PATH="C:\Program Files\nodejs\node.exe"
else
    echo "Unknown user: $current_user"
    exit 1
fi

# EXECUTE THE JS FILE USING NODE
# # /usr/bin/node "$JS_FILE"
# NODE_PATH="/home/$current_user/.nvm/versions/node/v18.20.4/bin/node"

if [ -f "$JS_FILE" ]; then
    if [ -x "$NODE_PATH" ]; then
        "$NODE_PATH" "$JS_FILE"
    else
        echo "Node.js not found at $NODE_PATH"
        exit 1
    fi
else
    echo "JavaScript file not found at $JS_FILE"
    exit 1
fi

# End timer
end_time=$(date +%s)
end_time_readable=$(date +"%I:%M:%S %p")

# Calculate elapsed time
elapsed_time=$((end_time - start_time))
hours=$((elapsed_time / 3600))
minutes=$(( (elapsed_time % 3600) / 60 ))
seconds=$((elapsed_time % 60))

# Output times and execution duration
echo "Script started at: $start_time_readable"
echo "Script ended at: $end_time_readable"
echo "Total execution time: $hours hours, $minutes minutes, $seconds seconds"

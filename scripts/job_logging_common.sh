#!/bin/bash

# ======================================================
# Common logging + rotation for scheduled bash jobs
# - Per-run timestamped log file
# - Logs stored next to the calling script in ./logs/
# - Keeps logs for 7 days (deletes older)
# ======================================================

# Global variables used across helper functions
JOB_NAME=""
LOG_DIR=""
LOG_FILE=""
JOB_START_TIME_EPOCH=0
JOB_START_TIME_HUMAN=""

setup_logging() {
  JOB_NAME="$1"

  if [ -z "$JOB_NAME" ]; then
    echo "setup_logging ERROR: job name is required"
    exit 1
  fi

  # Directory of the calling script
  # - If this file is SOURCED:
  #     BASH_SOURCE[1] = the script that sourced it
  # - If this file is RUN DIRECTLY:
  #     BASH_SOURCE[0] = this file, BASH_SOURCE[1] is empty
  local caller_source
  if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    # Running directly
    caller_source="${BASH_SOURCE[0]}"
  else
    # Sourced from another script
    caller_source="${BASH_SOURCE[1]}"
  fi

  local caller_dir
  caller_dir="$(cd "$(dirname "${caller_source}")" && pwd)"

  # Logs directory: ./logs/<job_name> relative to the calling script
  LOG_DIR="${caller_dir}/logs/${JOB_NAME}"
  mkdir -p "$LOG_DIR"

  # Timestamped log file
  local log_timestamp
  log_timestamp="$(date +"%Y-%m-%d_%H-%M-%S")"
  LOG_FILE="${LOG_DIR}/${JOB_NAME}_${log_timestamp}.log"

  JOB_START_TIME_EPOCH=$(date +%s)
  JOB_START_TIME_HUMAN="$(date +"%Y-%m-%d %H:%M:%S")"

  # Mirror all output (stdout + stderr) to terminal AND log file
  exec > >(tee -a "$LOG_FILE") 2>&1

  echo "============================================="
  echo "Job: ${JOB_NAME}"
  echo "Started at: ${JOB_START_TIME_HUMAN}"
  echo "Log file: ${LOG_FILE}"
  echo "============================================="
}

finish_logging() {
  local end_epoch
  local end_human
  local elapsed
  local hours
  local minutes
  local seconds

  end_epoch=$(date +%s)
  end_human="$(date +"%Y-%m-%d %H:%M:%S")"
  elapsed=$((end_epoch - JOB_START_TIME_EPOCH))
  hours=$((elapsed / 3600))
  minutes=$(((elapsed % 3600) / 60))
  seconds=$((elapsed % 60))

  echo "---------------------------------------------"
  echo "Job: ${JOB_NAME}"
  echo "Ended at: ${end_human}"
  echo "Total execution time: ${hours}h ${minutes}m ${seconds}s"
  echo "Log directory: ${LOG_DIR}"
  echo "---------------------------------------------"

  # Log rotation: keep last 7 days
  # -mtime +6 → strictly older than ~7 days
  echo "Cleaning up logs older than 7 days in: ${LOG_DIR}"
  find "${LOG_DIR}" -type f -name "${JOB_NAME}_*.log" -mtime +6 -print -delete || true

  echo "Done."
}

# ======================================================
# Self-test: if this file is run directly, do a tiny demo
# ======================================================
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  # This runs only when you do:  ./job_logging_common.sh
  setup_logging "job_logging_common_selftest"

  echo "Self-test: running job_logging_common.sh directly."
  echo "This is a demo log entry."
  sleep 1
  echo "Self-test: finishing up."

  finish_logging
fi

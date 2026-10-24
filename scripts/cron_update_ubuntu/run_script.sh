
#!/usr/bin/env bash

set -euo pipefail

# Where to log (always overwrite, not append)
# --- Resolve script directory + log file (log lives next to this script) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGFILE="${SCRIPT_DIR}/ubuntu-update.log"

# --- Self-elevate: try passwordless sudo first; if not configured, prompt once ---
if [ "${EUID:-$(id -u)}" -ne 0 ]; then
  # Try noninteractive (works if sudoers NOPASSWD is set for this script)
  if sudo -n true 2>/dev/null; then
    echo "Logging to: ${LOGFILE}"
    exec sudo -n "$0" "$@"
  else
    echo "Logging to: ${LOGFILE}"
    exec sudo "$0" "$@"
  fi
fi

# From here we're root; send all output to the log file
exec >"$LOGFILE" 2>&1
export DEBIAN_FRONTEND=noninteractive

# --- Pretty output helpers ---
info()  { printf "\n\033[1;34m[INFO]\033[0m %s\n" "$*"; }
ok()    { printf "\033[1;32m[OK]\033[0m %s\n" "$*"; }
warn()  { printf "\033[1;33m[WARN]\033[0m %s\n" "$*"; }

start_ts=$(date +"%F %T")
export DEBIAN_FRONTEND=noninteractive

info "Refreshing package lists…"
apt-get update -y # -qq

info "Upgrading installed packages…"
apt-get upgrade -y # -qq

info "Applying full upgrade (handles dependency changes)…"
apt-get -o APT::Get::Always-Include-Phased-Updates=true dist-upgrade -y # -qq

info "Removing unused packages and cleaning cache…"
apt-get autoremove --purge -y # -qq
apt-get clean -qq

# --- Cloudflare Tunnel: restart only when running an outdated binary ---
# apt updates the installed executable but may leave the old process running.
# A running executable marked "(deleted)" indicates the binary was replaced.
# Restart only in that case to activate the newly installed version.
# Preserve existing tunnel configuration and authentication.

info "Checking Cloudflare Tunnel…"

if systemctl is-active --quiet cloudflared; then
  cloudflared_pid="$(systemctl show -p MainPID --value cloudflared)"
  running_binary="$(readlink "/proc/${cloudflared_pid}/exe" 2>/dev/null || true)"

  if [[ "$running_binary" == *" (deleted)" ]]; then
    info "Cloudflare Tunnel binary was upgraded. Restarting service…"

    if systemctl restart cloudflared && systemctl is-active --quiet cloudflared; then
      ok "Cloudflare Tunnel restarted successfully: $(cloudflared --version)"
    else
      warn "Cloudflare Tunnel failed to restart. Check systemctl status cloudflared."
      exit 1
    fi
  else
    ok "Cloudflare Tunnel is already using the current binary."
  fi
else
  warn "Cloudflare Tunnel is not running. Skipping automatic restart."
fi

# Kernel check (current vs newest installed)
current_kernel="$(uname -r)"
newest_kernel="$(dpkg -l | awk '/^ii  linux-image-[0-9]/{print $3}' | sort -V | tail -1)"

info "Current kernel: ${current_kernel}"
ok "Newest installed kernel package version: ${newest_kernel:-unknown}"

# Reboot flag
if [ -f /var/run/reboot-required ]; then
  warn "A reboot is required to finish applying updates."
  echo "    -> Run: sudo reboot"
else
  ok "No reboot required."
fi

end_ts=$(date +"%F %T")
info "Started:  $start_ts"
ok   "Finished: $end_ts"
ok   "Log written to: ${LOGFILE}"


# ================ NOTES START ===================
# Save it, make it executable, run it
# nano ~/ubuntu-update.sh         # paste the script, save, exit
# chmod +x ./run_script.sh
# ~/ubuntu-update.sh

# NOTE: OPEN THE LOG FILE TO WATCH IT RUN

# LOG FILE
# ~/ubuntu-update.log
# cat ~/ubuntu-update.log
# tac ~/ubuntu-update.log

# SETUP PASSWORDLESS
# Here’s the clean, final setup so your script can run passwordless (and still works if passwordless isn’t set yet).
# sudo visudo -f /etc/sudoers.d/ubuntu_update
# steve-calla ALL=(root) NOPASSWD: /home/steve-calla/development/wrestling/wrestling_stats/scripts/cron_update_ubuntu/run_script.sh
# sudo chmod 440 /etc/sudoers.d/ubuntu_update

# SETUP CRON JOB
# crontab -e
# 0 3 * * 0 /home/steve-calla/development/ezhire/mysql_load/scheduled_jobs/cron_update_ubuntu/run_script.sh

# CURRENT SCHEDULE (LATITUDE 7420)
# /etc/cron.d/system_maintenance
# 30 09 * * * steve-calla $UBUNTU_UPDATE_PATH >> /home/steve-calla/logs/ubuntu_update.log 2>&1
# 45 09 * * * steve-calla $UBUNTU_LOG_PATH >> /home/steve-calla/logs/ubuntu_update.log 2>&1

# CLOUDFLARE TUNNEL
# Check installed version:
# cloudflared --version
#
# Check service status:
# sudo systemctl is-active cloudflared
#
# Check running executable (look for "(deleted)"):
# sudo readlink -f /proc/$(systemctl show -p MainPID --value cloudflared)/exe
#
# Check recent tunnel logs:
# sudo journalctl -u cloudflared -n 30 --no-pager
#
# Manually restart if needed:
# sudo systemctl restart cloudflared
#
# Automatic restart behavior:
# After successful apt upgrades, this script checks the running executable.
# If marked "(deleted)", it restarts cloudflared and logs the result.
# Otherwise, no restart occurs.
# Restart briefly interrupts connections through the tunnel.
# If the restart fails, the script exits with code 1.

# SETUP ALIAS
# alias ubuntu_update='~/ubuntu-update.sh'
# source ~/.bashrc
# ubuntu_update
# =============== NOTES END ============================

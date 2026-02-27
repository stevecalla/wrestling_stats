#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
log_file="${script_dir}/system_metrics.log"
metrics_file="${script_dir}/metrics.jsonl"

retention_hours=72
retention_seconds="$((retention_hours * 3600))"
now_epoch="$(date +%s)"
cutoff_epoch="$((now_epoch - retention_seconds))"

# ----------------------------
# trim metrics.jsonl by time (mawk-safe)
# ----------------------------
trim_metrics_file() {
  [ ! -f "$metrics_file" ] && return 0
  tmp_file="${metrics_file}.tmp"

  awk -v cutoff_epoch="$cutoff_epoch" '
    function to_epoch(ts, y,mo,d,h,mi,s) {
      y=substr(ts,1,4); mo=substr(ts,6,2); d=substr(ts,9,2)
      h=substr(ts,12,2); mi=substr(ts,15,2); s=substr(ts,18,2)
      return mktime(y " " mo " " d " " h " " mi " " s)
    }
    {
      ts=$0
      sub(/.*"ts_utc":"/, "", ts)
      sub(/".*/, "", ts)

      if (ts ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$/) {
        if (to_epoch(ts) >= cutoff_epoch) print $0
      }
    }
  ' "$metrics_file" > "$tmp_file" || true

  mv "$tmp_file" "$metrics_file"
}

# ----------------------------
# collect metrics
# ----------------------------
ts_utc="$(date -u +"%F %T")"
ts_local="$(date +"%F %T")"
host_name="$(hostname)"
cores="$(nproc)"

read -r load_1 load_5 load_15 < <(
  uptime | awk -F'load average:' '{gsub(/,/, "", $2); print $2}' | awk '{print $1, $2, $3}'
)

read -r mem_total_b mem_used_b mem_free_b mem_shared_b mem_cache_b mem_available_b < <(
  free -b | awk '/^Mem:/ {print $2,$3,$4,$5,$6,$7}'
)

read -r swap_total_b swap_used_b swap_free_b < <(
  free -b | awk '/^Swap:/ {print $2,$3,$4}'
)

vm_line="$(vmstat 1 2 | tail -1)"
read -r vm_r vm_b vm_swpd vm_free vm_buff vm_cache vm_si vm_so vm_bi vm_bo vm_in vm_cs vm_us vm_sy vm_id vm_wa vm_st < <(echo "$vm_line")

iostat_out="$(iostat -xz 1 2 2>/dev/null || true)"
cpu_iowait="$(echo "$iostat_out" | awk '/avg-cpu/ {f=1; next} f && NF>0 {print $4; exit}' || true)"
nvme_util="$(echo "$iostat_out" | awk '$1 ~ /^nvme/ {val=$NF} END{print val}' || true)"

read -r disk_size_b disk_used_b disk_avail_b disk_use_pct < <(
  df -B1 / | awk 'NR==2 {gsub(/%/,"",$5); print $2,$3,$4,$5}'
)

psi_some_avg10="$(awk '/^some/ {print $2}' /proc/pressure/memory | sed 's/avg10=//')"
psi_full_avg10="$(awk '/^full/ {print $2}' /proc/pressure/memory | sed 's/avg10=//')"

cpu_temp_c=""
nvme_temp_c=""
if command -v sensors >/dev/null 2>&1; then
  cpu_temp_c="$(sensors 2>/dev/null | awk '/Package id 0:/ {gsub(/[+°C]/,"",$4); print $4; exit}' || true)"
  nvme_temp_c="$(sensors 2>/dev/null | awk '/Composite:/ {gsub(/[+°C]/,"",$2); print $2; exit}' || true)"
fi

swappiness="$(cat /proc/sys/vm/swappiness 2>/dev/null || true)"

# ----------------------------
# write jsonl
# ----------------------------
json_line="$(printf '{"ts_utc":"%s","ts_local":"%s","host_name":"%s","cores":%s,"load_1":%s,"load_5":%s,"load_15":%s,"mem_total_b":%s,"mem_used_b":%s,"mem_available_b":%s,"swap_total_b":%s,"swap_used_b":%s,"vm_si":%s,"vm_so":%s,"vm_wa":%s,"disk_size_b":%s,"disk_used_b":%s,"disk_avail_b":%s,"disk_use_pct":%s,"cpu_temp_c":"%s","nvme_temp_c":"%s","nvme_util":"%s","cpu_iowait":"%s","psi_some_avg10":"%s","psi_full_avg10":"%s","swappiness":"%s"}' \
  "$ts_utc" "$ts_local" "$host_name" "$cores" \
  "$load_1" "$load_5" "$load_15" \
  "$mem_total_b" "$mem_used_b" "$mem_available_b" \
  "$swap_total_b" "$swap_used_b" \
  "$vm_si" "$vm_so" "$vm_wa" \
  "$disk_size_b" "$disk_used_b" "$disk_avail_b" "$disk_use_pct" \
  "${cpu_temp_c:-}" "${nvme_temp_c:-}" "${nvme_util:-}" "${cpu_iowait:-}" \
  "${psi_some_avg10:-}" "${psi_full_avg10:-}" "${swappiness:-}")"

echo "$json_line" >> "$metrics_file"

mem_used_gib="$(awk "BEGIN{print ${mem_used_b}/1024/1024/1024}")"
mem_total_gib="$(awk "BEGIN{print ${mem_total_b}/1024/1024/1024}")"
mem_avail_gib="$(awk "BEGIN{print ${mem_available_b}/1024/1024/1024}")"
swap_used_gib="$(awk "BEGIN{print ${swap_used_b}/1024/1024/1024}")"
swap_total_gib="$(awk "BEGIN{print ${swap_total_b}/1024/1024/1024}")"
disk_used_gib="$(awk "BEGIN{print ${disk_used_b}/1024/1024/1024}")"
disk_total_gib="$(awk "BEGIN{print ${disk_size_b}/1024/1024/1024}")"

printf "%s load=%s/%s/%s mem=%.1f/%.1fGi(avail=%.1fGi) swap=%.1f/%.1fGi disk=%.1f/%.1fGi(%s%%) vm_wa=%s%% si=%s so=%s cpu_temp=%sC nvme_temp=%sC\n" \
  "$ts_local" "$load_1" "$load_5" "$load_15" \
  "$mem_used_gib" "$mem_total_gib" "$mem_avail_gib" \
  "$swap_used_gib" "$swap_total_gib" \
  "$disk_used_gib" "$disk_total_gib" "$disk_use_pct" \
  "$vm_wa" "$vm_si" "$vm_so" \
  "${cpu_temp_c:-NA}" "${nvme_temp_c:-NA}" \
  >> "$log_file"

trim_metrics_file
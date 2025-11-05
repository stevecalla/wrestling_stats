/**
 * Detect whether a given UTC date is in DST for Mountain Time.
 * Returns correct offset: UTC−7 standard / UTC−6 during DST.
 */
function get_mountain_time_offset_hours(date = new Date()) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Denver",
    timeZoneName: "shortOffset",
  });
  const parts = fmt.formatToParts(date);
  const tz_part = parts.find(p => p.type === "timeZoneName")?.value || "";
  const match = tz_part.match(/GMT([+-]\d+)/);
  return match ? -Number(match[1]) : -7; // fallback −7 if parse fails
}

export { get_mountain_time_offset_hours };
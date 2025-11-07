/**
 * Mountain Time offset (hours) for a given UTC date.
 * Returns -7 during standard time (MST), -6 during daylight (MDT).
 * Use it like: local = new Date(utc.getTime() + offsetHours * 3600_000)
 */
function get_mountain_time_offset_hours(date = new Date()) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Denver",
    timeZoneName: "shortOffset",
  });
  const parts = fmt.formatToParts(date);
  const tz = parts.find(p => p.type === "timeZoneName")?.value || ""; // e.g., "GMT-07:00"
  const m = tz.match(/GMT([+-])(\d{2})(?::?(\d{2}))?/);
  if (!m) return -7; // sensible fallback

  const sign = m[1] === "-" ? -1 : 1;
  const hours = parseInt(m[2], 10);
  const minutes = m[3] ? parseInt(m[3], 10) : 0;
  return sign * (hours + minutes / 60);
}

export { get_mountain_time_offset_hours };

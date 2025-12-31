const ForecastRing = ({
  label,
  now,
  forecast,
  unit,
  type,
}) => {
  const safeNow = now ?? 0;
  const safeForecast = forecast ?? 0;

  // ---- Severity color ----
  let color = "#22c55e";
  if (
    (type === "pm25" && safeForecast > 35) ||
    (type === "co2" && safeForecast > 650)
  ) {
    color = "#ef4444";
  } else if (
    (type === "pm25" && safeForecast > 25) ||
    (type === "co2" && safeForecast > 500)
  ) {
    color = "#facc15";
  }

  // ---- Trend ----
  const trend =
    forecast == null || now == null
      ? "→"
      : safeForecast > safeNow
      ? "↑"
      : safeForecast < safeNow
      ? "↓"
      : "→";

  const trendColor =
    forecast == null || now == null
      ? "text-gray-400"
      : safeForecast > safeNow
      ? "text-red-400"
      : safeForecast < safeNow
      ? "text-green-400"
      : "text-gray-400";

  // ---- Circle math ----
  const radius = 32;
  const stroke = 6;
  const circumference = 2 * Math.PI * radius;

  const max = type === "pm25" ? 50 : 800;
  const progress = Math.min(safeForecast / max, 1);
  const offset = circumference * (1 - progress);

  return (
    <div className="flex flex-col items-center">
      {/* Ring + Arrow row */}
      <div className="flex items-center gap-4">
        {/* Ring */}
        <svg width="80" height="80" viewBox="0 0 80 80">
          <circle
            cx="40"
            cy="40"
            r={radius}
            stroke="#374151"
            strokeWidth={stroke}
            fill="none"
          />
          <circle
            cx="40"
            cy="40"
            r={radius}
            stroke={color}
            strokeWidth={stroke}
            fill="none"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
            transform="rotate(-90 40 40)"
          />

          {/* Value */}
          <text
            x="40"
            y="38"
            textAnchor="middle"
            fill="white"
            fontSize="12"
            fontWeight="600"
          >
            {forecast == null ? "--" : safeForecast}
          </text>

          {/* Unit */}
          <text
            x="40"
            y="52"
            textAnchor="middle"
            fill="#9ca3af"
            fontSize="9"
          >
            {unit}
          </text>
        </svg>

        {/* Trend Arrow */}
        <div
          className={`text-3xl font-bold ${trendColor}`}
          aria-label="trend"
        >
          {trend}
        </div>
      </div>

      {/* Label */}
      <div className="mt-2 text-xs opacity-80">
        {label}
      </div>
    </div>
  );
};

export default ForecastRing;

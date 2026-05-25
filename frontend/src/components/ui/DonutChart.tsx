interface DonutChartProps {
  value: number; // 0-100
  size?: number;
  strokeWidth?: number;
  color?: string;
  trackColor?: string;
  label?: string;
  sublabel?: string;
  white?: boolean;
}

export function DonutChart({
  value,
  size = 120,
  strokeWidth = 10,
  color = "#16a34a",
  trackColor,
  label,
  sublabel,
  white,
}: DonutChartProps) {
  const r = (size - strokeWidth) / 2;
  const circ = 2 * Math.PI * r;
  const offset = circ - (Math.min(Math.max(value, 0), 100) / 100) * circ;
  const track = trackColor ?? (white ? "rgba(255,255,255,0.2)" : "#e5e7eb");
  const textColor = white ? "white" : "var(--text)";
  const subColor = white ? "rgba(255,255,255,0.7)" : "var(--muted)";

  return (
    <div className="relative inline-flex items-center justify-center" style={{ width: size, height: size }}>
      <svg width={size} height={size} className="donut-ring">
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={track} strokeWidth={strokeWidth} />
        <circle
          cx={size / 2} cy={size / 2} r={r} fill="none"
          stroke={color} strokeWidth={strokeWidth}
          strokeDasharray={circ} strokeDashoffset={offset}
          strokeLinecap="round"
        />
      </svg>
      {(label || sublabel) && (
        <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
          {label && <span className="font-bold leading-tight" style={{ fontSize: size * 0.22, color: textColor }}>{label}</span>}
          {sublabel && <span className="leading-tight" style={{ fontSize: size * 0.12, color: subColor }}>{sublabel}</span>}
        </div>
      )}
    </div>
  );
}

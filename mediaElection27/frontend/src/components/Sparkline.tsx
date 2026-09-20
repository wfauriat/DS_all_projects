interface SparklineProps {
  values: number[];
  width?: number;
  height?: number;
  color?: string;
  ariaLabel?: string;
}

export function Sparkline({
  values,
  width = 90,
  height = 24,
  color = "#64748b",
  ariaLabel,
}: SparklineProps) {
  if (values.length === 0) {
    return (
      <svg
        width={width}
        height={height}
        role="img"
        aria-label={ariaLabel ?? "sparkline vide"}
      />
    );
  }
  const max = Math.max(...values, 1);
  const stepX = values.length > 1 ? width / (values.length - 1) : 0;
  const points = values
    .map((v, i) => {
      const x = i * stepX;
      const y = height - (v / max) * (height - 2) - 1;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg
      width={width}
      height={height}
      role="img"
      aria-label={ariaLabel ?? `sparkline ${values.length} jours, max ${max}`}
    >
      <polyline
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        points={points}
      />
    </svg>
  );
}

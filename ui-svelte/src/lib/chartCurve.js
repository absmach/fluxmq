/**
 * Shape-preserving cubic interpolation through measured samples. Shared tangents
 * avoid a flat spot at every sample; sign changes stay flat to prevent overshoot.
 * Call separately for each continuous run, so outages remain visible as gaps.
 * @param {{x: number, y: number}[]} points Strictly increasing x coordinates.
 */
export function smoothPath(points) {
  if (!points.length) return '';
  const widths = points.slice(1).map((p, i) => p.x - points[i].x);
  const slopes = points.slice(1).map((p, i) => (p.y - points[i].y) / widths[i]);
  const tangents = points.map((_, i) => {
    if (i === 0) return slopes[0] ?? 0;
    if (i === points.length - 1) return slopes[i - 1];
    const before = slopes[i - 1],
      after = slopes[i];
    if (before * after <= 0) return 0;
    const w1 = 2 * widths[i] + widths[i - 1];
    const w2 = widths[i] + 2 * widths[i - 1];
    return (w1 + w2) / (w1 / before + w2 / after);
  });
  return points
    .map((p, i) => {
      if (i === 0) return `M ${p.x} ${p.y}`;
      const previous = points[i - 1],
        step = widths[i - 1] / 3;
      return `C ${previous.x + step} ${previous.y + step * tangents[i - 1]}, ${p.x - step} ${p.y - step * tangents[i]}, ${p.x} ${p.y}`;
    })
    .join(' ');
}

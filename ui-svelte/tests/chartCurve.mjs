import assert from 'node:assert/strict';
import { smoothPath } from '../src/lib/chartCurve.js';
assert.equal(smoothPath([]), '');
assert.equal(smoothPath([{ x: 2, y: 7 }]), 'M 2 7');
for (const samples of [
  [
    { x: 0, y: 0 },
    { x: 1, y: 1 },
    { x: 2, y: 2 },
  ],
  [
    { x: 0, y: 0 },
    { x: 1, y: 10 },
    { x: 3, y: 2 },
    { x: 4, y: 2 },
    { x: 9, y: 20 },
  ],
  [
    { x: 0, y: 0 },
    { x: 0.1, y: 0.2 },
    { x: 80, y: 0.3 },
    { x: 81, y: 100 },
  ],
]) {
  const curves = smoothPath(samples)
    .split(' C ')
    .slice(1)
    .map((s) => s.replaceAll(',', '').split(' ').map(Number));
  curves.forEach(([x1, y1, x2, y2, x, y], i) => {
    const start = samples[i],
      end = samples[i + 1];
    assert.equal(x, end.x);
    assert.equal(y, end.y);
    for (let t = 0; t <= 1; t += 0.01) {
      const value =
        (1 - t) ** 3 * start.y + 3 * (1 - t) ** 2 * t * y1 + 3 * (1 - t) * t * t * y2 + t ** 3 * y;
      assert.ok(
        value >= Math.min(start.y, end.y) - 1e-9 && value <= Math.max(start.y, end.y) + 1e-9,
        'no invented peaks or negative overshoot',
      );
    }
    if (i > 0) {
      const previous = curves[i - 1];
      const incoming = (start.y - previous[3]) / (start.x - previous[2]);
      const outgoing = (y1 - start.y) / (x1 - start.x);
      assert.ok(Math.abs(incoming - outgoing) < 1e-9, 'continuous tangent across samples');
    }
    if (samples.length === 3)
      assert.ok(y1 > start.y && y2 < end.y, 'straight trend has no per-sample flat spots');
  });
}
console.log('PASS: interpolation preserves samples, continuous slopes and bounds');

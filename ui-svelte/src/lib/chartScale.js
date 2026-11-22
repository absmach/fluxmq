/** Fraction of the axis height. log1p preserves zero and fractional rates.
 * @param {number} value
 * @param {number} maximum
 * @param {boolean} logarithmic
 */
export function scalePosition(value, maximum, logarithmic) {
  return logarithmic ? Math.log1p(value) / Math.log1p(maximum) : value / maximum;
}

/** Readable ticks in actual units, omitting labels that would collide.
 * @param {number} maximum
 * @param {boolean} logarithmic
 */
export function scaleTicks(maximum, logarithmic) {
  if (!logarithmic) return [1, 0.75, 0.5, 0.25, 0].map((f) => maximum * f);
  const ticks = [0];
  const highest = Math.floor(Math.log10(maximum));
  for (let power = Math.min(-1, highest); power <= highest; power++) {
    const value = 10 ** power;
    if (
      scalePosition(value, maximum, true) - scalePosition(ticks.at(-1) ?? 0, maximum, true) >=
      0.07
    )
      ticks.push(value);
  }
  return ticks.reverse();
}

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

/** Preserve a linear overview while giving sub-pixel signals their own labeled
 * detail scale. Zero/missing series are not magnified into invented activity.
 * @param {{key:string, axis?:'right', peak:number}[]} series
 * @param {boolean} logRates
 */
export function linearDetails(series, logRates) {
  /** @type {{axis:'left'|'right', keys:string[], maximum:number}[]} */
  const groups = [];
  for (const axis of /** @type {const} */ (['left', 'right'])) {
    let logarithmic = axis === 'right' && logRates;
    let items = series.filter((s) => (s.axis ?? 'left') === axis && s.peak > 0);
    while (items.length > 1) {
      const largest = Math.max(...items.map((s) => s.peak));
      const smaller = items.filter((s) =>
        logarithmic ? scalePosition(s.peak, largest * 1.16, true) < 0.08 : s.peak / largest < 0.16,
      );
      if (!smaller.length) break;
      groups.push({
        axis,
        keys: smaller.map((s) => s.key),
        maximum: Math.max(...smaller.map((s) => s.peak)) * 1.16,
      });
      items = smaller;
      logarithmic = false;
    }
  }
  return groups;
}

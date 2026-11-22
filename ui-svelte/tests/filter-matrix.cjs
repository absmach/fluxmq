const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
const shots = process.env.FLUXMQ_SCREENSHOT_DIR || '/tmp/fluxmq-console-screenshots';
const keys = [
  'queue.pending.current',
  'queue.acknowledgments.poison_pending',
  'queue.acknowledgments.ack',
  'queue.acknowledgments.nack',
  'queue.acknowledgments.reject',
  'queue.acknowledgments.dlq',
  'queue.capture_failures',
  'queue.capture_dropped',
];
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const snapshot = await (await page.request.get(base + '/console-api/snapshot')).json();
  const now = Date.now();
  const scopes = ['all', ...snapshot.nodes.map((n) => n.id)];
  const histories = {};
  for (const [scopeIndex, scope] of scopes.entries())
    histories[scope] = Array.from({ length: 90 }, (_, i) => ({
      time: now - (89 - i) * 10000,
      incoming: 0,
      outgoing: 0,
      bandwidth: 0,
      connections: 0,
      metrics: Object.fromEntries(
        keys.map((key, k) => [
          key,
          [120 + (i % 30), 0.5, 11000 + i * 10, 5 + (i % 3), 1.5, 0.02, 0, 0][k] / (scopeIndex + 1),
        ]),
      ),
    }));
  snapshot.history = histories;
  snapshot.points = Object.fromEntries(scopes.map((s) => [s, histories[s].at(-1)]));
  await page.route('**/console-api/stream', (route) =>
    route.fulfill({
      contentType: 'text/event-stream',
      body: 'event: snapshot\ndata: ' + JSON.stringify(snapshot) + '\n\n',
    }),
  );
  await page.route('**/console-api/snapshot', (route) => route.fulfill({ json: snapshot }));
  let states = 0;
  async function matrix() {
    return page.evaluate(async (keys) => {
      const root = document.querySelector('.queue-diagnostics');
      const buttons = [...root.querySelectorAll('.series-readout button')];
      const flush = () => new Promise((r) => setTimeout(r, 0));
      function fail(message, mask) {
        throw Error(`${message}; mask=${mask}; scale=${root.querySelector('select').value}`);
      }
      // Establish the full-data geometry once. Every subset must retain it.
      for (const button of buttons)
        if (button.getAttribute('aria-pressed') !== 'true') button.click();
      await flush();
      const geometry = () => ({
        height: root.getBoundingClientRect().height,
        axes: [...root.querySelectorAll('.plot-y,.detail-y')].map((e) => e.textContent),
        plots: [...root.querySelectorAll('.plot-main svg,.detail-main svg')].map((e) => {
          const r = e.getBoundingClientRect();
          return [r.width, r.height, r.top - root.getBoundingClientRect().top];
        }),
      });
      const baseline = JSON.stringify(geometry());
      const pathGeometry = new Map(
        [...root.querySelectorAll('path[data-series],path[data-detail-series]')].map((p) => [
          p.dataset.series
            ? 'main:' + p.dataset.series
            : 'detail:' +
              p.closest('.linear-detail').dataset.maximum +
              ':' +
              p.dataset.detailSeries,
          p.getAttribute('d'),
        ]),
      );
      function inspect(mask) {
        if (JSON.stringify(geometry()) !== baseline)
          fail('selection changed chart scales or layout', mask);
        for (const p of root.querySelectorAll('path[data-series],path[data-detail-series]')) {
          const key = p.dataset.series
            ? 'main:' + p.dataset.series
            : 'detail:' +
              p.closest('.linear-detail').dataset.maximum +
              ':' +
              p.dataset.detailSeries;
          if (p.getAttribute('d') !== pathGeometry.get(key))
            fail('selection changed remaining curve geometry', mask);
        }
        const expected = keys.filter((_, i) => mask & (1 << i));
        const paths = [...root.querySelectorAll('path[data-series]')];
        const actual = [...new Set(paths.map((p) => p.dataset.series))];
        if (JSON.stringify(actual) !== JSON.stringify(expected)) fail('wrong plotted series', mask);
        const inputs = [...root.querySelectorAll('.series-option input')];
        for (let i = 0; i < 8; i++) {
          const on = !!(mask & (1 << i));
          if (
            (buttons[i].getAttribute('aria-pressed') === 'true') !== on ||
            inputs[i].checked !== on
          )
            fail('legend/checkbox mismatch', mask);
          if (!on && root.querySelector(`path[data-detail-series="${keys[i]}"]`))
            fail('hidden trace left in detail', mask);
          if (on && i < 6) {
            const candidates = [
              ...paths.filter((p) => p.dataset.series === keys[i]),
              ...root.querySelectorAll(`path[data-detail-series="${keys[i]}"]`),
            ];
            const readable = candidates.some((p) => {
              const svg = p.ownerSVGElement;
              const h = svg.viewBox.baseVal.height;
              return ((h - p.getBBox().y) * svg.getBoundingClientRect().height) / h >= 8;
            });
            if (!readable) fail(`selected nonzero signal is unreadable: ${keys[i]}`, mask);
          }
          if (on && i >= 6 && !buttons[i].textContent.includes('Zero reported'))
            fail('zero series lacks explicit status', mask);
        }
        const tooltip = root.querySelector('.plot-tooltip');
        if (
          expected.length &&
          tooltip?.querySelectorAll(':scope > span').length !== expected.length
        )
          fail('tooltip series mismatch', mask);
        for (const p of root.querySelectorAll('path[data-series],path[data-detail-series]'))
          if (/NaN|Infinity/.test(p.getAttribute('d'))) fail('non-finite geometry', mask);
        if (mask === 0 && !root.textContent.includes('Choose a series to display'))
          fail('empty selection missing', mask);
      }
      // Gray code exercises all subsets while using the actual legend click handlers.
      for (let n = 0; n < 256; n++) {
        const mask = n ^ (n >> 1);
        for (let i = 0; i < 8; i++)
          if ((buttons[i].getAttribute('aria-pressed') === 'true') !== !!(mask & (1 << i)))
            buttons[i].click();
        root
          .querySelector('.plot-input')
          .dispatchEvent(new KeyboardEvent('keydown', { key: 'End', bubbles: true }));
        await flush();
        inspect(mask);
        // For every subset of the other seven signals, independently toggle ACK both ways.
        if (!(mask & 4)) {
          buttons[2].click();
          await flush();
          inspect(mask | 4);
          buttons[2].click();
          await flush();
          inspect(mask);
        }
      }
      return 512;
    }, keys);
  }
  for (const route of ['queues', 'health']) {
    await page.goto(base + '/dashboard/' + route);
    await page.locator('.queue-diagnostics path[data-series]').first().waitFor();
    await page.getByRole('button', { name: 'Pause updates', exact: true }).click();
    const scopeList = route === 'queues' ? scopes : ['all'];
    for (const scope of scopeList) {
      await page.getByRole('button', { name: 'Select node scope', exact: true }).click();
      await page
        .getByRole('option', { name: scope === 'all' ? 'All nodes' : scope, exact: true })
        .click();
      const queue = page.locator('.queue-diagnostics');
      for (const range of route === 'queues' ? [1, 5, 15] : [5]) {
        await queue.getByRole('button', { name: `${range}m`, exact: true }).click();
        for (const scale of ['linear', 'log']) {
          await queue.getByRole('combobox', { name: 'Queue rate scale' }).selectOption(scale);
          states += await matrix();
          console.log(
            `PASS ${route} / ${scope} / ${range}m / ${scale}: all 256 subsets + all ACK transitions`,
          );
        }
      }
    }
  }
  // Real pointer interactions through every menu option, plus mobile hit testing.
  await page.goto(base + '/dashboard/queues');
  const queue = page.locator('.queue-diagnostics');
  await queue.locator('path[data-series]').first().waitFor();
  await queue.getByRole('combobox', { name: 'Queue rate scale' }).selectOption('linear');
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 1100 });
    for (let i = 0; i < 8; i++) {
      await queue.locator('.series-menu summary').click();
      const checkbox = queue.getByRole('checkbox').nth(i);
      await checkbox.uncheck();
      await checkbox.check();
      await queue.locator('.series-option button').nth(i).click();
      assert.equal(await queue.locator('.series-readout button[aria-pressed=true]').count(), 1);
      await queue.locator('.series-menu summary').click();
      await queue.getByRole('button', { name: 'Show all', exact: true }).click();
      await page.keyboard.press('Escape');
    }
    assert.ok(
      await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
      'no page overflow',
    );
    fs.mkdirSync(shots, { recursive: true });
    await queue.screenshot({ path: `${shots}/filter-matrix-linear-${width}.png` });
  }
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    `PASS ${states} checked chart states/transitions, real menu controls, desktop/mobile`,
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

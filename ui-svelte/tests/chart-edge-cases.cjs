const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
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
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const original = await (await page.request.get(base + '/console-api/snapshot')).json();
  let snapshot = structuredClone(original);
  const point = (i, metrics) => ({
    time: Date.now() - (59 - i) * 10000,
    incoming: 0,
    outgoing: 0,
    bandwidth: 0,
    connections: 0,
    metrics,
  });
  function fixture(points) {
    snapshot = structuredClone(original);
    snapshot.history = { all: points };
    snapshot.points = { all: points.at(-1) };
  }
  await page.route('**/console-api/stream', (r) =>
    r.fulfill({
      contentType: 'text/event-stream',
      body: 'event: snapshot\ndata: ' + JSON.stringify(snapshot) + '\n\n',
    }),
  );
  await page.route('**/console-api/snapshot', (r) => r.fulfill({ json: snapshot }));
  const queue = page.locator('.queue-diagnostics');
  async function open(points) {
    fixture(points);
    await page.goto(base + '/dashboard/queues');
    await queue.waitFor();
    await page.getByRole('button', { name: 'Pause updates', exact: true }).click();
    await queue.getByRole('combobox', { name: 'Queue rate scale' }).selectOption('linear');
  }
  const values = Object.fromEntries(
    keys.map((k, i) => [k, [120, 0.1, 12000, 6, 1.5, 0.02, 0, 0][i]]),
  );
  await open(Array.from({ length: 60 }, (_, i) => point(i, values)));
  await queue.getByRole('button', { name: '15m', exact: true }).click();
  const slider = queue.getByRole('slider');
  await slider.focus();
  await slider.press('End');
  assert.equal(await slider.getAttribute('aria-valuenow'), '59');
  await queue.getByRole('button', { name: '1m', exact: true }).click();
  assert.ok(
    Number(await slider.getAttribute('aria-valuenow')) <=
      Number(await slider.getAttribute('aria-valuemax')),
  );
  assert.doesNotMatch(
    await queue.locator('.series-readout').innerText(),
    /—/,
    'range change keeps readouts valid',
  );
  // Empty history and missing metrics are distinct from measured zero.
  await open([]);
  await queue.getByText('Collecting history…', { exact: true }).waitFor();
  assert.equal(await queue.locator('path[data-series]').count(), 0);
  assert.match(await queue.locator('.series-readout').innerText(), /—/);
  await open([point(59, {})]);
  assert.equal(await queue.locator('path[data-series]').count(), 0);
  assert.equal(await queue.locator('.zero-note').count(), 0);
  await open([point(59, Object.fromEntries(keys.map((k) => [k, 0])))]);
  assert.equal(await queue.locator('.zero-note').count(), 8);
  assert.equal(await queue.locator('.linear-detail').count(), 0);
  assert.doesNotMatch(await queue.locator('.series-readout').innerText(), /—/);
  // One-sample and fractional data remain visible without inventing history.
  await open([point(59, values)]);
  assert.equal(await queue.locator('path[data-series]').count(), 8);
  assert.ok((await queue.locator('.plot-main circle').count()) >= 8);
  assert.match(
    await queue.getByRole('button', { name: 'Toggle Dead-lettered', exact: true }).innerText(),
    /0.02/,
  );
  // Missing samples split both overview and magnified paths.
  await open([
    point(55, values),
    point(56, values),
    point(57, {}),
    point(58, values),
    point(59, values),
  ]);
  assert.equal(await queue.locator('path[data-series="queue.acknowledgments.nack"]').count(), 2);
  assert.equal(
    await queue.locator('path[data-detail-series="queue.acknowledgments.nack"]').count(),
    2,
  );
  // Shared renderer must label protocol rate details by their units, not axis side.
  fixture([
    point(58, {
      'protocol.mqtt.messages.received': 12000,
      'protocol.mqtt.messages.sent': 11000,
      'protocol.amqp.messages.received': 10,
      'protocol.amqp.messages.sent': 5,
    }),
    point(59, {
      'protocol.mqtt.messages.received': 13000,
      'protocol.mqtt.messages.sent': 12000,
      'protocol.amqp.messages.received': 11,
      'protocol.amqp.messages.sent': 6,
    }),
  ]);
  await page.goto(base + '/dashboard/health');
  const protocol = page.locator('.protocol-diagnostics');
  await protocol.locator('.linear-detail').waitFor();
  assert.match(await protocol.locator('.detail-heading').innerText(), /Magnified rates/);
  // Real SSE updates must preserve filters and their magnified view.
  await page.unroute('**/console-api/stream');
  await page.unroute('**/console-api/snapshot');
  await page.goto(base + '/dashboard/queues');
  await queue.locator('path[data-series]').first().waitFor();
  await queue.getByRole('combobox', { name: 'Queue rate scale' }).selectOption('linear');
  for (const name of ['Pending', 'Poison pending', 'Capture failed', 'Capture dropped'])
    await queue.getByRole('button', { name: `Toggle ${name}`, exact: true }).click();
  const before = await queue
    .locator('path[data-series="queue.acknowledgments.ack"]')
    .first()
    .getAttribute('d');
  await page.waitForFunction(
    (before) =>
      document
        .querySelector('.queue-diagnostics path[data-series="queue.acknowledgments.ack"]')
        ?.getAttribute('d') !== before,
    before,
    { timeout: 10000 },
  );
  assert.deepEqual(
    await queue
      .locator('path[data-series]')
      .evaluateAll((ps) => [...new Set(ps.map((p) => p.dataset.series))]),
    keys.slice(2, 6),
  );
  assert.ok(
    (await queue.locator('path[data-detail-series="queue.acknowledgments.nack"]').count()) > 0,
  );
  await queue.getByRole('button', { name: 'Toggle ACK', exact: true }).click();
  assert.ok(
    await queue
      .locator('path[data-series="queue.acknowledgments.nack"]')
      .evaluate((p) => p.getBBox().y > 168),
  );
  await queue.getByRole('button', { name: 'Toggle ACK', exact: true }).click();
  assert.ok(
    (await queue.locator('path[data-detail-series="queue.acknowledgments.nack"]').count()) > 0,
  );
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'PASS: range inspection, empty/missing/zero/fractional/single-sample data, outage gaps, and real SSE filter persistence',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

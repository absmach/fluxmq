const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
const shots = process.env.FLUXMQ_SCREENSHOT_DIR || '/tmp/fluxmq-console-screenshots';
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const snapshot = await (await page.request.get(base + '/console-api/snapshot')).json();
  snapshot.nodes.forEach((n, i) => {
    n.queue_metrics.pending.current = (i + 1) * 10;
    n.queue_metrics.acknowledgments.poison_pending = 0;
  });
  const now = Date.now();
  for (const scope of ['all', ...snapshot.nodes.map((n) => n.id)]) {
    const point = {
      time: now,
      incoming: 0,
      outgoing: 0,
      bandwidth: 0,
      connections: 0,
      metrics: {
        'queue.acknowledgments.nack': 2.5,
        'queue.acknowledgments.dlq': 0,
        'queue.acknowledgments.reject': 0,
        'queue.capture_failures': 0.02,
        'queue.capture_dropped': 0,
      },
    };
    snapshot.history[scope] = [point];
    snapshot.points[scope] = point;
  }
  await page.route('**/console-api/stream', (route) =>
    route.fulfill({
      contentType: 'text/event-stream',
      body: 'event: snapshot\ndata: ' + JSON.stringify(snapshot) + '\n\n',
    }),
  );
  await page.goto(base + '/dashboard/health');
  const summary = page.getByRole('region', { name: 'Queue health summary' });
  const protocol = page.getByRole('region', { name: 'Protocol diagnostics' });
  await summary.waitFor();
  assert.equal(await page.locator('.queue-diagnostics').count(), 0);
  assert.match(await summary.locator('.pending-summary strong').innerText(), /60/);
  assert.match(await summary.locator('dl > div').filter({ hasText: 'NACK' }).innerText(), /2.5/);
  assert.match(
    await summary.locator('dl > div').filter({ hasText: 'Capture failed' }).innerText(),
    /0.02/,
  );
  const a = await protocol.boundingBox(),
    b = await summary.boundingBox();
  assert.ok(
    Math.abs(a.y - b.y) < 1 && b.x >= a.x + a.width,
    'summary sits right of protocol traffic',
  );
  fs.mkdirSync(shots, { recursive: true });
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page
    .locator('.health-activity-row')
    .screenshot({ path: shots + '/health-summary-light.png' });
  await page.getByRole('button', { name: 'Dark appearance', exact: true }).click();
  await page
    .locator('.health-activity-row')
    .screenshot({ path: shots + '/health-summary-dark.png' });
  const node = snapshot.nodes[1].id;
  await page.goto(base + '/dashboard/health?node=' + node);
  await summary.waitFor();
  assert.match(await summary.locator('.pending-summary strong').innerText(), /20/);
  await summary.getByRole('link', { name: 'View queues' }).click();
  await page.waitForURL('**/dashboard/queues?node=' + node);
  await page.locator('.queue-diagnostics').waitFor();
  await page.goto(base + '/dashboard/health');
  await page.setViewportSize({ width: 390, height: 844 });
  await summary.waitFor();
  const mobileProtocol = await protocol.boundingBox(),
    mobileSummary = await summary.boundingBox();
  assert.ok(mobileSummary.y >= mobileProtocol.y + mobileProtocol.height, 'panels stack on mobile');
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
  snapshot.nodes.forEach((n) => {
    delete n.queue_metrics;
  });
  await page.reload();
  await summary.getByText('0 / 3 nodes reporting queue metrics').waitFor();
  assert.match(await summary.locator('.pending-summary strong').innerText(), /—/);
  assert.ok((await summary.locator('dd').allTextContents()).every((v) => v.includes('—')));
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'PASS health summary: scoped counts, rates, missing telemetry, scoped navigation, side-by-side/stacked layout',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

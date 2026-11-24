// Run with examples/queue-load active on all three cluster nodes.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const base = process.env.FLUXMQ_TEST_URL;
if (!base) throw new Error('Set FLUXMQ_TEST_URL');
(async () => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));
    const snapshot = await (await page.request.get(base + '/console-api/snapshot')).json();
    for (const n of snapshot.nodes) {
      assert.equal(n.healthy, true);
      const samples = snapshot.history[n.id].slice(-10);
      for (const key of ['ack', 'nack', 'reject', 'dlq']) {
        assert.ok(n.queue_metrics.acknowledgments[key] > 0, n.id + ' counter ' + key);
        assert.ok(
          samples.some((p) => p.metrics?.['queue.acknowledgments.' + key] > 0),
          n.id + ' rate ' + key,
        );
      }
      assert.ok(n.queue_metrics.pending.current > 0, n.id + ' pending');
      assert.equal(n.queue_metrics.capture_failures, 0);
      assert.equal(n.queue_metrics.capture_dropped, 0);
    }
    await page.goto(base + '/dashboard/queues');
    const chart = page.getByRole('region', { name: 'Queue delivery diagnostics' });
    await chart.getByRole('img', { name: 'Queue activity history', exact: true }).waitFor();
    for (const key of ['ack', 'nack', 'reject', 'dlq']) {
      const path = chart.locator(`.plot-main path[data-series="queue.acknowledgments.${key}"]`);
      await path.last().waitFor();
      assert.ok(
        await path.last().evaluate((p) => p.getBBox().y < 165),
        key + ' is visibly above zero',
      );
    }
    await page.setViewportSize({ width: 390, height: 844 });
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
    assert.deepEqual(errors, []);
    console.log(
      'PASS: live ACK/NACK/reject/DLQ counters and rates on every node, pending deliveries, healthy capture, visible queue chart and mobile layout',
    );
  } finally {
    await browser.close();
  }
})().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});

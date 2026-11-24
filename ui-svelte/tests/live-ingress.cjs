// Requires a live broker with active publishers on all six protocol/transport categories.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const base = process.env.FLUXMQ_TEST_URL;
if (!base) throw new Error('Set FLUXMQ_TEST_URL to the live console');
(async () => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    const errors = [];
    page.on('pageerror', (error) => errors.push(error.message));
    const keys = ['mqtt', 'websocket', 'http', 'coap', 'amqp', 'amqp1'];
    await page.goto(base + '/dashboard');
    const panel = page.getByRole('region', { name: 'Publish traffic by protocol' });
    await panel.waitFor();
    for (const key of keys) {
      const card = panel.locator(`[data-protocol="${key}"]`);
      await card.waitFor();
      await page.waitForFunction((key) => {
        const text = document.querySelector(
          `.protocol-ingress [data-protocol="${key}"] strong`,
        )?.textContent;
        return text && Number.parseFloat(text) > 0;
      }, key);
    }
    assert.ok(
      await panel
        .locator('article')
        .evaluateAll((cards) => cards.every((card) => card.scrollWidth <= card.clientWidth + 1)),
      'protocol card overflow',
    );
    await panel.getByRole('button', { name: '1m', exact: true }).click();
    assert.equal(
      await panel.getByRole('button', { name: '1m', exact: true }).getAttribute('aria-pressed'),
      'true',
    );
    await page.goto(base + '/dashboard/health');
    await page.locator('.health-activity-row').waitFor();
    assert.equal(
      await page.getByRole('region', { name: 'Publish traffic by protocol' }).count(),
      0,
      'ingress belongs on Overview, not Health',
    );
    const response = await page.request.get(base + '/console-api/snapshot');
    assert.equal(response.status(), 200);
    const snapshot = await response.json();
    assert.equal(snapshot.mode, 'live');
    const node = snapshot.nodes.find((n) => n.healthy && n.ingress_metrics);
    assert.ok(node);
    await page.goto(base + '/dashboard?node=' + encodeURIComponent(node.id));
    await page.getByRole('region', { name: 'Publish traffic by protocol' }).waitFor();
    for (const key of keys) {
      assert.ok(node.ingress_metrics[key].publishes > 0, key);
      assert.ok(snapshot.points.all.metrics[`ingress.${key}.publishes`] > 0, key + ' live rate');
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page
      .getByRole('region', { name: 'Publish traffic by protocol' })
      .scrollIntoViewIfNeeded();
    assert.ok(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1),
      'mobile overflow',
    );
    assert.deepEqual(errors, []);
    console.log(
      'PASS: live HTTP, CoAP, WebSocket, MQTT and AMQP rates on Overview, absent from Health, node scope, ranges and mobile layout',
    );
    if (process.env.FLUXMQ_SCREENSHOT_DIR) {
      await page.setViewportSize({ width: 1440, height: 1100 });
      await page
        .getByRole('region', { name: 'Publish traffic by protocol' })
        .screenshot({ path: process.env.FLUXMQ_SCREENSHOT_DIR + '/protocol-ingress.png' });
    }
  } finally {
    await browser.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

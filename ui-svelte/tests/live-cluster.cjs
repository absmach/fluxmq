// Run against three real nodes, each with 8 MQTT 3/5 TCP clients,
// 8 MQTT 3/5 WebSocket clients, and at least 8 AMQP 0.9.1 clients.
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
    const response = await page.request.get(base + '/console-api/snapshot');
    assert.equal(response.status(), 200);
    const snapshot = await response.json();
    assert.equal(snapshot.mode, 'live');
    assert.equal(snapshot.nodes.length, 3);
    assert.equal(snapshot.nodes.filter((n) => n.healthy).length, 3);
    assert.equal(snapshot.nodes.filter((n) => n.leader).length, 1);
    for (const n of snapshot.nodes) {
      assert.equal(n.cluster_mode, true);
      for (const key of ['mqtt', 'websocket', 'http', 'coap', 'amqp', 'amqp1'])
        assert.ok(snapshot.points[n.id].metrics[`ingress.${key}.publishes`] > 0, n.id + ' ' + key);
    }
    await page.goto(base + '/dashboard');
    await page.getByRole('region', { name: 'Publish traffic by protocol' }).waitFor();
    await page.goto(base + '/dashboard/sessions');
    await page.waitForFunction(() => document.querySelectorAll('.protocol-lane').length === 5);
    const counts = await page.locator('.protocol-count').allTextContents();
    const total = counts.reduce((n, value) => n + parseInt(value), 0);
    assert.ok(total >= 144);
    const wsCard = page.getByRole('button', { name: 'Filter sessions by WebSocket', exact: true });
    assert.match(await wsCard.innerText(), /48\s+clients/);
    const amqp1Card = page.getByRole('button', {
      name: 'Filter sessions by AMQP 1.0 · TCP',
      exact: true,
    });
    assert.match(await amqp1Card.innerText(), /24\s+clients/);
    await amqp1Card.click();
    await page.waitForFunction(() =>
      document.querySelector('.filter-row')?.textContent.includes('24 results'),
    );
    await page.locator('.client-link').first().click();
    await page.getByRole('heading', { name: 'Connection', exact: true }).waitFor();
    assert.match(await page.locator('.inspector-fields').innerText(), /AMQP 1.0/);
    await page.goBack();
    await amqp1Card.click();
    await page.waitForFunction(
      (n) => document.querySelector('.filter-row')?.textContent.includes(`${n} results`),
      total,
    );
    await wsCard.click();
    await page.waitForFunction(() =>
      document.querySelector('.filter-row')?.textContent.includes('48 results'),
    );
    assert.equal(await page.getByLabel('Transport', { exact: true }).inputValue(), 'websocket');
    await wsCard.click();
    await page.waitForFunction(
      (n) => document.querySelector('.filter-row')?.textContent.includes(`${n} results`),
      total,
    );
    await page
      .getByRole('button', { name: 'Filter sessions by MQTT 3.1.1 · TCP', exact: true })
      .click();
    await page.waitForFunction(() =>
      document.querySelector('.filter-row')?.textContent.includes('24 results'),
    );
    assert.equal(await page.getByLabel('Transport', { exact: true }).inputValue(), 'tcp');
    await page
      .getByRole('button', { name: 'Filter sessions by MQTT 3.1.1 · TCP', exact: true })
      .click();
    await page.waitForFunction(
      (n) => document.querySelector('.filter-row')?.textContent.includes(`${n} results`),
      total,
    );
    await wsCard.click();
    await page.waitForFunction(() =>
      document.querySelector('.filter-row')?.textContent.includes('48 results'),
    );
    const rows = page.locator('table tbody tr');
    assert.ok((await rows.count()) > 0);
    for (const row of await rows.all())
      assert.equal((await row.locator('td').nth(3).innerText()).trim(), 'WebSocket');
    await page.locator('.client-link').first().click();
    await page.getByRole('heading', { name: 'Connection', exact: true }).waitFor();
    const transport = page
      .locator('.inspector-fields div')
      .filter({ has: page.locator('dt').filter({ hasText: /^Transport$/ }) });
    assert.equal((await transport.locator('dd').innerText()).trim(), 'WebSocket');
    await page.goBack();
    await page.getByLabel('Transport', { exact: true }).waitFor();
    assert.equal(await page.getByLabel('Transport', { exact: true }).inputValue(), 'websocket');
    await page.getByLabel('Transport', { exact: true }).selectOption('tcp');
    await page.waitForFunction(
      (n) => document.querySelector('.filter-row')?.textContent.includes(`${n} results`),
      total - 48,
    );
    for (const row of await page.locator('table tbody tr').all())
      assert.equal((await row.locator('td').nth(3).innerText()).trim(), 'TCP');
    await page.goto(base + '/dashboard/sessions?node=node2');
    await page.getByLabel('Transport', { exact: true }).selectOption('websocket');
    await page.waitForFunction(() =>
      document.querySelector('.filter-row')?.textContent.includes('16 results'),
    );
    for (const row of await page.locator('table tbody tr').all())
      assert.equal((await row.locator('td').nth(4).innerText()).trim(), 'node2');
    await page.setViewportSize({ width: 390, height: 844 });
    assert.ok(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1),
      'mobile sessions overflow',
    );
    await page.setViewportSize({ width: 1440, height: 1100 });
    await page.goto(base + '/dashboard/health');
    await page.locator('.health-activity-row').waitFor();
    assert.equal(await page.locator('.protocol-ingress').count(), 0);
    const chart = page.getByRole('region', { name: 'Protocol diagnostics' });
    for (const label of [
      'MQTT · TCP',
      'WebSocket · MQTT',
      'HTTP',
      'CoAP',
      'AMQP 0.9.1',
      'AMQP 1.0',
    ]) {
      await chart.getByRole('button', { name: `Toggle ${label} incoming`, exact: true }).waitFor();
    }
    for (const metric of ['bandwidth', 'connections', 'errors', 'messages']) {
      await page.getByLabel('Protocol chart metric').selectOption(metric);
      await chart.getByRole('img', { name: `Protocol ${metric} history`, exact: true }).waitFor();
    }
    assert.deepEqual(errors, []);
    console.log(
      'PASS: three healthy nodes, one leader, all protocol rates, WebSocket and AMQP 1.0 cards, TCP/WS filtering, session details, Back navigation, node scope and Health placement',
    );
  } finally {
    await browser.close();
  }
})().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});

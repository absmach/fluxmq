const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(15000);
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto(`${base}/dashboard/sessions`);
  await page.locator('.client-link').first().waitFor();
  await page.getByLabel('Rows per page', { exact: true }).selectOption('5');
  assert.equal(await page.locator('tbody tr').count(), 5);
  await page.getByRole('button', { name: 'Last page', exact: true }).click();
  assert.match(await page.locator('.pagination').textContent(), /8 \/ 8/);
  await page.getByRole('button', { name: 'First page', exact: true }).click();
  await page.getByLabel('Protocol', { exact: true }).selectOption('amqp0.9.1');
  await page.locator('.client-link').first().click();
  await page.locator('.client-detail-page').getByText('Connection name', { exact: true }).waitFor();
  assert.equal(
    await page
      .locator('.client-detail-page')
      .getByText('Session settings', { exact: true })
      .count(),
    0,
    'AMQP must not show MQTT settings',
  );
  await page
    .locator('.client-detail-page')
    .getByRole('heading', { name: /Consumers/ })
    .waitFor();
  await page.locator('.detail-breadcrumb a').click();
  await page.getByRole('button', { name: 'Select node scope' }).click();
  await page.getByLabel('Find a node').press('Escape');
  assert.equal(await page.getByRole('listbox', { name: 'Node scope' }).count(), 0);
  await page.getByRole('button', { name: 'Select node scope' }).click();
  await page.locator('.summary-strip').click();
  assert.equal(await page.getByRole('listbox', { name: 'Node scope' }).count(), 0);

  let calls = 0;
  await page.route('**/console-api/subscription-clients?*', (route) => {
    calls++;
    if (calls === 1) return route.fulfill({ status: 502, body: 'unavailable' });
    const query = new URL(route.request().url()).searchParams;
    assert.equal(query.get('filter'), 'factory/+/telemetry');
    if (!query.has('cursor'))
      return route.fulfill({
        json: {
          clients: [{ client_id: 'outside-snapshot', node_id: 'eu-west-01', qos: 1 }],
          next: { 'eu-west-02': '' },
          failed_nodes: ['eu-west-02'],
        },
      });
    assert.deepEqual(JSON.parse(query.get('cursor')), { 'eu-west-02': '' });
    return route.fulfill({
      json: {
        clients: [{ client_id: 'outside-snapshot', node_id: 'eu-west-02', qos: 2 }],
        next: {},
        failed_nodes: [],
      },
    });
  });
  await page.route('**/console-api/session?*', (route) => {
    const query = new URL(route.request().url()).searchParams;
    assert.equal(query.get('node'), 'eu-west-02');
    return route.fulfill({
      json: {
        client_id: query.get('id'),
        node_id: query.get('node'),
        protocol: 'mqtt',
        version: 5,
        connected: true,
        clean_start: false,
        expiry_interval: 0,
        subscription_count: 0,
        inflight_count: 0,
        offline_queue_depth: 0,
      },
    });
  });
  await page.goto(
    `${base}/dashboard/subscriptions/details?filter=${encodeURIComponent('factory/+/telemetry')}`,
  );
  await page.getByRole('button', { name: 'Retry', exact: true }).waitFor();
  await page.getByRole('button', { name: 'Retry', exact: true }).click();
  await page.getByText(/Incomplete results/).waitFor();
  await page.getByRole('button', { name: 'Load more / retry', exact: true }).click();
  await page.waitForFunction(
    () => document.querySelectorAll('.subscriber-table tbody tr').length === 2,
  );
  await page.locator('.subscriber-table .client-link').nth(1).click();
  await page.locator('.client-detail-page').getByText('Clean start', { exact: true }).waitFor();
  assert.equal(
    (
      await page
        .locator('.client-detail-page .inspector-fields > div')
        .filter({ has: page.getByText('Node', { exact: true }) })
        .locator('dd')
        .innerText()
    ).trim(),
    'eu-west-02',
  );
  await page.locator('.detail-breadcrumb a').click();
  await page.locator('.subscriber-table').waitFor();
  assert.equal(await page.locator('.subscriber-table tbody tr').count(), 2);
  assert.equal(calls, 3, 'Back restores loaded pages without losing continuation results');
  await page.locator('.detail-breadcrumb a').click();
  await page.goto(`${base}/dashboard/broker-info`);
  await page.getByRole('heading', { name: 'Cluster health', exact: true }).waitFor();

  await page.unroute('**/console-api/subscription-clients?*');
  await page.unroute('**/console-api/session?*');
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${base}/dashboard/subscriptions`);
  await page
    .getByRole('link', { name: 'View clients for factory/+/telemetry', exact: true })
    .click();
  await page.locator('.subscriber-table .client-link').first().waitFor();
  const content = await page.locator('.subscription-detail-page').boundingBox();
  assert.ok(content.width <= 390 && content.x >= 0);
  assert.equal(await page.locator('dialog[open]').count(), 0);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 390);
  await page.getByRole('button', { name: 'Select node scope' }).click();
  await page.getByRole('option', { name: 'eu-west-03', exact: true }).click();
  await page.locator('.subscriber-table .client-link').first().waitFor();
  assert.equal(new URL(page.url()).searchParams.get('node'), 'eu-west-03');
  assert.equal(await page.locator('.subscriber-table tbody tr').count(), 2);
  for (const cell of await page
    .locator('.subscriber-table tbody tr td:nth-child(2)')
    .allTextContents())
    assert.equal(cell.trim(), 'eu-west-03');
  assert.equal((await page.locator('.scope-trigger').innerText()).trim(), 'eu-west-03');
  await page.locator('.subscriber-table .client-link').first().click();
  await page.locator('.client-detail-page').getByText('Clean start', { exact: true }).waitFor();
  const clientURL = page.url();
  await page.reload();
  await page.locator('.client-detail-page').getByText('Clean start', { exact: true }).waitFor();
  assert.equal(page.url(), clientURL);
  await page.locator('.session-subscription a').first().click();
  await page.locator('.subscriber-table .client-link').first().waitFor();
  assert.equal(new URL(page.url()).searchParams.get('node'), 'eu-west-03');
  await page.goBack();
  await page.locator('.client-detail-page').getByText('Clean start', { exact: true }).waitFor();
  assert.equal(page.url(), clientURL);
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 390);
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'PASS: page-size/first/last controls, AMQP client details, keyboard/outside picker dismissal, legacy links, subscription failure/retry/continuation, duplicate IDs on different nodes, direct client fetch beyond snapshot, mobile drilldown.',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

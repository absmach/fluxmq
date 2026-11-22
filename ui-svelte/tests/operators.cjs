const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
const shots = process.env.FLUXMQ_SCREENSHOT_DIR || '/tmp/fluxmq-console-screenshots';
fs.mkdirSync(shots, { recursive: true });
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  page.setDefaultTimeout(15000);
  await page.goto(base + '/dashboard/queues');
  await page.waitForFunction(
    () => document.querySelectorAll('.queue-inventory .client-link').length === 9,
  );
  await page.getByRole('region', { name: 'Queue delivery diagnostics' }).waitFor();
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path: shots + '/queue-diagnostics-dark.png', fullPage: true });
  await page.locator('.queue-inventory .client-link').first().click();
  await page.getByRole('heading', { name: 'Delivery progress', exact: true }).waitFor();
  assert.match(page.url(), /queue=device-events/);
  assert.equal(await page.locator('.group-lane').count(), 2);
  await page.locator('.pending-link').first().click();
  await page.waitForFunction(
    () => document.querySelectorAll('.pending-panel tbody tr').length === 12,
  );
  await page.getByRole('button', { name: 'Latest', exact: true }).click();
  await page.waitForFunction(
    () => document.querySelectorAll('.message-panel tbody tr').length === 20,
  );
  await page.locator('.payload-preview').first().click();
  await page.locator('.payload-detail pre').first().waitFor();
  assert.match(await page.locator('.payload-detail pre').first().innerText(), /temperature/);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path: shots + '/queue-inspector-dark.png', fullPage: true });
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path: shots + '/queue-inspector-light.png', fullPage: true });
  await page.getByRole('button', { name: 'Close pending deliveries', exact: true }).click();
  await page.getByRole('button', { name: 'Close message', exact: true }).click();
  await page.getByRole('button', { name: 'Oldest', exact: true }).click();
  await page.waitForFunction(() =>
    document.querySelector('.message-panel tbody tr')?.textContent.includes('12,000'),
  );
  await page.getByRole('button', { name: 'Next messages', exact: true }).click();
  await page.waitForFunction(() =>
    document.querySelector('.message-panel tbody tr')?.textContent.includes('12,020'),
  );
  await page.reload();
  await page.getByRole('heading', { name: 'Delivery progress', exact: true }).waitFor();
  await page.goBack();
  await page.locator('.queue-inventory .client-link').first().waitFor();
  // Preserve exact uint64 offsets and render untrusted payloads as text.
  const huge = '9007199254740993';
  await page.route('**/console-api/queues?*', (route) => {
    const q = new URL(route.request().url()).searchParams;
    if (q.get('action') === 'info')
      return route.fulfill({
        json: { headOffset: huge, tailOffset: '9007199254741053', messageCount: '60' },
      });
    if (q.get('action') === 'messages') {
      assert.equal(q.get('offset'), huge);
      return route.fulfill({
        json: {
          messages: [
            { offset: huge, value: Buffer.from('<img src=x onerror=alert(1)>').toString('base64') },
          ],
        },
      });
    }
    return route.continue();
  });
  await page.goto(base + '/dashboard/queues?node=eu-west-01&queue=device-events');
  await page.getByRole('heading', { name: 'Delivery progress', exact: true }).waitFor();
  await page.getByRole('button', { name: 'Oldest', exact: true }).click();
  await page.locator('.payload-preview').click();
  assert.match(await page.locator('.payload-detail pre').innerText(), /<img src=x/);
  assert.equal(await page.locator('.payload-detail img').count(), 0);
  assert.match(await page.locator('.message-panel tbody tr').innerText(), /9,007,199,254,740,993/);
  await page.unroute('**/console-api/queues?*');
  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  await page.getByRole('heading', { name: 'Delivery progress', exact: true }).waitFor();
  await page.screenshot({ path: shots + '/queue-mobile.png', fullPage: true });
  assert.ok(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
    'queue view overflows mobile viewport',
  );
  // Search traverses empty first pages; normal browsing retains bounded pages.
  await page.setViewportSize({ width: 1440, height: 1000 });
  let calls = 0;
  const session = (i, node = 'eu-west-01') => ({
    client_id: i === 200 ? 'outside-first-page' : `client-${String(i).padStart(3, '0')}`,
    node_id: node,
    protocol: 'mqtt5',
    connected: true,
    subscription_count: 1,
    inflight_count: 2,
    offline_queue_depth: 3,
  });
  await page.route('**/console-api/inventory?*', (route) => {
    calls++;
    const q = new URL(route.request().url()).searchParams;
    const later = q.has('cursor');
    const sessions = q.get('q')
      ? later
        ? [session(200)]
        : []
      : later
        ? [session(200), session(200, 'eu-west-02')]
        : Array.from({ length: 200 }, (_, i) => session(i));
    return route.fulfill({
      json: {
        sessions,
        subscriptions: [],
        next: later ? {} : { 'eu-west-01': 'more' },
        failed_nodes: [],
      },
    });
  });
  await page.goto(base + '/dashboard/sessions');
  await page.locator('.client-link').first().waitFor();
  await page.getByRole('button', { name: 'Last page', exact: true }).click();
  await page.waitForFunction(() =>
    document.querySelector('.pagination')?.textContent.includes('21 / 21'),
  );
  assert.equal(
    await page.locator('.client-link').count(),
    2,
    'same client on different nodes remains distinct',
  );
  await page.getByLabel('Search sessions').fill('outside-first-page');
  await page.waitForFunction(() => document.querySelectorAll('.client-link').length === 1);
  assert.match(await page.locator('.client-link').innerText(), /outside-first-page/);
  assert.equal(calls, 4, 'normal browse and search request exactly two pages each');
  await page.unroute('**/console-api/inventory?*');
  await page.goto(base + '/dashboard/sessions');
  await page.locator('.protocol-lane').first().waitFor();
  await page.screenshot({ path: shots + '/protocol-delivery-light.png', fullPage: true });
  const snapshot = await (await page.request.get(base + '/console-api/snapshot')).json();
  snapshot.nodes[0].queue_metrics.acknowledgments.poison_pending = 3;
  snapshot.nodes[0].queue_metrics.acknowledgments.poison_pending_no_destination = 2;
  snapshot.nodes[0].queue_metrics.capture_dropped = 9;
  snapshot.nodes.push({
    id: 'reported-peer',
    address: '',
    transport_address: '10.0.0.9:7946',
    version: 'test',
    unmonitored: true,
    reported_healthy: true,
    healthy: false,
    uptime: 80,
    connections: 0,
    subscriptions: 0,
    received: 0,
    sent: 0,
    bytes_in: 0,
    bytes_out: 0,
    errors: 0,
    incoming: 0,
    outgoing: 0,
  });
  const diagnostic = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  diagnostic.on('pageerror', (e) => errors.push(e.message));
  await diagnostic.addInitScript((data) => {
    window.EventSource = class {
      addEventListener(name, cb) {
        if (name === 'snapshot') setTimeout(() => cb({ data: JSON.stringify(data) }), 0);
      }
      close() {}
    };
  }, snapshot);
  await diagnostic.goto(base + '/dashboard/health');
  await diagnostic.locator('.queue-health-summary').waitFor();
  assert.match(
    await diagnostic.locator('.pending-summary').innerText(),
    /3 poison messages pending/,
  );
  assert.match(
    await diagnostic.locator('.queue-health-summary').innerText(),
    /2 have no dead-letter destination/,
  );
  await diagnostic.getByRole('region', { name: 'Protocol diagnostics' }).waitFor();
  await diagnostic.getByText('Protocol internals', { exact: true }).click();
  await diagnostic.getByText('PUBLISH received / sent', { exact: true }).first().waitFor();
  await diagnostic.getByRole('link', { name: /Cluster nodes/ }).click();
  assert.equal(await diagnostic.locator('.nodes-table tbody tr').count(), 4);
  const peer = diagnostic.locator('.nodes-table tbody tr').filter({ hasText: 'reported-peer' });
  assert.match(await peer.innerText(), /Not monitored/);
  assert.ok(await peer.getByRole('button', { name: 'Dashboard for reported-peer' }).isDisabled());
  await diagnostic.getByRole('button', { name: 'Inspect reported-peer', exact: true }).click();
  await diagnostic.getByText('Not configured', { exact: true }).waitFor();
  assert.equal(
    await diagnostic.getByRole('button', { name: 'View dashboard', exact: true }).count(),
    0,
  );
  await diagnostic.getByRole('button', { name: 'Close node details', exact: true }).click();
  await diagnostic.getByRole('button', { name: 'Counters', exact: true }).click();
  assert.match(await peer.innerText(), /—/);
  await diagnostic.screenshot({ path: shots + '/unmonitored-peer.png', fullPage: true });
  await diagnostic.close();
  assert.deepEqual(errors, []);
  await browser.close();
  console.log('Operator queue, protocol, pagination, payload and responsive checks passed');
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

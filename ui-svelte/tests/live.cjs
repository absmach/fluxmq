// Run against a disposable broker populated by tests/integration/console-fixture.
// No mocked responses: every browser request goes through the real console adapter.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const base = process.env.FLUXMQ_TEST_URL;
if (!base) throw new Error('Set FLUXMQ_TEST_URL to the live test console');
const shots = process.env.FLUXMQ_SCREENSHOT_DIR || '/tmp/fluxmq-console-screenshots';
(async () => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    page.setDefaultTimeout(20000);
    const errors = [];
    page.on('pageerror', (e) => errors.push(e.message));
    async function api(path) {
      const response = await page.request.get(base + '/console-api/' + path);
      assert.equal(response.status(), 200, path + ': ' + (await response.text()));
      return response.json();
    }
    const snapshot = await api('snapshot');
    assert.equal(snapshot.mode, 'live');
    assert.equal(snapshot.warning, '');
    const node = snapshot.nodes.find((n) => n.healthy);
    assert.ok(node);
    const q = (action, queue, extra = {}) =>
      'queues?' + new URLSearchParams({ node: node.id, action, queue, ...extra });
    const inventory = await api('inventory?kind=sessions');
    assert.equal(inventory.failed_nodes.length, 0);
    assert.ok(
      inventory.sessions.some((s) => s.client_id === 'console-live-subscriber' && s.connected),
    );
    assert.ok(
      inventory.sessions.some(
        (s) => s.connection_name === 'console-live-amqp' && s.protocol === 'amqp0.9.1',
      ),
    );
    assert.ok(
      inventory.sessions.some(
        (s) =>
          s.client_id === 'console-live-offline' && !s.connected && s.offline_queue_depth === 3,
      ),
    );
    const first = await api('inventory?kind=subscriptions');
    assert.ok(Object.keys(first.next).length > 0, 'real broker pagination exercised');
    const second = await api(
      'inventory?kind=subscriptions&cursor=' + encodeURIComponent(JSON.stringify(first.next)),
    );
    assert.ok(second.subscriptions.length > 0);
    const filter = 'console/live/+/temperature';
    const clients = await api('subscription-clients?filter=' + encodeURIComponent(filter));
    assert.ok(clients.clients.some((c) => c.client_id === 'console-live-subscriber'));
    const queues = await api(q('list', ''));
    assert.equal(queues.queues.find((q) => q.name === 'console-live-stream').type, 'stream');
    assert.equal(queues.queues.find((q) => q.name === 'console-live-classic').type, 'classic');
    const before = await api(q('info', 'console-live-stream'));
    const messages = await api(q('messages', 'console-live-stream', { offset: '0' }));
    assert.equal(messages.messages.length, 20);
    assert.deepEqual([...new Set(messages.messages.map((m) => m.sourceProtocol))].sort(), [
      'amqp',
      'mqtt',
    ]);
    const after = await api(q('info', 'console-live-stream'));
    assert.deepEqual(after, before, 'browsing must not consume retained messages');
    const groups = await api(q('groups', 'console-live-classic'));
    const group = groups.groups.find((g) => Number(g.pendingCount) > 0);
    assert.ok(group);
    const pending = await api(q('pending', 'console-live-classic', { group: group.groupId }));
    assert.ok(pending.entries?.length > 0 || pending.pending?.length > 0, JSON.stringify(pending));
    await page.goto(base + '/dashboard');
    // Confirm the browser receives multiple real SSE samples.
    await page.waitForFunction(() =>
      document.querySelector('.console-statusbar')?.textContent.includes('Healthy'),
    );
    const times = await page.evaluate(
      () =>
        new Promise((resolve, reject) => {
          const stream = new EventSource('/console-api/stream');
          const times = [];
          const timer = setTimeout(() => {
            stream.close();
            reject(new Error('live SSE timeout'));
          }, 10000);
          stream.addEventListener('snapshot', (e) => {
            times.push(JSON.parse(e.data).timestamp);
            if (times.length === 2) {
              clearTimeout(timer);
              stream.close();
              resolve(times);
            }
          });
        }),
    );
    assert.ok(times[1] > times[0]);
    const updated = await api('snapshot');
    assert.ok(updated.points.all.incoming > 0, 'actual MQTT/AMQP traffic produces rates');
    await page.goto(base + '/dashboard/sessions');
    await page.getByLabel('Search sessions').fill('console-live-subscriber');
    await page.locator('.client-link').filter({ hasText: 'console-live-subscriber' }).click();
    await page.getByRole('heading', { name: 'console-live-subscriber', exact: true }).waitFor();
    assert.match(await page.locator('main').innerText(), /console\/live\/\+\/temperature/);
    await page.goto(base + '/dashboard/subscriptions');
    await page.getByLabel('Search topic filters', { exact: true }).fill('console/live/page/204');
    await page.getByText('console/live/page/204', { exact: true }).click();
    await page.getByText('console-live-subscriber', { exact: true }).waitFor();
    await page.goto(base + '/dashboard/queues');
    await page
      .locator('.queue-inventory .client-link')
      .filter({ hasText: 'console-live-stream' })
      .click();
    await page.getByRole('button', { name: 'Oldest', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('.payload-preview').length === 20);
    await page.getByLabel('Message source protocol', { exact: true }).selectOption('mqtt');
    assert.equal(await page.locator('.payload-preview').count(), 10);
    await page.locator('.payload-preview').first().click();
    assert.match(await page.locator('.payload-detail').innerText(), /mqtt/);
    await page.getByRole('button', { name: 'Close message', exact: true }).click();
    await page.getByLabel('Message source protocol', { exact: true }).selectOption('all');
    await page.getByRole('button', { name: 'Next messages', exact: true }).click();
    await page.waitForFunction(() => document.querySelectorAll('.payload-preview').length === 4);
    fs.mkdirSync(shots, { recursive: true });
    await page.screenshot({ path: shots + '/live-queue.png', fullPage: true });
    for (const route of ['nodes', 'health']) {
      await page.goto(base + '/dashboard/' + route);
      await page.locator('main h1').waitFor();
      assert.ok(!(await page.locator('main').innerText()).includes('Simulated'));
    }
    assert.deepEqual(errors, []);
    console.log(
      'PASS: live inventory, pagination, subscription drill-down, MQTT/AMQP metrics, SSE, queue types, pending deliveries, mixed-source messages and read-only browsing',
    );
  } finally {
    await browser.close();
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

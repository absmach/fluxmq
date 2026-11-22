const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
const shots = process.env.FLUXMQ_SCREENSHOT_DIR || '/tmp/fluxmq-console-screenshots';
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto(base + '/dashboard/queues');
  await page.waitForFunction(
    () => document.querySelectorAll('.queue-inventory .client-link').length === 9,
  );
  for (const [type, total] of [
    ['classic', 6],
    ['stream', 3],
    ['', 0],
    ['all', 9],
  ]) {
    await page.getByLabel('Queue type', { exact: true }).selectOption(type);
    assert.equal(await page.locator('.queue-inventory .client-link').count(), total);
  }
  await page.getByLabel('Queue type', { exact: true }).selectOption('stream');
  await page.getByLabel('Find queues', { exact: true }).fill('eu-west-02');
  assert.equal(await page.locator('.queue-inventory .client-link').count(), 1);
  assert.match(await page.locator('.queue-inventory tbody').innerText(), /telemetry-archive/);
  await page.getByLabel('Find queues', { exact: true }).fill('');
  await page.getByLabel('Queue type', { exact: true }).selectOption('all');
  fs.mkdirSync(shots, { recursive: true });
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page.waitForTimeout(300);
  await page
    .locator('.queue-inventory')
    .screenshot({ path: shots + '/queue-types-light.png', animations: 'disabled' });
  const requests = [];
  await page.route('**/console-api/queues?*', async (route) => {
    const params = new URL(route.request().url()).searchParams;
    if (params.get('action') !== 'messages') return route.continue();
    const start = BigInt(params.get('offset'));
    requests.push(start);
    const origins = ['mqtt', 'amqp', 'amqp1', 'http', 'coap', '', 'future-protocol'];
    return route.fulfill({
      json: {
        messages: Array.from({ length: 20 }, (_, i) => ({
          offset: String(start + BigInt(i)),
          sourceProtocol: start >= 12040n ? 'amqp' : origins[i % origins.length],
          value: Buffer.from('message ' + i).toString('base64'),
          headers: { sourceProtocol: Buffer.from('spoofed').toString('base64') },
        })),
      },
    });
  });
  await page.locator('.queue-inventory .client-link').first().click();
  await page.getByRole('button', { name: 'Oldest', exact: true }).click();
  const filter = page.getByLabel('Message source protocol', { exact: true });
  await filter.waitFor();
  for (const [protocol, total] of [
    ['mqtt', 3],
    ['amqp', 3],
    ['amqp1', 3],
    ['http', 3],
    ['coap', 3],
    ['', 3],
    ['future-protocol', 2],
    ['all', 20],
  ]) {
    await filter.selectOption(protocol);
    assert.equal(await page.locator('.message-scroll .payload-preview').count(), total);
    assert.ok(!(await page.locator('.message-scroll').innerText()).includes('spoofed'));
  }
  await filter.selectOption('mqtt');
  await page.locator('.payload-preview').first().click();
  assert.match(await page.locator('.payload-detail').innerText(), /Source protocol MQTT/);
  await page.getByRole('button', { name: 'Next messages', exact: true }).click();
  await page.waitForFunction(() =>
    document.querySelector('.message-scroll tbody tr')?.textContent.includes('12,020'),
  );
  assert.deepEqual(
    requests,
    [12000n, 12020n],
    'pagination uses full batch, not filtered final offset',
  );
  assert.equal(await filter.inputValue(), 'mqtt');
  await page
    .locator('.message-panel')
    .screenshot({ path: shots + '/message-protocol-light.png', animations: 'disabled' });
  await page.getByRole('button', { name: 'Next messages', exact: true }).click();
  await page.getByText('No messages from this protocol in the loaded batch.').waitFor();
  assert.equal(await filter.inputValue(), 'mqtt');
  await page.getByRole('button', { name: 'Next messages', exact: true }).click();
  await page.waitForFunction(
    () => document.querySelector('.message-panel input')?.value === '12060',
  );
  assert.deepEqual(requests, [12000n, 12020n, 12040n, 12060n]);
  await filter.selectOption('all');
  assert.equal(await page.locator('.message-scroll .payload-preview').count(), 20);
  await page.getByRole('button', { name: 'Dark appearance', exact: true }).click();
  await page.waitForTimeout(300);
  await page
    .locator('.message-panel')
    .screenshot({ path: shots + '/message-protocol-dark.png', animations: 'disabled' });
  // Older broker payloads: no queue type or recorded message protocol.
  await page.unroute('**/console-api/queues?*');
  await page.route('**/console-api/queues?*', async (route) => {
    const response = await route.fetch();
    const data = await response.json();
    delete data.type;
    for (const q of data.queues ?? []) delete q.type;
    for (const m of data.messages ?? []) delete m.sourceProtocol;
    await route.fulfill({ json: data });
  });
  await page.reload();
  await page.getByRole('button', { name: 'Latest', exact: true }).click();
  await filter.waitFor();
  await filter.selectOption('');
  assert.equal(await page.locator('.message-scroll .payload-preview').count(), 20);
  assert.match(
    await page.locator('.queue-config').first().innerText(),
    /Queue type\s+Not reported/,
  );
  await page.getByRole('link', { name: 'All queues' }).click();
  await page.getByLabel('Queue type', { exact: true }).selectOption('');
  await page.waitForFunction(
    () => document.querySelectorAll('.queue-inventory .client-link').length === 9,
  );
  assert.match(await page.locator('.queue-inventory tbody').innerText(), /Not reported/);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForTimeout(400);
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
  await page.screenshot({
    path: shots + '/queue-types-mobile.png',
    fullPage: true,
    animations: 'disabled',
  });
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'Queue type and source-protocol filters, mixed batches, pagination and older brokers passed',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

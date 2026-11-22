const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const source = await (await page.request.get(`${base}/console-api/snapshot`)).json();
  let count = 500;
  await page.route('**/console-api/stream', (route) => {
    const nodes = Array.from({ length: count }, (_, i) => ({
      ...source.nodes[0],
      id: `broker-${String(i).padStart(4, '0')}`,
      leader: i === 0,
    }));
    return route.fulfill({
      contentType: 'text/event-stream',
      body: `event: snapshot\ndata: ${JSON.stringify({ ...source, timestamp: Date.now(), nodes })}\n\n`,
    });
  });
  await page.goto(`${base}/dashboard/nodes`);
  await page.getByText('500 / 500 responding', { exact: true }).waitFor();
  await page.waitForFunction(
    () => parseInt(document.querySelector('output[aria-label="Zoom level"]').textContent) < 100,
  );
  assert.equal(await page.locator('.node-map canvas').count(), 1);
  await page.getByLabel('Find node', { exact: true }).fill('broker-0499');
  await page.getByRole('button', { name: 'broker-0499', exact: true }).click();
  await page.locator('.selection').getByText('broker-0499', { exact: true }).waitFor();
  const canvas = page.locator('.node-map canvas');
  const rect = await canvas.boundingBox();
  // Search centers the chosen node: clear selection, then hit-test its center.
  await canvas.press('Escape');
  assert.equal(await page.locator('.selection').count(), 0);
  await canvas.click({ position: { x: rect.width / 2, y: rect.height / 2 } });
  await page.locator('.selection').getByText('broker-0499', { exact: true }).waitFor();
  await canvas.press('Escape');
  await page.mouse.move(rect.x + rect.width / 2, rect.y + rect.height / 2);
  await page.mouse.down();
  await page.mouse.move(rect.x + rect.width / 2 + 100, rect.y + rect.height / 2 + 60, { steps: 5 });
  await page.mouse.up();
  assert.equal(await page.locator('.selection').count(), 0, 'drag does not select');
  await canvas.click({ position: { x: rect.width / 2 + 100, y: rect.height / 2 + 60 } });
  await page.locator('.selection').getByText('broker-0499', { exact: true }).waitFor();
  await canvas.press('Home');
  assert.ok(parseInt(await page.getByLabel('Zoom level').textContent()) < 100);
  await page.getByRole('button', { name: 'Inspect node', exact: false }).click();
  assert.equal(await page.getByLabel('Cluster scope').inputValue(), 'broker-0499');
  for (const size of [0, 1]) {
    count = size;
    await page.goto(`${base}/dashboard/nodes`);
    await page.getByText(`${size} / ${size} responding`, { exact: true }).waitFor();
    if (!size) await page.getByText('No configured nodes', { exact: true }).waitFor();
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole('button', { name: 'Fit view', exact: true }).click();
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth), 390);
  await page.getByLabel('Find node', { exact: true }).fill('broker-0000');
  await page.getByRole('button', { name: 'broker-0000', exact: true }).click();
  await page.locator('.selection').getByText('broker-0000', { exact: true }).waitFor();
  assert.deepEqual(errors, []);
  console.log(
    'PASS: 500-node layout, search, selection, pan hit-testing, keyboard fit, health drilldown, empty/single node, mobile overflow.',
  );
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

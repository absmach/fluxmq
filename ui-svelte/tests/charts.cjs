const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
const shots = process.env.FLUXMQ_SCREENSHOT_DIR || '/tmp/fluxmq-console-screenshots';
fs.mkdirSync(shots, { recursive: true });
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto(base + '/dashboard/health');
  const queue = page.getByRole('region', { name: 'Queue delivery diagnostics' }),
    protocol = page.getByRole('region', { name: 'Protocol diagnostics' });
  await queue.getByRole('img', { name: 'Queue pending history', exact: true }).waitFor();
  await page.waitForFunction(
    () => document.querySelector('.queue-diagnostics .plot-main path') !== null,
  );
  assert.ok((await queue.locator('.plot-main path').count()) >= 2);
  assert.equal(
    await protocol.locator('.table-scroll').isVisible(),
    false,
    'tables start collapsed',
  );
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await queue.screenshot({ path: shots + '/queue-chart-light.png' });
  await protocol.screenshot({ path: shots + '/protocol-chart-light.png' });
  await queue.getByRole('button', { name: 'Acknowledgements', exact: true }).click();
  await queue.getByRole('img', { name: 'Queue acknowledgements history', exact: true }).waitFor();
  assert.doesNotMatch(await queue.locator('.series-readout').innerText(), /—/);
  await queue.getByRole('button', { name: '1m', exact: true }).click();
  await queue.getByText('Last 1 minute', { exact: true }).waitFor();
  const slider = queue.getByRole('slider');
  await slider.focus();
  await slider.press('Home');
  assert.equal(await slider.getAttribute('aria-valuenow'), '0');
  assert.equal(await queue.locator('.plot-tooltip').count(), 1);
  await slider.press('ArrowRight');
  assert.equal(await slider.getAttribute('aria-valuenow'), '1');
  await slider.press('Escape');
  await protocol.getByRole('button', { name: 'AMQP 0.9.1', exact: true }).click();
  await protocol.getByRole('img', { name: 'AMQP 0.9.1 messages history', exact: true }).waitFor();
  assert.doesNotMatch(await protocol.locator('.series-readout').innerText(), /—/);
  await protocol.getByRole('button', { name: 'Bandwidth', exact: true }).click();
  await protocol.getByRole('img', { name: 'AMQP 0.9.1 bandwidth history', exact: true }).waitFor();
  await page.getByRole('button', { name: 'Dark appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await queue.screenshot({ path: shots + '/queue-chart-dark.png' });
  await protocol.screenshot({ path: shots + '/protocol-chart-dark.png' });
  await queue.getByText('Counters & node comparison', { exact: true }).click();
  await queue.getByRole('columnheader', { name: 'Node', exact: true }).waitFor();
  await protocol.getByText('Compare node counters', { exact: true }).click();
  await protocol.getByRole('columnheader', { name: 'Node / protocol', exact: true }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(base + '/dashboard/health');
  await queue.getByRole('img', { name: 'Queue pending history', exact: true }).waitFor();
  assert.ok(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
    'mobile overflow',
  );
  await queue.screenshot({ path: shots + '/queue-chart-mobile.png' });
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'PASS: queue/protocol history charts, rate data, tabs, ranges, keyboard inspection, disclosure and responsive layouts',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

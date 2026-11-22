const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const base = process.env.FLUXMQ_TEST_URL || 'http://127.0.0.1:3002';
const shots = process.env.FLUXMQ_SCREENSHOT_DIR || '/tmp/fluxmq-console-screenshots';
fs.mkdirSync(shots, { recursive: true });
(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
    deviceScaleFactor: 1,
    timezoneId: 'Europe/Paris',
  });
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto(`${base}/dashboard`);
  await page.getByText('All systems operational', { exact: true }).waitFor();
  assert.doesNotMatch(await page.locator('body').innerText(), /workspace/i);
  await page.getByRole('button', { name: 'Broker connection details' }).click();
  await page.getByRole('heading', { name: 'Demo brokers. Real console.' }).waitFor();
  await page.getByRole('button', { name: 'Close broker connection details' }).click();
  const metric = page.locator('.metric-value').nth(1);
  const original = await metric.textContent();
  await page.waitForTimeout(4200);
  assert.notEqual(await metric.textContent(), original, 'streamed metric should change');
  await page.getByRole('button', { name: 'Pause updates' }).click();
  const frozen = await metric.textContent();
  await page.waitForTimeout(3000);
  assert.equal(await metric.textContent(), frozen, 'paused metrics should remain fixed');
  await page.getByRole('button', { name: 'Resume updates' }).click();
  await page.getByRole('button', { name: '1m', exact: true }).click();
  await page.getByText('Last 1 minute', { exact: true }).waitFor();
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await page.screenshot({ path: path.join(shots, 'overview-light.png'), fullPage: true });
  await page.getByRole('link', { name: 'Sessions', exact: true }).click();
  await page.getByLabel('Search sessions').fill('edge-gateway');
  assert.equal(await page.locator('tbody tr').count(), 6);
  await page.locator('.client-link').first().click();
  await page.getByText('SESSION INSPECTOR').waitFor();
  await page.locator('.drawer-subscription').first().waitFor();
  await page.screenshot({ path: path.join(shots, 'session-detail.png'), fullPage: true });
  await page.getByRole('button', { name: 'Close session details' }).click();
  await page.getByLabel('Search sessions').fill('nothing-matches-xyz');
  await page.getByRole('heading', { name: 'No matching sessions' }).waitFor();
  await page.getByRole('button', { name: 'Clear filters' }).click();
  await page.getByRole('button', { name: 'Next', exact: true }).click();
  assert.match(await page.locator('.pagination').textContent(), /2 \/ 4/);
  await page.getByRole('link', { name: 'Subscriptions', exact: true }).click();
  await page.getByLabel('Search topic filters').fill('factory');
  assert.equal(await page.locator('tbody tr').count(), 1);
  await page.getByRole('link', { name: 'Cluster nodes', exact: false }).click();
  await page.getByRole('heading', { name: 'One cluster.' }).waitFor();
  await page.screenshot({ path: path.join(shots, 'nodes-light.png'), fullPage: true });
  await page.getByRole('link', { name: 'Broker health', exact: true }).click();
  assert.equal(await page.locator('.health-card').count(), 3);
  await page.getByLabel('Cluster scope').selectOption('eu-west-01');
  assert.equal(await page.locator('.health-card').count(), 1);
  await page.keyboard.press('Control+k');
  await page.getByLabel('Search pages').fill('overview');
  await page.locator('.command-results button').click();
  await page.getByLabel('Cluster scope').selectOption('all');
  const download = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Export', exact: true }).click();
  assert.match((await download).suggestedFilename(), /fluxmq-demo-overview/);
  await page.getByRole('button', { name: 'Dark appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await page.screenshot({ path: path.join(shots, 'overview-dark.png'), fullPage: true });
  await page.reload();
  await page.getByText('All systems operational', { exact: true }).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-theme'), 'dark');
  const mobile = await browser.newPage({
    viewport: { width: 390, height: 844 },
    isMobile: true,
    deviceScaleFactor: 1,
    timezoneId: 'Europe/Paris',
  });
  mobile.on('pageerror', (e) => errors.push(e.message));
  await mobile.goto(`${base}/dashboard`);
  await mobile.getByText('DEMO DATA', { exact: true }).waitFor();
  assert.equal(
    await mobile.evaluate(() => document.documentElement.scrollWidth),
    390,
    'no horizontal page overflow',
  );
  await mobile.screenshot({ path: path.join(shots, 'mobile-dark.png'), fullPage: true });
  await mobile.getByRole('button', { name: 'Open navigation' }).click();
  await mobile.getByRole('link', { name: 'Sessions', exact: true }).click();
  assert.equal(
    await mobile.locator('.sidebar').evaluate((el) => el.classList.contains('open')),
    false,
  );
  await mobile.locator('.client-link').first().click();
  await mobile.locator('.drawer-subscription').first().waitFor();
  await mobile.screenshot({ path: path.join(shots, 'mobile-detail.png'), fullPage: true });
  assert.deepEqual(errors, []);
  console.log(
    'PASS: SSE refresh, pause/resume, ranges, theme persistence, search/filter, session detail, pagination, subscriptions, cluster, health, scope, command palette, CSV export, mobile navigation and overflow. No browser errors.',
  );
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

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
  const chooseScope = async (id) => {
    await page.getByRole('button', { name: 'Select node scope', exact: true }).click();
    await page.getByRole('option', { name: id === 'all' ? 'All nodes' : id, exact: true }).click();
  };
  page.setDefaultTimeout(15000);
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto(`${base}/dashboard`);
  await page.locator('.console-statusbar').getByText('Healthy', { exact: true }).waitFor();
  assert.doesNotMatch(await page.locator('body').innerText(), /workspace/i);
  await page.getByRole('button', { name: 'Broker connection details' }).click();
  await page.getByRole('heading', { name: 'Demo environment' }).waitFor();
  await page.getByRole('button', { name: 'Close broker connection details' }).click();
  assert.equal((await page.locator('.brand').innerText()).replace(/\s/g, ''), 'FluxMQ');
  assert.equal(await page.locator('.broker-connection').count(), 0);
  const metric = page.locator('.chart-legend strong').first();
  const original = await metric.textContent();
  await page.waitForFunction(
    (previous) => document.querySelector('.chart-legend strong')?.textContent !== previous,
    original,
    { timeout: 10000 },
  );
  await page.getByRole('button', { name: 'Pause updates' }).click();
  const frozen = await metric.textContent();
  await page.waitForTimeout(3000);
  assert.equal(await metric.textContent(), frozen, 'paused metrics should remain fixed');
  await page.getByRole('button', { name: 'Resume updates' }).click();
  await page.getByRole('button', { name: '1m', exact: true }).click();
  await page.getByText('Last 1 minute', { exact: true }).waitFor();
  await page.getByRole('button', { name: 'Bandwidth', exact: true }).click();
  await page.getByRole('img', { name: 'bandwidth history', exact: true }).waitFor();
  await page.getByRole('button', { name: 'Connections', exact: true }).click();
  await page.getByRole('img', { name: 'connections history', exact: true }).waitFor();
  await page.getByRole('button', { name: 'Messages', exact: true }).click();
  await page.getByRole('button', { name: 'Select node scope', exact: true }).click();
  await page.getByLabel('Find a node').fill('eu-west-02');
  await page.screenshot({ path: path.join(shots, 'node-picker-dark.png'), fullPage: true });
  await page.getByLabel('Find a node').press('ArrowDown');
  await page.getByRole('option', { name: 'All nodes', exact: true }).press('ArrowDown');
  await page.getByRole('option', { name: 'eu-west-02', exact: true }).press('Enter');
  assert.equal((await page.locator('.scope-trigger').innerText()).trim(), 'eu-west-02');
  await chooseScope('all');
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await page.screenshot({ path: path.join(shots, 'overview-light.png'), fullPage: true });
  await page.getByRole('link', { name: 'Sessions', exact: true }).click();
  await page.screenshot({ path: path.join(shots, 'sessions-light.png'), fullPage: true });
  await page.evaluate(() => window.scrollTo(0, 600));
  assert.equal(
    Math.round((await page.locator('.console-statusbar').boundingBox()).y),
    0,
    'cluster status stays visible while scrolling',
  );
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.getByLabel('Search sessions').fill('edge-gateway');
  assert.equal(await page.locator('tbody tr').count(), 6);
  await page.locator('.client-link').first().click();
  await page.locator('.client-detail-page').waitFor();
  assert.equal(new URL(page.url()).pathname, '/dashboard/sessions/details');
  await page.locator('.session-subscription').first().waitFor();
  await page.locator('.client-detail-page').getByText('Clean start', { exact: true }).waitFor();
  await page.locator('.client-detail-page').getByText('Will configured', { exact: true }).waitFor();
  assert.equal(await page.locator('dialog[open]').count(), 0, 'client details are a page');
  await page.screenshot({ path: path.join(shots, 'session-detail.png'), fullPage: true });
  await page.locator('.detail-breadcrumb a').click();
  await page.getByLabel('Search sessions').waitFor();
  assert.equal(await page.getByLabel('Search sessions').inputValue(), 'edge-gateway');
  await page.getByLabel('Search sessions').fill('nothing-matches-xyz');
  await page.getByRole('heading', { name: 'No matching sessions' }).waitFor();
  await page.getByRole('button', { name: 'Clear filters' }).click();
  await page.getByRole('button', { name: 'Next', exact: true }).click();
  assert.match(await page.locator('.pagination').textContent(), /2 \/ 4/);
  await page.getByRole('link', { name: 'Subscriptions', exact: true }).click();
  await page.getByLabel('Search topic filters').fill('factory');
  assert.equal(await page.locator('tbody tr').count(), 1);
  await page
    .getByRole('link', { name: 'View clients for factory/+/telemetry', exact: true })
    .click();
  await page.locator('.subscriber-table .client-link').first().waitFor();
  assert.equal(await page.locator('.subscriber-table tbody tr').count(), 6);
  await page.screenshot({
    path: path.join(shots, 'subscription-clients-light.png'),
    fullPage: true,
  });
  await page.locator('.subscriber-table .client-link').first().click();
  await page.locator('.client-detail-page').getByText('Clean start', { exact: true }).waitFor();
  await page.locator('.detail-breadcrumb a').click();
  await page.locator('.subscriber-table .client-link').first().waitFor();
  await page.locator('.detail-breadcrumb a').click();
  await page.getByLabel('Search topic filters').waitFor();
  assert.equal(await page.getByLabel('Search topic filters').inputValue(), 'factory');
  await chooseScope('eu-west-01');
  await page.getByRole('link', { name: 'Cluster nodes', exact: false }).click();
  await page.getByRole('heading', { name: 'Node inventory' }).waitFor();
  assert.equal(await page.locator('.nodes-table tbody tr').count(), 3);
  await page.screenshot({ path: path.join(shots, 'nodes-light.png'), fullPage: true });
  await page.getByRole('button', { name: 'Inspect eu-west-02', exact: true }).click();
  await page.getByRole('heading', { name: 'eu-west-02', exact: true }).waitFor();
  await page.locator('.node-detail-dialog').getByText('Follower', { exact: true }).waitFor();
  assert.equal(
    new URL(page.url()).pathname,
    '/dashboard/nodes',
    'inspection must not navigate away',
  );
  await page.screenshot({ path: path.join(shots, 'node-details-light.png'), fullPage: true });
  await page.getByRole('button', { name: 'Close node details', exact: true }).click();
  assert.equal(
    await page.locator('.nodes-table tbody tr').count(),
    3,
    'inspection must not filter inventory',
  );
  await page.getByRole('button', { name: 'Inspect eu-west-02', exact: true }).click();
  await page.getByRole('button', { name: 'View clients', exact: true }).click();
  assert.equal((await page.locator('.scope-trigger').innerText()).trim(), 'eu-west-02');
  await page.getByRole('link', { name: 'Broker health', exact: true }).click();
  assert.equal(await page.locator('.health-card').count(), 3, 'navigation starts at cluster scope');
  await page.getByRole('heading', { name: 'Cluster health', exact: true }).waitFor();
  await chooseScope('eu-west-02');
  await page.getByRole('heading', { name: 'Node health', exact: true }).waitFor();
  await page.getByText('Node uptime', { exact: true }).waitFor();
  assert.equal(await page.getByText('Available cluster nodes', { exact: true }).count(), 0);
  assert.equal(await page.locator('.health-card').count(), 1);
  await chooseScope('all');
  assert.equal(await page.locator('.health-card').count(), 3);
  await page.screenshot({ path: path.join(shots, 'health-light.png'), fullPage: true });
  await chooseScope('eu-west-01');
  assert.equal(
    (await page.getByRole('button', { name: 'View all nodes', exact: true }).innerText()).trim(),
    '3 nodes',
  );
  assert.equal(await page.locator('.health-card').count(), 1);
  await page.getByRole('link', { name: 'Overview', exact: true }).click();
  await page.getByRole('heading', { name: 'Cluster overview', exact: true }).waitFor();
  await page.getByRole('button', { name: 'View dashboard for eu-west-02', exact: true }).click();
  await page.getByRole('heading', { name: 'Node overview', exact: true }).waitFor();
  assert.equal(new URL(page.url()).searchParams.get('node'), 'eu-west-02');
  await page.getByRole('heading', { name: 'Node traffic', exact: true }).waitFor();
  await page.reload();
  await page.getByRole('heading', { name: 'Node overview', exact: true }).waitFor();
  await page.locator('.console-statusbar').getByText('Healthy', { exact: true }).waitFor();
  assert.equal((await page.locator('.scope-trigger').innerText()).trim(), 'eu-west-02');
  await page.screenshot({ path: path.join(shots, 'node-overview-light.png'), fullPage: true });
  await page.getByRole('link', { name: 'Sessions', exact: true }).click();
  assert.equal((await page.locator('.scope-trigger').innerText()).trim(), 'All nodes');
  await page.getByRole('button', { name: 'Filter by eu-west-02', exact: true }).click();
  assert.equal((await page.locator('.scope-trigger').innerText()).trim(), 'eu-west-02');
  for (const row of await page.locator('tbody tr').allTextContents())
    assert.ok(row.includes('eu-west-02'));
  await page.getByRole('link', { name: 'Overview', exact: true }).click();
  await chooseScope('all');
  const download = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Export', exact: true }).click();
  assert.match((await download).suggestedFilename(), /fluxmq-demo-overview/);
  await page.getByRole('button', { name: 'Dark appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await page.screenshot({ path: path.join(shots, 'overview-dark.png'), fullPage: true });
  await page.reload();
  await page.locator('.console-statusbar').getByText('Healthy', { exact: true }).waitFor();
  assert.equal(await page.locator('html').getAttribute('data-theme'), 'dark');
  const mobile = await browser.newPage({
    viewport: { width: 390, height: 844 },
    isMobile: true,
    deviceScaleFactor: 1,
    timezoneId: 'Europe/Paris',
  });
  mobile.on('pageerror', (e) => errors.push(e.message));
  await mobile.goto(`${base}/dashboard`);
  await mobile.getByRole('button', { name: 'Broker connection details', exact: true }).waitFor();
  assert.equal(
    await mobile.evaluate(() => document.documentElement.scrollWidth),
    390,
    'no horizontal page overflow',
  );
  await mobile.screenshot({ path: path.join(shots, 'mobile-dark.png'), fullPage: true });
  await mobile.getByRole('button', { name: 'Open navigation' }).click();
  await mobile.getByRole('link', { name: 'Documentation', exact: true }).waitFor();
  await mobile.getByRole('button', { name: 'Light appearance', exact: true }).click();
  assert.equal(await mobile.locator('html').getAttribute('data-theme'), 'light');
  await mobile.getByRole('button', { name: 'Dark appearance', exact: true }).click();
  await mobile.getByRole('link', { name: 'Sessions', exact: true }).click();
  assert.equal(
    await mobile.locator('.sidebar').evaluate((el) => el.classList.contains('open')),
    false,
  );
  await mobile.locator('.client-link').first().click();
  await mobile.locator('.session-subscription').first().waitFor();
  await mobile.screenshot({ path: path.join(shots, 'mobile-detail.png'), fullPage: true });
  assert.deepEqual(errors, []);
  console.log(
    'PASS: SSE refresh, pause/resume, ranges, theme persistence, search/filter, session detail, pagination, subscriptions, cluster, health, scope, CSV export, mobile navigation and overflow. No browser errors.',
  );
  await browser.close();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

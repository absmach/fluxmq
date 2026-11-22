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
  await page.goto(base + '/dashboard/queues?node=eu-west-02');
  const nav = page.getByRole('navigation', { name: 'Main navigation' });
  assert.deepEqual(
    await nav
      .getByRole('group', { name: 'Messaging', exact: true })
      .getByRole('link')
      .evaluateAll((els) => els.map((el) => el.getAttribute('aria-label'))),
    ['Sessions', 'Subscriptions', 'Queues'],
  );
  assert.deepEqual(
    await nav
      .getByRole('group', { name: 'Cluster', exact: true })
      .getByRole('link')
      .evaluateAll((els) => els.map((el) => el.getAttribute('aria-label'))),
    ['Cluster nodes', 'Broker health'],
  );
  const expanded = await page.locator('.console').boundingBox();
  fs.mkdirSync(shots, { recursive: true });
  await page.screenshot({ path: shots + '/sidebar-expanded-dark.png', animations: 'disabled' });
  const toggle = page.getByRole('button', { name: 'Collapse sidebar', exact: true });
  await toggle.focus();
  await page.keyboard.press('Enter');
  await page.getByRole('button', { name: 'Expand sidebar', exact: true }).waitFor();
  assert.equal(
    await page.locator('.sidebar').evaluate((el) => el.getBoundingClientRect().width),
    72,
  );
  assert.ok((await page.locator('.console').boundingBox()).width > expanded.width);
  assert.equal(new URL(page.url()).searchParams.get('node'), 'eu-west-02');
  assert.equal(
    await nav.getByRole('link', { name: 'Queues', exact: true }).getAttribute('aria-current'),
    'page',
  );
  for (const link of await nav.getByRole('link').all()) {
    assert.ok(await link.isVisible());
    assert.equal(await link.getAttribute('title'), await link.getAttribute('aria-label'));
  }
  await page.reload();
  await page.getByRole('button', { name: 'Expand sidebar', exact: true }).waitFor();
  assert.equal(
    await page.locator('.sidebar').evaluate((el) => el.getBoundingClientRect().width),
    72,
  );
  await nav.getByRole('link', { name: 'Sessions', exact: true }).click();
  assert.match(page.url(), /dashboard\/sessions/);
  assert.equal(
    await nav.getByRole('link', { name: 'Sessions', exact: true }).getAttribute('aria-current'),
    'page',
  );
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page.waitForTimeout(300);
  await page.screenshot({ path: shots + '/sidebar-collapsed-light.png', animations: 'disabled' });
  for (const width of [1024, 801]) {
    await page.setViewportSize({ width, height: 900 });
    assert.equal(
      await page.locator('.sidebar').evaluate((el) => el.getBoundingClientRect().width),
      72,
    );
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1));
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole('button', { name: 'Open navigation', exact: true }).click();
  await page.waitForTimeout(250);
  assert.equal(
    await page.locator('.sidebar').evaluate((el) => el.getBoundingClientRect().width),
    232,
  );
  assert.equal(
    await nav.getByRole('link', { name: 'Sessions', exact: true }).getAttribute('title'),
    null,
  );
  await page.screenshot({ path: shots + '/sidebar-mobile.png', animations: 'disabled' });
  await nav.getByRole('link', { name: 'Queues', exact: true }).click();
  assert.equal(
    await page.locator('.sidebar').evaluate((el) => el.classList.contains('open')),
    false,
  );
  await page.getByRole('button', { name: 'Open navigation', exact: true }).click();
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByRole('button', { name: 'Expand sidebar', exact: true }).waitFor();
  assert.equal(await page.locator('.mobile-backdrop').count(), 0);
  await page.getByRole('button', { name: 'Expand sidebar', exact: true }).click();
  await page.reload();
  await page.getByRole('button', { name: 'Collapse sidebar', exact: true }).waitFor();
  assert.equal(
    await page.locator('.sidebar').evaluate((el) => el.getBoundingClientRect().width),
    232,
  );
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'Sidebar grouping, collapse, preference persistence, navigation, keyboard and responsive checks passed',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

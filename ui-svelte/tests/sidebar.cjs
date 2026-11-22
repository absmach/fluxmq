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
  assert.equal(await page.locator('.console-statusbar .sidebar-toggle').count(), 0);
  const toggleBox = await toggle.boundingBox();
  assert.ok(
    Math.abs(toggleBox.x + toggleBox.width / 2 - expanded.x) < 1,
    'toggle straddles sidebar edge',
  );
  assert.equal(
    await toggle.locator('svg').evaluate((el) => getComputedStyle(el).transform),
    'matrix(-1, 0, 0, -1, 0, 0)',
  );
  await toggle.focus();
  await page.keyboard.press('Enter');
  await page.getByRole('button', { name: 'Expand sidebar', exact: true }).waitFor();
  assert.equal(
    await page.locator('.sidebar').evaluate((el) => el.getBoundingClientRect().width),
    72,
  );
  assert.ok((await page.locator('.console').boundingBox()).width > expanded.width);
  const expand = page.getByRole('button', { name: 'Expand sidebar', exact: true });
  assert.ok(
    await expand.evaluate((el) => el === document.activeElement),
    'toggle retains keyboard focus',
  );
  const collapsedToggle = await expand.boundingBox();
  assert.equal(collapsedToggle.y, toggleBox.y, 'toggle keeps its vertical position');
  assert.ok(Math.abs(collapsedToggle.x + collapsedToggle.width / 2 - 72) < 1);
  await expand.evaluate((el) => el.blur());
  await page.mouse.move(500, 100);
  assert.equal(await expand.locator('svg').evaluate((el) => getComputedStyle(el).opacity), '1');
  assert.equal(
    await expand.locator('svg').evaluate((el) => getComputedStyle(el).transform),
    'none',
  );
  assert.ok(
    await page.getByRole('link', { name: 'FluxMQ overview', exact: true }).isVisible(),
    'logo remains a navigation link',
  );
  assert.equal(await expand.locator('img').count(), 0, 'toggle is separate from branding');
  await expand.click();
  await page.getByRole('button', { name: 'Collapse sidebar', exact: true }).click();

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
  for (const width of [1024, 801]) {
    await page.setViewportSize({ width, height: 900 });
    const logo = await page.locator('.sidebar-header .brand').boundingBox();
    const collapse = await page
      .getByRole('button', { name: 'Collapse sidebar', exact: true })
      .boundingBox();
    const sidebar = await page.locator('.sidebar').boundingBox();
    assert.ok(
      Math.abs(collapse.x + collapse.width / 2 - sidebar.width) < 1 &&
        collapse.y >= logo.y + logo.height,
      'toggle stays on the edge without crowding the logo on narrower desktops',
    );
  }
  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'Sidebar grouping, collapse, preference persistence, navigation, keyboard and responsive checks passed',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

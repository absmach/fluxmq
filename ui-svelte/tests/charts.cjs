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
  await page.goto(base + '/dashboard/queues');
  const queue = page.getByRole('region', { name: 'Queue delivery diagnostics' }),
    protocol = page.getByRole('region', { name: 'Protocol diagnostics' });
  await queue.getByRole('img', { name: 'Queue activity history', exact: true }).waitFor();
  await page.waitForFunction(
    () => document.querySelector('.queue-diagnostics .plot-main path') !== null,
  );
  assert.ok((await queue.locator('.plot-main path').count()) >= 2);
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await queue.screenshot({ path: shots + '/queue-chart-light.png' });
  assert.equal(await queue.locator('.series-readout button[aria-pressed="true"]').count(), 8);
  assert.equal(await queue.locator('.plot-y-right').count(), 1);
  assert.ok((await queue.boundingBox()).height < 490, 'queue panel stays compact');
  // Regression: ACK must not visually flatten the smaller rate series.
  await page.getByRole('button', { name: 'Pause updates', exact: true }).click();
  const nack = queue.locator('path[data-series="queue.acknowledgments.nack"]');
  const rateScale = queue.getByRole('combobox', { name: 'Queue rate scale' });
  assert.equal(await rateScale.inputValue(), 'log');
  assert.ok(await nack.evaluate((p) => p.getBBox().y < 145), 'NACK visible with ACK enabled');
  const readings = await queue.locator('.series-readout').innerText();
  await queue.getByRole('button', { name: 'Toggle ACK', exact: true }).click();
  await queue.getByRole('button', { name: 'Toggle ACK', exact: true }).click();
  assert.ok(await nack.evaluate((p) => p.getBBox().y < 145), 're-enabling ACK keeps NACK visible');
  assert.equal(await queue.locator('.series-readout button[aria-pressed="true"]').count(), 8);
  await rateScale.selectOption('linear');
  assert.ok(await nack.evaluate((p) => p.getBBox().y > 168), 'linear retains absolute magnitude');
  assert.ok(
    await queue
      .locator('path[data-detail-series="queue.acknowledgments.nack"]')
      .evaluate((p) => p.getBBox().y < 50),
    'linear detail keeps NACK readable',
  );
  const linearHeight = (await queue.boundingBox()).height;
  const linearNack = await nack.getAttribute('d');
  await queue.screenshot({ path: shots + '/queue-ack-on.png' });
  await queue.getByRole('button', { name: 'Toggle ACK', exact: true }).click();
  assert.equal(
    (await queue.boundingBox()).height,
    linearHeight,
    'hiding ACK preserves card height',
  );
  assert.equal(await nack.getAttribute('d'), linearNack, 'hiding ACK preserves remaining curves');
  await queue.screenshot({ path: shots + '/queue-ack-off.png' });
  await queue.getByRole('button', { name: 'Toggle ACK', exact: true }).click();
  await rateScale.selectOption('log');
  assert.equal(
    await queue.locator('.series-readout').innerText(),
    readings,
    'scale never changes measured values',
  );
  assert.match(await queue.locator('.plot-y-right').innerText(), /10K/);
  await page.getByRole('button', { name: 'Resume updates', exact: true }).click();
  await queue.locator('.series-menu summary').click();
  await queue.getByRole('button', { name: 'Show only ACK', exact: true }).click();
  assert.equal(await queue.locator('.series-readout button[aria-pressed="true"]').count(), 1);
  assert.equal(
    await queue.locator('.plot-y-right').count(),
    1,
    'isolating a series preserves axes',
  );
  assert.match(await queue.locator('.axis-labels').innerText(), /msg/);
  await queue.locator('.series-menu summary').click();
  await queue.getByRole('button', { name: 'Hide all', exact: true }).click();
  await queue.getByText('Choose a series to display', { exact: true }).waitFor();
  await queue.getByRole('button', { name: 'Show all', exact: true }).click();
  await queue.getByRole('checkbox').first().uncheck();
  assert.equal(
    await queue
      .getByRole('button', { name: 'Toggle Pending', exact: true })
      .getAttribute('aria-pressed'),
    'false',
  );
  await page.keyboard.press('Escape');
  assert.equal(await queue.locator('.series-menu').getAttribute('open'), null);
  await queue.getByRole('button', { name: 'Toggle Pending', exact: true }).click();
  assert.equal(await queue.locator('.series-readout button[aria-pressed="true"]').count(), 8);
  // Verify the actual plot and tooltip for every filter, including menu options
  // that require scrolling; keep a selection through a real SSE update.
  for (const [name, key] of [
    ['Pending', 'queue.pending.current'],
    ['Poison pending', 'queue.acknowledgments.poison_pending'],
    ['ACK', 'queue.acknowledgments.ack'],
    ['NACK', 'queue.acknowledgments.nack'],
    ['Rejected', 'queue.acknowledgments.reject'],
    ['Dead-lettered', 'queue.acknowledgments.dlq'],
    ['Capture failed', 'queue.capture_failures'],
    ['Capture dropped', 'queue.capture_dropped'],
  ]) {
    await queue.locator('.series-menu summary').click();
    const option = queue.getByRole('button', { name: `Show only ${name}`, exact: true });
    await option.scrollIntoViewIfNeeded();
    assert.ok(
      await option.evaluate((e) => {
        const r = e.getBoundingClientRect();
        return e.contains(document.elementFromPoint(r.x + r.width / 2, r.y + r.height / 2));
      }),
      'filter option receives pointer events',
    );
    await option.click();
    assert.deepEqual(
      await queue
        .locator('path[data-series]')
        .evaluateAll((paths) => [...new Set(paths.map((p) => p.dataset.series))]),
      [key],
    );
    await queue.getByRole('slider').focus();
    await queue.getByRole('slider').press('End');
    const tip = queue.locator('.plot-tooltip');
    assert.equal(await tip.locator('span').count(), 1);
    assert.ok((await tip.innerText()).includes(name));
    await queue.getByRole('slider').press('Escape');
  }
  await page.waitForTimeout(2300);
  assert.deepEqual(
    await queue
      .locator('path[data-series]')
      .evaluateAll((paths) => [...new Set(paths.map((p) => p.dataset.series))]),
    ['queue.capture_dropped'],
  );
  await queue.locator('.series-menu summary').click();
  await queue.getByRole('button', { name: 'Show all', exact: true }).click();
  await page.keyboard.press('Escape');
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
  await queue.screenshot({ path: shots + '/queue-chart-dark.png' });
  await queue.getByText('Counters & node comparison', { exact: true }).click();
  await queue.getByRole('columnheader', { name: 'Node', exact: true }).waitFor();
  await page.goto(base + '/dashboard/health');
  await protocol.getByRole('img', { name: 'Protocol messages history', exact: true }).waitFor();
  assert.equal(await page.locator('.queue-diagnostics').count(), 0);
  await page.getByRole('region', { name: 'Queue health summary' }).waitFor();
  assert.equal(
    await protocol.locator('.table-scroll').isVisible(),
    false,
    'tables start collapsed',
  );
  await protocol.screenshot({ path: shots + '/protocol-chart-light.png' });

  assert.equal(
    await protocol.locator('.series-readout button').count(),
    4,
    'both protocols overlay by default',
  );
  assert.doesNotMatch(await protocol.locator('.series-readout').innerText(), /—/);
  await protocol.locator('.series-menu summary').click();
  await protocol
    .getByRole('button', { name: 'Show only AMQP 0.9.1 incoming', exact: true })
    .click();
  assert.equal(await protocol.locator('.series-readout button[aria-pressed="true"]').count(), 1);
  await protocol.getByRole('combobox', { name: 'Protocol chart metric' }).selectOption('bandwidth');
  await protocol.getByRole('img', { name: 'Protocol bandwidth history', exact: true }).waitFor();
  assert.equal(await protocol.locator('.series-readout button[aria-pressed="true"]').count(), 4);
  await page.getByRole('button', { name: 'Dark appearance', exact: true }).click();
  await page.waitForTimeout(250);
  await protocol.screenshot({ path: shots + '/protocol-chart-dark.png' });
  await protocol.getByText('Compare node counters', { exact: true }).click();
  await protocol.getByRole('columnheader', { name: 'Node / protocol', exact: true }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(base + '/dashboard/queues');
  await queue.getByRole('img', { name: 'Queue activity history', exact: true }).waitFor();
  assert.ok(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
    'mobile overflow',
  );
  await queue.screenshot({ path: shots + '/queue-chart-mobile.png' });
  await queue.locator('.series-menu summary').click();
  const popup = await queue.locator('.series-popover').boundingBox();
  assert.ok(popup.x >= 0 && popup.x + popup.width <= 390, 'series menu fits mobile');
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.goto(base + '/dashboard/sessions');
  const delivery = page.getByRole('region', { name: 'Protocols & delivery' });
  await delivery.getByText('3 disconnected', { exact: true }).waitFor();
  await page.getByRole('button', { name: 'Light appearance', exact: true }).click();
  assert.notEqual(
    await delivery
      .locator('.protocol-legend .disconnected')
      .evaluate((e) => getComputedStyle(e).backgroundColor),
    await delivery
      .locator('.protocol-bar')
      .first()
      .evaluate((e) => getComputedStyle(e).backgroundColor),
  );
  await delivery.screenshot({ path: shots + '/protocol-delivery-light.png' });
  await page.goto(base + '/dashboard/subscriptions');
  const topics = page.locator('.topics-table');
  await topics.getByRole('columnheader', { name: 'Subscribers', exact: true }).waitFor();
  assert.equal(await topics.locator('thead th').count(), 5);
  assert.equal(await topics.locator('.distribution-track').count(), 0);
  await topics.locator('.subscriber-count').first().waitFor();
  const header = await topics.locator('thead th').nth(1).boundingBox();
  const cells = await topics.locator('tbody tr').first().locator('td').all();
  assert.equal(cells.length, 5);
  const count = await cells[1].locator('.subscriber-count').boundingBox();
  assert.ok(
    count.x >= header.x && count.x + count.width <= header.x + header.width,
    'subscriber count sits under its header',
  );
  await topics.screenshot({ path: shots + '/topic-columns-light.png' });
  const arrows = await topics
    .locator('.topic-action svg')
    .evaluateAll((es) => es.map((e) => e.getBoundingClientRect().right));
  assert.ok(
    arrows.every((x) => Math.abs(x - arrows[0]) < 1),
    'subscriber drilldown arrows align',
  );
  await topics.locator('.subscriber-count').first().click();
  await page.waitForURL(/subscription/);
  await page.getByRole('heading', { name: 'system/health', exact: true }).waitFor();

  assert.deepEqual(errors, []);
  await browser.close();
  console.log(
    'PASS: queue/protocol history charts, rate data, overlay controls, ranges, keyboard inspection, disclosure and responsive layouts',
  );
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

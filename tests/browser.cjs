/* Run against the local console. Only the object created by this run is deleted. */
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs/promises');
const { chromium } = require('playwright');

async function main() {
  const base = 'http://127.0.0.1:5173';
  const browser = await chromium.launch({ channel: process.env.QA_BROWSER || 'msedge' });
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  let id;
  try {
    await page.goto(base);
    await page.waitForSelector('#connection.online');
    assert.equal(await page.locator('.distribution-lane').count(), 4);
    console.log('PASS: worker distribution and connected controller');
    const bytes = crypto.randomBytes(1048577);
    const name = 'browser-' + crypto.randomUUID() + '.bin';
    await page.click('#upload-open');
    await page.setInputFiles('#upload-file', { name, mimeType: 'application/octet-stream', buffer: bytes });
    await page.fill('#upload-metadata', '{"source":"browser"}');
    const uploaded = page.waitForResponse(r => r.request().method() === 'POST' && new URL(r.url()).pathname === '/api/v1/objects');
    await page.click('#upload-form button[type=submit]');
    const response = await uploaded;
    assert.equal(response.status(), 201);
    id = (await response.json()).object_id;
    await page.waitForFunction(() => document.querySelector('#chunk-note').textContent.includes('Target'));
    assert.equal(await page.locator('#chunk-rows tr').count(), 2);
    await page.fill('#metadata-patch', '[');
    await page.click('#patch-form button');
    await page.waitForSelector('#detail-result.error');
    await page.fill('#metadata-patch', '{"checked":true}');
    await page.click('#patch-form button');
    await page.waitForFunction(() => document.querySelector('#custom-fields').textContent.includes('"checked": true'));
    const download = page.waitForEvent('download');
    await page.click('#download');
    assert.deepEqual(await fs.readFile(await (await download).path()), bytes);
    const objectUrl = base + '/api/v1/objects/' + id;
    await page.route(objectUrl, async route => {
      const original = await route.fetch();
      const damaged = Buffer.from(bytes);
      damaged[0] ^= 1;
      await route.fulfill({response: original, body: damaged});
    });
    await page.click('#download');
    await page.waitForFunction(() => document.querySelector('#detail-result').textContent.includes('SHA-256 mismatch'));
    await page.unroute(objectUrl);
    console.log('PASS: upload, chunk placement, metadata patch and verified download');
    await page.click('#detail-dialog [data-close]');
    if (process.env.QA_SCREENSHOT) {
      await page.reload();
      await page.waitForSelector('#connection.online');
      await page.screenshot({path: process.env.QA_SCREENSHOT, fullPage: true});
    }
    await page.click('[data-page=testing]');
    await page.click('#probe-form button');
    await page.waitForFunction(() => document.querySelector('#probe-result').textContent.includes('"passed": true'));
    await page.setInputFiles('#original', {name, mimeType:'application/octet-stream', buffer:bytes});
    await page.setInputFiles('#downloaded', {name, mimeType:'application/octet-stream', buffer:bytes});
    await page.click('#compare-form button');
    await page.waitForFunction(() => document.querySelector('#compare-result').textContent.includes('"match": true'));
    console.log('PASS: API error display and file comparison');
    await page.click('[data-page=cluster]');
    await page.waitForSelector('#worker-rows tr:nth-child(4)');
    await page.selectOption('#replication-filter','healthy');
    await page.locator('#replication-rows tr').filter({hasText:id}).first().waitFor();
    await page.selectOption('#replication-filter','under');
    assert.equal(await page.locator('#replication-rows tr').filter({hasText:id}).count(),0);
    await page.click('[data-page=objects]');
    await page.click('#lookup-open');
    await page.fill('#lookup-id', id);
    await page.click('#lookup-form button');
    await page.waitForFunction(() => document.querySelector('#detail-title').textContent.startsWith('browser-'));
    assert.equal(JSON.parse(await page.textContent('#custom-fields')).checked,true);
    page.once('dialog', dialog => dialog.accept());
    await page.click('#delete');
    await page.waitForSelector('#detail-dialog', {state:'hidden'});
    assert.equal((await page.request.get(base+'/api/v1/objects/'+id+'/metadata')).status(),404);
    id = null;
    console.log('PASS: cluster navigation, lookup, persisted metadata and delete');
    for (const width of [1440,1024,768,390]) {
      await page.setViewportSize({width,height:900});
      for (const section of ['testing','cluster','objects']) {
        await page.click('[data-page='+section+']');
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Overflow at '+width+' on '+section);
      }
    }
    await page.route('**/health', route => route.fulfill({status:503,body:'{}'}));
    await page.click('#refresh');
    await page.waitForSelector('#connection.offline');
    assert.deepEqual(errors, []);
    console.log('PASS: responsive navigation and offline state');
  } finally {
    try {
      if (id) await page.request.delete(base+'/api/v1/objects/'+id);
    } finally {
      await browser.close();
    }
  }
}
main().catch(error => { console.error(error); process.exitCode = 1; });

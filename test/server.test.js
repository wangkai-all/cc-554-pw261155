'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createServer } = require('../server');
const { DemoSession } = require('../lib/core');

async function withServer(fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'maint-http-'));
  const server = createServer(new DemoSession({ dir }));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    await fn(base);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

const post = (base, p) => fetch(base + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }).then((r) => r.json());

test('HTTP：页面含 data-evidence-result，三个按钮接口按序联动', async () => {
  await withServer(async (base) => {
    // 页面：业务结果区带 data-evidence-result 属性，三个指定按钮存在
    const html = await (await fetch(base + '/')).text();
    assert.match(html, /data-evidence-result="init"/);
    for (const label of ['接收不连续保养包', '补齐开始事件', '查看工单回执']) {
      assert.ok(html.includes(label), `页面缺少按钮：${label}`);
    }

    // 1) 接收不连续保养包：高水位 2，缺口 [3]，WO-1002 被拦截
    const lossy = await post(base, '/api/receive-lossy');
    assert.equal(lossy.ack.highWatermark, 2);
    assert.deepEqual(lossy.ack.gaps, [3]);
    assert.equal(lossy.orders.find((o) => o.orderId === 'WO-1002').status, 'blocked_missing_start');

    // 2) 补齐开始事件：高水位推进到 6，WO-1002 复核通过
    const fill = await post(base, '/api/fill-start');
    assert.equal(fill.ack.highWatermark, 6);
    assert.deepEqual(fill.ack.gaps, []);
    assert.equal(fill.orders.find((o) => o.orderId === 'WO-1002').status, 'reviewed');

    // 3) 查看工单回执
    const receipts = await (await fetch(base + '/api/receipts')).json();
    assert.equal(receipts.orders.length, 2);
    assert.ok(receipts.orders.every((o) => o.status === 'reviewed'));

    // 4) 复核回退后重发旧包：识别为旧状态
    await post(base, '/api/rollback');
    const resend = await post(base, '/api/resend-old');
    assert.equal(resend.results[0].status, 'stale');
    assert.equal(resend.orders.find((o) => o.orderId === 'WO-1001').status, 'completed');

    // 5) 重置后可重跑
    const reset = await post(base, '/api/reset');
    assert.equal(reset.ok, true);
    assert.equal(reset.ack.persistedCount, 0);

    // 6) 未知路径 404
    const nf = await fetch(base + '/nope');
    assert.equal(nf.status, 404);
  });
});

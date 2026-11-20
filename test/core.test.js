'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Controller, Receiver, DemoSession } = require('../lib/core');

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'maint-demo-'));
}

test('正常流程：不连续补传 -> 缺口确认 -> 补齐开始事件 -> 工单完成', () => {
  const demo = new DemoSession({ dir: tmpDir() });

  // 动作一：接收不连续保养包（包大小 2、4，第二包丢 seq=3）
  const r1 = demo.receiveLossy();
  assert.deepEqual(r1.steps[0].deliveredSeqs, [1, 2]);
  assert.deepEqual(r1.steps[1].deliveredSeqs, [4, 5, 6]);
  assert.deepEqual(r1.steps[1].lostSeqs, [3]);
  // 确认：连续高水位=2，缺口=[3]，只确认已持久化序号
  assert.deepEqual(r1.ack, { highWatermark: 2, gaps: [3], maxSeen: 6, persistedCount: 5 });
  // 发送端队列只剩缺口，等待补传（队列大小最小）
  assert.deepEqual(r1.pendingRetransmit, [3]);
  // 异常结果：WO-1002 有结束事件但缺开始事件，不得定为已完成
  const wo1002 = r1.orders.find((o) => o.orderId === 'WO-1002');
  assert.equal(wo1002.status, 'blocked_missing_start');
  assert.notEqual(wo1002.status, 'completed');
  // WO-1001 正常复核通过
  assert.equal(r1.orders.find((o) => o.orderId === 'WO-1001').status, 'reviewed');

  // 动作二：补齐开始事件（只重传缺口，包大小=1）
  const r2 = demo.fillStart();
  assert.equal(r2.packetSize, 1);
  assert.deepEqual(r2.deliveredSeqs, [3]);
  assert.deepEqual(r2.ack, { highWatermark: 6, gaps: [], maxSeen: 6, persistedCount: 6 });
  assert.equal(r2.orders.find((o) => o.orderId === 'WO-1002').status, 'reviewed');

  // 动作三：工单回执
  const r3 = demo.receipts();
  assert.equal(r3.ack.highWatermark, 6);
  assert.deepEqual(r3.ack.gaps, []);
  assert.equal(r3.orders.length, 2);
});

test('异常：结束事件不得跨过缺失的开始事件定为已完成', () => {
  const receiver = new Receiver({ dir: tmpDir() });
  const controller = new Controller('C');
  controller.record('end', 'WO-9', 1, { result: 'ok' }); // 只有结束，没有开始
  const r = receiver.receivePacket(controller.sendPacket(controller.makeBatch(1)));
  assert.equal(r.results[0].status, 'persisted'); // 事件照常持久化
  const order = receiver.projectOrders().find((o) => o.orderId === 'WO-9');
  assert.equal(order.status, 'blocked_missing_start'); // 但工单不得完成
  assert.match(order.note, /不得定为已完成/);
});

test('只确认已持久化的序号，重启后确认状态可恢复', () => {
  const dir = tmpDir();
  const receiver = new Receiver({ dir });
  const controller = new Controller('C');
  controller.record('start', 'WO-1', 1);
  controller.record('start', 'WO-2', 1);
  controller.record('start', 'WO-3', 1);
  // 只投递 seq 1 和 3（seq 2 丢失）
  const batch = controller.makeBatch(3);
  receiver.receivePacket(controller.sendPacket(batch.filter((e) => e.seq !== 2)));
  const ack = receiver.ack();
  assert.deepEqual(ack, { highWatermark: 1, gaps: [2], maxSeen: 3, persistedCount: 2 });
  assert.ok(!ack.gaps.includes(4) && ack.maxSeen === 3, '不得确认从未持久化的序号');

  // 模拟重启：从同一目录重新加载，确认结果一致（确认依据是持久化日志）
  const reloaded = new Receiver({ dir });
  assert.deepEqual(reloaded.ack(), ack);
});

test('异常：重复投递幂等，不重复落盘', () => {
  const dir = tmpDir();
  const receiver = new Receiver({ dir });
  const controller = new Controller('C');
  controller.record('start', 'WO-1', 1);
  const packet = controller.sendPacket(controller.makeBatch(1));
  receiver.receivePacket(packet);
  const r2 = receiver.receivePacket(packet); // 同一个包再来一次
  assert.equal(r2.results[0].status, 'duplicate');
  const lines = fs.readFileSync(path.join(dir, 'events.jsonl'), 'utf8').trim().split('\n');
  assert.equal(lines.length, 1, '持久化日志不得重复追加');
});

test('复核回退后，重发旧包被识别为旧状态且不改变工单', () => {
  const demo = new DemoSession({ dir: tmpDir() });
  demo.receiveLossy();
  demo.fillStart();
  assert.equal(demo.receiver.projectOrders().find((o) => o.orderId === 'WO-1001').status, 'reviewed');

  // 复核回退：版本 1 -> 2，工单回到 completed（旧复核失效）
  const rb = demo.rollback('WO-1001');
  assert.equal(rb.rollback.version, 2);
  const afterRollback = demo.receiver.projectOrders().find((o) => o.orderId === 'WO-1001');
  assert.equal(afterRollback.status, 'completed');

  // 重发旧包（version=1 的复核事件）：必须能看出是旧状态
  const r = demo.resendOld('WO-1001');
  assert.deepEqual(r.resentSeqs, [5]);
  assert.equal(r.results[0].status, 'stale');
  assert.equal(r.results[0].stale, true);
  assert.equal(r.stale.length, 1);
  assert.equal(r.stale[0].eventVersion, 1);
  assert.equal(r.stale[0].currentVersion, 2);
  // 工单不被旧状态改回去
  const order = r.orders.find((o) => o.orderId === 'WO-1001');
  assert.equal(order.status, 'completed');
  assert.equal(order.currentVersion, 2);

  // 新版本的复核事件正常生效
  demo.controller.record('review', 'WO-1001', 2, { decision: 'approve', reviewer: '王工' });
  const ev = demo.controller.makeBatch(1);
  demo.receiver.receivePacket(demo.controller.sendPacket(ev));
  assert.equal(demo.receiver.projectOrders().find((o) => o.orderId === 'WO-1001').status, 'reviewed');
});

test('异常：非法事件被拒绝，非法包报错', () => {
  const receiver = new Receiver({ dir: tmpDir() });
  const r = receiver.receivePacket({ controllerId: 'C', events: [{ seq: 1, type: 'bogus', orderId: 'WO-1' }] });
  assert.equal(r.results[0].status, 'rejected');
  assert.equal(receiver.ack().persistedCount, 0);
  assert.throws(() => receiver.receivePacket({}), /非法包/);
});

test('发送端按高水位+缺口清理队列：已确认出队，缺口留队重传', () => {
  const controller = new Controller('C');
  for (let i = 0; i < 5; i++) controller.record('start', `WO-${i}`, 1);
  assert.equal(controller.queue.length, 5);
  // 接收端已持久化 1,2,4,5（缺 3）：高水位 2，缺口 [3]
  controller.handleAck({ highWatermark: 2, gaps: [3], maxSeen: 5 });
  assert.deepEqual(controller.queue.map((e) => e.seq), [3], '队列只保留缺口');
  controller.handleAck({ highWatermark: 5, gaps: [], maxSeen: 5 });
  assert.equal(controller.queue.length, 0, '全部确认后队列清空');
});

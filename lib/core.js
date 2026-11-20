'use strict';

/**
 * 保养事件补传核心逻辑（仅用 Node 内置模块）。
 *
 * 角色：
 *  - Controller  工位控制器：离线期间缓存 开始/结束/复核 事件，上线后按不同大小组包补传。
 *  - Receiver    接收端：只确认“已持久化”的序号，回复 连续高水位 + 缺口清单。
 *  - 工单投影     由持久化日志推导工单状态：缺开始事件时，结束事件不得定为已完成；
 *                复核回退后版本号 +1，重发的旧包被识别为旧状态（stale），不参与投影。
 */

const fs = require('node:fs');
const path = require('node:path');

const EVENT_TYPES = new Set(['start', 'end', 'review']);

/** 工位控制器（发送端） */
class Controller {
  constructor(id) {
    this.id = id;
    this.nextSeq = 1;
    this.queue = [];        // 未确认的出站事件（按 seq 升序）
    this.sentPackets = [];  // 已发送包的历史（用于 ACK 丢失后的重发 / 演示重发旧包）
  }

  /** 离线期间记录一条事件，进入待传队列 */
  record(type, orderId, version, payload = {}) {
    if (!EVENT_TYPES.has(type)) throw new Error(`未知事件类型: ${type}`);
    const ev = { seq: this.nextSeq++, type, orderId, version, payload };
    this.queue.push(ev);
    return ev;
  }

  /** 按指定大小组包（不同大小的包：取队首 size 条） */
  makeBatch(size) {
    return this.queue.slice(0, Math.max(0, size));
  }

  /** 发送一个包：记录历史并返回 */
  sendPacket(events) {
    const packet = { controllerId: this.id, events };
    this.sentPackets.push(packet);
    return packet;
  }

  /**
   * 处理接收端确认 { highWatermark, gaps, maxSeen }：
   * 已确认 = seq <= maxSeen 且不在 gaps 中；其余保留在队列里等待补传。
   * 这样队列只保留缺口与未发送事件，队列大小最小。
   */
  handleAck(ack) {
    const gaps = new Set(ack.gaps || []);
    this.queue = this.queue.filter((ev) => ev.seq > ack.maxSeen || gaps.has(ev.seq));
    return this.queue.length;
  }

  /** 按缺口清单取需要重传的事件 */
  retransmitGaps(ack) {
    const gaps = new Set(ack.gaps || []);
    return this.queue.filter((ev) => gaps.has(ev.seq));
  }
}

/** 接收端：持久化日志 + 高水位/缺口确认 + 工单投影 */
class Receiver {
  constructor({ dir }) {
    this.dir = dir;
    fs.mkdirSync(dir, { recursive: true });
    this.eventsFile = path.join(dir, 'events.jsonl');
    this.rollbacksFile = path.join(dir, 'rollbacks.jsonl');
    this.events = new Map();   // seq -> event（仅已持久化）
    this.rollbacks = [];       // [{ orderId, version, reason, ts }]
    this.staleLog = [];        // 被识别为旧状态的投递记录（证据）
    this._load();
  }

  _load() {
    for (const line of this._readLines(this.eventsFile)) {
      const ev = JSON.parse(line);
      this.events.set(ev.seq, ev);
    }
    for (const line of this._readLines(this.rollbacksFile)) {
      this.rollbacks.push(JSON.parse(line));
    }
  }

  _readLines(file) {
    if (!fs.existsSync(file)) return [];
    return fs.readFileSync(file, 'utf8').split('\n').filter((l) => l.trim() !== '');
  }

  _append(file, obj) {
    fs.appendFileSync(file, JSON.stringify(obj) + '\n');
  }

  /** 工单当前状态版本：每次复核回退 +1 */
  currentVersion(orderId) {
    let v = 1;
    for (const rb of this.rollbacks) {
      if (rb.orderId === orderId && rb.version > v) v = rb.version;
    }
    return v;
  }

  /**
   * 接收一个补传包。逐条处理：
   *  - 先持久化（写盘），再进入可确认集合 —— 只确认已持久化的序号；
   *  - 重复序号 -> duplicate，幂等不重复落盘；
   *  - 事件版本低于工单当前版本 -> stale（旧状态），落盘留痕但不参与投影。
   * 返回每条结果 + 最新确认。
   */
  receivePacket(packet) {
    if (!packet || !Array.isArray(packet.events)) {
      throw new Error('非法包：缺少 events 数组');
    }
    const results = [];
    for (const ev of packet.events) {
      if (!ev || typeof ev.seq !== 'number' || !EVENT_TYPES.has(ev.type) || !ev.orderId) {
        results.push({ seq: ev && ev.seq, status: 'rejected', reason: '事件格式非法' });
        continue;
      }
      const current = this.currentVersion(ev.orderId);
      // 版本只约束复核事件：回退的是复核结论，开始/结束是事实，不随回退失效
      const stale = ev.type === 'review' && (ev.version || 1) < current;
      if (this.events.has(ev.seq)) {
        results.push({ seq: ev.seq, status: stale ? 'stale' : 'duplicate', stale });
      } else {
        this._append(this.eventsFile, ev); // 先持久化
        this.events.set(ev.seq, ev);       // 再允许被确认
        results.push({ seq: ev.seq, status: stale ? 'stale' : 'persisted', stale });
      }
      if (stale) {
        this.staleLog.push({
          seq: ev.seq,
          orderId: ev.orderId,
          eventVersion: ev.version || 1,
          currentVersion: current,
          note: '复核回退后重发的旧包，已识别为旧状态，不参与工单投影',
        });
      }
    }
    return { results, ack: this.ack() };
  }

  /** 确认 = 连续高水位 + 缺口清单，只依据已持久化的序号 */
  ack() {
    const seqs = [...this.events.keys()].sort((a, b) => a - b);
    let highWatermark = 0;
    for (const s of seqs) {
      if (s === highWatermark + 1) highWatermark = s;
      else break;
    }
    const maxSeen = seqs.length ? seqs[seqs.length - 1] : 0;
    const gaps = [];
    for (let s = highWatermark + 1; s <= maxSeen; s++) {
      if (!this.events.has(s)) gaps.push(s);
    }
    return { highWatermark, gaps, maxSeen, persistedCount: seqs.length };
  }

  /** 复核回退：版本 +1，旧的复核事件随即失效 */
  rollbackReview(orderId, reason) {
    const rb = {
      orderId,
      version: this.currentVersion(orderId) + 1,
      reason: reason || '复核回退',
      ts: new Date().toISOString(),
    };
    this._append(this.rollbacksFile, rb);
    this.rollbacks.push(rb);
    return rb;
  }

  /**
   * 工单投影：由已持久化且非旧状态的事件推导。
   * 关键约束：缺开始事件时，结束事件存在也不得定为已完成。
   */
  projectOrders() {
    const orders = new Map();
    const seqs = [...this.events.keys()].sort((a, b) => a - b);
    for (const seq of seqs) {
      const ev = this.events.get(seq);
      // 旧版本的复核事件不参与投影；开始/结束事件不受复核回退影响
      if (ev.type === 'review' && (ev.version || 1) < this.currentVersion(ev.orderId)) continue;
      let o = orders.get(ev.orderId);
      if (!o) {
        o = { orderId: ev.orderId, start: null, end: null, review: null, appliedSeqs: [] };
        orders.set(ev.orderId, o);
      }
      o.appliedSeqs.push(seq);
      if (ev.type === 'start') o.start = ev;
      else if (ev.type === 'end') o.end = ev;
      else if (ev.type === 'review') o.review = ev;
    }
    const list = [];
    for (const o of orders.values()) {
      const persistedEndWithoutStart = !o.start && !!o.end;
      if (!o.start && o.end) {
        o.status = 'blocked_missing_start';
        o.note = `缺少开始事件，结束事件 seq=${o.end.seq} 不得定为已完成`;
      } else if (!o.start) {
        o.status = 'pending';
      } else if (!o.end) {
        o.status = 'in_progress';
      } else if (!o.review) {
        o.status = 'completed';
      } else {
        o.status = o.review.payload && o.review.payload.decision === 'reject'
          ? 'review_rejected'
          : 'reviewed';
      }
      o.currentVersion = this.currentVersion(o.orderId);
      o.persistedEndWithoutStart = persistedEndWithoutStart;
      list.push(o);
    }
    return list.sort((a, b) => (a.orderId < b.orderId ? -1 : 1));
  }

  /** 工单回执：投影 + 确认 + 旧状态记录 */
  receipts() {
    return {
      ack: this.ack(),
      orders: this.projectOrders().map((o) => ({
        orderId: o.orderId,
        status: o.status,
        currentVersion: o.currentVersion,
        appliedSeqs: o.appliedSeqs,
        note: o.note,
      })),
      stale: this.staleLog,
    };
  }
}

/** 本地样例：离线期间控制器缓存的 6 条事件 */
function buildSampleEvents(controller) {
  controller.record('start', 'WO-1001', 1, { worker: '张三', station: 'S-01' });
  controller.record('end', 'WO-1001', 1, { result: 'ok' });
  controller.record('start', 'WO-1002', 1, { worker: '李四', station: 'S-02' });
  controller.record('end', 'WO-1002', 1, { result: 'ok' });
  controller.record('review', 'WO-1001', 1, { decision: 'approve', reviewer: '王工' });
  controller.record('review', 'WO-1002', 1, { decision: 'approve', reviewer: '王工' });
}

/**
 * 演示会话：把控制器与接收端串起来，提供页面按钮对应的动作。
 */
class DemoSession {
  constructor({ dir }) {
    this.dir = dir;
    this.receiver = new Receiver({ dir });
    this.controller = new Controller('CTRL-01');
    this.lastAck = this.receiver.ack();
    this.lossyDone = false;
    buildSampleEvents(this.controller);
  }

  /**
   * 动作一：接收不连续保养包。
   * 上线后按不同大小组包（2 条、4 条），第二包模拟丢失 seq=3（WO-1002 的开始事件）。
   */
  receiveLossy() {
    if (this.lossyDone) {
      return { action: 'receive-lossy', message: '不连续包已接收过（幂等），如需重跑请先重置', ack: this.lastAck, orders: this.receiver.projectOrders() };
    }
    const steps = [];
    // 第一包：2 条，全部到达
    const p1 = this.controller.sendPacket(this.controller.makeBatch(2));
    const r1 = this.receiver.receivePacket(p1);
    this.controller.handleAck(r1.ack);
    steps.push({ packetSize: p1.events.length, deliveredSeqs: p1.events.map((e) => e.seq), ack: r1.ack });
    // 第二包：4 条，模拟 seq=3 在补传途中丢失，只到达 4,5,6
    const batch2 = this.controller.makeBatch(4);
    const delivered = batch2.filter((ev) => ev.seq !== 3);
    const p2 = this.controller.sendPacket(delivered);
    const r2 = this.receiver.receivePacket(p2);
    this.controller.handleAck(r2.ack);
    steps.push({
      packetSize: p2.events.length,
      deliveredSeqs: p2.events.map((e) => e.seq),
      lostSeqs: batch2.filter((ev) => ev.seq === 3).map((e) => e.seq),
      ack: r2.ack,
    });
    this.lastAck = r2.ack;
    this.lossyDone = true;
    return {
      action: 'receive-lossy',
      steps,
      ack: this.lastAck,
      pendingRetransmit: this.controller.queue.map((e) => e.seq),
      orders: this.receiver.projectOrders(),
    };
  }

  /**
   * 动作二：补齐开始事件。发送端按缺口清单只重传缺失的 seq（包大小=缺口数）。
   */
  fillStart() {
    const gaps = this.receiver.ack().gaps;
    if (gaps.length === 0) {
      return { action: 'fill-start', message: '没有缺口，无需补齐', ack: this.receiver.ack(), orders: this.receiver.projectOrders() };
    }
    const resend = this.controller.retransmitGaps(this.receiver.ack());
    const packet = this.controller.sendPacket(resend);
    const r = this.receiver.receivePacket(packet);
    this.controller.handleAck(r.ack);
    this.lastAck = r.ack;
    return {
      action: 'fill-start',
      packetSize: packet.events.length,
      deliveredSeqs: packet.events.map((e) => e.seq),
      ack: r.ack,
      orders: this.receiver.projectOrders(),
    };
  }

  /** 动作三：查看工单回执 */
  receipts() {
    return { action: 'receipts', ...this.receiver.receipts() };
  }

  /** 复核回退：工单版本 +1 */
  rollback(orderId) {
    const rb = this.receiver.rollbackReview(orderId);
    return { action: 'rollback', rollback: rb, orders: this.receiver.projectOrders() };
  }

  /**
   * 重发旧包：从发送历史里找出含旧版本复核事件的包重发，
   * 接收端应识别为旧状态（stale），不改变工单投影。
   */
  resendOld(orderId) {
    const current = this.receiver.currentVersion(orderId);
    for (let i = this.controller.sentPackets.length - 1; i >= 0; i--) {
      const p = this.controller.sentPackets[i];
      const oldEvents = p.events.filter(
        (ev) => ev.orderId === orderId && ev.type === 'review' && (ev.version || 1) < current,
      );
      if (oldEvents.length > 0) {
        const r = this.receiver.receivePacket(this.controller.sendPacket(oldEvents));
        return {
          action: 'resend-old',
          resentSeqs: oldEvents.map((e) => e.seq),
          results: r.results,
          stale: this.receiver.staleLog,
          orders: this.receiver.projectOrders(),
        };
      }
    }
    return { action: 'resend-old', message: '没有找到可重发的旧复核包', orders: this.receiver.projectOrders() };
  }

  /** 重置：清空持久化文件，重建样例 */
  reset() {
    for (const f of [this.receiver.eventsFile, this.receiver.rollbacksFile]) {
      if (fs.existsSync(f)) fs.unlinkSync(f);
    }
    return new DemoSession({ dir: this.dir });
  }
}

module.exports = { Controller, Receiver, DemoSession, buildSampleEvents, EVENT_TYPES };

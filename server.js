'use strict';

/**
 * 保养事件补传演示服务：node server.js 启动，读取 PORT，绑定 127.0.0.1。
 * 仅用 Node 内置模块，不接任何外部服务。
 */

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { DemoSession } = require('./lib/core');

const ROOT = __dirname;
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT, 'data');
const INDEX_HTML = path.join(ROOT, 'public', 'index.html');

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj, null, 2);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (err) {
        reject(new Error('请求体不是合法 JSON'));
      }
    });
    req.on('error', reject);
  });
}

/** 创建服务（session 可注入，便于测试） */
function createServer(session) {
  let demo = session || new DemoSession({ dir: DATA_DIR });

  return http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    try {
      if (req.method === 'GET' && url.pathname === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(fs.readFileSync(INDEX_HTML));
        return;
      }
      if (req.method === 'POST' && url.pathname === '/api/receive-lossy') {
        sendJson(res, 200, demo.receiveLossy());
        return;
      }
      if (req.method === 'POST' && url.pathname === '/api/fill-start') {
        sendJson(res, 200, demo.fillStart());
        return;
      }
      if (req.method === 'GET' && url.pathname === '/api/receipts') {
        sendJson(res, 200, demo.receipts());
        return;
      }
      if (req.method === 'POST' && url.pathname === '/api/rollback') {
        const body = await readBody(req);
        sendJson(res, 200, demo.rollback(body.orderId || 'WO-1001'));
        return;
      }
      if (req.method === 'POST' && url.pathname === '/api/resend-old') {
        const body = await readBody(req);
        sendJson(res, 200, demo.resendOld(body.orderId || 'WO-1001'));
        return;
      }
      if (req.method === 'POST' && url.pathname === '/api/reset') {
        demo = demo.reset();
        sendJson(res, 200, { action: 'reset', ok: true, ack: demo.receiver.ack() });
        return;
      }
      sendJson(res, 404, { error: 'not found', path: url.pathname });
    } catch (err) {
      sendJson(res, 400, { error: err.message });
    }
  });
}

if (require.main === module) {
  const port = Number(process.env.PORT || 3000);
  const server = createServer();
  server.listen(port, '127.0.0.1', () => {
    console.log(`保养补传演示服务已启动: http://127.0.0.1:${server.address().port}`);
  });
}

module.exports = { createServer };

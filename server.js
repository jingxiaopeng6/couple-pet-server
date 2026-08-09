const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  // 心跳检测：每 15 秒发 ping，10 秒无 pong 则判定断开
  pingInterval: 15000,
  pingTimeout: 10000
});

// 优先使用云平台分配的端口，如果没有（本地调试时）则使用 3000
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.send('WebSocket Relay Server is running.');
});

// 存储每个 socket 所属的房间
const socketRooms = new Map();
// 存储每个房间的成员数量
const roomMembers = new Map();

// ==================== 速率限制 ====================
const rateLimiter = new Map(); // socket.id -> { count, resetTime }
const RATE_LIMIT = 20;  // 每窗口最多 20 条消息
const RATE_WINDOW = 5000; // 5 秒窗口

function checkRateLimit(socketId) {
  const now = Date.now();
  let entry = rateLimiter.get(socketId);
  if (!entry || now > entry.resetTime) {
    entry = { count: 0, resetTime: now + RATE_WINDOW };
    rateLimiter.set(socketId, entry);
  }
  entry.count++;
  return entry.count <= RATE_LIMIT;
}

function cleanupRateLimiter(socketId) {
  rateLimiter.delete(socketId);
}

// ==================== 输入校验 ====================
const VALID_ACTIONS = ['开心', '比心', '比耶', '奔跑', '喝奶茶', '跌倒', '坠落'];
const MAX_NOTE_LENGTH = 200;
const MAX_STROKE_POINTS = 2000;

function validatePetAction(data) {
  return data && typeof data.action === 'string' && VALID_ACTIONS.includes(data.action);
}
function validateNote(data) {
  return data && typeof data.text === 'string' && data.text.trim().length > 0 && data.text.length <= MAX_NOTE_LENGTH;
}
function validateDrawStroke(data) {
  return data && Array.isArray(data.points) && data.points.length > 0 && data.points.length <= MAX_STROKE_POINTS
    && typeof data.color === 'string' && typeof data.lineWidth === 'number';
}

io.on('connection', (socket) => {
  console.log(`[连接] 客户端已连接: ${socket.id}`);
  socketRooms.set(socket.id, null);

  // --- 配对房间（限制最多 2 人）---
  socket.on('join-room', (roomId, ack) => {
    if (typeof roomId !== 'string' || !/^\d{6}$/.test(roomId)) {
      const err = { ok: false, error: 'invalid', message: '无效的配对码' };
      socket.emit('room-error', { message: err.message });
      if (typeof ack === 'function') ack(err);
      return;
    }

    // 离开旧房间
    const oldRoom = socketRooms.get(socket.id);
    if (oldRoom) {
      socket.leave(oldRoom);
      const oldCount = (roomMembers.get(oldRoom) || 1) - 1;
      if (oldCount <= 0) {
        roomMembers.delete(oldRoom);
      } else {
        roomMembers.set(oldRoom, oldCount);
      }
      socket.to(oldRoom).emit('partner-status', { status: 'offline' });
    }

    // 检查目标房间是否已满（最多 2 人）
    const memberCount = roomMembers.get(roomId) || 0;
    if (memberCount >= 2) {
      const err = { ok: false, error: 'full', message: '该配对码房间已满，请确认配对码是否正确' };
      socket.emit('room-error', { message: err.message });
      if (typeof ack === 'function') ack(err);
      return;
    }

    // 加入新房间
    socket.join(roomId);
    socketRooms.set(socket.id, roomId);
    roomMembers.set(roomId, memberCount + 1);
    console.log(`[房间] ${socket.id} 加入房间: ${roomId} (当前 ${memberCount + 1} 人)`);

    // memberCount 是加入前的人数，>0 说明房间里已有对方，通知新成员对方在线
    const payload = { partnerOnline: memberCount > 0 };
    socket.emit('room-joined', roomId, payload);
    socket.to(roomId).emit('partner-status', { status: 'online' });
    if (typeof ack === 'function') ack({ ok: true, ...payload });
  });

  // --- 退出房间 ---
  socket.on('leave-room', () => {
    const room = socketRooms.get(socket.id);
    if (room) {
      socket.leave(room);
      const count = (roomMembers.get(room) || 1) - 1;
      if (count <= 0) {
        roomMembers.delete(room);
      } else {
        roomMembers.set(room, count);
      }
      socket.to(room).emit('partner-status', { status: 'offline' });
      socketRooms.set(socket.id, null);
    }
  });

  // --- 转发动作（双人共看，io.to 广播给房间内所有人含发送者）---
  socket.on('pet-action', (data, ack) => {
    if (!checkRateLimit(socket.id)) return;
    if (!validatePetAction(data)) return;
    const room = socketRooms.get(socket.id);
    if (room) {
      const payload = { ...data, ts: Date.now(), by: socket.id };
      io.to(room).emit('pet-action', payload);
      if (typeof ack === 'function') ack({ ok: true });
    }
  });

  // --- 转发状态更新 ---
  socket.on('status-update', (data) => {
    const room = socketRooms.get(socket.id);
    if (room && data && typeof data.status === 'string') {
      socket.to(room).emit('partner-status', data);
    }
  });

  // --- 转发字条（双人都能看到，io.to 广播含发送者，带 ack 确认）---
  socket.on('send-note', (data, ack) => {
    if (!checkRateLimit(socket.id)) {
      if (typeof ack === 'function') ack({ ok: false, error: 'rate-limited' });
      return;
    }
    if (!validateNote(data)) {
      if (typeof ack === 'function') ack({ ok: false, error: 'invalid' });
      return;
    }
    const room = socketRooms.get(socket.id);
    if (room) {
      const payload = { text: data.text, ts: Date.now(), by: socket.id };
      io.to(room).emit('receive-note', payload);
      if (typeof ack === 'function') ack({ ok: true });
    }
  });

  // --- 转发画板笔画轨迹 ---
  socket.on('draw-stroke', (data) => {
    if (!checkRateLimit(socket.id)) return;
    if (!validateDrawStroke(data)) return;
    const room = socketRooms.get(socket.id);
    if (room) {
      socket.to(room).emit('draw-stroke', data);
    }
  });

  // --- 转发清空画布 ---
  socket.on('clear-canvas', () => {
    const room = socketRooms.get(socket.id);
    if (room) {
      socket.to(room).emit('clear-canvas');
    }
  });

  // --- 断开连接 ---
  socket.on('disconnect', () => {
    const room = socketRooms.get(socket.id);
    if (room) {
      const count = (roomMembers.get(room) || 1) - 1;
      if (count <= 0) {
        roomMembers.delete(room);
      } else {
        roomMembers.set(room, count);
      }
      socket.to(room).emit('partner-status', { status: 'offline' });
    }
    socketRooms.delete(socket.id);
    cleanupRateLimiter(socket.id);
    console.log(`[断开] 客户端已断开: ${socket.id}`);
  });
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

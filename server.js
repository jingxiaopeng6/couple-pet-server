const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// 优先使用云平台分配的端口，如果没有（本地调试时）则使用 3000
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.send('WebSocket Relay Server is running.');
});

io.on('connection', (socket) => {
  console.log(`[连接] 客户端已连接: ${socket.id}`);

  socket.on('send-action', (data) => {
    console.log(`[转发] ${socket.id} 发送 send-action:`, data);
    socket.broadcast.emit('send-action', data);
  });

  socket.on('disconnect', () => {
    console.log(`[断开] 客户端已断开: ${socket.id}`);
  });
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

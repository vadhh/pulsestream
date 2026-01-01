const { Server } = require("socket.io");
const Redis = require("ioredis");
const http = require("http");

// 1. Setup the HTTP server and Socket.io
const httpServer = http.createServer();
const io = new Server(httpServer, {
  cors: {
    origin: "*", // Allow connections from Next.js (usually port 3000)
    methods: ["GET", "POST"]
  }
});

// 2. Connect to Redis Pub/Sub
const redis = new Redis({
  host: "localhost", // Or '127.0.0.1' if localhost fails
  port: 6379,
});

redis.subscribe("news_updates", (err, count) => {
  if (err) console.error("Failed to subscribe: %s", err.message);
  else console.log(`🔗 Subscribed to ${count} Redis channel(s). Waiting for updates...`);
});

// 3. When Redis gets a message -> Broadcast to WebSockets
redis.on("message", (channel, message) => {
  console.log(`⚡ Event received from ${channel}`);
  // Forward the raw JSON string directly to the client
  io.emit("market_event", message);
});

// 4. Start the Bridge
const PORT = 3001;
httpServer.listen(PORT, () => {
  console.log(`🌉 WebSocket Bridge running on port ${PORT}`);
});
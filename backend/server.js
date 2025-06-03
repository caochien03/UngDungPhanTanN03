const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const { io: socketIoClient } = require("socket.io-client");
const cors = require("cors");
const nodeConfigs = require("./config");
const fs = require("fs");
const path = require("path");

// Constants
const HEARTBEAT_INTERVAL = 5000; // ms
const HEARTBEAT_TIMEOUT = 10000; // ms
const NODE_LIST = ["node1", "node2", "node3"];
const SNAPSHOT_REQUEST_DELAY = 10000; // ms
const PEER_CONNECTION_DELAY = 5000; // ms
const MAX_STORE_SIZE = 1000; // Thêm giới hạn kích thước store

// Lấy node ID từ tham số dòng lệnh
const nodeId = process.argv[2] || "node1";
const config = nodeConfigs[nodeId];

if (!config) {
    console.error("Invalid node ID");
    process.exit(1);
}

// Khởi tạo Express và Socket.IO
const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"],
    },
    pingTimeout: 120000,
    pingInterval: 50000,
    connectTimeout: 30000,
    transports: ["websocket", "polling"],
});

// State management
const store = {};
const peerConnections = {};
const lastHeartbeat = {};
const failedPeers = new Set();
const pendingSnapshotPeers = new Set();
const storeFile = path.join(__dirname, `store_${nodeId}.json`);

// Utility functions
function saveStoreToFile() {
    try {
        // Giới hạn kích thước store trước khi lưu
        const limitedStore = {};
        let count = 0;
        for (const key in store) {
            if (count >= MAX_STORE_SIZE) break;
            limitedStore[key] = store[key];
            count++;
        }
        fs.writeFileSync(
            storeFile,
            JSON.stringify(limitedStore, null, 2),
            "utf-8"
        );
    } catch (err) {
        console.error(`[${nodeId}] Error saving store to file:`, err);
    }
}

function loadStoreFromFile() {
    if (fs.existsSync(storeFile)) {
        try {
            const data = fs.readFileSync(storeFile, "utf-8");
            const obj = JSON.parse(data);
            Object.assign(store, obj);
            console.log(`[${nodeId}] Loaded store from ${storeFile}`);
        } catch (err) {
            console.error(`[${nodeId}] Error loading store from file:`, err);
        }
    }
}

// Node management functions
function getPrimaryAndSecondaryNodes(key) {
    const availableNodes = NODE_LIST.filter((node) => !failedPeers.has(node));
    if (availableNodes.length === 0) return [null, null];

    const hash = key
        .split("")
        .reduce((acc, char) => acc + char.charCodeAt(0), 0);
    const primaryIdx = hash % availableNodes.length;
    const secondaryIdx = (primaryIdx + 1) % availableNodes.length;
    return [availableNodes[primaryIdx], availableNodes[secondaryIdx]];
}

// Heartbeat management
function startHeartbeat() {
    setInterval(() => {
        for (const peerId in peerConnections) {
            const peer = peerConnections[peerId];
            if (peer && peer.connected) {
                peer.emit("heartbeat", { from: nodeId, timestamp: Date.now() });
            }
        }
    }, HEARTBEAT_INTERVAL);
}

function checkHeartbeatTimeout() {
    setInterval(() => {
        const now = Date.now();
        for (const peerId of NODE_LIST) {
            if (peerId === nodeId) continue;

            // Nếu chưa có heartbeat từ peer này
            if (!lastHeartbeat[peerId]) {
                // Nếu peer đang trong failedPeers, xóa nó ra
                if (failedPeers.has(peerId)) {
                    failedPeers.delete(peerId);
                    console.log(
                        `[${nodeId}] Node recovered (first heartbeat): ${peerId}`
                    );
                    handleNodeRecovery(peerId);
                }
                continue;
            }

            // Kiểm tra timeout
            if (now - lastHeartbeat[peerId] > HEARTBEAT_TIMEOUT) {
                if (!failedPeers.has(peerId)) {
                    failedPeers.add(peerId);
                    console.log(`[${nodeId}] Node failed: ${peerId}`);
                    handleNodeFailure(peerId);
                }
            } else if (failedPeers.has(peerId)) {
                failedPeers.delete(peerId);
                console.log(`[${nodeId}] Node recovered: ${peerId}`);
                handleNodeRecovery(peerId);
            }
        }
    }, 1000);
}

// Snapshot management
function handleSnapshot(data, socket) {
    if (!data || data.to !== nodeId || !data.data) return;

    const peerInfo = socket?.handshake?.address || socket?.id || "peerSocket";
    console.log(`[${nodeId}] Received snapshot from ${peerInfo}`);

    let updatedKeys = 0;
    let removedKeys = 0;

    // Remove keys that this node is no longer responsible for
    for (const key of Object.keys(store)) {
        const [primary, secondary] = getPrimaryAndSecondaryNodes(key);
        if (!(nodeId === primary || nodeId === secondary)) {
            delete store[key];
            removedKeys++;
        }
    }

    // Update values from snapshot for keys this node is responsible for
    for (const key in data.data) {
        const [primary, secondary] = getPrimaryAndSecondaryNodes(key);
        if (nodeId === primary || nodeId === secondary) {
            store[key] = data.data[key];
            updatedKeys++;
        }
    }

    saveStoreToFile();
    io.emit("store", store);
    console.log(
        `[${nodeId}] Snapshot processed: ${updatedKeys} updated, ${removedKeys} removed`
    );
}

function tryRequestSnapshot(peerId) {
    const peer = peerConnections[peerId];
    if (peer?.connected) {
        console.log(`[${nodeId}] Requesting snapshot from peer: ${peerId}`);
        peer.emit("request_snapshot", { requester: nodeId });
        pendingSnapshotPeers.delete(peerId);
    } else {
        console.log(
            `[${nodeId}] Peer ${peerId} not connected, adding to pending snapshot requests`
        );
        pendingSnapshotPeers.add(peerId);
    }
}

// Peer connection management
function connectToPeer(peer) {
    console.log(`[${nodeId}] Connecting to peer: ${peer.id}`);

    const peerSocket = socketIoClient(`http://${peer.host}:${peer.port}`, {
        reconnection: true,
        reconnectionAttempts: Infinity,
        reconnectionDelay: 5000,
        reconnectionDelayMax: 20000,
        timeout: 30000,
        transports: ["websocket", "polling"],
        forceNew: true,
        multiplex: false,
    });

    peerConnections[peer.id] = peerSocket;
    setupPeerEventHandlers(peerSocket, peer.id);
    return peerSocket;
}

function setupPeerEventHandlers(peerSocket, peerId) {
    peerSocket.on("heartbeat", (data) => {
        if (data?.from && NODE_LIST.includes(data.from)) {
            lastHeartbeat[data.from] = data.timestamp;
        }
    });

    peerSocket.on("connect", () => {
        console.log(`[${nodeId}] Connected to peer: ${peerId}`);
        handlePeerConnection(peerId);
    });

    peerSocket.on("disconnect", (reason) => {
        console.log(`[${nodeId}] Disconnected from peer ${peerId}: ${reason}`);
    });

    peerSocket.on("replicate", handleReplicate);
    peerSocket.on("replicateDelete", handleReplicateDelete);
    peerSocket.on("snapshot", (data) => handleSnapshot(data, peerSocket));
}

// Event handlers
function handlePeerConnection(peerId) {
    if (pendingSnapshotPeers.has(peerId)) {
        const snapshot = { ...store };
        const peer = peerConnections[peerId];

        if (peer && peer.connected) {
            peer.emit("snapshot", {
                to: peerId,
                from: nodeId,
                data: snapshot,
                timestamp: Date.now(),
            });
            pendingSnapshotPeers.delete(peerId);
        }
    }
}

function handleReplicate(data) {
    const [primary, secondary] = getPrimaryAndSecondaryNodes(data.key);
    if (nodeId === primary || nodeId === secondary) {
        console.log(`[${nodeId}] Replicating key: ${data.key}`);
        store[data.key] = data.value;
        saveStoreToFile();
        io.emit("update", { key: data.key, value: data.value });
    }
}

function handleReplicateDelete(data) {
    const [primary, secondary] = getPrimaryAndSecondaryNodes(data.key);
    if (nodeId === primary || nodeId === secondary) {
        console.log(`[${nodeId}] Replicating delete for key: ${data.key}`);
        delete store[data.key];
        saveStoreToFile();
        io.emit("delete", data.key);
    }
}

function handleNodeFailure(peerId) {
    // Implement node failure handling logic
    console.log(`[${nodeId}] Handling failure of node: ${peerId}`);
}

function handleNodeRecovery(peerId) {
    // Implement node recovery handling logic
    console.log(`[${nodeId}] Handling recovery of node: ${peerId}`);
}

// Socket.IO event handlers
io.on("connection", (socket) => {
    console.log(`[${nodeId}] New client connected`);
    socket.emit("store", store);

    socket.on("heartbeat", (data) => {
        if (data?.from && NODE_LIST.includes(data.from)) {
            lastHeartbeat[data.from] = data.timestamp;
        }
    });

    socket.on("put", handlePut);
    socket.on("get", (key) => handleGet(key, socket));
    socket.on("delete", (data) => {
        // Xử lý cả trường hợp data là string hoặc object
        if (typeof data === "string") {
            handleDelete(data, false);
        } else {
            handleDelete(data.key, data.isBroadcast);
        }
    });
    socket.on("replicate", handleReplicate);
    socket.on("replicateDelete", handleReplicateDelete);
    socket.on("request_snapshot", handleRequestSnapshot);
    socket.on("snapshot", (data) => handleSnapshot(data, socket));
});

// Request handlers
function handlePut(data) {
    // Kiểm tra kích thước store trước khi thêm
    if (Object.keys(store).length >= MAX_STORE_SIZE) {
        console.log(`[${nodeId}] Store is full, cannot add more data`);
        return;
    }

    const [primary, secondary] = getPrimaryAndSecondaryNodes(data.key);
    console.log(
        `[${nodeId}] PUT ${data.key}: primary=${primary}, secondary=${secondary}`
    );

    if (!primary || !secondary) {
        console.log(`[${nodeId}] Not enough nodes for replication`);
        return;
    }

    if (nodeId === primary && nodeId === secondary) {
        store[data.key] = data.value;
        saveStoreToFile();
        io.emit("update", { key: data.key, value: data.value });
        return;
    }

    if (nodeId === secondary) {
        forwardToPrimary(primary, "put", data);
        return;
    }

    if (nodeId === primary) {
        store[data.key] = data.value;
        saveStoreToFile();
        io.emit("update", { key: data.key, value: data.value });

        if (secondary !== primary && peerConnections[secondary]) {
            peerConnections[secondary].emit("replicate", {
                key: data.key,
                value: data.value,
                fromPeer: nodeId,
            });
        }
    } else {
        forwardToPrimary(primary, "put", data);
    }
}

function handleGet(key, socket) {
    const [primary, secondary] = getPrimaryAndSecondaryNodes(key);
    console.log(
        `[${nodeId}] GET ${key}: primary=${primary}, secondary=${secondary}`
    );

    if (!primary && !secondary) {
        console.log(`[${nodeId}] No available nodes for key: ${key}`);
        socket.emit("value", { key, value: undefined });
        return;
    }

    if (store[key] !== undefined) {
        console.log(`[${nodeId}] Found key in local store: ${key}`);
        socket.emit("value", { key, value: store[key] });
        return;
    }

    forwardGetRequest(key, primary, secondary, socket);
}

function handleDelete(key, isBroadcast = false) {
    const realKey = typeof key === "string" ? key : key.key;
    console.log(
        `[${nodeId}] Handling delete request for key: ${realKey} (isBroadcast: ${isBroadcast})`
    );

    if (store[realKey] !== undefined) {
        console.log(`[${nodeId}] Deleting key from local store: ${realKey}`);
        delete store[realKey];
        saveStoreToFile();
        io.emit("delete", realKey);
        console.log(`[${nodeId}] Key deleted successfully: ${realKey}`);
    } else {
        console.log(`[${nodeId}] Key not found in local store: ${realKey}`);
    }

    // Chỉ broadcast nếu đây là request delete ban đầu, không phải từ broadcast
    if (!isBroadcast) {
        broadcastDelete(realKey);
    }
}

function handleRequestSnapshot(data) {
    if (!data?.requester) return;

    console.log(`[${nodeId}] Received snapshot request from ${data.requester}`);
    const peer = peerConnections[data.requester];
    const snapshot = { ...store };

    if (peer?.connected) {
        peer.emit("snapshot", {
            to: data.requester,
            from: nodeId,
            data: snapshot,
            timestamp: data.timestamp,
        });
    } else {
        pendingSnapshotPeers.add(data.requester);
    }
}

// Helper functions
function forwardToPrimary(primary, event, data) {
    if (peerConnections[primary]) {
        console.log(`[${nodeId}] Forwarding ${event} to primary: ${primary}`);
        peerConnections[primary].emit(event, data);
    } else {
        console.log(
            `[${nodeId}] Cannot forward to primary ${primary} - not connected`
        );
    }
}

function forwardGetRequest(key, primary, secondary, socket) {
    let responded = false;

    if (peerConnections[primary]) {
        peerConnections[primary].emit("get", key);
        peerConnections[primary].once("value", (data) => {
            if (!responded) {
                responded = true;
                socket.emit("value", data);
            }
        });
    }

    if (peerConnections[secondary]) {
        setTimeout(() => {
            if (!responded) {
                peerConnections[secondary].emit("get", key);
                peerConnections[secondary].once("value", (data) => {
                    if (!responded) {
                        responded = true;
                        socket.emit("value", data);
                    }
                });
            }
        }, 500);
    }

    setTimeout(() => {
        if (!responded) {
            console.log(`[${nodeId}] GET failed for key: ${key}`);
            socket.emit("value", { key, value: undefined });
        }
    }, 1000);
}

function broadcastDelete(key) {
    console.log(`[${nodeId}] Broadcasting delete for key: ${key}`);
    let broadcastCount = 0;

    for (const peerId in peerConnections) {
        const peer = peerConnections[peerId];
        if (peer?.connected) {
            // Gửi kèm flag isBroadcast để các node khác biết đây là broadcast
            peer.emit("delete", { key, isBroadcast: true });
            broadcastCount++;
            console.log(`[${nodeId}] Delete broadcasted to peer: ${peerId}`);
        } else {
            console.log(
                `[${nodeId}] Cannot broadcast to peer ${peerId} - not connected`
            );
        }
    }

    console.log(
        `[${nodeId}] Delete broadcast completed. Sent to ${broadcastCount} peers`
    );
}

// Initialize
loadStoreFromFile();

// Start server
const PORT = config.port;
server.listen(PORT, () => {
    console.log(`[${nodeId}] Server running on port ${PORT}`);

    // Connect to peers after a delay
    setTimeout(() => {
        console.log(`[${nodeId}] Connecting to peers...`);
        config.peers.forEach(connectToPeer);
    }, PEER_CONNECTION_DELAY);

    // Start heartbeat and failure detection
    startHeartbeat();
    checkHeartbeatTimeout();

    // Request snapshot from peers after a delay
    setTimeout(() => {
        console.log(`[${nodeId}] Requesting snapshot from peers...`);
        for (const peerId in peerConnections) {
            tryRequestSnapshot(peerId);
        }
    }, SNAPSHOT_REQUEST_DELAY);
});

// Error handling
process.on("uncaughtException", (err) => {
    console.error(`[${nodeId}] Uncaught Exception:`, err);
    // Cleanup before exit
    for (const peerId in peerConnections) {
        if (peerConnections[peerId]) {
            peerConnections[peerId].disconnect();
        }
    }
    process.exit(1);
});

process.on("unhandledRejection", (reason) => {
    console.error(`[${nodeId}] Unhandled Rejection:`, reason);
});

// Memory leak prevention
setInterval(() => {
    global.gc && global.gc();
}, 30000);

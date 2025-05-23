const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const { io: socketIoClient } = require("socket.io-client");
const cors = require("cors");
const nodeConfigs = require("./config");
const fs = require("fs");
const path = require("path");

// Lấy node ID từ tham số dòng lệnh
const nodeId = process.argv[2] || "node1";
const config = nodeConfigs[nodeId];

if (!config) {
    console.error("Invalid node ID");
    process.exit(1);
}

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"],
    },
    pingTimeout: 60000,
    pingInterval: 25000,
});

// Lưu trữ dữ liệu key-value
const store = {};

// Kết nối với các node khác
const peerConnections = {};

const nodeList = ["node1", "node2", "node3"];

const heartbeatInterval = 2000; // ms
const heartbeatTimeout = 2000; // ms (giảm xuống để phát hiện node chết nhanh hơn)
const lastHeartbeat = {};
const failedPeers = new Set();
const pendingSnapshotPeers = new Set();

const storeFile = path.join(__dirname, `store_${nodeId}.json`);

// Hàm lưu store ra file
function saveStoreToFile() {
    fs.writeFileSync(storeFile, JSON.stringify(store, null, 2), "utf-8");
}

// Hàm nạp store từ file (nếu có)
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

// Nạp store khi khởi động
loadStoreFromFile();

// Hàm xử lý snapshot dùng chung
function handleSnapshot(data, socket) {
    if (!data || data.to !== nodeId || !data.data) return;
    const peerInfo =
        socket && socket.handshake && socket.handshake.address
            ? socket.handshake.address
            : socket && socket.id
            ? socket.id
            : "peerSocket";
    console.log(
        `[${nodeId}] [snapshot] Received snapshot from peer (${peerInfo}) with keys:`,
        Object.keys(data.data)
    );
    let updatedKeys = 0;
    let removedKeys = 0;

    // Xóa các key mà node không còn là primary/secondary
    for (const key of Object.keys(store)) {
        const [primary, secondary] = getPrimaryAndSecondaryNodes(key);
        if (!(nodeId === primary || nodeId === secondary)) {
            delete store[key];
            removedKeys++;
        }
    }

    // Cập nhật value mới từ snapshot cho các key có trong snapshot mà node là primary/secondary
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
        `[${nodeId}] [snapshot] Restored ${updatedKeys} keys from snapshot, removed ${removedKeys} keys. Current store:`,
        store
    );
}

function connectToPeer(peer) {
    console.log(
        `Attempting to connect to peer: ${peer.id} at http://${peer.host}:${peer.port}`
    );

    const peerSocket = socketIoClient(`http://${peer.host}:${peer.port}`, {
        reconnection: true,
        reconnectionAttempts: Infinity,
        reconnectionDelay: 2000,
        reconnectionDelayMax: 10000,
        timeout: 20000,
        transports: ["websocket", "polling"],
    });

    peerConnections[peer.id] = peerSocket;

    // Đảm bảo nhận heartbeat và cập nhật lastHeartbeat đúng peerId
    peerSocket.on("heartbeat", (data) => {
        if (data && data.from && nodeList.includes(data.from)) {
            lastHeartbeat[data.from] = data.timestamp;
            // console.log(
            //     `[${nodeId}] [heartbeat] Received heartbeat from ${data.from} at ${data.timestamp}`
            // );
        }
    });

    peerSocket.on("connect", () => {
        console.log(`[${nodeId}] Successfully connected to peer: ${peer.id}`);
        if (pendingSnapshotPeers.has(peer.id)) {
            console.log(
                `[${nodeId}] [connect] Peer ${peer.id} vừa connected, gửi lại snapshot`
            );
            const snapshot = { ...store };
            peerSocket.emit("snapshot", {
                to: peer.id,
                from: nodeId,
                data: snapshot,
                timestamp: Date.now(),
            });
            pendingSnapshotPeers.delete(peer.id);
        }
        if (pendingSnapshotPeers.has(peer.id)) {
            console.log(
                `[${nodeId}] [connect] Peer ${peer.id} vừa connected, gửi lại request_snapshot`
            );
            tryRequestSnapshot(peer.id);
        }
    });

    peerSocket.on("disconnect", (reason) => {
        console.log(`Disconnected from peer: ${peer.id}, reason: ${reason}`);
    });

    peerSocket.on("connect_error", (error) => {
        console.log(`Connection error with peer ${peer.id}:`, error.message);
        // Thử kết nối lại sau một khoảng thời gian
        setTimeout(() => {
            if (!peerSocket.connected) {
                console.log(`Retrying connection to peer ${peer.id}...`);
                peerSocket.connect();
            }
        }, 5000);
    });

    peerSocket.on("reconnect", (attemptNumber) => {
        console.log(
            `Reconnected to peer ${peer.id} after ${attemptNumber} attempts`
        );
    });

    peerSocket.on("reconnect_error", (error) => {
        console.log(`Reconnection error with peer ${peer.id}:`, error.message);
    });

    peerSocket.on("replicate", (data) => {
        const [primary, secondary] = getPrimaryAndSecondaryNodes(
            data.key,
            nodeId
        );
        if (nodeId === primary || nodeId === secondary) {
            console.log(
                `[${nodeId}] Nhận replicate và lưu key '${data.key}' vào store local (REPLICATE, server-side)`
            );
            store[data.key] = data.value;
            saveStoreToFile();
            io.emit("update", { key: data.key, value: data.value });
        }
    });

    peerSocket.on("replicateDelete", (data) => {
        const [primary, secondary] = getPrimaryAndSecondaryNodes(
            data.key,
            nodeId
        );
        if (nodeId === primary || nodeId === secondary) {
            console.log(
                `[${nodeId}] Nhận replicateDelete và xóa key '${data.key}' khỏi store local (REPLICATE, server-side)`
            );
            delete store[data.key];
            saveStoreToFile();
            io.emit("delete", data.key);
        }
    });

    peerSocket.on("snapshot", (data) => {
        handleSnapshot(data, socket);
    });

    return peerSocket;
}

function connectToPeers() {
    // Đợi server khởi động hoàn toàn trước khi kết nối với peers
    setTimeout(() => {
        console.log(`Starting peer connections for node ${nodeId}...`);
        config.peers.forEach((peer) => {
            connectToPeer(peer);
        });
    }, 2000); // Đợi 2 giây để đảm bảo server đã sẵn sàng
}

// Hàm xác định node nào sẽ lưu trữ key
function getNodeForKey(key) {
    const nodes = nodeList.filter((node) => !failedPeers.has(node));
    if (nodes.length === 0) return null;

    const hash = key
        .split("")
        .reduce((acc, char) => acc + char.charCodeAt(0), 0);
    return nodes[hash % nodes.length];
}

function getPrimaryAndSecondaryNodes(key) {
    const availableNodes = nodeList.filter((node) => !failedPeers.has(node));
    // console.log(
    //     `[${nodeId}] [hash] availableNodes for key '${key}':`,
    //     availableNodes
    // );
    if (availableNodes.length === 0) return [null, null];
    const hash = key
        .split("")
        .reduce((acc, char) => acc + char.charCodeAt(0), 0);
    const primaryIdx = hash % availableNodes.length;
    const secondaryIdx = (primaryIdx + 1) % availableNodes.length;
    return [availableNodes[primaryIdx], availableNodes[secondaryIdx]];
}

function startHeartbeat() {
    setInterval(() => {
        for (const peerId in peerConnections) {
            const peer = peerConnections[peerId];
            if (peer && peer.connected) {
                peer.emit("heartbeat", { from: nodeId, timestamp: Date.now() });
            }
        }
    }, heartbeatInterval);
}

function checkHeartbeatTimeout() {
    setInterval(() => {
        const now = Date.now();
        // console.log(
        //     `[${nodeId}] lastHeartbeat:`,
        //     JSON.stringify(lastHeartbeat)
        // );
        // console.log(
        //     `[${nodeId}] failedPeers:`,
        //     JSON.stringify(Array.from(failedPeers))
        // );
        for (const peerId of nodeList) {
            if (peerId === nodeId) continue; // Không kiểm tra chính mình
            if (
                lastHeartbeat[peerId] &&
                now - lastHeartbeat[peerId] > heartbeatTimeout
            ) {
                if (!failedPeers.has(peerId)) {
                    failedPeers.add(peerId);
                    console.log(
                        `[${nodeId}] Phát hiện node hỏng: ${peerId}. failedPeers=${JSON.stringify(
                            Array.from(failedPeers)
                        )}`
                    );
                }
            } else {
                if (
                    failedPeers.has(peerId) &&
                    lastHeartbeat[peerId] &&
                    now - lastHeartbeat[peerId] <= heartbeatTimeout
                ) {
                    failedPeers.delete(peerId);
                    console.log(
                        `[${nodeId}] Node đã hồi phục: ${peerId}. failedPeers=${JSON.stringify(
                            Array.from(failedPeers)
                        )}`
                    );
                }
            }
        }
    }, 1000);
}

function tryRequestSnapshot(peerId) {
    const peer = peerConnections[peerId];
    if (peer && peer.connected) {
        console.log(
            `[${nodeId}] [tryRequestSnapshot] Gửi request_snapshot tới peer: ${peerId}`
        );
        peer.emit("request_snapshot", { requester: nodeId });
        pendingSnapshotPeers.delete(peerId);
    } else {
        console.log(
            `[${nodeId}] [tryRequestSnapshot] Peer ${peerId} chưa connected, thêm vào pendingSnapshotPeers`
        );
        pendingSnapshotPeers.add(peerId);
    }
}

function requestSnapshotFromPeers() {
    for (const peerId in peerConnections) {
        tryRequestSnapshot(peerId);
    }
}

function getNextAvailableNode(sourceNode) {
    const nodes = nodeList;
    const currentIndex = nodes.indexOf(sourceNode);

    // Tìm node tiếp theo còn hoạt động
    for (let i = 1; i < nodes.length; i++) {
        const nextIndex = (currentIndex + i) % nodes.length;
        const nextNode = nodes[nextIndex];
        if (!failedPeers.has(nextNode)) {
            return nextNode;
        }
    }
    return null; // Trả về null nếu không tìm thấy node nào còn hoạt động
}

// Xử lý các kết nối socket
io.on("connection", (socket) => {
    console.log(`[${nodeId}] New client connected`);
    socket.emit("store", store); // Gửi toàn bộ store cho client mới

    // Lắng nghe heartbeat từ các peer kết nối inbound
    socket.on("heartbeat", (data) => {
        if (data && data.from && nodeList.includes(data.from)) {
            lastHeartbeat[data.from] = data.timestamp;
        }
    });

    // Xử lý các sự kiện từ client
    socket.on("put", (data) => {
        const [primary, secondary] = getPrimaryAndSecondaryNodes(data.key);
        console.log(
            `[${nodeId}] PUT key='${data.key}', value='${data.value}', primary=${primary}, secondary=${secondary}, current=${nodeId}`
        );
        if (!primary || !secondary) {
            console.log(
                `[${nodeId}] WARNING: Not enough available nodes for replication`
            );
            return;
        }
        // Nếu chỉ còn 1 node sống, primary === secondary === nodeId
        if (nodeId === primary && nodeId === secondary) {
            store[data.key] = data.value;
            saveStoreToFile();
            io.emit("update", { key: data.key, value: data.value });
            // Không cần replicate
            return;
        }
        if (nodeId === secondary) {
            console.log(
                `[${nodeId}] Forwarding PUT request to primary: ${primary}`
            );
            if (peerConnections[primary]) {
                peerConnections[primary].emit("put", data);
            } else {
                console.log(
                    `[${nodeId}] WARNING: Cannot forward to primary ${primary} - not connected`
                );
            }
            return;
        }
        if (nodeId === primary) {
            console.log(`[${nodeId}] Storing key '${data.key}' as primary`);
            store[data.key] = data.value;
            saveStoreToFile();
            io.emit("update", { key: data.key, value: data.value });
            if (secondary !== primary && peerConnections[secondary]) {
                console.log(
                    `[${nodeId}] Replicating key '${data.key}' to secondary: ${secondary}`
                );
                peerConnections[secondary].emit("replicate", {
                    key: data.key,
                    value: data.value,
                    fromPeer: nodeId,
                });
            } else if (secondary !== primary) {
                console.log(
                    `[${nodeId}] WARNING: Cannot replicate to ${secondary} - not connected`
                );
            }
        } else if (peerConnections[primary]) {
            console.log(
                `[${nodeId}] Forwarding PUT request to primary: ${primary}`
            );
            peerConnections[primary].emit("put", data);
        }
    });

    socket.on("get", (key) => {
        const [primary, secondary] = getPrimaryAndSecondaryNodes(key);
        console.log(
            `[${nodeId}] GET key='${key}', primary=${primary}, secondary=${secondary}, current=${nodeId}`
        );
        if (!primary && !secondary) {
            console.log(`[${nodeId}] No available nodes for key '${key}'`);
            socket.emit("value", { key, value: undefined });
            return;
        }
        if (store[key] !== undefined) {
            console.log(`[${nodeId}] Found key '${key}' in local store`);
            socket.emit("value", { key, value: store[key] });
            return;
        }
        let responded = false;
        if (peerConnections[primary]) {
            console.log(`[${nodeId}] Forwarding GET to primary: ${primary}`);
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
                    console.log(
                        `[${nodeId}] Forwarding GET to secondary: ${secondary}`
                    );
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
                console.log(
                    `[${nodeId}] GET for key '${key}' failed: no response from peers`
                );
                socket.emit("value", { key, value: undefined });
            }
        }, 1000);
    });
    socket.on("delete", (key) => {
        const realKey = typeof key === "string" ? key : key.key;

        // Xóa local nếu có
        if (store[realKey] !== undefined) {
            delete store[realKey];
            saveStoreToFile();
            io.emit("delete", realKey);
        }

        // Gửi sự kiện delete đến tất cả các peer (broadcast, các node không lưu key sẽ không bị ảnh hưởng)
        for (const peerId in peerConnections) {
            if (peerConnections[peerId] && peerConnections[peerId].connected) {
                peerConnections[peerId].emit("delete", realKey);
            }
        }
    });

    socket.on("replicate", (data) => {
        console.log(
            `[${nodeId}] REPLICATE key='${data.key}', value='${data.value}', fromPeer=${data.fromPeer}, current=${nodeId}`
        );
        store[data.key] = data.value;
        saveStoreToFile();
        io.emit("update", { key: data.key, value: data.value });
    });

    socket.on("replicateDelete", (data) => {
        console.log(
            `[${nodeId}] REPLICATE_DELETE key='${data.key}', fromPeer=${data.fromPeer}, current=${nodeId}`
        );
        delete store[data.key];
        saveStoreToFile();
        io.emit("delete", data.key);
    });

    socket.on("request_snapshot", (data) => {
        if (!data || !data.requester) return;
        console.log(
            `[${nodeId}] [request_snapshot] Nhận request_snapshot từ ${data.requester}`
        );
        const peer = peerConnections[data.requester];
        const snapshot = { ...store };
        if (peer && peer.connected) {
            console.log(
                `[${nodeId}] [request_snapshot] Sending snapshot to ${data.requester} with keys:`,
                Object.keys(snapshot)
            );
            peer.emit("snapshot", {
                to: data.requester,
                from: nodeId,
                data: snapshot,
                timestamp: data.timestamp,
            });
        } else {
            console.log(
                `[${nodeId}] [request_snapshot] Peer ${data.requester} chưa connected, thêm vào pendingSnapshotPeers để gửi snapshot sau`
            );
            pendingSnapshotPeers.add(data.requester);
        }
    });

    socket.on("snapshot", (data) => {
        handleSnapshot(data, socket);
    });

    socket.on("disconnect", () => {
        // ... giữ lại logic nếu cần ...
    });
});

// Khởi động server
const PORT = config.port;
server.listen(PORT, () => {
    console.log(`Node ${nodeId} running on port ${PORT}`);
    connectToPeers();
    startHeartbeat();
    checkHeartbeatTimeout();
    setTimeout(() => {
        console.log(`[${nodeId}] [startup] Requesting snapshot from peers...`);
        requestSnapshotFromPeers();
    }, 5000); // Đợi 5 giây trước khi request snapshot
});

process.on("uncaughtException", function (err) {
    console.error("Uncaught Exception:", err);
});
process.on("unhandledRejection", function (reason, promise) {
    console.error("Unhandled Rejection:", reason);
});

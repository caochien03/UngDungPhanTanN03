const nodeConfigs = {
    node1: {
        id: "node1",
        port: 8080,
        peers: [
            { id: "node2", host: "localhost", port: 8081 },
            { id: "node3", host: "localhost", port: 8082 },
        ],
    },
    node2: {
        id: "node2",
        port: 8081,
        peers: [
            { id: "node1", host: "localhost", port: 8080 },
            { id: "node3", host: "localhost", port: 8082 },
        ],
    },
    node3: {
        id: "node3",
        port: 8082,
        peers: [
            { id: "node1", host: "localhost", port: 8080 },
            { id: "node2", host: "localhost", port: 8081 },
        ],
    },
};

module.exports = nodeConfigs;

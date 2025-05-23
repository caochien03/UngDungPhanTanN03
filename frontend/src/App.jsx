import React, { useState, useEffect } from "react";
import io from "socket.io-client";
import "./App.css";

const NODES = [
    { id: "node1", port: 8080 },
    { id: "node2", port: 8081 },
    { id: "node3", port: 8082 },
];

function App() {
    const [selectedNode, setSelectedNode] = useState(NODES[0]);
    const [socket, setSocket] = useState(null);
    const [isConnected, setIsConnected] = useState(false);
    const [key, setKey] = useState("");
    const [value, setValue] = useState("");
    const [store, setStore] = useState({});
    const [error, setError] = useState(null);
    const [getResult, setGetResult] = useState(null);

    useEffect(() => {
        // Kết nối đến node được chọn
        const newSocket = io(`http://localhost:${selectedNode.port}`, {
            reconnection: true,
            reconnectionAttempts: Infinity,
            reconnectionDelay: 1000,
            timeout: 20000,
        });

        newSocket.on("connect", () => {
            console.log(`Connected to ${selectedNode.id}`);
            setIsConnected(true);
            setError(null);
            newSocket.emit("getAll");
        });

        newSocket.on("disconnect", () => {
            console.log(`Disconnected from ${selectedNode.id}`);
            setIsConnected(false);
        });

        newSocket.on("connect_error", (error) => {
            console.error(`Connection error: ${error.message}`);
            setError(`Cannot connect to ${selectedNode.id}: ${error.message}`);
            setIsConnected(false);
        });

        newSocket.on("update", (data) => {
            console.log("Received update:", data);
            setStore((prevStore) => ({ ...prevStore, [data.key]: data.value }));
        });

        newSocket.on("delete", (data) => {
            const key = typeof data === "string" ? data : data.key;
            setStore((prevStore) => {
                const newStore = { ...prevStore };
                delete newStore[key];
                return newStore;
            });
        });

        newSocket.on("store", (data) => {
            setStore(data);
        });

        setSocket(newSocket);

        return () => {
            newSocket.close();
        };
    }, [selectedNode]);

    const handlePut = () => {
        if (!key || !value) {
            setError("Key and value are required");
            return;
        }
        socket.emit("put", { key, value });
        setError(null);
    };

    const handleGet = () => {
        if (!key) {
            setError("Key is required");
            return;
        }
        socket.emit("get", key);
        socket.once("value", (data) => {
            setGetResult(
                data.value !== undefined ? data.value : "Không tồn tại"
            );
            setError(null);
        });
    };

    const handleDelete = () => {
        if (!key) {
            setError("Key is required");
            return;
        }
        socket.emit("delete", key);
        setKey("");
        setValue("");
        setError(null);
    };

    return (
        <div className="container">
            <h1>Distributed Key-Value Store</h1>

            <div className="node-selector">
                <label>Select Node: </label>
                <select
                    value={selectedNode.id}
                    onChange={(e) =>
                        setSelectedNode(
                            NODES.find((n) => n.id === e.target.value)
                        )
                    }
                >
                    {NODES.map((node) => (
                        <option key={node.id} value={node.id}>
                            {node.id} (Port {node.port})
                        </option>
                    ))}
                </select>
                <span
                    className={`connection-status ${
                        isConnected ? "connected" : "disconnected"
                    }`}
                >
                    {isConnected ? "Connected" : "Disconnected"}
                </span>
            </div>

            {error && <div className="error-message">{error}</div>}

            <div className="input-group">
                <input
                    type="text"
                    placeholder="Key"
                    value={key}
                    onChange={(e) => setKey(e.target.value)}
                />
                <input
                    type="text"
                    placeholder="Value"
                    value={value}
                    onChange={(e) => setValue(e.target.value)}
                />
            </div>

            <div className="button-group">
                <button onClick={handlePut} disabled={!isConnected}>
                    PUT
                </button>
                <button onClick={handleGet} disabled={!isConnected}>
                    GET
                </button>
                <button onClick={handleDelete} disabled={!isConnected}>
                    DELETE
                </button>
            </div>

            <div className="store-display">
                <h2>Current Store State (Local):</h2>
                <pre>{JSON.stringify(store, null, 2)}</pre>
                <div>
                    <strong>Kết quả GET:</strong>{" "}
                    {getResult !== null ? getResult : "(Chưa truy vấn)"}
                </div>
            </div>
        </div>
    );
}

export default App;

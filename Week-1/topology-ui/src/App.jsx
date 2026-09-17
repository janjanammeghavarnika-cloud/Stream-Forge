import { useEffect, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";
import "./App.css";

const initialNodes = [
  {
    id: "kafka",
    position: { x: 50, y: 220 },
    data: { label: "Apache Kafka\nTruck Telemetry" },
  },
  {
    id: "processor",
    position: { x: 350, y: 220 },
    data: { label: "Stream Processor\nFaust / Bytewax" },
  },
  {
    id: "worker-1",
    position: { x: 650, y: 100 },
    data: { label: "Worker 1\nWaiting for metrics..." },
  },
  {
    id: "worker-2",
    position: { x: 650, y: 340 },
    data: { label: "Worker 2\nWaiting for metrics..." },
  },
  {
    id: "state",
    position: { x: 950, y: 220 },
    data: { label: "RocksDB\nState Store" },
  },
];

const initialEdges = [
  {
    id: "kafka-processor",
    source: "kafka",
    target: "processor",
    animated: true,
  },
  {
    id: "processor-worker1",
    source: "processor",
    target: "worker-1",
    animated: true,
  },
  {
    id: "processor-worker2",
    source: "processor",
    target: "worker-2",
    animated: true,
  },
  {
    id: "worker1-state",
    source: "worker-1",
    target: "state",
    animated: true,
  },
  {
    id: "worker2-state",
    source: "worker-2",
    target: "state",
    animated: true,
  },
];

function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, , onEdgesChange] = useEdgesState(initialEdges);

  const [metrics, setMetrics] = useState({});
  const [bottleneck, setBottleneck] = useState("Checking...");

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const response = await fetch("http://localhost:8000/metrics");
        const text = await response.text();

        const workerMetrics = {};

        for (const worker of ["worker-1", "worker-2"]) {
          const workerData = {};

          const eventsMatch = text.match(
            new RegExp(
              `streamforge_events_processed_total\\{worker="${worker}"\\}\\s+([\\d.]+)`
            )
          );

          const rateMatch = text.match(
            new RegExp(
              `streamforge_events_per_second\\{worker="${worker}"\\}\\s+([\\d.]+)`
            )
          );

          const lagMatch = text.match(
            new RegExp(
              `streamforge_processing_lag_ms\\{worker="${worker}"\\}\\s+([\\d.]+)`
            )
          );

          const statusMatch = text.match(
            new RegExp(
              `streamforge_worker_status\\{worker="${worker}"\\}\\s+([\\d.]+)`
            )
          );

          const processingTimeMatch = text.match(
            new RegExp(
              `streamforge_processing_time_seconds_sum\\{worker="${worker}"\\}\\s+([\\d.]+)`
            )
          );

          workerData.events = eventsMatch
            ? Number(eventsMatch[1])
            : 0;

          workerData.rate = rateMatch
            ? Number(rateMatch[1])
            : 0;

          workerData.lag = lagMatch
            ? Number(lagMatch[1])
            : 0;

          workerData.status = statusMatch
            ? Number(statusMatch[1])
            : 0;

          workerData.processingTime = processingTimeMatch
            ? Number(processingTimeMatch[1])
            : 0;

          workerMetrics[worker] = workerData;
        }

        setMetrics(workerMetrics);

        const activeWorkers = Object.entries(workerMetrics).filter(
          ([, data]) => data.status === 1
        );

        if (activeWorkers.length > 0) {
          const slowest = activeWorkers.reduce((previous, current) =>
            current[1].lag > previous[1].lag ? current : previous
          );

          setBottleneck(slowest[0]);
        } else {
          setBottleneck("No active workers");
        }
      } catch (error) {
        console.error("Failed to fetch metrics:", error);
      }
    };

    fetchMetrics();

    const interval = setInterval(fetchMetrics, 3000);

    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    setNodes((currentNodes) =>
      currentNodes.map((node) => {
        if (node.id === "worker-1" || node.id === "worker-2") {
          const worker = node.id;
          const data = metrics[worker];

          if (!data) {
            return node;
          }

          const isBottleneck = bottleneck === worker;

          return {
            ...node,
            data: {
              label:
                `${worker.toUpperCase()}\n` +
                `Events: ${data.events.toFixed(0)}\n` +
                `Rate: ${data.rate.toFixed(2)} events/sec\n` +
                `Lag: ${data.lag.toFixed(2)} ms\n` +
                `Status: ${data.status === 1 ? "RUNNING" : "STOPPED"}` +
                (isBottleneck ? "\n⚠ BOTTLENECK" : ""),
            },
            style: {
              border: isBottleneck
                ? "3px solid red"
                : "2px solid #22c55e",
              background: isBottleneck ? "#fee2e2" : "#dcfce7",
              padding: "10px",
              borderRadius: "10px",
              width: 220,
              fontSize: "12px",
              fontWeight: "500",
            },
          };
        }

        return node;
      })
    );
  }, [metrics, bottleneck, setNodes]);

  return (
    <div className="app">
      <header className="header">
        <h1>StreamForge Topology Monitor</h1>
        <p>Distributed Python Event Processing Engine</p>

        <div className="dashboard-status">
          <strong>Current Bottleneck:</strong>{" "}
          {bottleneck.toUpperCase()}
        </div>
      </header>

      <div className="flow-container">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          fitView
        >
          <Background />
          <Controls />
          <MiniMap />
        </ReactFlow>
      </div>
    </div>
  );
}

export default App;
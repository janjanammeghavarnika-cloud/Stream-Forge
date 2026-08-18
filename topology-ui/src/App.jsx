import { useCallback } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  addEdge,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";
import "./App.css";

const initialNodes = [
  {
    id: "kafka",
    position: { x: 50, y: 180 },
    data: { label: "Apache Kafka\nTruck Telemetry" },
  },
  {
    id: "processor",
    position: { x: 350, y: 180 },
    data: { label: "Stream Processor\nFaust / Bytewax" },
  },
  {
    id: "workers",
    position: { x: 650, y: 180 },
    data: { label: "Python Workers\n20 Parallel Nodes" },
  },
  {
    id: "state",
    position: { x: 950, y: 180 },
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
    id: "processor-workers",
    source: "processor",
    target: "workers",
    animated: true,
  },
  {
    id: "workers-state",
    source: "workers",
    target: "state",
    animated: true,
  },
];

function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const onConnect = useCallback(
    (connection) =>
      setEdges((currentEdges) => addEdge(connection, currentEdges)),
    [setEdges]
  );

  return (
    <div className="app">
      <header className="header">
        <h1>StreamForge Topology Monitor</h1>
        <p>Distributed Python Event Processing Engine</p>
      </header>

      <div className="flow-container">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
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
"use client";

import React, { useMemo } from "react";
import ReactFlow, {
  Background,
  Node,
  Edge,
  Position,
  MarkerType,
} from "reactflow";
import "reactflow/dist/style.css";

const blue = "#2F69B3";
const orange = "#F9A32A";

export function NodeNetwork() {
  const nodes: Node[] = useMemo(
    () => [
      // 🔵 Protocol Inputs
      {
        id: "mqtt",
        position: { x: 0, y: 50 },
        data: { label: "MQTT" },
        sourcePosition: Position.Right,
        type: "input",
        style: protocolStyle,
      },
      {
        id: "http",
        position: { x: 0, y: 200 },
        data: { label: "HTTP" },
        sourcePosition: Position.Right,
        type: "input",
        style: protocolStyle,
      },
      {
        id: "ws",
        position: { x: 0, y: 350 },
        data: { label: "WebSocket" },
        sourcePosition: Position.Right,
        type: "input",
        style: protocolStyle,
      },
      {
        id: "tcp",
        position: { x: 0, y: 500 },
        data: { label: "TCP" },
        sourcePosition: Position.Right,
        type: "input",
        style: protocolStyle,
      },

      // 🟠 FluxMQ Cluster
      {
        id: "broker-a",
        position: { x: 380, y: 0 },
        data: { label: "FluxMQ Node A" },
        style: brokerStyle,
      },
      {
        id: "broker-b",
        position: { x: 520, y: 250 },
        data: { label: "FluxMQ Node B" },
        style: brokerStyle,
      },
      {
        id: "broker-c",
        position: { x: 240, y: 250 },
        data: { label: "FluxMQ Node C" },
        style: brokerStyle,
      },

      // 🗄 Durable Queue Layer
      {
        id: "storage",
        position: { x: 380, y: 500 },
        data: { label: "Durable Queues" },
        targetPosition: Position.Top,
        type: "output",
        style: storageStyle,
      },

      // 🟣 Consumers
      {
        id: "analytics",
        position: { x: 760, y: 100 },
        data: { label: "Analytics" },
        targetPosition: Position.Left,
        type: "output",
        style: consumerStyle,
      },
      {
        id: "apps",
        position: { x: 760, y: 250 },
        data: { label: "Applications" },
        targetPosition: Position.Left,
        type: "output",
        style: consumerStyle,
      },
      {
        id: "services",
        position: { x: 760, y: 400 },
        data: { label: "Services" },
        targetPosition: Position.Left,
        type: "output",
        style: consumerStyle,
      },
    ],
    [],
  );

  const edges: Edge[] = useMemo(
    () => [
      // Protocols → Cluster
      edge("mqtt", "broker-a"),
      edge("http", "broker-c"),
      edge("ws", "broker-b"),
      edge("tcp", "broker-c"),

      // Cluster mesh (clustering)
      clusterEdge("broker-c", "broker-a"),
      clusterEdge("broker-a", "broker-b"),
      clusterEdge("broker-b", "broker-c"),

      // Cluster → Durable storage
      edge("broker-a", "storage"),
      edge("broker-b", "storage"),
      edge("broker-c", "storage"),

      // Cluster → Consumers
      edge("broker-a", "analytics"),
      edge("broker-b", "apps"),
      edge("broker-c", "services"),
    ],
    [],
  );

  return (
    <div style={{ width: "100%", height: "100%", cursor: "default", pointerEvents: "none" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        panOnDrag={false}
        zoomOnScroll={false}
        zoomOnPinch={false}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        preventScrolling={false}
      >
        <Background gap={32} size={1} color="rgba(47,105,179,0.08)" />
      </ReactFlow>
    </div>
  );
}

/* ---------- Edge Helpers ---------- */

const edge = (source: string, target: string): Edge => ({
  id: `${source}-${target}`,
  source,
  target,
  animated: true,
  style: {
    stroke: blue,
    strokeWidth: 1.5,
  },
  markerEnd: {
    type: MarkerType.ArrowClosed,
    color: blue,
  },
});

const clusterEdge = (a: string, b: string): Edge => ({
  id: `${a}-${b}`,
  source: a,
  target: b,
  animated: true,
  style: {
    stroke: orange,
    strokeWidth: 2,
    strokeDasharray: "6 6",
  },
});

/* ---------- Node Styles ---------- */

const baseBox = {
  borderRadius: 8,
  padding: "10px 14px",
  fontSize: 12,
  fontWeight: 600,
  border: `1px solid ${blue}`,
  color: blue,
  background: "rgba(47,105,179,0.05)",
};

const protocolStyle = {
  ...baseBox,
};

const consumerStyle = {
  ...baseBox,
};

const storageStyle = {
  ...baseBox,
  border: `1px solid ${orange}`,
  color: orange,
  background: "rgba(249,163,42,0.08)",
};

const brokerStyle = {
  borderRadius: 10,
  padding: "14px 18px",
  fontSize: 13,
  fontWeight: 700,
  border: `2px solid ${orange}`,
  color: orange,
  background: "rgba(249,163,42,0.08)",
  boxShadow: "0 0 18px rgba(249,163,42,0.35)",
};

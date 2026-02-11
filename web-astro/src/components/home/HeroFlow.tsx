"use client";

import { useMemo } from "react";
import {
  Background,
  ConnectionMode,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import { SmartBezierEdge } from "@tisoap/react-flow-smart-edge";
import "@xyflow/react/dist/style.css";

const blue = "#2F69B3";
const orange = "#F9A32A";
const green = "#28b828";

const nodeTypes = {
  broker: BrokerNode,
};

const edgeTypes = {
  smart: SmartBezierEdge,
};

export default function HeroFlow() {
  const nodes: Node[] = useMemo(
    () => [
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
        position: { x: 0, y: 250 },
        data: { label: "HTTP" },
        sourcePosition: Position.Right,
        type: "input",
        style: protocolStyle,
      },
      {
        id: "ws",
        position: { x: 0, y: 450 },
        data: { label: "WebSocket" },
        sourcePosition: Position.Right,
        type: "input",
        style: protocolStyle,
      },
      {
        id: "amqp",
        position: { x: 0, y: 650 },
        data: { label: "AMQP" },
        sourcePosition: Position.Right,
        type: "input",
        style: protocolStyle,
      },
      {
        id: "broker-a",
        position: { x: 300, y: 50 },
        data: { label: "FluxMQ Node A" },
        style: brokerStyle,
        type: "broker",
      },
      {
        id: "broker-b",
        position: { x: 300, y: 350 },
        data: { label: "FluxMQ Node B" },
        style: brokerStyle,
        type: "broker",
      },
      {
        id: "broker-c",
        position: { x: 300, y: 600 },
        data: { label: "FluxMQ Node C" },
        style: brokerStyle,
        type: "broker",
      },
      {
        id: "analytics",
        position: { x: 600, y: 150 },
        data: { label: "Analytics" },
        targetPosition: Position.Left,
        type: "output",
        style: consumerStyle,
      },
      {
        id: "apps",
        position: { x: 600, y: 350 },
        data: { label: "Applications" },
        targetPosition: Position.Left,
        type: "output",
        style: consumerStyle,
      },
      {
        id: "services",
        position: { x: 600, y: 550 },
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
      edge("mqtt", "broker-a"),
      edge("mqtt", "broker-b"),
      edge("mqtt", "broker-c"),
      edge("http", "broker-b"),
      edge("http", "broker-c"),
      edge("ws", "broker-a"),
      edge("ws", "broker-b"),
      edge("ws", "broker-c"),
      edge("amqp", "broker-c"),
      clusterEdge("broker-a", "broker-b", "bottom", "top"),
      clusterEdge("broker-b", "broker-c", "bottom", "top"),
      clusterEdge("broker-c", "broker-a", "bottom", "left"),
      clusterEdge("broker-c", "broker-b", "bottom", "left"),
      clusterEdge("broker-b", "broker-a", "right", "top"),
      consumerEdge("broker-a", "analytics"),
      consumerEdge("broker-b", "analytics"),
      consumerEdge("broker-a", "apps"),
      consumerEdge("broker-b", "apps"),
      consumerEdge("broker-c", "apps"),
      consumerEdge("broker-b", "services"),
      consumerEdge("broker-c", "services"),
    ],
    [],
  );

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        cursor: "default",
        pointerEvents: "none",
      }}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        panOnDrag={false}
        zoomOnScroll={false}
        zoomOnPinch={false}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        preventScrolling={true}
        connectionMode={ConnectionMode.Loose}
        proOptions={{ hideAttribution: true }}
      >
        <Background gap={32} size={1} color="rgba(47,105,179,0.08)" />
      </ReactFlow>
    </div>
  );
}

const edge = (
  source: string,
  target: string,
  sourceHandle?: string,
  targetHandle?: string,
): Edge => ({
  id: `${source}-${target}`,
  source,
  target,
  sourceHandle,
  targetHandle,
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

const consumerEdge = (source: string, target: string): Edge => ({
  id: `${source}-${target}`,
  source,
  target,
  animated: true,
  style: {
    stroke: green,
    strokeWidth: 1.5,
  },
  markerEnd: {
    type: MarkerType.ArrowClosed,
    color: blue,
  },
});

const clusterEdge = (
  source: string,
  target: string,
  sourceHandle?: string,
  targetHandle?: string,
): Edge => ({
  id: `${source}-${target}`,
  source,
  target,
  sourceHandle,
  targetHandle,
  animated: true,
  type: "smoothstep",
  style: {
    stroke: orange,
    strokeWidth: 2,
    strokeDasharray: "6 6",
  },
});

const protocolStyle = {
  borderRadius: 8,
  padding: "16px 20px",
  fontSize: 16,
  fontWeight: 600,
  border: `1px solid ${blue}`,
  color: blue,
  background: "rgba(47,105,179,0.05)",
  minWidth: 120,
  textAlign: "center" as const,
};

const consumerStyle = {
  ...protocolStyle,
  color: green,
  border: `1px solid ${green}`,
  background: "rgba(40, 184, 40, 0.08)",
};

const brokerStyle = {
  borderRadius: 12,
  padding: "20px 24px",
  fontSize: 18,
  fontWeight: 700,
  border: `1px solid ${orange}`,
  color: orange,
  background: "rgba(249,163,42,0.08)",
  boxShadow: "0 0 18px rgba(249,163,42,0.35)",
  minWidth: 160,
  textAlign: "center" as const,
};

function BrokerNode() {
  return (
    <div className="flex w-full flex-col gap-2">
      <div className="w-full rounded-md border p-1 text-center">
        <span className="text-(--flux-blue)">Flux</span>
        <span className="text-(--flux-orange)">MQ</span>
      </div>
      <div className="w-full rounded-md border p-1 text-center">
        Durable Queue
      </div>
      <Handle type="target" position={Position.Left} id="left" />
      <Handle type="target" position={Position.Top} id="top" />
      <Handle type="source" position={Position.Right} id="right" />
      <Handle type="source" position={Position.Bottom} id="bottom" />
    </div>
  );
}

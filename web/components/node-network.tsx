"use client";

import {
  ConnectionMode,
  type Edge,
  type EdgeProps,
  getBezierPath,
  Handle,
  MarkerType,
  type Node,
  type NodeProps,
  Position,
  ReactFlow,
} from "@xyflow/react";
import { Activity, Database, Layers3, Radio, Server } from "lucide-react";
import { useMemo } from "react";
import "@xyflow/react/dist/style.css";

const COLORS = {
  blue: "#2F69B3",
  orange: "#F9A32A",
  green: "#2FB07A",
  violet: "#8A63D2",
};

type NodeKind = "protocol" | "broker" | "consumer";

type NetworkNodeData = {
  label: string;
  subtitle: string;
  metric: string;
};

type DiagramNode = Node<NetworkNodeData, NodeKind>;

type NetworkEdgeData = {
  start: string;
  end: string;
  emphasize?: boolean;
  bidirectional?: boolean;
};

type DiagramEdge = Edge<NetworkEdgeData, "flow">;

const nodesTypes = {
  protocol: ProtocolNode,
  broker: BrokerNode,
  consumer: ConsumerNode,
};

const edgeTypes = {
  flow: FlowEdge,
};

export function NodeNetwork() {
  const nodes: DiagramNode[] = useMemo(
    () => [
      protocolNode("mqtt-v5", "MQTT 5.0", "stateful ingest", "94k msgs/s", 0),
      protocolNode("mqtt-v3", "MQTT v3.1.1","IoT ingest", "41k msgs/s",160,),
      protocolNode("http", "HTTP/Websocket", "gateway events", "22k req/s", 320),
      protocolNode("coap", "CoAP", "edge telemetry", "17k conn", 480),
      protocolNode("amqp", "AMQP", "durable workloads", "8.4k chan", 640),

      brokerNode(
        "broker-a",
        "FluxMQ Node A",
        "Leader",
        "p95 3.9 ms",
        30,
      ),
      brokerNode(
        "broker-b",
        "FluxMQ Node B",
        "Follower",
        "p95 4.1 ms",
        310,
      ),
      brokerNode(
        "broker-c",
        "FluxMQ Node C",
        "Follower",
        "p95 4.0 ms",
        590,
      ),

      consumerNode(
        "analytics",
        "Analytics",
        "stream processing",
        "1.2 GB/s",
        80,
      ),
      consumerNode(
        "apps",
        "Applications",
        "business workflows",
        "36k acks/s",
        360,
      ),
      consumerNode("services", "Services", "internal APIs", "9.8k jobs/s", 640),
    ],
    [],
  );

  const edges: DiagramEdge[] = useMemo(
    () => [
      ingressEdge("mqtt-v5", "broker-a"),
      ingressEdge("mqtt-v5", "broker-b"),
      ingressEdge("mqtt-v3", "broker-a"),
      ingressEdge("mqtt-v3", "broker-b"),
      ingressEdge("http", "broker-b"),
      ingressEdge("http", "broker-c"),
      ingressEdge("ws", "broker-a"),
      ingressEdge("ws", "broker-b"),
      ingressEdge("ws", "broker-c"),
      ingressEdge("amqp", "broker-c"),

      clusterEdge("broker-a", "broker-b", "bottom", "top"),
      clusterEdge("broker-b", "broker-c", "bottom", "top"),

      egressEdge("broker-a", "analytics"),
      egressEdge("broker-b", "analytics"),
      egressEdge("broker-a", "apps"),
      egressEdge("broker-b", "apps"),
      egressEdge("broker-c", "apps"),
      egressEdge("broker-b", "services"),
      egressEdge("broker-c", "services"),
    ],
    [],
  );

  return (
    <div className="relative h-full w-full pointer-events-none">
      <div className="h-full w-full">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodesTypes}
          edgeTypes={edgeTypes}
          fitView
          fitViewOptions={{ padding: 0.02, duration: 350 }}
          panOnDrag={false}
          zoomOnScroll={false}
          zoomOnPinch={false}
          zoomOnDoubleClick={false}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
          preventScrolling
          connectionMode={ConnectionMode.Loose}
          proOptions={{ hideAttribution: true }}
        />
      </div>
    </div>
  );
}

function ProtocolNode({ data }: NodeProps<DiagramNode>) {
  return (
    <div className="relative min-w-[208px] rounded-xl border border-(--flux-blue)/30 bg-(--flux-bg)/70 px-4 py-3.5 shadow-[0_8px_22px_rgba(47,105,179,0.22)] backdrop-blur-sm">
      <div className="mb-2 flex items-center gap-2 text-sm font-semibold text-(--flux-blue)">
        <Radio className="size-4" />
        <span>{data.label}</span>
      </div>
      <p className="text-[11px] uppercase tracking-[0.14em] text-(--flux-text-muted)">
        {data.subtitle}
      </p>
      <div className="mt-3 inline-flex rounded-full bg-(--flux-blue)/12 px-2.5 py-1 text-[10px] font-medium text-(--flux-blue)">
        {data.metric}
      </div>

      <Handle
        type="target"
        position={Position.Left}
        style={HIDDEN_HANDLE_STYLE}
      />
      <Handle
        type="source"
        position={Position.Right}
        style={getHandleStyle(COLORS.blue)}
      />
    </div>
  );
}

function ConsumerNode({ data }: NodeProps<DiagramNode>) {
  return (
    <div className="relative min-w-[208px] rounded-xl border border-[#2FB07A]/50 bg-(--flux-bg)/70 px-4 py-3.5 shadow-[0_8px_22px_rgba(47,176,122,0.2)] backdrop-blur-sm">
      <div className="mb-2 flex items-center gap-2 text-sm font-semibold text-(--flux-green)">
        <Activity className="size-4" />
        <span>{data.label}</span>
      </div>
      <p className="text-[11px] uppercase tracking-[0.14em] text-(--flux-text-muted)">
        {data.subtitle}
      </p>
      <div className="mt-3 inline-flex rounded-full bg-(--flux-green)/12 px-2.5 py-1 text-[10px] font-medium text-(--flux-green)">
        {data.metric}
      </div>

      <Handle
        type="target"
        position={Position.Left}
        style={getHandleStyle(COLORS.green)}
      />
      <Handle
        type="source"
        position={Position.Right}
        style={HIDDEN_HANDLE_STYLE}
      />
    </div>
  );
}

function BrokerNode({ data }: NodeProps<DiagramNode>) {
  return (
    <div className="relative min-w-[268px] rounded-2xl border border-(--flux-orange)/30 bg-gradient-to-br from-(--flux-orange)/14 via-(--flux-bg)/80 to-(--flux-blue)/12 px-4 py-3.5 shadow-[0_14px_32px_rgba(249,163,42,0.24)] backdrop-blur-sm">
      <div className="absolute -right-6 -top-6 size-20 rounded-full bg-(--flux-orange)/25 blur-2xl" />

      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 text-sm font-semibold text-(--flux-orange)">
          <Server className="size-4" />
          <span>
            {data.label.startsWith("FluxMQ") ? (
              <>
                <span className="text-(--flux-blue)">Flux</span>
                {data.label.slice(4)}
              </>
            ) : (
              data.label
            )}
          </span>
        </div>
        <span className="rounded-full bg-(--flux-orange)/20 px-2 py-0.5 text-[9px] font-semibold tracking-[0.14em] text-(--flux-orange)">
          RAFT
        </span>
      </div>

      <p className="mt-1 text-[11px] uppercase tracking-[0.14em] text-(--flux-text-muted)">
        {data.subtitle}
      </p>

      <div className="mt-3 grid grid-cols-2 gap-2 text-[10px] text-(--flux-text)">
        <StatusChip icon={<Database className="size-3.5" />} label="segments" />
        <StatusChip icon={<Layers3 className="size-3.5" />} label="replicas" />
      </div>

      <div className="mt-3 inline-flex rounded-full bg-(--flux-orange)/18 px-2.5 py-1 text-[10px] font-medium text-(--flux-orange)">
        {data.metric}
      </div>

      <Handle
        type="target"
        position={Position.Left}
        id="left"
        style={getHandleStyle(COLORS.orange)}
      />
      <Handle
        type="target"
        position={Position.Top}
        id="top"
        style={getHandleStyle(COLORS.orange)}
      />
      <Handle
        type="source"
        position={Position.Right}
        id="right"
        style={getHandleStyle(COLORS.orange)}
      />
      <Handle
        type="source"
        position={Position.Bottom}
        id="bottom"
        style={getHandleStyle(COLORS.orange)}
      />
    </div>
  );
}

function StatusChip({ icon, label }: { icon: React.ReactNode; label: string }) {
  return (
    <div className="inline-flex items-center gap-1.5 rounded-md bg-(--flux-bg)/70 px-2 py-1 text-(--flux-text-muted)">
      {icon}
      <span className="uppercase tracking-[0.1em]">{label}</span>
    </div>
  );
}

function FlowEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  markerEnd,
  markerStart,
}: EdgeProps<DiagramEdge>) {
  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    curvature: 0.32,
  });

  const gradientId = `flux-edge-${id}`;
  const isBidirectional = data?.bidirectional;

  return (
    <>
      <defs>
        <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor={data?.start ?? COLORS.blue} />
          <stop offset="100%" stopColor={data?.end ?? COLORS.orange} />
        </linearGradient>
      </defs>

      <path
        d={edgePath}
        fill="none"
        stroke={isBidirectional ? (data?.start ?? COLORS.violet) : `url(#${gradientId})`}
        strokeWidth={data?.emphasize ? 3 : 2.2}
        opacity={0.92}
        markerEnd={markerEnd as string | undefined}
        markerStart={markerStart as string | undefined}
      />

      <path
        d={edgePath}
        fill="none"
        stroke="rgba(255,255,255,0.75)"
        strokeWidth={1.05}
        strokeDasharray="7 14"
        opacity={0.75}
        markerEnd={markerEnd as string | undefined}
        markerStart={markerStart as string | undefined}
      >
        <animate
          attributeName="stroke-dashoffset"
          values="0;-280"
          dur={data?.emphasize ? "6.2s" : "8.5s"}
          repeatCount="indefinite"
        />
      </path>
    </>
  );
}

const getHandleStyle = (color: string) => ({
  width: 9,
  height: 9,
  borderRadius: 999,
  border: "none",
  background: "#ffffff",
  boxShadow: `0 0 0 4px ${color}22`,
});

const HIDDEN_HANDLE_STYLE = {
  width: 6,
  height: 6,
  opacity: 0,
};

const protocolNode = (
  id: string,
  label: string,
  subtitle: string,
  metric: string,
  y: number,
): DiagramNode => ({
  id,
  type: "protocol",
  position: { x: 0, y },
  data: {
    label,
    subtitle,
    metric,
  },
});

const brokerNode = (
  id: string,
  label: string,
  subtitle: string,
  metric: string,
  y: number,
): DiagramNode => ({
  id,
  type: "broker",
  position: { x: 320, y },
  data: {
    label,
    subtitle,
    metric,
  },
});

const consumerNode = (
  id: string,
  label: string,
  subtitle: string,
  metric: string,
  y: number,
): DiagramNode => ({
  id,
  type: "consumer",
  position: { x: 660, y },
  data: {
    label,
    subtitle,
    metric,
  },
});

const ingressEdge = (source: string, target: string): DiagramEdge =>
  createEdge(source, target, {
    start: COLORS.blue,
    end: COLORS.orange,
  });

const egressEdge = (source: string, target: string): DiagramEdge =>
  createEdge(source, target, {
    start: COLORS.orange,
    end: COLORS.green,
  });

const clusterEdge = (
  source: string,
  target: string,
  sourceHandle?: string,
  targetHandle?: string,
): DiagramEdge =>
  createEdge(source, target, {
    start: COLORS.orange,
    end: COLORS.violet,
    emphasize: true,
    bidirectional: true,
    sourceHandle,
    targetHandle,
  });

function createEdge(
  source: string,
  target: string,
  {
    start,
    end,
    emphasize,
    bidirectional,
    sourceHandle,
    targetHandle,
  }: {
    start: string;
    end: string;
    emphasize?: boolean;
    bidirectional?: boolean;
    sourceHandle?: string;
    targetHandle?: string;
  },
): DiagramEdge {
  return {
    id: `${source}-${target}-${sourceHandle ?? ""}-${targetHandle ?? ""}`,
    source,
    target,
    type: "flow",
    sourceHandle,
    targetHandle,
    data: {
      start,
      end,
      emphasize,
      bidirectional,
    },
    ...(bidirectional
      ? {}
      : {
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: end,
        },
      }),
  };
}

import { MermaidDiagram } from "@/components/mermaid-diagram";

const architectureDiagram = `
graph TD
    A[Setup and configuration] --> B[MQTT/WS/HTTP/CoAP<br/>Servers]
    A --> C[AMQP 1.0<br/>Server]
    A --> D[AMQP 0.9.1<br/>Server]
    B --> E[MQTT v3.1.1/v5 Broker]
    C --> F[AMQP Broker<br/> 1.0]
    D --> G[AMQP Broker<br/>0.9.1]
    E --> H[Queue Manager<br/>Bindings + Delivery]
    F --> H
    G --> H
    H --> I[Log Storage<br/>+ Topic Index]

    style A fill:#4a4a4a,color:#ffffff
    style B fill:#2F69B3,color:#ffffff
    style C fill:#2F69B3,color:#ffffff
    style D fill:#2F69B3,color:#ffffff
    style E fill:#2F69B3,color:#ffffff
    style F fill:#2F69B3,color:#ffffff
    style G fill:#2F69B3,color:#ffffff
    style H fill:#F9A32A,color:#111111
    style I fill:#F9A32A,color:#111111
`;

export function ArchitectureSection() {
  return (
    <section
      id="architecture"
      className="py-20 border-b-2 border-theme bg-theme-alt"
    >
      <div className="container mx-auto px-6">
        <h2 className="text-4xl md:text-5xl font-bold mb-12">
          <span className="border-l-4 border-(--flux-orange) pl-4">
            ARCHITECTURE
          </span>
        </h2>

        <div className="max-w-4xl mx-auto">
          <MermaidDiagram chart={architectureDiagram} />
        </div>

        <div className="mt-12 max-w-3xl mx-auto brutalist-border bg-theme p-6">
          <h3 className="font-bold mono text-lg mb-4">KEY COMPONENTS</h3>
          <ul className="space-y-3 text-theme-muted text-base">
            <li className="flex items-start">
              <span className="text-(--flux-blue) mr-2 font-bold">▸</span>
              <span>
                <strong>Transport Layer:</strong> Multi-protocol servers (MQTT,
                AMQP 1.0, AMQP 0.9.1, CoAP, HTTP, WebSocket)
              </span>
            </li>
            <li className="flex items-start">
              <span className="text-(--flux-blue) mr-2 font-bold">▸</span>
              <span>
                <strong>Protocol Brokers:</strong> FSM-based protocol handlers
                with zero-copy parsing
              </span>
            </li>
            <li className="flex items-start">
              <span className="text-(--flux-blue) mr-2 font-bold">▸</span>
              <span>
                <strong>Queue Manager:</strong> Durable queue bindings with
                FIFO, priority, and topic-based delivery
              </span>
            </li>
            <li className="flex items-start">
              <span className="text-(--flux-blue) mr-2 font-bold">▸</span>
              <span>
                <strong>Storage:</strong> For message persistence and topic
                indexing
              </span>
            </li>
          </ul>
        </div>
      </div>
    </section>
  );
}

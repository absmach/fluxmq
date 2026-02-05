import { Network, Database, Server, Zap, Shield, Code2 } from "lucide-react";

export function FeaturesSection() {
  return (
    <section id="features" className="py-20 border-b-2 border-(--flux-border)">
      <div className="container mx-auto px-6">
        <h2 className="text-4xl md:text-5xl font-bold mb-12">
          <span className="border-l-4 border-(--flux-orange) pl-4">
            FEATURES
          </span>
        </h2>

        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
          {/* Feature 1 */}
          <div className="brutalist-card p-6 accent-line pl-8">
            <Network
              className="mb-4 text-(--flux-blue)"
              size={32}
              strokeWidth={2}
            />
            <h3 className="text-xl font-bold mb-3 mono">
              Multi-Protocol Support
            </h3>
            <p className="text-theme-muted leading-relaxed text-base">
              Full MQTT 3.1.1 and 5.0 over TCP and WebSocket, plus HTTP-MQTT and
              CoAP bridges. All protocols share the same broker core — messages
              flow seamlessly across transports.
            </p>
          </div>

          {/* Feature 2 */}
          <div className="brutalist-card p-6 accent-line pl-8">
            <Database
              className="mb-4 text-(--flux-orange)"
              size={32}
              strokeWidth={2}
            />
            <h3 className="text-xl font-bold mb-3 mono">Durable Queues</h3>
            <p className="text-theme-muted leading-relaxed text-base">
              Persistent message queues with consumer groups, ack/nack/reject
              semantics, dead-letter queues, and Kafka-style retention.
              Raft-based replication with automatic failover.
            </p>
          </div>

          {/* Feature 3 */}
          <div className="brutalist-card p-6 accent-line pl-8">
            <Server
              className="mb-4 text-(--flux-blue)"
              size={32}
              strokeWidth={2}
            />
            <h3 className="text-xl font-bold mb-3 mono">
              Clustering & High Availability
            </h3>
            <p className="text-theme-muted leading-relaxed text-base">
              Embedded etcd for coordination, gRPC-based inter-broker
              communication with mTLS, automatic session ownership, and graceful
              shutdown with session transfer. No external dependencies.
            </p>
          </div>

          {/* Feature 4 */}
          <div className="brutalist-card p-6 accent-line pl-8">
            <Zap
              className="mb-4 text-(--flux-orange)"
              size={32}
              strokeWidth={2}
            />
            <h3 className="text-xl font-bold mb-3 mono">
              Performance Optimized
            </h3>
            <p className="text-theme-muted leading-relaxed text-base">
              Zero-copy packet parsing, object pooling, efficient trie-based
              topic matching, and direct instrumentation. 300K-500K msg/s per
              node with sub-10ms latency.
            </p>
          </div>

          {/* Feature 5 */}
          <div className="brutalist-card p-6 accent-line pl-8">
            <Shield
              className="mb-4 text-(--flux-blue)"
              size={32}
              strokeWidth={2}
            />
            <h3 className="text-xl font-bold mb-3 mono">Security</h3>
            <p className="text-theme-muted leading-relaxed text-base">
              TLS/mTLS for client connections, mTLS for inter-broker gRPC,
              DTLS/mDTLS for CoAP, WebSocket origin validation, and
              per-IP/per-client rate limiting.
            </p>
          </div>

          {/* Feature 6 */}
          <div className="brutalist-card p-6 accent-line pl-8">
            <Code2
              className="mb-4 text-(--flux-orange)"
              size={32}
              strokeWidth={2}
            />
            <h3 className="text-xl font-bold mb-3 mono">
              Open-Source & Extensible
            </h3>
            <p className="text-theme-muted leading-relaxed text-base">
              Licensed under Apache 2.0 with a clean layered architecture
              (Transport → Protocol → Domain). Pluggable storage backends,
              protocol-agnostic domain logic, and easy extensibility.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}

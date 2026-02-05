import Link from 'next/link';

export default function HomePage() {
  return (
    <main className="min-h-screen bg-theme">
      {/* Hero Section */}
      <section className="relative border-b-2 border-[var(--flux-border)] py-20 md:py-32">
        <div className="container mx-auto px-6">
          <div className="max-w-5xl">
            {/* Title with technical specs overlay */}
            <div className="mb-8">
              <h1 className="text-6xl md:text-8xl font-bold mb-4" style={{ lineHeight: '1.1' }}>
                FluxMQ
              </h1>
              <div className="mono text-sm md:text-base text-theme-muted mb-6 border-l-4 border-[var(--flux-orange)] pl-4">
                <div>PROTOCOL: MQTT 3.1.1 / 5.0 | HTTP | WebSocket | CoAP</div>
                <div>THROUGHPUT: 300K-500K msg/s per node | LATENCY: &lt;10ms</div>
                <div>ARCHITECTURE: Event-Driven | Clustered | Durable Queues</div>
              </div>
            </div>

            <p className="text-xl md:text-2xl mb-8 max-w-3xl leading-relaxed">
              A high-performance, multi-protocol message broker written in Go for scalable IoT and
              event-driven architectures. Single binary. No external dependencies.
            </p>

            <div className="flex flex-wrap gap-4">
              <Link
                href="/docs"
                className="brutalist-border px-6 py-3 font-bold hover:bg-[var(--flux-orange)] hover:text-white transition-colors inline-block"
              >
                READ DOCUMENTATION
              </Link>
              <a
                href="https://github.com/absmach/fluxmq"
                className="brutalist-border px-6 py-3 font-bold hover:bg-[var(--flux-blue)] hover:text-white transition-colors inline-block"
                target="_blank"
                rel="noopener noreferrer"
              >
                VIEW ON GITHUB →
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="py-20 border-b-2 border-[var(--flux-border)]">
        <div className="container mx-auto px-6">
          <h2 className="text-4xl md:text-5xl font-bold mb-12">
            <span className="border-l-4 border-[var(--flux-orange)] pl-4">FEATURES</span>
          </h2>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            {/* Feature 1 */}
            <div className="brutalist-card p-6 accent-line pl-8">
              <h3 className="text-xl font-bold mb-3 mono">Multi-Protocol Support</h3>
              <p className="text-theme-muted leading-relaxed">
                Full MQTT 3.1.1 and 5.0 over TCP and WebSocket, plus HTTP-MQTT and CoAP bridges.
                All protocols share the same broker core — messages flow seamlessly across transports.
              </p>
            </div>

            {/* Feature 2 */}
            <div className="brutalist-card p-6 accent-line pl-8">
              <h3 className="text-xl font-bold mb-3 mono">Durable Queues</h3>
              <p className="text-theme-muted leading-relaxed">
                Persistent message queues with consumer groups, ack/nack/reject semantics,
                dead-letter queues, and Kafka-style retention. Raft-based replication with automatic failover.
              </p>
            </div>

            {/* Feature 3 */}
            <div className="brutalist-card p-6 accent-line pl-8">
              <h3 className="text-xl font-bold mb-3 mono">Clustering & High Availability</h3>
              <p className="text-theme-muted leading-relaxed">
                Embedded etcd for coordination, gRPC-based inter-broker communication with mTLS,
                automatic session ownership, and graceful shutdown with session transfer. No external dependencies.
              </p>
            </div>

            {/* Feature 4 */}
            <div className="brutalist-card p-6 accent-line pl-8">
              <h3 className="text-xl font-bold mb-3 mono">Performance Optimized</h3>
              <p className="text-theme-muted leading-relaxed">
                Zero-copy packet parsing, object pooling, efficient trie-based topic matching,
                and direct instrumentation. 300K-500K msg/s per node with sub-10ms latency.
              </p>
            </div>

            {/* Feature 5 */}
            <div className="brutalist-card p-6 accent-line pl-8">
              <h3 className="text-xl font-bold mb-3 mono">Security</h3>
              <p className="text-theme-muted leading-relaxed">
                TLS/mTLS for client connections, mTLS for inter-broker gRPC, DTLS/mDTLS for CoAP,
                WebSocket origin validation, and per-IP/per-client rate limiting.
              </p>
            </div>

            {/* Feature 6 */}
            <div className="brutalist-card p-6 accent-line pl-8">
              <h3 className="text-xl font-bold mb-3 mono">Open-Source & Extensible</h3>
              <p className="text-theme-muted leading-relaxed">
                Licensed under Apache 2.0 with a clean layered architecture (Transport → Protocol → Domain).
                Pluggable storage backends, protocol-agnostic domain logic, and easy extensibility.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Performance Metrics Section */}
      <section id="performance" className="py-20 border-b-2 border-theme bg-theme-alt">
        <div className="container mx-auto px-6">
          <h2 className="text-4xl md:text-5xl font-bold mb-12">
            <span className="border-l-4 border-(--flux-orange) pl-4">PERFORMANCE</span>
          </h2>

          <div className="max-w-4xl">
            <table className="metrics-table w-full mono text-sm md:text-base">
              <thead>
                <tr>
                  <th>METRIC</th>
                  <th>VALUE</th>
                </tr>
              </thead>
              <tbody className="bg-white">
                <tr>
                  <td>Concurrent Connections</td>
                  <td className="font-bold">500K+ per node</td>
                </tr>
                <tr>
                  <td>Message Throughput</td>
                  <td className="font-bold">300K-500K msg/s per node</td>
                </tr>
                <tr>
                  <td>Latency (local)</td>
                  <td className="font-bold">&lt;10ms</td>
                </tr>
                <tr>
                  <td>Latency (cross-node)</td>
                  <td className="font-bold">~5ms</td>
                </tr>
                <tr>
                  <td>Session Takeover</td>
                  <td className="font-bold">&lt;100ms</td>
                </tr>
              </tbody>
            </table>

            <div className="mt-8 brutalist-border bg-theme p-6">
              <h3 className="font-bold mb-4 mono text-lg">CLUSTER SCALING</h3>
              <ul className="space-y-2 mono text-sm">
                <li className="border-l-4 border-[var(--flux-blue)] pl-4">
                  <strong>3-node cluster:</strong> 1-2M msg/s
                </li>
                <li className="border-l-4 border-[var(--flux-blue)] pl-4">
                  <strong>5-node cluster:</strong> 2-4M msg/s
                </li>
                <li className="border-l-4 border-[var(--flux-blue)] pl-4">
                  <strong>Scaling:</strong> Linear with topic sharding
                </li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      {/* Use Cases Section */}
      <section id="use-cases" className="py-20 border-b-2 border-[var(--flux-border)]">
        <div className="container mx-auto px-6">
          <h2 className="text-4xl md:text-5xl font-bold mb-12">
            <span className="border-l-4 border-[var(--flux-orange)] pl-4">USE CASES</span>
          </h2>

          <div className="grid md:grid-cols-3 gap-6">
            {/* Event-Driven Architecture */}
            <div className="brutalist-card p-6">
              <div className="border-b-2 border-[var(--flux-border)] pb-4 mb-4">
                <h3 className="text-2xl font-bold mono">Event-Driven Architecture</h3>
              </div>
              <ul className="space-y-3 text-theme-muted">
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Event backbone for microservices</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>CQRS systems with durable queues</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Asynchronous workflows</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Real-time event processing</span>
                </li>
              </ul>
            </div>

            {/* IoT & Real-Time Systems */}
            <div className="brutalist-card p-6">
              <div className="border-b-2 border-[var(--flux-border)] pb-4 mb-4">
                <h3 className="text-2xl font-bold mono">IoT & Real-Time</h3>
              </div>
              <ul className="space-y-3 text-theme-muted">
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Device communication (MQTT 3.1.1/5.0)</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Edge computing deployments</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Browser clients via WebSocket</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Constrained devices (CoAP bridge)</span>
                </li>
              </ul>
            </div>

            {/* High Availability */}
            <div className="brutalist-card p-6">
              <div className="border-b-2 border-[var(--flux-border)] pb-4 mb-4">
                <h3 className="text-2xl font-bold mono">High Availability</h3>
              </div>
              <ul className="space-y-3 text-theme-muted">
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Clustered deployments (3-5 nodes)</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Automatic failover (&lt;100ms)</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Geographic distribution</span>
                </li>
                <li className="flex items-start">
                  <span className="text-[var(--flux-orange)] mr-2 font-bold">■</span>
                  <span>Linear scalability</span>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      {/* Architecture Diagram Section */}
      <section id="architecture" className="py-20 border-b-2 border-theme bg-theme-alt">
        <div className="container mx-auto px-6">
          <h2 className="text-4xl md:text-5xl font-bold mb-12">
            <span className="border-l-4 border-[var(--flux-orange)] pl-4">ARCHITECTURE</span>
          </h2>

          <div className="brutalist-border bg-theme p-8 mono text-xs md:text-sm overflow-x-auto">
            <pre className="text-theme">
{`┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ TCP Server  │  │ WebSocket   │  │ HTTP Bridge │  │ CoAP Bridge │
│   :1883     │  │   :8083     │  │   :8080     │  │   :5683     │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       └────────────────┴────────────────┴────────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │     Protocol Detection        │
                  └───────────────┬───────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │                               │
           ┌──────▼──────┐                 ┌──────▼──────┐
           │ V3 Handler  │                 │ V5 Handler  │
           │ (MQTT 3.1.1)│                 │ (MQTT 5.0)  │
           └──────┬──────┘                 └──────┬──────┘
                  └───────────────┬───────────────┘
                                  ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                     Domain Layer                            │
    │                                                             │
    │  Sessions  │  Router (Trie)  │  Pub/Sub  │  Durable Queues  │
    │                                                             │
    │  Built-in: Logging (slog) • Metrics • Instrumentation       │
    └──────────────────────────────┬──────────────────────────────┘
                                   │
    ┌──────────────────────────────┴──────────────────────────────┐
    │                    Infrastructure                           │
    │                                                             │
    │ ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌────────────┐  │
    │ │ Storage  │  │ Cluster   │  │ Session   │  │   Queue    │  │
    │ │ BadgerDB │  │ etcd+gRPC │  │ Cache     │  │  Storage   │  │
    │ └──────────┘  └───────────┘  └───────────┘  └────────────┘  │
    └─────────────────────────────────────────────────────────────┘`}
            </pre>
          </div>
        </div>
      </section>

      {/* Quick Start Section */}
      <section id="quick-start" className="py-20 border-b-2 border-[var(--flux-border)]">
        <div className="container mx-auto px-6">
          <h2 className="text-4xl md:text-5xl font-bold mb-12">
            <span className="border-l-4 border-[var(--flux-orange)] pl-4">QUICK START</span>
          </h2>

          <div className="max-w-4xl space-y-6">
            {/* Build & Run */}
            <div>
              <h3 className="mono font-bold mb-3 text-lg">1. BUILD & RUN</h3>
              <div className="terminal p-4 overflow-x-auto">
                <pre>
{`$ git clone https://github.com/absmach/fluxmq.git
$ cd fluxmq
$ make build
$ ./build/fluxmq`}
                </pre>
              </div>
            </div>

            {/* Test */}
            <div>
              <h3 className="mono font-bold mb-3 text-lg">2. TEST WITH MQTT</h3>
              <div className="terminal p-4 overflow-x-auto">
                <pre>
{`# Subscribe
$ mosquitto_sub -p 1883 -t "test/#" -v

# Publish
$ mosquitto_pub -p 1883 -t "test/hello" -m "Hello FluxMQ"`}
                </pre>
              </div>
            </div>

            {/* Configuration Example */}
            <div>
              <h3 className="mono font-bold mb-3 text-lg">3. CONFIGURATION</h3>
              <div className="terminal p-4 overflow-x-auto">
                <pre>
{`server:
  tcp:
    plain:
      addr: ":1883"
      max_connections: 10000
  websocket:
    plain:
      addr: ":8083"
      path: "/mqtt"

broker:
  max_message_size: 1048576
  max_retained_messages: 10000

storage:
  type: badger
  path: "./data"`}
                </pre>
              </div>
            </div>

            <div className="brutalist-border bg-theme p-6">
              <p className="font-bold mb-2">Next Steps:</p>
              <ul className="space-y-2">
                <li>
                  <Link href="/docs" className="text-[var(--flux-blue)] hover:underline font-bold">
                    → Read the full documentation
                  </Link>
                </li>
                <li>
                  <a
                    href="https://github.com/absmach/fluxmq/tree/main/examples"
                    className="text-[var(--flux-blue)] hover:underline font-bold"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    → Explore code examples
                  </a>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      {/* Newsletter Section */}
      <section className="py-20 border-b-2 border-theme bg-theme-alt">
        <div className="container mx-auto px-6">
          <div className="max-w-2xl mx-auto">
            <h2 className="text-3xl md:text-4xl font-bold mb-6 text-center">
              SUBSCRIBE TO NEWSLETTER
            </h2>
            <p className="text-center text-theme-muted mb-8">
              Stay updated with the latest FluxMQ news, updates and announcements.
            </p>

            <form
              action="https://absmach.us11.list-manage.com/subscribe/post?u=70b43c7181d005024187bfb31&id=0a319b6b63&f_id=002611e1f0"
              method="post"
              target="_blank"
              className="max-w-md mx-auto"
            >
              <div className="flex gap-0">
                <input
                  type="email"
                  name="EMAIL"
                  placeholder="Enter your email"
                  required
                  className="flex-1 brutalist-border border-r-0 px-4 py-3 focus:outline-none focus:border-[var(--flux-orange)]"
                />
                <button
                  type="submit"
                  className="brutalist-border bg-[var(--flux-orange)] hover:bg-[var(--flux-blue)] text-white px-6 py-3 font-bold transition-colors"
                >
                  SUBSCRIBE
                </button>
              </div>
              <input type="hidden" name="tags" value="8115259" />
              <div style={{ position: 'absolute', left: '-5000px' }} aria-hidden="true">
                <input type="text" name="b_70b43c7181d005024187bfb31_0a319b6b63" tabIndex={-1} value="" readOnly />
              </div>
              <p className="text-xs text-theme-muted mt-4 text-center">
                By subscribing, you agree to our{' '}
                <a href="https://absmach.eu/privacy/" className="underline" target="_blank" rel="noopener noreferrer">
                  Privacy Policy
                </a>{' '}
                and{' '}
                <a href="https://absmach.eu/terms/" className="underline" target="_blank" rel="noopener noreferrer">
                  Terms of Service
                </a>
                . You can unsubscribe at any time.
              </p>
            </form>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="py-12 bg-theme-inverse">
        <div className="container mx-auto px-6">
          <div className="grid md:grid-cols-4 gap-8">
            {/* About */}
            <div>
              <h3 className="font-bold mb-4 text-lg">ABOUT</h3>
              <p className="text-sm opacity-80 leading-relaxed">
                FluxMQ is developed by Abstract Machines, an IoT infrastructure and security company located in Paris.
              </p>
            </div>

            {/* Products */}
            <div>
              <h3 className="font-bold mb-4 text-lg">PRODUCTS</h3>
              <ul className="space-y-2 text-sm">
                <li>
                  <a href="https://magistrala.absmach.eu" className="opacity-80 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    Magistrala
                  </a>
                </li>
                <li>
                  <a href="https://absmach.eu/supermq/" className="opacity-80 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    SuperMQ
                  </a>
                </li>
                <li>
                  <a href="https://absmach.eu/propeller/" className="text-gray-300 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    Propeller
                  </a>
                </li>
              </ul>
            </div>

            {/* Resources */}
            <div>
              <h3 className="font-bold mb-4 text-lg">RESOURCES</h3>
              <ul className="space-y-2 text-sm">
                <li>
                  <Link href="/docs" className="opacity-80 hover:text-[var(--flux-orange)]">
                    Documentation
                  </Link>
                </li>
                <li>
                  <a href="https://github.com/absmach/fluxmq" className="text-gray-300 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    GitHub
                  </a>
                </li>
                <li>
                  <a href="https://absmach.eu/blog/" className="text-gray-300 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    Blog
                  </a>
                </li>
              </ul>
            </div>

            {/* Contact */}
            <div>
              <h3 className="font-bold mb-4 text-lg">CONTACT</h3>
              <ul className="space-y-2 text-sm">
                <li>
                  <a href="mailto:info@absmach.eu" className="opacity-80 hover:text-[var(--flux-orange)]">
                    info@absmach.eu
                  </a>
                </li>
                <li>
                  <a href="https://github.com/absmach" className="text-gray-300 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    GitHub
                  </a>
                </li>
                <li>
                  <a href="https://twitter.com/absmach" className="text-gray-300 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    Twitter
                  </a>
                </li>
                <li>
                  <a href="https://www.linkedin.com/company/abstract-machines" className="text-gray-300 hover:text-[var(--flux-orange)]" target="_blank" rel="noopener noreferrer">
                    LinkedIn
                  </a>
                </li>
              </ul>
            </div>
          </div>

          <div className="border-t border-theme mt-8 pt-8 text-center text-sm opacity-70">
            <p>© 2024 Abstract Machines. Licensed under Apache 2.0.</p>
          </div>
        </div>
      </footer>
    </main>
  );
}

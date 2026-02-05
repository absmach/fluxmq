import Link from "next/link";
import { NodeNetwork } from "@/components/node-network";

export function HeroSection() {
  return (
    <section className="relative border-b-2 border-(--flux-border) py-20 md:py-20 h-[90vh]">
      <div className="container mx-auto px-6">
        <div className="grid md:grid-cols-2 gap-12 items-center">
          {/* Left side - Content */}
          <div className="max-w-2xl">
            {/* Title with technical specs overlay */}
            <div className="mb-14">
              <h1
                className="text-6xl md:text-8xl font-bold mb-4 animate-fade-in"
                style={{ lineHeight: "1.1" }}
              >
                <span className="text-(--flux-blue)">Flux</span>
                <span className="text-(--flux-orange)">MQ</span>
              </h1>
              <div className="mono text-base md:text-xl text-theme-muted mb-6 border-l-4 border-(--flux-orange) pl-4 animate-slide-up">
                <div>PROTOCOL: MQTT 3.1.1 / 5.0 | HTTP | WebSocket | CoAP</div>
                <div>
                  THROUGHPUT: 300K-500K msg/s per node | LATENCY: &lt;10ms
                </div>
                <div>
                  ARCHITECTURE: Event-Driven | Clustered | Durable Queues
                </div>
              </div>
            </div>

            <p className="text-xl md:text-2xl mb-14 max-w-3xl leading-relaxed animate-fade-in">
              A high-performance, multi-protocol message broker written in Go
              for scalable IoT and event-driven architectures. Single binary. No
              external dependencies.
            </p>

            <div className="flex flex-wrap gap-4 animate-slide-up">
              <Link
                href="https://github.com/absmach/fluxmq"
                className="brutalist-border px-6 py-3 font-bold hover:bg-(--flux-blue) hover:text-white transition-colors inline-block"
                target="_blank"
                rel="noopener noreferrer"
              >
                VIEW ON GITHUB →
              </Link>
              <Link
                href="/docs"
                className="brutalist-border px-6 py-3 font-bold hover:bg-(--flux-orange) hover:text-white transition-colors inline-block"
              >
                READ DOCUMENTATION
              </Link>
            </div>
          </div>

          {/* Right side - Node Network Animation */}
          <div className="hidden md:flex items-center justify-center h-[80vh]">
            <NodeNetwork />
          </div>
        </div>
      </div>
    </section>
  );
}

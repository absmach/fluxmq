import { Activity, Cpu, ShieldCheck } from "lucide-react";

export function UseCasesSection() {
  return (
    <section id="use-cases" className="py-20 border-b-2 border-(--flux-border)">
      <div className="container mx-auto px-6">
        <h2 className="text-4xl md:text-5xl font-bold mb-12">
          <span className="border-l-4 border-(--flux-orange) pl-4">
            USE CASES
          </span>
        </h2>

        <div className="grid md:grid-cols-3 gap-6">
          {/* Event-Driven Architecture */}
          <div className="brutalist-card p-6">
            <Activity
              className="mb-4 text-(--flux-blue)"
              size={36}
              strokeWidth={2}
            />
            <div className="border-b-2 border-(--flux-border) pb-4 mb-4">
              <h3 className="text-2xl font-bold mono">
                Event-Driven Architecture
              </h3>
            </div>
            <ul className="space-y-3 text-theme-muted text-base">
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Event backbone for microservices</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>CQRS systems with durable queues</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Asynchronous workflows</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Real-time event processing</span>
              </li>
            </ul>
          </div>

          {/* IoT & Real-Time Systems */}
          <div className="brutalist-card p-6">
            <Cpu
              className="mb-4 text-(--flux-orange)"
              size={36}
              strokeWidth={2}
            />
            <div className="border-b-2 border-(--flux-border) pb-4 mb-4">
              <h3 className="text-2xl font-bold mono">IoT & Real-Time</h3>
            </div>
            <ul className="space-y-3 text-theme-muted text-base">
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Device communication (MQTT 3.1.1/5.0)</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Edge computing deployments</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Browser clients via WebSocket</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Constrained devices (CoAP bridge)</span>
              </li>
            </ul>
          </div>

          {/* High Availability */}
          <div className="brutalist-card p-6">
            <ShieldCheck
              className="mb-4 text-(--flux-blue)"
              size={36}
              strokeWidth={2}
            />
            <div className="border-b-2 border-(--flux-border) pb-4 mb-4">
              <h3 className="text-2xl font-bold mono">High Availability</h3>
            </div>
            <ul className="space-y-3 text-theme-muted text-base">
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Clustered deployments (3-5 nodes)</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Automatic failover (&lt;100ms)</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Geographic distribution</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Linear scalability</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}

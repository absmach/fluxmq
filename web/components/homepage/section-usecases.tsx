import { Activity, Cpu, Gauge } from "lucide-react";

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
          {/* Event-Driven Systems */}
          <div className="brutalist-card p-6">
            <Activity
              className="mb-4 text-(--flux-blue)"
              size={36}
              strokeWidth={2}
            />
            <div className="border-b-2 border-(--flux-border) pb-4 mb-4">
              <h3 className="text-2xl font-bold mono">Event-Driven Systems</h3>
            </div>
            <ul className="space-y-3 text-theme-muted text-base">
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Decouple microservices with event streams</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Reliable command & event pipelines (CQRS)</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Background jobs & asynchronous workflows</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Real-time data processing pipelines</span>
              </li>
            </ul>
          </div>

          {/* IoT & Real-Time */}
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
                <span>IoT device telemetry ingestion (MQTT)</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Edge deployments with intermittent connectivity</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Live dashboards & browser updates (WebSocket)</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Constrained device messaging via protocol bridges</span>
              </li>
            </ul>
          </div>

          {/* High-Throughput Data Pipelines */}
          <div className="brutalist-card p-6">
            <Gauge
              className="mb-4 text-(--flux-blue)"
              size={36}
              strokeWidth={2}
            />
            <div className="border-b-2 border-(--flux-border) pb-4 mb-4">
              <h3 className="text-2xl font-bold mono">
                High-Throughput Pipelines
              </h3>
            </div>
            <ul className="space-y-3 text-theme-muted text-base">
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Stream millions of events per second</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Buffer traffic bursts with durable queues</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Decouple ingestion from downstream processing</span>
              </li>
              <li className="flex items-start">
                <span className="text-(--flux-orange) mr-2 font-bold">■</span>
                <span>Power analytics & observability data streams</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}

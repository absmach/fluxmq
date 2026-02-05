export function PerformanceSection() {
  return (
    <section
      id="performance"
      className="py-20 border-b-2 border-theme bg-theme-alt"
    >
      <div className="container mx-auto px-6">
        <h2 className="text-4xl md:text-5xl font-bold mb-12">
          <span className="border-l-4 border-(--flux-orange) pl-4">
            PERFORMANCE
          </span>
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
              <li className="border-l-4 border-(--flux-blue) pl-4">
                <strong>3-node cluster:</strong> 1-2M msg/s
              </li>
              <li className="border-l-4 border-(--flux-blue) pl-4">
                <strong>5-node cluster:</strong> 2-4M msg/s
              </li>
              <li className="border-l-4 border-(--flux-blue) pl-4">
                <strong>Scaling:</strong> Linear with topic sharding
              </li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}

import Link from "next/link";
import { CodeBlock } from "@/components/code-block";

export function QuickStartSection() {
  return (
    <section
      id="quick-start"
      className="py-20 border-b-2 border-(--flux-border)"
    >
      <div className="container mx-auto px-6">
        <h2 className="text-4xl md:text-5xl font-bold mb-12">
          <span className="border-l-4 border-(--flux-orange) pl-4">
            QUICK START
          </span>
        </h2>

        <div className="max-w-4xl space-y-8">
          {/* Build & Run */}
          <div>
            <h3 className="mono font-bold mb-4 text-xl">1. BUILD & RUN</h3>
            <CodeBlock
              code={`git clone https://github.com/absmach/fluxmq.git
cd fluxmq
make build
./build/fluxmq`}
            />
          </div>

          {/* Test */}
          <div>
            <h3 className="mono font-bold mb-4 text-xl">2. TEST WITH MQTT</h3>
            <CodeBlock
              code={`# Subscribe to all topics
mosquitto_sub -p 1883 -t "test/#" -v

# Publish a message
mosquitto_pub -p 1883 -t "test/hello" -m "Hello FluxMQ"`}
            />
          </div>

          {/* Configuration Example */}
          <div>
            <h3 className="mono font-bold mb-4 text-xl">3. CONFIGURATION</h3>
            <CodeBlock
              language="yaml"
              code={`server:
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
            />
          </div>

          <div className="brutalist-border bg-theme p-8 mt-12">
            <p className="font-bold mb-4 text-lg">Next Steps:</p>
            <ul className="space-y-3">
              <li>
                <Link
                  href="/docs"
                  className="text---flux-blue) hover:underline font-bold text-lg"
                >
                  → Read the full documentation
                </Link>
              </li>
              <li>
                <a
                  href="https://github.com/absmach/fluxmq/tree/main/examples"
                  className="text-(--flux-blue) hover:underline font-bold text-lg"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  → Explore code examples on GitHub
                </a>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}

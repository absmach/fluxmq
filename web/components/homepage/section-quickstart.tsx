import Link from "next/link";
import { CodeBlock } from "@/components/code-block";
import { BookOpen, Github } from "lucide-react";

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

        <div className="max-w-4xl mx-auto space-y-8">
          {/* Docker Compose */}
          <div>
            <h3 className="mono font-bold mb-4 text-xl">1. RUN WITH DOCKER</h3>
            <CodeBlock
              code={`git clone https://github.com/absmach/fluxmq.git
cd fluxmq
docker compose -f docker/compose.yaml up -d`}
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

          {/* Local Build */}
          <div>
            <h3 className="mono font-bold mb-4 text-xl">3. OR BUILD LOCALLY</h3>
            <CodeBlock
              code={`git clone https://github.com/absmach/fluxmq.git
cd fluxmq
make build
./build/fluxmq --config examples/no-cluster.yaml`}
            />
          </div>

          <div className="brutalist-border bg-theme p-8 mt-12">
            <p className="font-bold mb-4 text-lg">
              Defaults (MQTT TCP: :1883, AMQP 0.9.1: :5682, Data:
              /tmp/fluxmq/data)
            </p>
            <p className="font-bold mb-4 text-lg">Next Steps:</p>
            <ul className="space-y-3">
              <li>
                <Link
                  href="https://github.com/absmach/fluxmq/tree/main/examples"
                  className="text-(--flux-blue) hover:underline font-bold text-lg flex items-center gap-2"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <Github size={20} />
                  Explore code examples on GitHub
                </Link>
              </li>
              <li>
                <Link
                  href="/docs"
                  className="text-(--flux-orange) hover:underline font-bold text-lg flex items-center gap-2"
                >
                  <BookOpen size={20} />
                  Read the full documentation
                </Link>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}

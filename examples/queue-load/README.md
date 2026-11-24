# Queue delivery load

Run against an existing local test cluster alongside protocol publishers:

```sh
go run ./examples/queue-load -amqp 'amqp://guest:guest@127.0.0.1:25682/,amqp://guest:guest@127.0.0.1:26682/,amqp://guest:guest@127.0.0.1:27682/'
```

Creates a dedicated classic queue per endpoint, named `console-queue-load-nodeN`.
Two connections share its consumer group and publish a combined 20 messages/sec
per node (`-interval` changes the per-node interval). Delivery processing takes
350–750 ms, exposing real pending deliveries. Every tenth message is NACKed on
its first delivery and acknowledged after redelivery; every seventeenth is
rejected into the broker's automatic dead-letter queue. Other messages are ACKed.
Failures and capture drops are never fabricated.

The two consumers exercise cross-consumer work stealing. The existing classic
single-consumer claim path does not retry its own pending entries.

Stops on SIGINT/SIGTERM and reconnects after broker restarts. Queue data remains
available for inspection. Use `-prefix` to isolate independent runs.

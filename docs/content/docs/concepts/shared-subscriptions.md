---
title: Shared Subscriptions
description: Load-balanced MQTT pub/sub delivery across a group of subscribers, including across a cluster
---

# Shared Subscriptions

**Last Updated:** 3rd September 2026

A shared subscription spreads one topic's messages across a group of subscribers instead of delivering a copy to each. Subscribe to `$share/<group>/<filter>` and every member of `<group>` shares the work matching `<filter>`: each message goes to exactly one of them.

```
$share/workers/sensors/#
   |      |         |
   |      |         +-- topic filter, ordinary MQTT wildcard rules
   |      +------------ share group name
   +------------------- shared subscription marker
```

Both MQTT 3.1.1 and 5.0 clients can use them. The prefix is parsed from the filter, so no protocol version negotiation is involved.

The group name must be non-empty and may not contain `+` or `#`, and a topic filter must follow it: `$share/workers` alone is rejected, as is `$share//sensors/#`. The filter after the group name follows ordinary wildcard rules.

Shared subscriptions are a **pub/sub** feature. They are not queue consumer groups: there is no log, no offset, no acknowledgment, and no redelivery of a message a member received but never processed. If you need those, use a [durable queue with a consumer group](/concepts/consumer-groups).

## How a Message Is Shared

Members take turns in a round robin. Publishing four messages to a group of two hands two to each.

The decision is made entirely on the node that received the publish, with no coordination and no round trip to anywhere else. That node:

1. **Builds the candidate list.** Its own members it holds directly. Members on other nodes it reads from the cluster's subscription index, which resolves each one to the node holding its session. The list is always its own members first, then the rest.
2. **Takes a turn.** It keeps one cursor per group and advances it: position `n` of a group of `size` members is `n = cursor++ % size`.
3. **Delivers.** If the position falls inside its own members, it delivers to that session directly. Otherwise it addresses the message to that one client on the node holding it, and that node delivers it as told — it does not match the topic again, which is what stops it choosing a second member of its own.

## Across a Cluster

A share group spans the whole cluster. Members connected to different nodes are one group, not one group per node, and each message reaches exactly one member wherever it is connected.

### A Worked Example

Three nodes. Node 1 holds three members, nodes 2 and 3 hold one each — five members in one group:

| Ingress node | Its own members | Members elsewhere | Rotation order |
| --- | --- | --- | --- |
| Node 1 | A, B, C | D, E | A B C D E |
| Node 2 | D | A, B, C, E | D A B C E |
| Node 3 | E | A, B, C, D | E A B C D |

Every node sees all five members and rotates over all five. What differs is the order: each node puts its own members first.

Ten messages published to node 1 go `A B C D E A B C D E` — a clean round robin. Ten messages spread across all three nodes do not form one global round robin: each node walks its own order with its own cursor, so a short burst can reach A twice before it reaches E once. Over a run of any length, each member still converges on its fifth of the traffic.

### Members Are Weighted, Not Nodes

Node 1 holds three of the five members, so it receives three fifths of the group's messages. Scaling a group means adding subscribers, and it does not matter where you add them — a node with more workers is given more work, which is what makes the group a worker pool rather than a node-level load balancer.

### Why the Cursor Is Node-Local

Nodes do not agree on whose turn it is, deliberately. Each spreads its own publishes across the whole group, and that balances the group without a coordination round trip on every message. The alternative — one cursor the cluster agrees on — would put a consensus operation in the publish path.

Two consequences follow:

- Balance is statistical rather than exact, as the worked example shows. Two nodes publishing at the same instant can choose the same member; those are two different messages, so nothing is duplicated.
- A publisher pinned to one node still spreads its work over every member of the group, not just the members on that node.

### Membership Propagation

Group membership travels through etcd, so a member that subscribed moments ago becomes visible to other nodes as the subscription watch propagates. Until then, publishes on other nodes choose among the members they can already see. No message is lost to this — some other member takes it — but a brand-new member does not start receiving instantly.

## Why a Message Is Never Duplicated

Only the node that received the publish ever chooses a member, and the ordinary cross-node broadcast is closed to share groups from both ends:

- The broadcast that carries a publish to nodes with matching subscribers skips shared subscriptions entirely. A share group member's node is not added to it, because the chosen member is sent to directly and a broadcast cannot tell whose turn it was.
- A publish that arrives from another node does not select from share groups at all. It reached this node for some ordinary subscription; selecting here would hand the group a second copy.

## When a Member Cannot Take a Message

The rotation skips forward. A member whose session has gone, or whose node is unreachable or reports the client is no longer there, is passed over for the next member in the group, and so on for one full pass. The group loses a message only when no member can take it.

A member that has gone but is still counted — a client that disconnected a moment ago, or reconnected to a different node before the ownership record caught up — therefore costs no messages, but does skew the share: its turn falls to whichever member follows it, until it is pruned from the group. A member on an unreachable node keeps taking its turn, but once that node's circuit breaker opens the attempt fails immediately rather than waiting on the network, and the turn passes on.

## Delivery Details

| Aspect | Behaviour |
| --- | --- |
| QoS | Capped per member at that member's own subscription QoS, and at the publish QoS. |
| Retain flag | Never set on a message delivered to a share group. |
| Retained messages | Not replayed when a shared subscription is created. |
| `NoLocal`, `RetainAsPublished` | Not applied to share group delivery. |
| Empty group | A group with no members matches nothing; the message is not stored for later. |
| Unsubscribing | Removes the member from the group; the group disappears with its last member. |

## Usage

Three workers share a sensor feed. Any MQTT client can publish to `sensors/room1/temp` as usual; the publisher needs to know nothing about the group.

```
worker-1:  SUBSCRIBE $share/telemetry/sensors/#   (QoS 1)
worker-2:  SUBSCRIBE $share/telemetry/sensors/#   (QoS 1)
worker-3:  SUBSCRIBE $share/telemetry/sensors/#   (QoS 1)

publisher: PUBLISH   sensors/room1/temp           (QoS 1)
```

One worker receives each message. Adding a fourth worker widens the rotation; stopping one narrows it, and its in-flight turn passes to another member.

A client can hold shared and ordinary subscriptions at once. A subscriber to both `$share/telemetry/sensors/#` and `sensors/#` receives every message through the ordinary subscription and its share of them through the group — they are separate subscriptions and do not deduplicate.

## See Also

- [Topics](/concepts/topics) — topic and filter syntax, reserved namespaces
- [Consumer Groups](/concepts/consumer-groups) — durable, acknowledged work distribution
- [Clustering](/concepts/clustering) — how sessions and subscriptions are shared between nodes

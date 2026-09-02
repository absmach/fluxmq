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

The turn is decided per publish, on the node that received the publish. That node counts the group's members — the ones connected to it and the ones connected to every other node — takes the next position in a single rotation across all of them, and delivers to the member whose turn it is.

## Across a Cluster

A share group spans the whole cluster. Members connected to different nodes are one group, not one group per node, and each message reaches exactly one member wherever it is connected.

When the chosen member is connected elsewhere, the publishing node addresses the message to that one client on the node holding its session. The receiving node delivers it as told rather than matching the topic again, which is what keeps the group to one copy per message.

**The rotation cursor is deliberately node-local.** Nodes do not agree on whose turn it is. Each spreads its own publishes across the whole group, which balances the group without a coordination round trip on every message. Two consequences follow:

- Balance is statistical rather than exact. Over enough messages from enough publishers, members receive an even share; over a handful of messages, they may not.
- A publisher pinned to one node still spreads its work over every member of the group, not just the members on that node.

Group membership travels through etcd, so a member that subscribed moments ago becomes visible to other nodes as the subscription watch propagates. Until then, publishes on other nodes choose among the members they can already see. No message is lost to this — some other member takes it — but a brand-new member does not start receiving instantly.

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

## Example

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

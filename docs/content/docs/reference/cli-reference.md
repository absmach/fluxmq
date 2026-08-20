---
title: CLI Reference
description: Command-line flags for starting FluxMQ
---

# CLI Reference

**Last Updated:** 5th February 2026

## fluxmq

```bash
./build/fluxmq [--config /path/to/config.yaml]
```

### Flags

- `--config` Path to a YAML configuration file. If omitted or the file is missing, defaults are used.

## Examples

```bash
./build/fluxmq
./build/fluxmq --config examples/no-cluster.yaml
```

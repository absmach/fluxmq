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

- `--config` Path to a YAML configuration file. A path that does not exist is a
  startup error: a typo used to start a broker on built-in defaults, which means
  no auth and no TLS, and report itself healthy.
- `--config-optional` Fall back to built-in defaults when the `--config` path is
  missing, instead of failing. Opt in deliberately; it is the old behaviour.

Omitting `--config` entirely still runs on defaults.

## Examples

```bash
./build/fluxmq
./build/fluxmq --config examples/no-cluster.yaml
./build/fluxmq --config /etc/fluxmq/config.yaml --config-optional
```

<div align="center">
  <h1>ST-RMQTT</h1>
  <p><strong>A high-performance, enterprise-ready MQTT broker and client library in Rust</strong></p>
  <p>
    Forked from <a href="https://github.com/bytebeamio/rumqtt">rumqtt</a> with significant enhancements for IoT/SCADA deployments
  </p>
</div>

<br/>

## Overview

ST-RMQTT is a fork of the [rumqtt](https://github.com/bytebeamio/rumqtt) project, extended with enterprise features for industrial IoT and SCADA systems. It provides a high-performance MQTT broker (`rumqttd`) and client library (`rumqttc`) written in Rust, with added support for multi-version protocol detection, session-aware topic handling, direct client messaging, multi-tenancy, and more.

| Crate | Description | Version |
|-------|-------------|---------|
| [rumqttd](./rumqttd/) | High-performance, embeddable MQTT broker | 0.20.0 |
| [rumqttc](./rumqttc/) | Async MQTT client library | 0.25.0 |

## Key Features (ST-RMQTT Additions)

### Unified Multi-Version MQTT Protocol Detection

A single broker port automatically detects and handles **MQTT 3.1, 3.1.1, and 5.0** clients simultaneously. No separate listeners needed per protocol version.

- Inspects the CONNECT packet's protocol level byte to determine version
- Zero-copy detection with minimal overhead (~10-20 bytes inspection)
- Existing MQTT clients work without any changes

```toml
[unified.unified]
name = "unified"
listen = "0.0.0.0:1883"
next_connection_delay_ms = 1
  [unified.unified.connections]
  connection_timeout_ms = 10000
  max_payload_size = 262144
  max_inflight_count = 100
  dynamic_filters = true
```

### Session-Aware `/me/` Topic Handling

ThingsBoard-compatible topic patterns where device identity comes from the authenticated session context, not the topic itself.

- Device ID stored securely in session after authentication -- cannot be spoofed
- Works with any MQTT client (mosquitto_pub, paho-mqtt, etc.)
- Publisher client ID tracked via `publisher_client_id` in message metadata

**Supported topic patterns:**
- `v1/devices/me/telemetry`
- `v1/devices/me/attributes`
- `v1/devices/me/rpc/request/{id}`
- `v1/devices/me/rpc/response/{id}`

### DirectWrite -- Targeted Client Messaging

Deliver messages directly to a specific client's outgoing buffer, bypassing the pub/sub routing engine entirely. This is the rumqttd equivalent of Netty's `writeAndFlush()`.

- Messages go straight to the target client -- no commitlog, no subscription matching
- Ideal for RPC responses, command delivery, and server-initiated notifications
- Gracefully handles disconnected clients (message dropped with debug log)

```rust
// Send directly to a specific device
link_tx.direct_write_to_client(
    "v1/devices/me/rpc/response/1",
    payload,
    "device-client-id".to_string(),
)?;
```

### Dynamic Topic Filters

Runtime topic filter creation -- when a publish arrives on a topic with no existing subscribers, a filter is dynamically created if enabled.

```toml
[v4.1.connections]
dynamic_filters = true  # Enable per-connection
```

Also available programmatically via `link_with_dynamic_filters()` for embedded broker usage.

### Multi-Tenancy with Tenant Prefix Validation

Certificate-based tenant extraction with enforced topic prefix isolation. Prevents cross-tenant topic access.

```bash
# Build with multi-tenancy support
cargo build --features validate-tenant-prefix
```

- Tenant ID extracted from mTLS client certificate (Org field)
- Topics must start with the authenticated tenant prefix
- Feature-gated: `validate-tenant-prefix` (implies `verify-client-cert`)

### Enhanced Authentication

- **Static credentials** configured in TOML
- **External auth handler** -- plug in custom async authentication logic
- **Constant-time password comparison** using `subtle::ConstantTimeEq` to prevent timing attacks

```toml
[v4.1.connections]
  [v4.1.connections.auth]
  user1 = "p@ssw0rd"
  user2 = "password"
```

### Duplicate Client ID Handling

Configurable behavior when clients connect with the same client ID.

```bash
# Allow duplicate client IDs instead of disconnecting existing client
cargo build --features allow-duplicate-clientid
```

## All Features

### Broker (rumqttd)

| Feature | Status | Notes |
|---------|--------|-------|
| MQTT 3.1 / 3.1.1 / 5.0 | Complete | All versions supported |
| Unified protocol detection | Complete | Single port, auto-detect version |
| Session-aware `/me/` topics | Complete | ThingsBoard-compatible |
| DirectWrite messaging | Complete | Bypass pub/sub for targeted delivery |
| Dynamic topic filters | Complete | Configurable per-connection |
| QoS 0, 1, and 2 | Complete | Full quality of service support |
| TLS / mTLS | Complete | rustls and native-tls backends |
| WebSocket + WSS | Complete | MQTT over WebSocket |
| Retained messages | Complete | Auto-forwarded to new subscribers |
| Last Will and Testament | Complete | With delay interval (MQTTv5) |
| Shared subscriptions | Complete | Round-robin, sticky, or random strategies |
| Multi-tenancy | Complete | Certificate-based tenant isolation |
| Constant-time auth | Complete | Timing-attack resistant |
| Duplicate client ID control | Complete | Feature-gated |
| Bridge linking | Complete | One-way broker-to-broker bridging with TLS |
| Prometheus metrics | Complete | Built-in exporter |
| Console HTTP API | Complete | Runtime config/metrics/log management |
| Custom segment sizes | Complete | Per-topic retention policies |
| Structured logging | Complete | `tracing` with `EnvFilter` |
| Random client ID assignment | Complete | UUID for empty client_id |

### Client (rumqttc)

| Feature | Status |
|---------|--------|
| MQTT 3.1.1 | Complete |
| MQTT 5.0 | Complete |
| TLS (rustls / native-tls) | Complete |
| WebSocket | Complete |
| Proxy support | Complete |
| Async and sync clients | Complete |
| Manual acknowledgement | Complete |

## Feature Flags

### rumqttd

| Flag | Default | Description |
|------|---------|-------------|
| `use-rustls` | Yes | TLS via rustls |
| `use-native-tls` | No | TLS via native-tls |
| `websocket` | Yes | MQTT over WebSocket support |
| `verify-client-cert` | No | mTLS client certificate verification |
| `validate-tenant-prefix` | No | Multi-tenant topic prefix validation |
| `allow-duplicate-clientid` | No | Allow multiple connections with same client ID |

### rumqttc

| Flag | Default | Description |
|------|---------|-------------|
| `use-rustls` | Yes | TLS via rustls |
| `use-native-tls` | No | TLS via native-tls |
| `websocket` | No | MQTT over WebSocket |
| `proxy` | No | HTTP proxy support |

## Installation and Usage

### Compile from source

```bash
git clone https://github.com/QuakeString/ST-RMQTT.git
cd ST-RMQTT
cargo run --release --bin rumqttd -- -c rumqttd/rumqttd.toml -vvv
```

### Run with Docker

```bash
docker build -t st-rmqtt .
docker run -p 1883:1883 -p 1884:1884 -p 8083:8083 -it st-rmqtt
```

Or mount a custom config:

```bash
docker run -p 1883:1883 -v /path/to/rumqttd.toml:/rumqttd.toml -it st-rmqtt -c /rumqttd.toml
```

### Using rumqttc in your project

```bash
cargo add rumqttc --git https://github.com/QuakeString/ST-RMQTT.git
```

### Using as a path dependency (for co-development)

```toml
[dependencies]
rumqttd = { path = "../ST-RMQTT/rumqttd" }
```

## Configuration

The broker is configured via a TOML file. Here is a reference configuration:

```toml
id = 0

[router]
id = 0
max_connections = 10010
max_outgoing_packet_count = 200
max_segment_size = 104857600     # 100MB per segment
max_segment_count = 10
# shared_subscriptions_strategy = "roundrobin"  # "sticky" | "roundrobin" | "random"

# Custom segment sizes per topic pattern
# [router.custom_segment.'/office/+/devices/status']
# max_segment_size = 102400
# max_segment_count = 2

# --- Listeners ---

# MQTT v4 (3.1.1)
[v4.1]
name = "v4-1"
listen = "0.0.0.0:1883"
next_connection_delay_ms = 1
  [v4.1.connections]
  connection_timeout_ms = 60000
  max_payload_size = 20480
  max_inflight_count = 100
  dynamic_filters = true
  # [v4.1.connections.auth]
  # user1 = "password"

# MQTT v5
[v5.1]
name = "v5-1"
listen = "0.0.0.0:1884"
next_connection_delay_ms = 1
  [v5.1.connections]
  connection_timeout_ms = 60000
  max_payload_size = 20480
  max_inflight_count = 100

# Unified (auto-detect MQTT version)
# [unified.unified]
# name = "unified"
# listen = "0.0.0.0:1883"
# next_connection_delay_ms = 1
#   [unified.unified.connections]
#   connection_timeout_ms = 10000
#   max_payload_size = 262144
#   max_inflight_count = 100
#   dynamic_filters = true

# WebSocket
[ws.1]
name = "ws-1"
listen = "0.0.0.0:8083"
next_connection_delay_ms = 1
  [ws.1.connections]
  connection_timeout_ms = 60000
  max_payload_size = 20480
  max_inflight_count = 500

# Prometheus metrics exporter
[prometheus]
listen = "127.0.0.1:9042"
interval = 1

# Console HTTP API
[console]
listen = "0.0.0.0:3030"

# Bridge to another broker
# [bridge]
# name = "bridge-1"
# addr = "localhost:1883"
# qos = 0
# sub_path = "#"
# reconnection_delay = 5
```

## Default Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 1883 | TCP | MQTT v3.1.1 (or unified) |
| 1884 | TCP | MQTT v5.0 |
| 8083 | TCP | MQTT over WebSocket |
| 8883 | TCP | MQTTS (TLS) |
| 3030 | HTTP | Console management API |
| 9042 | HTTP | Prometheus metrics |

## Architecture

```
                    +-----------------------------------------+
                    |            ST-RMQTT Broker               |
                    |                                          |
  MQTT 3.1 ------> |  +------------------------+              |
  MQTT 3.1.1 ----> |  |  Unified Protocol      |   Router    |
  MQTT 5.0 ------> |  |  Detection Layer       |-->Engine    |
  WebSocket ------> |  +------------------------+   |         |
                    |          |                     |         |
                    |    +-----v-------+     +-------v-------+ |
                    |    |   Auth      |     |  Commit Log   | |
                    |    |  Layer      |     |  (Segments)   | |
                    |    +-------------+     +---------------+ |
                    |                                          |
                    |    +-------------+     +---------------+ |
                    |    | DirectWrite |     |  Pub/Sub      | |
                    |    | (Targeted)  |     |  (Broadcast)  | |
                    |    +-------------+     +---------------+ |
                    |                                          |
                    |  +------------+  +-------------------+   |
                    |  | Metrics    |  |  Console HTTP     |   |
                    |  | Prometheus |  |      API          |   |
                    |  +------------+  +-------------------+   |
                    +-----------------------------------------+
```

## Minimum Rust Version

- **rumqttd**: 1.70.0
- **rumqttc**: 1.64.0

## Contributing

Please follow the [code of conduct](docs/CoC.md) while opening issues to report bugs or before you contribute fixes. Also read our [contributor guide](CONTRIBUTING.md).

## License

This project is released under The Apache License, Version 2.0 ([LICENSE](./LICENSE) or http://www.apache.org/licenses/LICENSE-2.0)

## Acknowledgments

ST-RMQTT is built on the excellent [rumqtt](https://github.com/bytebeamio/rumqtt) project by [Bytebeam](https://bytebeam.io/). We are grateful for their work in creating a solid foundation for a Rust MQTT ecosystem.

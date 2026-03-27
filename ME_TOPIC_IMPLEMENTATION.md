# /me/ Topic Pattern - SENTIENT SCADA Implementation

## Overview
This document describes the handling of `/me/` topic patterns in the SENTIENT SCADA MQTT implementation.

**IMPORTANT:** As of the latest update, rumqttd does **NOT** perform topic substitution. Topics are passed through unchanged to the backend, which handles `/me/` resolution using session-aware processing.

## Background

### Original Approach (Deprecated)
Initially, this implementation substituted `/me/` with `{client_id}` at the broker layer. This approach had several issues:
- Required `client_id` to be the device UUID
- Device ID could be spoofed via client_id
- Did not match ThingsBoard's actual implementation
- Did not work with standard MQTT tools (mosquitto_pub)

### ThingsBoard's Actual Approach
After analyzing the ThingsBoard codebase, we discovered that ThingsBoard does **NOT** substitute `/me/` in topics. Instead:

1. **Authentication Phase (MQTT CONNECT):**
   - Device provides access token as MQTT username
   - Backend validates token and retrieves device ID
   - Device ID is stored in session context

2. **Message Processing Phase (MQTT PUBLISH):**
   - Topic "v1/devices/me/telemetry" is received unchanged
   - Topic is matched literally (string comparison)
   - Backend uses device ID from the authenticated session (not from topic)

3. **Key Insight:**
   - The "me" is not a placeholder - it's a literal topic string
   - Device identity comes from session authentication, not topic parsing
   - MQTT client_id is completely ignored

## Current Implementation

### rumqttd (Broker Layer)
**File:** `rumqttd/src/router/routing.rs`

Topics are **NOT** modified. All PUBLISH, SUBSCRIBE, and UNSUBSCRIBE packets pass through unchanged:

```rust
// Topics pass through unchanged - backend handles /me/ resolution via session context
```

### Backend (Application Layer)
**File:** `transport/src/mqtt/broker.rs`

The backend implements session-aware `/me/` topic resolution:

1. When a device connects, authentication creates a session mapping: `client_id` → `device_id`
2. When a message arrives on a `/me/` topic:
   - Backend looks up the session by client_id
   - Retrieves the authenticated device_id from session
   - Processes message using the authenticated device_id

This matches ThingsBoard's approach and provides:
- ✅ Security: Device ID cannot be spoofed
- ✅ Compatibility: Works with any MQTT client
- ✅ Standards compliance: No client_id requirements
- ✅ Tool support: Works with mosquitto_pub

## Usage

### Device Connection
```bash
# Only access token is required
mosquitto_pub \
  -h 127.0.0.1 -p 1883 \
  -t "v1/devices/me/telemetry" \
  -u "ACCESS_TOKEN" \
  -m '{"temperature":25}'

# Note: -i (client_id) is optional
```

### Topic Patterns Supported
- `v1/devices/me/telemetry` - Device telemetry
- `v1/devices/me/attributes` - Device attributes
- `v1/devices/me/rpc/request/{id}` - RPC requests
- `v1/devices/me/rpc/response/{id}` - RPC responses

## Implementation Details

See `/home/mz/Work/Development/INVENIA_IIoT/SENTIENT_SCADA/THINGSBOARD_ME_TOPIC_IMPLEMENTATION.md` for:
- Complete ThingsBoard code flow analysis
- Security comparison
- Detailed implementation plan
- Testing strategies

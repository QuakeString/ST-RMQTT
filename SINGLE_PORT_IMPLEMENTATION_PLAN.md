# Single-Port MQTT Protocol Auto-Detection Implementation Plan

## Problem Statement
Currently, rumqttd requires separate ports for MQTT v3.1.1/v4 (port 1883) and MQTT v5 (port 1885).
We want to support all protocol versions on a single port (1883), similar to how ThingsBoard's
netty-based MQTT implementation works.

## Netty's Approach (Reference)
Netty's `MqttDecoder` automatically detects the protocol version from the CONNECT packet:
1. Reads protocol name and level from CONNECT header
2. Uses `MqttVersion.fromProtocolNameAndLevel()` to detect version
3. Stores version in channel context
4. Routes to appropriate handler based on version

## Current rumqttd Architecture

### Key Files:
- `server/broker.rs`: Server creation and connection handling
- `link/network.rs`: Network I/O with protocol-specific encoding/decoding
- `link/remote.rs`: Remote connection handling and CONNECT processing
- `protocol/mod.rs`: Protocol trait and packet definitions
- `protocol/v4/`: V4-specific encoding/decoding
- `protocol/v5/`: V5-specific encoding/decoding

### Current Flow:
1. `Broker::start()` creates separate `Server<V4>` and `Server<V5>` instances
2. Each server spawns with a fixed protocol type
3. `Server::start()` accepts connections and calls `remote()`
4. `remote()` creates `Network<P>` with the fixed protocol
5. Protocol is used for all packet encoding/decoding

### Problem Location:
In `protocol/v4/connect.rs` line 40-42:
```rust
if protocol_level != 4 {
    return Err(Error::InvalidProtocolLevel(protocol_level));
}
```
This rejects v5 connections (protocol level 5) on v4 listener.

## Implementation Strategy

### Phase 1: Protocol Version Detection

**File: `protocol/unified.rs` (NEW)**

Create a unified protocol handler that auto-detects version:

```rust
use super::{v4::V4, v5::V5, Error, Packet, Protocol};
use bytes::BytesMut;

/// Detected protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    Unknown,
    V31,    // MQTT 3.1 (protocol level 3)
    V311,   // MQTT 3.1.1 (protocol level 4)
    V5,     // MQTT 5.0 (protocol level 5)
}

/// Unified protocol handler that auto-detects version from CONNECT packet
pub struct UnifiedProtocol {
    version: ProtocolVersion,
    v4: V4,
    v5: V5,
}

impl UnifiedProtocol {
    pub fn new() -> Self {
        Self {
            version: ProtocolVersion::Unknown,
            v4: V4,
            v5: V5,
        }
    }

    /// Detect protocol version from CONNECT packet bytes
    /// MQTT CONNECT packet structure:
    /// - Fixed header: 1 byte (packet type) + 1-4 bytes (remaining length)
    /// - Variable header:
    ///   - Protocol name length: 2 bytes
    ///   - Protocol name: "MQTT" (4 bytes) or "MQIsdp" (6 bytes)
    ///   - Protocol level: 1 byte (3=v3.1, 4=v3.1.1, 5=v5.0)
    pub fn detect_version(bytes: &[u8]) -> Option<ProtocolVersion> {
        // Need at least: fixed header (2) + proto name len (2) + "MQTT" (4) + level (1) = 9
        if bytes.len() < 9 {
            return None;
        }

        // Check packet type (high nibble of first byte) is CONNECT (0x10)
        if bytes[0] & 0xF0 != 0x10 {
            return None;
        }

        // Parse remaining length to find start of variable header
        let (remaining_len, header_len) = Self::parse_remaining_length(&bytes[1..])?;
        let var_header_start = 1 + header_len;

        if bytes.len() < var_header_start + 7 {
            return None;
        }

        // Read protocol name length
        let proto_name_len = u16::from_be_bytes([
            bytes[var_header_start],
            bytes[var_header_start + 1]
        ]) as usize;

        // Protocol level is after protocol name
        let proto_level_offset = var_header_start + 2 + proto_name_len;
        if bytes.len() <= proto_level_offset {
            return None;
        }

        let proto_level = bytes[proto_level_offset];

        match proto_level {
            3 => Some(ProtocolVersion::V31),
            4 => Some(ProtocolVersion::V311),
            5 => Some(ProtocolVersion::V5),
            _ => None,
        }
    }

    fn parse_remaining_length(bytes: &[u8]) -> Option<(usize, usize)> {
        let mut multiplier = 1;
        let mut value = 0usize;
        let mut i = 0;

        loop {
            if i >= bytes.len() || i >= 4 {
                return None;
            }
            let byte = bytes[i];
            value += ((byte & 0x7F) as usize) * multiplier;
            multiplier *= 128;
            i += 1;

            if byte & 0x80 == 0 {
                return Some((value, i));
            }
        }
    }

    pub fn version(&self) -> ProtocolVersion {
        self.version
    }

    pub fn set_version(&mut self, version: ProtocolVersion) {
        self.version = version;
    }
}

impl Protocol for UnifiedProtocol {
    fn read_mut(&mut self, stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
        // If version is unknown, detect it from CONNECT packet
        if self.version == ProtocolVersion::Unknown {
            if let Some(version) = Self::detect_version(stream) {
                self.version = version;
                tracing::info!("Detected MQTT protocol version: {:?}", version);
            }
        }

        // Use appropriate protocol based on detected version
        match self.version {
            ProtocolVersion::V5 => self.v5.read_mut(stream, max_size),
            _ => self.v4.read_mut(stream, max_size), // V31, V311, Unknown all use V4 codec
        }
    }

    fn write(&self, packet: Packet, write: &mut BytesMut) -> Result<usize, Error> {
        match self.version {
            ProtocolVersion::V5 => self.v5.write(packet, write),
            _ => self.v4.write(packet, write),
        }
    }
}

impl Clone for UnifiedProtocol {
    fn clone(&self) -> Self {
        Self {
            version: self.version,
            v4: V4,
            v5: V5,
        }
    }
}
```

### Phase 2: Update Protocol Module

**File: `protocol/mod.rs`**

Add the unified module:
```rust
pub mod unified;
pub use unified::{UnifiedProtocol, ProtocolVersion};
```

### Phase 3: Update Broker Configuration

**File: `lib.rs`**

Add unified server config:
```rust
pub struct Config {
    // ... existing fields ...
    pub unified: Option<HashMap<String, ServerSettings>>,
}
```

### Phase 4: Update Broker to Support Unified Server

**File: `server/broker.rs`**

Add unified server support:
```rust
// After v4 and v5 server spawning, add:
if let Some(unified_config) = &self.config.unified {
    for (_, config) in unified_config.clone() {
        let server_thread = thread::Builder::new().name(config.name.clone());
        let mut server = Server::new(config, self.router_tx.clone(), UnifiedProtocol::new());
        let handle = server_thread.spawn(move || {
            let mut runtime = tokio::runtime::Builder::new_current_thread();
            let runtime = runtime.enable_all().build().unwrap();

            runtime.block_on(async {
                if let Err(e) = server.start(LinkType::Remote).await {
                    error!(error=?e, "Server error - Unified");
                }
            });
        })?;
        server_thread_handles.push(handle)
    }
}
```

### Phase 5: Update V4 Connect Parser

**File: `protocol/v4/connect.rs`**

Remove strict protocol level check to allow v3.1 as well:
```rust
pub fn read(...) -> Result<...> {
    // ... existing code ...

    let protocol_level = read_u8(&mut bytes)?;
    if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
        return Err(Error::InvalidProtocol);
    }

    // Accept both v3.1 (level 3) and v3.1.1 (level 4)
    if protocol_level != 3 && protocol_level != 4 {
        return Err(Error::InvalidProtocolLevel(protocol_level));
    }

    // ... rest of code ...
}
```

## Testing Plan

1. **Unit Tests**: Add tests for protocol version detection
2. **Integration Tests**:
   - Connect v3.1.1 client to unified port
   - Connect v5 client to unified port
   - Mix v3.1.1 and v5 clients on same port
   - Cross-version pub/sub on unified port

## Backward Compatibility

- Existing `[v4.x]` and `[v5.x]` config sections continue to work
- New `[unified.x]` config section for single-port operation
- No breaking changes to existing deployments

## Files to Modify

1. `rumqttd/src/protocol/mod.rs` - Add unified module export
2. `rumqttd/src/protocol/unified.rs` - NEW: Unified protocol handler
3. `rumqttd/src/server/broker.rs` - Add unified server support
4. `rumqttd/src/lib.rs` - Add unified config
5. `rumqttd/src/protocol/v4/connect.rs` - Accept v3.1 protocol level

## Implementation Order

1. Create `protocol/unified.rs` with version detection
2. Export from `protocol/mod.rs`
3. Update `lib.rs` config
4. Update `server/broker.rs` for unified server
5. Test with single-port test script

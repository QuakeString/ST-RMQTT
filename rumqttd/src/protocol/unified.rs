//! Unified Protocol Handler for MQTT Protocol Auto-Detection
//!
//! This module provides a unified protocol handler that can auto-detect the MQTT
//! protocol version (v3.1, v3.1.1, or v5.0) from the CONNECT packet and delegate
//! to the appropriate protocol implementation.
//!
//! This approach is similar to how Netty's MqttDecoder works - it inspects the
//! protocol level byte in the CONNECT packet to determine the version.

use super::{v4::V4, v5::V5, Error, Packet, Protocol};
use bytes::BytesMut;

/// Detected MQTT protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// Version not yet detected (before CONNECT packet)
    #[default]
    Unknown,
    /// MQTT 3.1 (protocol level 3)
    V31,
    /// MQTT 3.1.1 (protocol level 4)
    V311,
    /// MQTT 5.0 (protocol level 5)
    V5,
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolVersion::Unknown => write!(f, "Unknown"),
            ProtocolVersion::V31 => write!(f, "MQTT 3.1"),
            ProtocolVersion::V311 => write!(f, "MQTT 3.1.1"),
            ProtocolVersion::V5 => write!(f, "MQTT 5.0"),
        }
    }
}

/// Unified protocol handler that auto-detects MQTT version from CONNECT packet.
///
/// This handler inspects the first packet (CONNECT) to determine the protocol
/// version, then delegates all encoding/decoding to the appropriate version-specific
/// handler (V4 for MQTT 3.1/3.1.1, V5 for MQTT 5.0).
///
/// # Protocol Detection
///
/// The CONNECT packet structure allows version detection:
/// - Fixed header: 1 byte (packet type) + 1-4 bytes (remaining length)
/// - Variable header:
///   - Protocol name length: 2 bytes
///   - Protocol name: "MQTT" (4 bytes) or "MQIsdp" (6 bytes for v3.1)
///   - Protocol level: 1 byte (3=v3.1, 4=v3.1.1, 5=v5.0)
///
/// # Example
///
/// ```ignore
/// use rumqttd::protocol::unified::UnifiedProtocol;
///
/// let protocol = UnifiedProtocol::new();
/// // Use with Network<UnifiedProtocol> for single-port operation
/// ```
#[derive(Debug)]
pub struct UnifiedProtocol {
    /// Detected protocol version
    version: ProtocolVersion,
    /// V4 protocol handler (for MQTT 3.1/3.1.1)
    v4: V4,
    /// V5 protocol handler (for MQTT 5.0)
    v5: V5,
}

impl Default for UnifiedProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl UnifiedProtocol {
    /// Create a new unified protocol handler with unknown version.
    /// Version will be detected from the first CONNECT packet.
    pub fn new() -> Self {
        Self {
            version: ProtocolVersion::Unknown,
            v4: V4,
            v5: V5,
        }
    }

    /// Get the detected protocol version.
    pub fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// Manually set the protocol version (useful for testing).
    pub fn set_version(&mut self, version: ProtocolVersion) {
        self.version = version;
    }

    /// Detect protocol version from CONNECT packet bytes.
    ///
    /// # MQTT CONNECT Packet Structure
    ///
    /// ```text
    /// Fixed Header:
    /// +--------+--------+--------+--------+
    /// | 0x10   | Remaining Length (1-4 bytes)
    /// +--------+--------+--------+--------+
    ///
    /// Variable Header:
    /// +--------+--------+
    /// | Protocol Name Length (2 bytes)     |
    /// +--------+--------+--------+--------+
    /// | Protocol Name ("MQTT" or "MQIsdp") |
    /// +--------+--------+--------+--------+
    /// | Protocol Level (1 byte)            |
    /// | 3 = v3.1, 4 = v3.1.1, 5 = v5.0    |
    /// +--------+--------+--------+--------+
    /// ```
    ///
    /// Returns `Some(version)` if detection succeeds, `None` if more bytes needed
    /// or if the packet is not a valid CONNECT packet.
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
        let (_, header_len) = Self::parse_remaining_length(&bytes[1..])?;
        let var_header_start = 1 + header_len;

        // Need at least var_header_start + protocol name len (2) + min name (4) + level (1)
        if bytes.len() < var_header_start + 7 {
            return None;
        }

        // Read protocol name length
        let proto_name_len = u16::from_be_bytes([
            bytes[var_header_start],
            bytes[var_header_start + 1],
        ]) as usize;

        // Validate protocol name length (4 for "MQTT", 6 for "MQIsdp")
        if proto_name_len != 4 && proto_name_len != 6 {
            return None;
        }

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

    /// Parse MQTT remaining length encoding.
    /// Returns (remaining_length, number_of_bytes_used).
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

            // If continuation bit is 0, we're done
            if byte & 0x80 == 0 {
                return Some((value, i));
            }
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

impl Protocol for UnifiedProtocol {
    /// Read a packet from the stream, auto-detecting protocol version if unknown.
    fn read_mut(&mut self, stream: &mut BytesMut, max_size: usize) -> Result<Packet, Error> {
        // If version is unknown, detect it from CONNECT packet
        if self.version == ProtocolVersion::Unknown {
            if let Some(version) = Self::detect_version(stream) {
                self.version = version;
                tracing::info!(
                    protocol_version = %version,
                    "Auto-detected MQTT protocol version"
                );
            }
            // If still unknown (insufficient bytes), the underlying protocol read
            // will return InsufficientBytes error
        }

        // Use appropriate protocol based on detected version
        match self.version {
            ProtocolVersion::V5 => self.v5.read_mut(stream, max_size),
            // V31, V311, and Unknown all use V4 codec
            // V4 codec accepts both v3.1 (level 3) and v3.1.1 (level 4)
            _ => self.v4.read_mut(stream, max_size),
        }
    }

    /// Write a packet to the stream using the detected protocol version.
    fn write(&self, packet: Packet, buffer: &mut BytesMut) -> Result<usize, Error> {
        match self.version {
            ProtocolVersion::V5 => self.v5.write(packet, buffer),
            // V31, V311, and Unknown all use V4 codec
            _ => self.v4.write(packet, buffer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_v311() {
        // MQTT 3.1.1 CONNECT packet
        let connect_v311 = [
            0x10, // CONNECT packet type
            0x10, // Remaining length
            0x00, 0x04, // Protocol name length
            b'M', b'Q', b'T', b'T', // Protocol name
            0x04, // Protocol level (v3.1.1)
            0x02, // Connect flags
            0x00, 0x3C, // Keep alive
            0x00, 0x04, // Client ID length
            b't', b'e', b's', b't', // Client ID
        ];

        let version = UnifiedProtocol::detect_version(&connect_v311);
        assert_eq!(version, Some(ProtocolVersion::V311));
    }

    #[test]
    fn test_detect_v5() {
        // MQTT 5.0 CONNECT packet
        let connect_v5 = [
            0x10, // CONNECT packet type
            0x11, // Remaining length
            0x00, 0x04, // Protocol name length
            b'M', b'Q', b'T', b'T', // Protocol name
            0x05, // Protocol level (v5.0)
            0x02, // Connect flags
            0x00, 0x3C, // Keep alive
            0x00, // Properties length
            0x00, 0x04, // Client ID length
            b't', b'e', b's', b't', // Client ID
        ];

        let version = UnifiedProtocol::detect_version(&connect_v5);
        assert_eq!(version, Some(ProtocolVersion::V5));
    }

    #[test]
    fn test_detect_v31() {
        // MQTT 3.1 CONNECT packet (uses MQIsdp)
        let connect_v31 = [
            0x10, // CONNECT packet type
            0x12, // Remaining length
            0x00, 0x06, // Protocol name length
            b'M', b'Q', b'I', b's', b'd', b'p', // Protocol name (v3.1)
            0x03, // Protocol level (v3.1)
            0x02, // Connect flags
            0x00, 0x3C, // Keep alive
            0x00, 0x04, // Client ID length
            b't', b'e', b's', b't', // Client ID
        ];

        let version = UnifiedProtocol::detect_version(&connect_v31);
        assert_eq!(version, Some(ProtocolVersion::V31));
    }

    #[test]
    fn test_detect_insufficient_bytes() {
        let partial = [0x10, 0x10, 0x00];
        let version = UnifiedProtocol::detect_version(&partial);
        assert_eq!(version, None);
    }

    #[test]
    fn test_detect_not_connect() {
        // PUBLISH packet (type 0x30)
        let publish = [0x30, 0x05, 0x00, 0x01, b'a', b'b', b'c'];
        let version = UnifiedProtocol::detect_version(&publish);
        assert_eq!(version, None);
    }
}

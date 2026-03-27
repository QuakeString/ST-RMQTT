//! Test unified protocol auto-detection on single port
//!
//! This example demonstrates MQTT protocol auto-detection:
//! - Single port (1883) accepts all MQTT versions (v3.1, v3.1.1, v5.0)
//! - Protocol version is auto-detected from CONNECT packet
//!
//! Run with: cargo run --release --example unified_test

use rumqttd::{Broker, Config, RouterConfig, ServerSettings, ConnectionSettings};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info,rumqttd=debug")
        .init();

    println!("=================================================");
    println!(" Unified MQTT Protocol Auto-Detection Test");
    println!("=================================================");
    println!();
    println!("This test starts a single MQTT broker on port 1883");
    println!("that auto-detects protocol version from CONNECT packet.");
    println!();

    // Create config with unified server
    let mut config = Config::default();
    config.id = 0;
    config.router = RouterConfig {
        max_connections: 10000,
        max_outgoing_packet_count: 100,
        max_segment_size: 10_485_760,
        max_segment_count: 5,
        ..Default::default()
    };

    // Configure unified server (auto-detect protocol version)
    let listen_addr: SocketAddr = "0.0.0.0:1883".parse().unwrap();
    let mut unified_servers = HashMap::new();
    unified_servers.insert(
        "unified".to_string(),
        ServerSettings {
            name: "unified".to_string(),
            listen: listen_addr,
            tls: None,
            next_connection_delay_ms: 1,
            connections: ConnectionSettings {
                connection_timeout_ms: 10000,
                max_payload_size: 262144, // 256 KB
                max_inflight_count: 100,
                dynamic_filters: true,
                auth: None,
                external_auth: None,
            },
        },
    );
    config.unified = Some(unified_servers);

    // No separate v4 or v5 servers
    config.v4 = None;
    config.v5 = None;
    config.console = None;

    println!("Starting unified MQTT broker on port 1883...");
    println!();

    // Start broker
    let mut broker = Broker::new(config);

    // Start broker in background thread
    thread::spawn(move || {
        if let Err(e) = broker.start() {
            eprintln!("Broker error: {:?}", e);
        }
    });

    // Wait for broker to start
    thread::sleep(Duration::from_secs(2));

    println!("✓ Broker started successfully!");
    println!();
    println!("=================================================");
    println!(" Test Instructions");
    println!("=================================================");
    println!();
    println!("The broker is now running on port 1883.");
    println!("You can test with different MQTT clients:");
    println!();
    println!("1. MQTT v3.1.1 client (mosquitto_pub):");
    println!("   mosquitto_pub -h localhost -p 1883 -t test/v4 -m 'Hello from v3.1.1' -V mqttv311");
    println!();
    println!("2. MQTT v5.0 client (mosquitto_pub):");
    println!("   mosquitto_pub -h localhost -p 1883 -t test/v5 -m 'Hello from v5' -V mqttv5");
    println!();
    println!("3. Subscribe to all topics:");
    println!("   mosquitto_sub -h localhost -p 1883 -t '#' -v");
    println!();
    println!("4. Python paho-mqtt v3.1.1:");
    println!("   import paho.mqtt.client as mqtt");
    println!("   client = mqtt.Client(protocol=mqtt.MQTTv311)");
    println!("   client.connect('localhost', 1883)");
    println!();
    println!("5. Python paho-mqtt v5.0:");
    println!("   import paho.mqtt.client as mqtt");
    println!("   client = mqtt.Client(protocol=mqtt.MQTTv5)");
    println!("   client.connect('localhost', 1883)");
    println!();
    println!("Press Ctrl+C to stop the broker.");
    println!("=================================================");

    // Keep main thread alive
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

use rumqttc::{MqttOptions, AsyncClient, QoS};
use rumqttc::v5::{MqttOptions as MqttOptionsV5, AsyncClient as AsyncClientV5};
use std::time::Duration;
use tokio::{task, time};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Subscriber (V5)
    let mut mqttoptions_v5 = MqttOptionsV5::new("subscriber-v5", "localhost", 1884);
    mqttoptions_v5.set_keep_alive(Duration::from_secs(5));
    
    let (mut client_v5, mut eventloop_v5) = AsyncClientV5::new(mqttoptions_v5, 10);
    
    // Subscribe
    client_v5.subscribe("hello/cross_version", rumqttc::v5::mqttbytes::QoS::AtLeastOnce).await?;
    println!("V5 Subscriber subscribed");

    // Publisher (V3)
    let mut mqttoptions_v3 = MqttOptions::new("publisher-v3", "localhost", 1883);
    mqttoptions_v3.set_keep_alive(Duration::from_secs(5));
    
    let (mut client_v3, mut eventloop_v3) = AsyncClient::new(mqttoptions_v3, 10);

    // Spawn V5 eventloop
    task::spawn(async move {
        loop {
            match eventloop_v5.poll().await {
                Ok(event) => {
                    // println!("V5 Event: {:?}", event);
                    if let rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(publish)) = event {
                        println!("V5 Received: {:?}", publish);
                        std::process::exit(0); // Success!
                    }
                }
                Err(e) => {
                    eprintln!("V5 Error: {:?}", e);
                    break;
                }
            }
        }
    });

    // Spawn V3 eventloop
    task::spawn(async move {
        loop {
            if let Err(e) = eventloop_v3.poll().await {
                eprintln!("V3 Error: {:?}", e);
                break;
            }
        }
    });

    // Wait for subscription to propagate
    time::sleep(Duration::from_secs(1)).await;

    // Publish from V3
    println!("V3 Publishing...");
    client_v3.publish("hello/cross_version", QoS::AtLeastOnce, false, vec![1, 2, 3]).await?;

    // Wait for reception
    time::sleep(Duration::from_secs(5)).await;
    println!("Timeout waiting for message");
    std::process::exit(1); // Failure
}

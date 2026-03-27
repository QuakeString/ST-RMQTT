use rumqttc::{MqttOptions, AsyncClient, QoS};
use std::time::Duration;
use tokio::{task, time};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Subscriber (V4/V3.1.1)
    let mut mqttoptions_v4 = MqttOptions::new("subscriber-v4", "localhost", 1883);
    mqttoptions_v4.set_keep_alive(Duration::from_secs(5));
    
    let (mut client_v4, mut eventloop_v4) = AsyncClient::new(mqttoptions_v4, 10);
    
    // Subscribe
    client_v4.subscribe("hello/v4_compat", QoS::AtLeastOnce).await?;
    println!("V4 Subscriber subscribed");

    // Publisher (V3)
    let mut mqttoptions_v3 = MqttOptions::new("publisher-v3-for-v4", "localhost", 1883);
    mqttoptions_v3.set_keep_alive(Duration::from_secs(5));
    
    let (mut client_v3, mut eventloop_v3) = AsyncClient::new(mqttoptions_v3, 10);

    // Spawn V4 eventloop
    task::spawn(async move {
        loop {
            match eventloop_v4.poll().await {
                Ok(event) => {
                    if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) = event {
                        println!("V4 Received: {:?}", publish);
                        std::process::exit(0); // Success!
                    }
                }
                Err(e) => {
                    eprintln!("V4 Error: {:?}", e);
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
    client_v3.publish("hello/v4_compat", QoS::AtLeastOnce, false, vec![1, 2, 3]).await?;

    // Wait for reception
    time::sleep(Duration::from_secs(5)).await;
    println!("Timeout waiting for message");
    std::process::exit(1); // Failure
}

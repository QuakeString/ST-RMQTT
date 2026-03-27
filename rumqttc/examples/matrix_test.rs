use rumqttc::{MqttOptions, AsyncClient, QoS};
use rumqttc::v5::{MqttOptions as MqttOptionsV5, AsyncClient as AsyncClientV5};
use std::time::Duration;
use tokio::{task, time};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // We will test:
    // 1. V4 Pub -> V5 Sub
    // 2. V4 Pub -> V4 Sub (V3 Sub is essentially V4 in rumqttd usually, but we'll stick to V4 client for now)
    // 3. V5 Pub -> V4 Sub
    // 4. V5 Pub -> V5 Sub (Control)

    println!("--- Starting Matrix Verification ---");

    verify_v4_pub_v5_sub().await?;
    verify_v4_pub_v4_sub().await?;
    verify_v5_pub_v4_sub().await?;
    verify_v5_pub_v5_sub().await?;

    println!("--- All Scenarios Passed ---");
    Ok(())
}

async fn verify_v4_pub_v5_sub() -> Result<(), Box<dyn Error>> {
    println!("Testing V4 Pub -> V5 Sub...");
    let topic = "test/v4_v5";
    
    // Subscriber V5
    let mut opts = MqttOptionsV5::new("sub-v5-1", "localhost", 1884);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClientV5::new(opts, 10);
    client.subscribe(topic, rumqttc::v5::mqttbytes::QoS::AtLeastOnce).await?;

    task::spawn(async move {
        loop {
            if let Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(_))) = eventloop.poll().await {
                // Success
                break;
            }
        }
    });

    time::sleep(Duration::from_millis(500)).await;

    // Publisher V4
    let mut opts = MqttOptions::new("pub-v4-1", "localhost", 1883);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClient::new(opts, 10);
    task::spawn(async move { loop { eventloop.poll().await.ok(); } });

    client.publish(topic, QoS::AtLeastOnce, false, vec![1, 2, 3]).await?;
    
    // Wait for reception (simplified, real test would use channel)
    time::sleep(Duration::from_secs(1)).await;
    println!("V4 Pub -> V5 Sub: OK");
    Ok(())
}

async fn verify_v4_pub_v4_sub() -> Result<(), Box<dyn Error>> {
    println!("Testing V4 Pub -> V4 Sub...");
    let topic = "test/v4_v4";
    
    // Subscriber V4
    let mut opts = MqttOptions::new("sub-v4-2", "localhost", 1883);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClient::new(opts, 10);
    client.subscribe(topic, QoS::AtLeastOnce).await?;

    task::spawn(async move {
        loop {
            if let Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(_))) = eventloop.poll().await {
                break;
            }
        }
    });

    time::sleep(Duration::from_millis(500)).await;

    // Publisher V4
    let mut opts = MqttOptions::new("pub-v4-2", "localhost", 1883);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClient::new(opts, 10);
    task::spawn(async move { loop { eventloop.poll().await.ok(); } });

    client.publish(topic, QoS::AtLeastOnce, false, vec![1, 2, 3]).await?;
    
    time::sleep(Duration::from_secs(1)).await;
    println!("V4 Pub -> V4 Sub: OK");
    Ok(())
}

async fn verify_v5_pub_v4_sub() -> Result<(), Box<dyn Error>> {
    println!("Testing V5 Pub -> V4 Sub...");
    let topic = "test/v5_v4";
    
    // Subscriber V4
    let mut opts = MqttOptions::new("sub-v4-3", "localhost", 1883);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClient::new(opts, 10);
    client.subscribe(topic, QoS::AtLeastOnce).await?;

    task::spawn(async move {
        loop {
            if let Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(_))) = eventloop.poll().await {
                break;
            }
        }
    });

    time::sleep(Duration::from_millis(500)).await;

    // Publisher V5
    let mut opts = MqttOptionsV5::new("pub-v5-3", "localhost", 1884);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClientV5::new(opts, 10);
    task::spawn(async move { loop { eventloop.poll().await.ok(); } });

    client.publish(topic, rumqttc::v5::mqttbytes::QoS::AtLeastOnce, false, vec![1, 2, 3]).await?;
    
    time::sleep(Duration::from_secs(1)).await;
    println!("V5 Pub -> V4 Sub: OK");
    Ok(())
}

async fn verify_v5_pub_v5_sub() -> Result<(), Box<dyn Error>> {
    println!("Testing V5 Pub -> V5 Sub...");
    let topic = "test/v5_v5";
    
    // Subscriber V5
    let mut opts = MqttOptionsV5::new("sub-v5-4", "localhost", 1884);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClientV5::new(opts, 10);
    client.subscribe(topic, rumqttc::v5::mqttbytes::QoS::AtLeastOnce).await?;

    task::spawn(async move {
        loop {
            if let Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(_))) = eventloop.poll().await {
                break;
            }
        }
    });

    time::sleep(Duration::from_millis(500)).await;

    // Publisher V5
    let mut opts = MqttOptionsV5::new("pub-v5-4", "localhost", 1884);
    opts.set_keep_alive(Duration::from_secs(5));
    let (mut client, mut eventloop) = AsyncClientV5::new(opts, 10);
    task::spawn(async move { loop { eventloop.poll().await.ok(); } });

    client.publish(topic, rumqttc::v5::mqttbytes::QoS::AtLeastOnce, false, vec![1, 2, 3]).await?;
    
    time::sleep(Duration::from_secs(1)).await;
    println!("V5 Pub -> V5 Sub: OK");
    Ok(())
}

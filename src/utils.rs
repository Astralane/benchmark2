//get slot stuff
// signature subscription
// get blockhash stuff
// csv writer
// ping rpc

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, MutexGuard};
use tokio::sync::mpsc::{Receiver};
use std::time::Duration;
use csv::{Writer, WriterBuilder};
use futures::StreamExt;
use log::{error, info, warn};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use serde_json::json;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_api::config::RpcSignatureSubscribeConfig;
use solana_rpc_client_api::response::{SlotUpdate};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::{ Instant};
use crate::{TestRPCHandle, TxnData};

pub async fn get_block_hash(blockhash_arc: std::sync::Arc<std::sync::RwLock<Option<solana_sdk::hash::Hash>>>) {
    task::spawn(async move {
        let rpc = solana_client::nonblocking::rpc_client::RpcClient::new_with_commitment(
            "https://staked.helius-rpc.com?api-key=467d4eb5-ef90-4148-91ce-bd0c247c0b11"
                .to_string(),
            CommitmentConfig::confirmed(),
        );
        loop {
            let blockhash = rpc
                .get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
                .await
                .unwrap();
            {
                let mut lock = blockhash_arc.write().unwrap();
                *lock = Some(blockhash.0);
            }
            info!(
                "[+] blockhash is updated to : {} ",
                blockhash_arc.read().unwrap().clone().unwrap()
            );
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}

pub async fn get_slot(slot_arc : std::sync::Arc<std::sync::RwLock<Option<u64>>>) {
    task::spawn(async move {
        let ws_rpc = solana_client::nonblocking::pubsub_client::PubsubClient::new(
            "wss://mainnet.helius-rpc.com/?api-key=467d4eb5-ef90-4148-91ce-bd0c247c0b11",
        )
            .await;
        match ws_rpc {
            Ok(pub_client) => {
                // info!("[+] slot subscription done");
                let subscription = pub_client.slot_updates_subscribe().await;
                match subscription {
                    Ok((mut stream, unsub)) => {
                        while let Some(slot) = stream.next().await {
                            if let SlotUpdate::FirstShredReceived { slot, .. } = slot {
                                // info!("[+] slot : {}", slot);
                                let mut lock = slot_arc.write().unwrap();
                                *lock = Some(slot);
                            }
                        }
                        unsub().await;
                    }
                    Err(e) => {
                        error!("[-] Error in slot subscription : {:?}", e);
                        panic!("Error in slot subscription")
                    }
                }
            }
            Err(e) => {
                error!("[-] Error in slot subscription : {:?}", e);
                panic!("Error in slot subscription")
            }
        }
    });
}

pub async fn get_sig_status(sig_sub : PubsubClient, mut receiver: Receiver<(Signature, String, tokio::sync::mpsc::Sender<u64>, u64)>) {
    let sig_sub = Arc::new(sig_sub);

    task::spawn(async move{
        loop {
            let (signature, name, sender, id) ;
            // let result = receiver.recv().await;
            match receiver.recv().await {
               Some(data) => {
                   signature = data.0;
                   name = data.1.clone();
                   sender = data.2;
                   id = data.3;
               }
               None => {
                   info!("[-] No data in receiver");
                   continue;
                }
            }
            // let name = name.clone();
            let sig_sub = Arc::clone(&sig_sub);
            task::spawn(async move {
                let (tx1, rx1) = oneshot::channel();
                let (tx2, rx2) = oneshot::channel();
                let name = name.clone();
                task::spawn(async move{
                    info!("[-] rpc : {} : Trying to subscribe to signature : {}", name.clone(), id);
                    let subscription = sig_sub.signature_subscribe(&signature, Some(RpcSignatureSubscribeConfig{
                        commitment: Some(CommitmentConfig::processed()),
                        enable_received_notification: Some(false),
                    })).await;
                    match subscription{
                        Ok((mut stream, unsub)) => {
                            // let a = stream.next().await;
                            loop {
                                match stream.next().await {
                                    Some(respone) => {
                                        tx1.send(respone.context.slot).expect("Error in sending tx1");
                                        break;
                                    }
                                    None => {
                                        warn!("[-] rpc : {} : No response from signature subscription", name.clone());
                                        continue;
                                    }
                                }
                            }

                            unsub().await;
                        }
                        Err(e) => {
                            error!("[-] rpc : {} : Error in try_function : {:?}", name.clone(), e);
                            panic!("Error in try_function")
                        }
                    }
                });
                task::spawn(async {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    tx2.send(6969).expect("Error in sending tx2");
                });
                tokio::select! {
                    val = rx1 => {
                        if sender.is_closed() {
                            warn!("[-]  : Sender is closed");
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        // info!("[+] rpc : {} : Transaction status received", name.clone());
                        sender.send(val.unwrap()).await.expect("Error in sending response");
                    }
                    val = rx2 => {
                        // error!("[-] rpc : {} : txn status time out", name.clone());
                        sender.send(6969).await.expect("Error in sending response");
                    }
                }
            });
        }
    });
}

pub async fn get_recent_priority_fee_estimate() -> f64 {
    let resp = reqwest::Client::new()
        .post("https://mainnet.helius-rpc.com/?api-key=467d4eb5-ef90-4148-91ce-bd0c247c0b11")
        .body(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getPriorityFeeEstimate",
                "params": [
                    {
                        "accountKeys":["11111111111111111111111111111111"],
                        "options": {
                        "priorityLevel": "High"
                    }
                    }
                ]
            })
                .to_string(),
        )
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    info!("[+] Priority fee : {:#?}", resp);
    let fee = resp["result"]["priorityFeeEstimate"]
        .as_f64()
        .unwrap_or(500000.0);
    fee
}

pub async fn ping_url(rpc_handle : Vec<TestRPCHandle>) {
    task::spawn(async move {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open("ping_rpc.csv")
            .unwrap();
        let name_vec = rpc_handle.iter().map(|rpc| rpc.name.clone()).collect::<Vec<String>>();
        let mut writer = WriterBuilder::new().from_writer(file);
        writer.serialize(&name_vec).expect("Failed to write to CSV");
        writer.flush().expect("Failed to flush CSV writer");
        loop {
            let mut ping_data = vec![];
            for rpc in rpc_handle.iter() {
                let client = reqwest::Client::new();
                let start = Instant::now();
                let response = client.get(rpc.url.clone()).send().await.unwrap();
                let duration = start.elapsed();
                ping_data.push(duration.as_millis())
            }
            writer.serialize(&ping_data).expect("Failed to write to CSV");
            writer.flush().expect("Failed to flush CSV writer");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });
}

pub fn csv_writer (mut csv_file_handle: MutexGuard<Writer<File>>, data: TxnData) {
    // let mut writer = csv_file_handle.lock().unwrap();
    csv_file_handle.serialize(&data).expect("Failed to write to CSV");
    csv_file_handle.flush().expect("Failed to flush CSV writer");
}

pub fn generate_random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}
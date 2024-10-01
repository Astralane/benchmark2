use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::panic::PanicInfo;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::mpsc::Sender;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};
use solana_client::rpc_client::RpcClient;
use tokio;
use simplelog::*;
use log::{info, error};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::signature::{EncodableKey, Keypair, Signature, Signer};
use solana_sdk::{system_instruction, system_program};
use solana_sdk::transaction::Transaction;
use serde::{Deserialize, Serialize};
use solana_transaction_status::UiTransactionEncoding;
use reqwest;
use solana_client::rpc_config::{RpcSignatureSubscribeConfig, RpcTransactionConfig};
use solana_client::pubsub_client::{PubsubClient, SignatureSubscription};
use csv::Writer;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use chrono::Utc;
use solana_client::rpc_response::{Response, RpcSignatureResult};
/*
todo :
1. multiple rpc for testing multiple network
2. same txn for multiple network
3. record time
4. record slot
5. record txn hash
7. get processed shred from top ledger to get time
8. time difference
9.
 */

struct Network {
    name : String,
    rpc_http: String,
    rpc_ws: String,
    client: RpcClient,
    keypair: Keypair,
    priority_fee_bool: bool,
    compute_unit_price: u64,
    compute_unit_limit: u32,
}

impl Network {
    fn new(name : String,
           rpc_http: String,
           rpc_ws: String,
           keypair: Keypair,
           priority_fee_bool : bool,
           compute_unit_price : u64,
           compute_unit_limit : u32
    ) -> Network {
        let client = RpcClient::new(rpc_http.clone());
        Network {
            name,
            rpc_http,
            rpc_ws,
            client,
            keypair,
            priority_fee_bool,
            compute_unit_price,
            compute_unit_limit
        }
    }

    fn test_transaction(&self) -> Result<(Signature, i64), solana_client::client_error::ClientError>  {

        // Get recent blockhash
        let message : Message;

        let recent_blockhash = self.client.get_latest_blockhash().unwrap();

        let transfer_instruction = system_instruction::transfer(&self.keypair.pubkey(), &self.keypair.pubkey(), 5000);
        let compute_unit_limit = ComputeBudgetInstruction::set_compute_unit_limit(self.compute_unit_limit);
        let compute_unit_price = ComputeBudgetInstruction::set_compute_unit_price(self.compute_unit_price);

        if self.priority_fee_bool {
            message = Message::new(&[compute_unit_limit, compute_unit_price, transfer_instruction], Some(&self.keypair.pubkey()));
        } else {
            message = Message::new(&[transfer_instruction], Some(&self.keypair.pubkey()));
        }
        // Create transaction
        let transaction = Transaction::new(&[&self.keypair], message, recent_blockhash);

        match self.client.send_transaction(&transaction) {
            Ok(signature) => {
                let time = Utc::now().timestamp();
                Ok((signature, time))
            }
            Err(e) => {
                error!("[-] rpc : {} : Error in test_transaction : {:?}",self.name.clone(), e);
                Err(e)
            }
        }
    }

    fn transaction_status(rpc_ws:String,name : String, signature: Signature, commitment_config: CommitmentConfig, send : Sender<(Response<RpcSignatureResult>,i64)>){
        let subcribe_rpc = PubsubClient::signature_subscribe(&rpc_ws,
                                                             &signature,
                                                             Some(RpcSignatureSubscribeConfig{
                                                                    commitment: Some(commitment_config),
                                                                    enable_received_notification: Some(false),
                                                                    })
                                                                );

        info!("[+] rpc : {} : Subscribing to signature", name.clone());
        match subcribe_rpc{
            Ok(mut subcribe_rpc) => {
                println!("Waiting for transaction to be confirmed");
                    let response = subcribe_rpc.1.recv();
                    match response {
                        Ok(response) => {
                            let time = Utc::now().timestamp();
                            info!("Transaction status : {:?}", response);
                            send.send((response.clone(),time)).expect("Error in sending response");
                            subcribe_rpc.0.shutdown().expect("Error in unsubscribing");
                            info!("Unsubscribed");
                        }
                        Err(e) => {
                            error!("[-] rpc : {} : Error in transaction_status : {:?}", name.clone(), e);
                            panic!("Error in transaction_status")
                        }
                    }
                    // println!("Waiting for transaction to be confirmed");
                }
            _ => {
                error!("[-] rpc : {} : Error in transaction_status", name.clone());
                panic!("Error in transaction_status")
            }
        }

    }

    fn start(&self) {
        info!("[+] Network : {} started", self.name);
        match self.test_transaction() {
            Ok((signature, time)) => {
                info!("[+] rpc : {} : Transaction sent : {:?}", self.name, signature);
                let time_start = Instant::now();
                // info!("[+] Time : {:?}", time);
                let time_at_txn_sent = time;
                let rpc_ws = self.rpc_ws.clone();
                let name = self.name.clone();
                let (send_processed, recv_processed) = std::sync::mpsc::channel();
                let self_clone = self.clone();
                thread::Builder::new().spawn(move || {
                    &Network::transaction_status(rpc_ws, name, signature, CommitmentConfig::processed(), send_processed);
                }).unwrap();
                let (response_when_txn_processed, time_at_txn_processed) = recv_processed.recv().unwrap();
                info!("[+] rpc : {} : Txn processed",self.name );

                let (send_confirmed, recv_confirmed) = std::sync::mpsc::channel();
                let duration1 = time_start.elapsed();
                let rpc_ws = self.rpc_ws.clone();
                let name = self.name.clone();
                thread::Builder::new().spawn(move || {
                    &Network::transaction_status(rpc_ws.clone(), name.clone(), signature, CommitmentConfig::confirmed(), send_confirmed);
                }).unwrap();
                // self.transaction_status(signature, CommitmentConfig::confirmed(),send_confirmed);
                let (response_when_txn_confirmed, time_at_txn_confirmed) = recv_confirmed.recv().unwrap();
                let duration2 = time_start.elapsed();
                info!("[+] rpc : {} : Txn confirmed",self.name );
                info!("Time taken for txn to be processed : {} {} {:#?} {:#?}", time_at_txn_processed , time_at_txn_sent, duration1, duration2);
            }
            Err(e) => {
                error!("Error in start : {:?}", e);
            }
        };
    }
}

fn main() {

    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed, Default::default()),
            WriteLogger::new(
                LevelFilter::Info,
                Config::default(),
                File::create("app.log").unwrap(),
            ),
        ]
    ).unwrap();

    let thread1 = std::thread::Builder::new().name("thread1".to_string()).spawn(|| {
        info!("[+] Thread 1 initialized");
        let network = Network::new(
            "mainnet".to_string(),
            "http://rpc:8899".to_string(),
            "ws://rpc:8900".to_string(),
            Keypair::read_from_file("/Users/rxw777/.config/solana/id.json").unwrap(),
            true,
            100000,
            450
        );
        network.start();
    }).unwrap();
    thread1.join().expect("TODO: panic message");
    //
    // let thread2 = std::thread::Builder::new().name("thread1".to_string()).spawn(|| {
    //     info!("[+] Thread 2 initialized");
    //     println!("thread1");
    // }).unwrap();
}

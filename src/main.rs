mod utils;

use std::collections::HashMap;
use std::{env, fs};
use chrono::Utc;
use csv::Writer;
use futures::StreamExt;
use log::{error, info, warn};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use simplelog::*;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSignatureSubscribeConfig};
use solana_client::rpc_response::{Response, RpcSignatureResult};
use solana_rpc_client_api::response::SlotUpdate;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::signature::{EncodableKey, Keypair, Signature, Signer};
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::pin::pin;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use dotenv::dotenv;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_sdk::signer::keypair;
use tokio;
use tokio::task;
use tokio::time::{sleep, sleep_until};
/*
todo :
1. subscribing to txn
 */
#[derive(Clone, Serialize, Deserialize, Debug)]
struct TxnData {
    id: u32,
    txn_id: String,
    status: bool,
    get_slot: u64,
    landed_slot: u64,
    slot_delta: u64,
    time_taken_processed: u128,
}

struct SingleTxnProcess {
    id: u32,
    name: String,
    rpc_http: String,
    client: Arc<RpcClient>,
    keypair: Keypair,
    recent_blockhashes: Arc<RwLock<Option<Hash>>>,
    current_slot: Arc<RwLock<Option<u64>>>,
    priority_fee_bool: bool,
    compute_unit_price: u64,
    compute_unit_limit: u32,
    sig_status_sender : Sender<(Signature, String, tokio::sync::mpsc::Sender<u64>, u64)>,
}
#[derive(Clone)]
struct TestRPCHandle {
    name : String,
    url : String,
    out_csv_file_name : String,
    out_csv_file_writer_handle : Arc<Mutex<Writer<File>>>,
}
#[derive(Clone)]
struct GlobalVariable {
    rpc_name_vec : Vec<String>,
    rpc_vec : Vec<String>,
    out_csv_file_name_vec : Vec<String>,
    no_of_burst_txn : u32,
    no_of_times : u32,
    time_between_txn : u32,
    ws_for_subscription : String,
    rpc_for_read : String,
    private_key_file : String,
    priority_fee_bool : bool,
    dynamic_fee_bool : bool,
    compute_unit_price : u64,
    compute_unit_limit : u32,
    verbose_log : bool,
}

#[derive(Debug, Deserialize)]
struct RpcDetail {
    url: String,
}

#[derive(Debug, Deserialize)]
struct YAMLConfig {
    rpc: HashMap<String, RpcDetail>,
    no_of_burst_txn: u32,
    time_between_txn: u32,
    no_of_times: u32,
    ws_for_subscription: String,
    rpc_for_read: String,
    private_key_file: String,
    priority_fee_bool: bool,
    dynamic_fee_bool: bool,
    compute_unit_price: u64,
    compute_unit_limit: u32,
    verbose_log: bool,
}

impl SingleTxnProcess {
    fn new(
        id: u32,
        name: String,
        rpc_http: String,
        keypair: Keypair,
        recent_blockhashes: Arc<RwLock<Option<Hash>>>,
        current_slot: Arc<RwLock<Option<u64>>>,
        priority_fee_bool: bool,
        compute_unit_price: u64,
        compute_unit_limit: u32,
        sig_status_sender : Sender<(Signature, String, tokio::sync::mpsc::Sender<u64>, u64)>
    ) -> SingleTxnProcess {
        let client =
            RpcClient::new_with_commitment(rpc_http.clone(), CommitmentConfig::processed());
        SingleTxnProcess {
            id,
            name,
            rpc_http,
            client: Arc::new(client),
            keypair,
            recent_blockhashes: recent_blockhashes,
            current_slot: current_slot,
            priority_fee_bool,
            compute_unit_price,
            compute_unit_limit,
            sig_status_sender,
        }
    }


    fn test_transaction(
        &self,
    ) -> Result<(Signature, Instant, u64), solana_client::client_error::ClientError> {
        // Get recent blockhash
        let message: Message;

        let pay =
            (5000 + self.id) as u64 + (rand::thread_rng().gen_range(1 ..1000/* high */));

        let memo = utils::generate_random_string(5);
        let recent_blockhash = self.recent_blockhashes.read().unwrap();
        let memo_instruction = Instruction {
            program_id: "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
                .parse()
                .unwrap(),
            accounts: vec![],
            data: memo.as_bytes().to_vec(),
        };

        let transfer_instruction =
            system_instruction::transfer(&self.keypair.pubkey(), &self.keypair.pubkey(), pay);
        let compute_unit_limit =
            ComputeBudgetInstruction::set_compute_unit_limit(self.compute_unit_limit);
        let compute_unit_price =
            ComputeBudgetInstruction::set_compute_unit_price(self.compute_unit_price);

        if self.priority_fee_bool {
            message = Message::new(
                &[compute_unit_limit, compute_unit_price, transfer_instruction],
                Some(&self.keypair.pubkey()),
            );
        } else {
            message = Message::new(&[transfer_instruction], Some(&self.keypair.pubkey()));
        }
        // Create transaction
        let transaction = Transaction::new(
            &[&self.keypair],
            message,
            recent_blockhash.unwrap_or_default(),
        );
        let start_time = Instant::now();
        match self.client.send_transaction_with_config(
            &transaction,
            RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: None,
                encoding: None,
                max_retries: None,
                min_context_slot: None,
            },
        ) {
            Ok(signature) => {
                let slot = { self.current_slot.read().unwrap() };
                info!(
                    "[+] rpc : {} : Transaction sent : {:?} : pay {} ",
                    self.name.clone(),
                    signature,
                    pay
                );
                Ok((signature, start_time, slot.unwrap_or_default()))
            }
            Err(e) => {
                error!(
                    "[-] rpc : {} : Error in test_transaction : {:?}",
                    self.name.clone(),
                    e
                );
                error!("[-] Blockhash : {}", recent_blockhash.unwrap().to_string());
                Err(e)
            }
        }
    }
    async fn send_transaction(&self) -> TxnData {
        // info!("[+] Network : {} started", self.name);
        // let ping_time = SingleTxnProcess::ping_url(&self.rpc_http).await;
        let mut status = true;
        match self.test_transaction() {
            Ok((signature, time, slot)) => {
                // info!("[+] rpc : {} : Transaction sent : {:?}", self.name, signature);
                // let time_start = Instant::now();
                // info!("[+] Time : {:?}", time);

                let (sig_result_sender, mut sig_result_receiver) = tokio::sync::mpsc::channel(128);
                let name = self.name.clone();
                let handle = task::spawn(async move{
                    let mut landed_slot ;

                    if sig_result_receiver.is_closed() {
                        warn!("[-] rpc : {} : Receiver is closed", name);
                        landed_slot = 0;
                    }
                    match sig_result_receiver.recv().await{
                        Some(data) => {
                            info!("[-] rpc : {} : Data received : {:#?}", name, data);
                            landed_slot = data;
                        }
                        None => {
                            if sig_result_receiver.is_closed() {
                                warn!("[-] rpc : {} : after sig recv Receiver is closed", name);
                                landed_slot = 0;
                            }
                            error!("[-] rpc : {} : No data in receiver ", name);
                            landed_slot = 0;
                        }
                    }
                    landed_slot
                });

                // if sig_result_receiver.is_closed() {
                //     warn!("[-] rpc : {} : before txn Receiver is closed", self.name);
                // }
                match self.sig_status_sender.send((signature.clone(), self.name.clone(), sig_result_sender, self.id as u64)).await {
                    Ok(_) => {
                        info!("[+] rpc : {} : Signature sent via channel: {:?}", self.name, signature);
                    }
                    Err(e) => {
                        error!("[-] rpc : {} : Error in sending signature : {:?}", self.name, e);
                    }
                }

                let mut landed_slot = handle.await.unwrap();

                if landed_slot == 6969 {
                    status = false;
                }
                let duration1 = time.elapsed();

                let txn_data = TxnData {
                    id: self.id,
                    txn_id: signature.to_string(),
                    status,
                    get_slot: slot,
                    landed_slot,
                    slot_delta: landed_slot - slot,
                    time_taken_processed: duration1.as_millis(),
                };
                // info!("[+] rpc : {} : Txn data : {:#?}",self.name, txn_data);
                txn_data
            }
            Err(e) => {
                error!("Error in start : {:?}", e);
                let txn_data = TxnData {
                    id: self.id,
                    txn_id: "Failed".to_string(),
                    status: false,
                    get_slot: 6969,
                    landed_slot: 6969,
                    slot_delta: 6969,
                    time_taken_processed: 6969,
                };
                txn_data
            }
        }
    }
}

async fn ping_url(url: &str) -> Duration {
    let client = reqwest::Client::new();
    let start = Instant::now();
    let response = client.get(url).send().await.unwrap();
    let duration = start.elapsed();
    // println!("Status: {}", response.status());
    duration
}

#[derive(Clone)]
struct Orchestrator {
    config: GlobalVariable,
    test_rpc_handle_vec: Vec<TestRPCHandle>,
    block_hash_arc: Arc<RwLock<Option<Hash>>>,
    slot_arc : Arc<RwLock<Option<u64>>>,
    // sig_sub : Arc<solana_client::nonblocking::pubsub_client::PubsubClient>,
    sig_status_sender: Sender<(Signature, String, tokio::sync::mpsc::Sender<u64>, u64)>,
}

impl Orchestrator {
    async fn init(global_variable: GlobalVariable) -> Orchestrator {
        let mut test_rpc_handle_vec = vec![];
        for i in 0..global_variable.rpc_vec.len() {
            let test_rpc_handle = TestRPCHandle {
                name : global_variable.rpc_name_vec[i].clone(),
                url : global_variable.rpc_vec[i].clone(),
                out_csv_file_name : global_variable.out_csv_file_name_vec[i].clone(),
                out_csv_file_writer_handle : Arc::new(Mutex::new(Writer::from_writer(
                    OpenOptions::new()
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(global_variable.out_csv_file_name_vec[i].clone())
                        .unwrap()
                )))
            };
            test_rpc_handle_vec.push(test_rpc_handle);
        }
        let sig_sub = match solana_client::nonblocking::pubsub_client::PubsubClient::new(
            &*global_variable.ws_for_subscription.clone(),
        ).await {
            Ok(sub) => {
                info!("[+] Signature subscription done");
                sub
            }
            Err(e) => {
                error!("[-] Error in signature subscription : {:?}", e);
                panic!("Error in signature subscription")
            }
        };

        let (sig_status_sender, sig_status_receiver) = tokio::sync::mpsc::channel(128);
        utils::get_sig_status(sig_sub, sig_status_receiver).await;
        // let sig_sub_arc = Arc::new(sig_sub);
        let block_hash_arc = Arc::new(RwLock::new(None));
        let slot_arc = Arc::new(RwLock::new(None));
        utils::get_block_hash(block_hash_arc.clone()).await;
        utils::get_slot(slot_arc.clone()).await;
        sleep(Duration::from_secs(5)).await;
        utils::ping_url(test_rpc_handle_vec.clone()).await;
        info!("[-] Orchestrator initialized");
        Orchestrator {
            config: global_variable,
            test_rpc_handle_vec,
            block_hash_arc,
            slot_arc,
            sig_status_sender,
        }
    }

    async fn start_test(&self) {
        let mut handle_vec = vec![];

        let priority_fee;

        if self.config.dynamic_fee_bool {
            priority_fee = utils::get_recent_priority_fee_estimate().await;
        }
        else if self.config.priority_fee_bool {
            priority_fee = self.config.compute_unit_price as f64;
        }
        else {
            priority_fee = 0.0;
        }

        info!("WE HAVE MADE IT TO THIS");
        let mut counter = 0;

        for j in 0..self.config.no_of_times {
            for i in 0..self.config.no_of_burst_txn {
                for test_rpc_handle in self.test_rpc_handle_vec.clone() {
                        let data = self.clone();
                    let handle = task::spawn(async move {
                        // info!("[+] rpc : {}, id : {}  initialized",rpc.rpc_name.clone(), i);
                        let rpc_handle = SingleTxnProcess::new(
                            counter,
                            test_rpc_handle.name.clone(),
                            test_rpc_handle.url.clone(),
                            Keypair::read_from_file(data.config.private_key_file.clone()).unwrap(),
                            data.block_hash_arc.clone(),
                            data.slot_arc.clone(),
                            data.config.priority_fee_bool,
                            priority_fee as u64,
                            data.config.compute_unit_limit,
                            data.sig_status_sender.clone(),
                        );
                        // single_txn_process.start().await;
                        let data = rpc_handle.send_transaction().await;
                        let mut lock = test_rpc_handle.out_csv_file_writer_handle.lock().unwrap();
                        utils::csv_writer(lock, data);


                    });
                    handle_vec.push(handle);
                }
                counter+=1;
            }
            //todo time set as well
            tokio::time::sleep(Duration::from_millis(self.config.time_between_txn as u64)).await;
        }

        for handle in handle_vec {
            handle.await.unwrap();
        }
    }
}

#[tokio::main]
async fn main() {
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
            Default::default(),
        ),
        WriteLogger::new(
            LevelFilter::Info,
            Config::default(),
            File::create("app.log").unwrap(),
        ),
    ])
    .unwrap();

    let yaml_content = fs::read_to_string("config.yaml").expect("Unable to read file");

    // Parse the YAML content
    let config: YAMLConfig = serde_yaml::from_str(&yaml_content).expect("Unable to parse YAML");
    info!("Config: {:#?}", config);
    let rpc_vec = config.rpc.keys().map(|x| config.rpc[x].url.clone()).collect();
    let out_csv_file_name_vec = config.rpc.keys().map(|x| format!("{}.csv", x)).collect();
    let rpc_name_vec = config.rpc.keys().map(|x| x.clone()).collect();
    let global_variable = GlobalVariable {
        rpc_name_vec,
        rpc_vec,
        out_csv_file_name_vec,
        no_of_burst_txn : config.no_of_burst_txn,
        no_of_times : config.no_of_times,
        time_between_txn : config.time_between_txn,
        ws_for_subscription : config.ws_for_subscription,
        rpc_for_read : config.rpc_for_read,
        private_key_file : config.private_key_file,
        priority_fee_bool : config.dynamic_fee_bool,
        dynamic_fee_bool : config.dynamic_fee_bool,
        compute_unit_price : config.compute_unit_price,
        compute_unit_limit : config.compute_unit_limit,
        verbose_log : config.verbose_log,
    };

    let orchestrator = Orchestrator::init(global_variable).await;
    orchestrator.start_test().await;
}

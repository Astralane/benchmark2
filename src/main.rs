mod utils;
mod txn_sender;
mod bundle_sender;

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
use std::str::FromStr;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use dotenv::dotenv;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair;
use tokio;
use tokio::task;
use tokio::time::{sleep, sleep_until};
use crate::bundle_sender::SingleBundleProcess;
use crate::txn_sender::SingleTxnProcess;
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


#[derive(Clone)]
struct TestRPCHandle {
    name : String,
    url : String,
    txn_or_bundle : String,
    out_csv_file_name : String,
    out_csv_file_writer_handle : Arc<Mutex<Writer<File>>>,
    jito_tip_bool: bool,
    jito_tip_amount: u64,
    jito_tip_addr: String,
}
#[derive(Clone)]
struct GlobalVariable {
    // rpc_name_vec : Vec<String>,
    // rpc_vec : Vec<String>,
    rpc_name_and_detail_hashmap: HashMap<String, RpcDetail>,
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

#[derive(Debug, Deserialize, Clone)]
struct RpcDetail {
    url: String,
    txn_or_bundle : String,
    jito_tip_bool: bool,
    jito_tip_amount: u64,
    jito_tip_addr: String,
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
        for (name, detials) in global_variable.rpc_name_and_detail_hashmap.iter().clone() {
            let test_rpc_handle = TestRPCHandle {
                name : name.clone(),
                url : detials.url.clone(),
                txn_or_bundle : detials.txn_or_bundle.clone(),
                out_csv_file_name : format!("{}.csv", name.clone()),
                out_csv_file_writer_handle : Arc::new(Mutex::new(Writer::from_writer(
                    OpenOptions::new()
                        .write(true)
                        .append(true)
                        .create(true)
                        .open(format!("{}.csv", name.clone()))
                        .unwrap()
                ))),
                jito_tip_bool : detials.jito_tip_bool,
                jito_tip_amount : detials.jito_tip_amount,
                jito_tip_addr : detials.jito_tip_addr.clone(),
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
                    match test_rpc_handle.txn_or_bundle.as_str() {
                        "txn" => {
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
                                    test_rpc_handle.jito_tip_bool,
                                    test_rpc_handle.jito_tip_amount,
                                    test_rpc_handle.jito_tip_addr.clone(),
                                    data.sig_status_sender.clone(),
                                );
                                // single_txn_process.start().await;
                                let data = rpc_handle.send_transaction().await;
                                let mut lock = test_rpc_handle.out_csv_file_writer_handle.lock().unwrap();
                                utils::csv_writer(lock, data);


                            });
                            handle_vec.push(handle);
                        }
                        "bundle" => {
                            let handle = task::spawn(async move {
                                // info!("[+] rpc : {}, id : {}  initialized",rpc.rpc_name.clone(), i);
                                let rpc_handle = SingleBundleProcess::new(
                                    counter,
                                    test_rpc_handle.name.clone(),
                                    test_rpc_handle.url.clone(),
                                    Keypair::read_from_file(data.config.private_key_file.clone()).unwrap(),
                                    data.block_hash_arc.clone(),
                                    data.slot_arc.clone(),
                                    data.config.priority_fee_bool,
                                    priority_fee as u64,
                                    data.config.compute_unit_limit,
                                    test_rpc_handle.jito_tip_bool,
                                    test_rpc_handle.jito_tip_amount,
                                    test_rpc_handle.jito_tip_addr.clone(),
                                    data.sig_status_sender.clone(),
                                );
                                // single_txn_process.start().await;
                                let data = rpc_handle.send_transaction().await;
                                let mut lock = test_rpc_handle.out_csv_file_writer_handle.lock().unwrap();
                                utils::csv_writer(lock, data);


                            });
                            handle_vec.push(handle);
                        }
                        _ => {
                            panic!("Invalid txn_or_bundle value");
                        }
                    }



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
    let out_csv_file_name_vec = config.rpc.keys().map(|x| format!("{}.csv", x)).collect();
    let global_variable = GlobalVariable {
        rpc_name_and_detail_hashmap : config.rpc,
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

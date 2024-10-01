use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};
use solana_client::rpc_client::RpcClient;
use tokio;
use simplelog::*;
use log::{info, error};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::message::Message;
use solana_sdk::signature::{EncodableKey, Keypair, Signature, Signer};
use solana_sdk::{system_instruction};
use solana_sdk::transaction::Transaction;
use serde::{Deserialize, Serialize};
use solana_transaction_status::{TransactionConfirmationStatus};
use solana_client::rpc_config::{RpcSignatureSubscribeConfig};
use solana_client::pubsub_client::{PubsubClient};
use csv::Writer;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use chrono::Utc;
use solana_client::rpc_response::{Response, RpcSignatureResult};
use solana_sdk::instruction::Instruction;
use tokio::task;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use tokio::time::sleep;
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
#[derive(Clone)]
#[derive(Serialize, Deserialize, Debug)]
struct TxnData {
    id: u32,
    txn_id: String,
    status: bool,
    get_slot: u64,
    landed_slot: u64,
    slot_delta: u64,
    time_at_txn_sent: i64,
    // time_at_txn_processed : i64,
    // time_at_txn_confirmed : i64,
    time_taken_processed: u128,
    // time_taken_confirmed : u128,
}

#[derive(Clone)]
struct RpcUrl {
    rpc_name: String,
    rpc_http: String,
    rpc_ws: String,
    txn_data_vec: Vec<TxnData>,
}

struct SingleTxnProcess {
    id: u32,
    name: String,
    rpc_http: String,
    rpc_ws: String,
    client: Arc<RpcClient>,
    keypair: Keypair,
    recent_blockhashes: Arc<RwLock<Option<Hash>>>,
    current_slot: Arc<RwLock<Option<u64>>>,
    priority_fee_bool: bool,
    compute_unit_price: u64,
    compute_unit_limit: u32,
}

impl SingleTxnProcess {
    fn new(id: u32,
           name: String,
           rpc_http: String,
           rpc_ws: String,
           keypair: Keypair,
           recent_blockhashes: Arc<RwLock<Option<Hash>>>,
           current_slot: Arc<RwLock<Option<u64>>>,
           priority_fee_bool: bool,
           compute_unit_price: u64,
           compute_unit_limit: u32,
    ) -> SingleTxnProcess {
        let client = RpcClient::new_with_commitment(rpc_http.clone(), CommitmentConfig::processed());
        SingleTxnProcess {
            id,
            name,
            rpc_http,
            rpc_ws,
            client: Arc::new(client),
            keypair,
            recent_blockhashes: recent_blockhashes,
            current_slot: current_slot,
            priority_fee_bool,
            compute_unit_price,
            compute_unit_limit,
        }
    }
    fn generate_random_string(length: usize) -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    fn test_transaction(&self) -> Result<(Signature, i64, u64), solana_client::client_error::ClientError> {
        // Get recent blockhash
        let message: Message;

        let memo = SingleTxnProcess::generate_random_string(5);
        let recent_blockhash = self.recent_blockhashes.read().unwrap();
        let memo_instruction = Instruction {
            program_id: "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr".parse().unwrap(),
            accounts: vec![],
            data: memo.as_bytes().to_vec(),
        };

        let transfer_instruction = system_instruction::transfer(&self.keypair.pubkey(), &self.keypair.pubkey(), (5000 + self.id) as u64);
        let compute_unit_limit = ComputeBudgetInstruction::set_compute_unit_limit(self.compute_unit_limit);
        let compute_unit_price = ComputeBudgetInstruction::set_compute_unit_price(self.compute_unit_price);

        if self.priority_fee_bool {
            message = Message::new(&[compute_unit_limit, compute_unit_price, transfer_instruction], Some(&self.keypair.pubkey()));
        } else {
            message = Message::new(&[transfer_instruction], Some(&self.keypair.pubkey()));
        }
        // Create transaction
        let transaction = Transaction::new(&[&self.keypair], message, recent_blockhash.unwrap_or_default());

        match self.client.send_transaction(&transaction) {
            Ok(signature) => {
                let slot = self.current_slot.read().unwrap();
                let time = Utc::now().timestamp();
                Ok((signature, time, slot.unwrap_or_default()))
            }
            Err(e) => {
                error!("[-] rpc : {} : Error in test_transaction : {:?}",self.name.clone(), e);
                Err(e)
            }
        }
    }

    fn transaction_status(rpc_ws: String, name: String, signature: Signature, commitment_config: CommitmentConfig, send: Sender<(Response<RpcSignatureResult>, i64)>) {
        let subcribe_rpc = PubsubClient::signature_subscribe(&rpc_ws,
                                                             &signature,
                                                             Some(RpcSignatureSubscribeConfig {
                                                                 commitment: Some(commitment_config),
                                                                 enable_received_notification: Some(false),
                                                             }),
        );

        info!("[+] rpc : {} : Subscribing to signature", name.clone());
        match subcribe_rpc {
            Ok(subcribe_rpc) => {
                println!("Waiting for transaction to be confirmed");
                let response = subcribe_rpc.1.recv();
                match response {
                    Ok(response) => {
                        let time = Utc::now().timestamp();
                        info!("Transaction status : {:?}", response);
                        send.send((response.clone(), time)).expect("Error in sending response");
                        info!("subscription done.. turn off here");
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

    async fn get_txn_status(&self, signature: Signature) -> Result<u64, String> {
        let rpc = solana_client::nonblocking::rpc_client::RpcClient::new_with_commitment("http://rpc:8899".to_string(), CommitmentConfig::processed());
        let start = Instant::now();
        loop {
            info!("[.] in the loop of get txn status");
            if start.elapsed().as_secs() > 60 {
                error!("[-] rpc : {} : Error in get_txn_status : {:?}", self.name, "Timeout");
                return Err("Timeout".to_string());
            }
            let rpc_signature_result = rpc.get_signature_statuses(&[signature]).await;
            match rpc_signature_result {
                Ok(rpc_signature_result) => {
                    match rpc_signature_result.value.get(0) {
                        Some(rpc_signature_result) => {
                            match rpc_signature_result {
                                Some(data) => {
                                    match data.confirmation_status {
                                        Some(TransactionConfirmationStatus::Processed) => {
                                            info!("[+] got rpc : {} : Transaction processed", self.name);
                                            return Ok(data.slot);
                                            // break;
                                        }
                                        _ => {
                                            continue
                                        }
                                    }
                                }
                                None => {
                                    continue;
                                }
                            }
                        }
                        None => {
                            error!("[-] rpc : {} : Error in get_txn_status : {:?}", self.name, "No data in rpc_signature_result");
                            // panic!("Error in get_txn_status")
                        }
                    }
                    // info!("[+] --- rpc : {} : Transaction status : {:?}", self.name, rpc_signature_result);
                }
                Err(e) => {
                    error!("[-] rpc : {} : Error in get_txn_status : {:?}", self.name, e);
                    // panic!("Error in get_txn_status")
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    fn get_txn_slot(rpc_signature_result: Response<RpcSignatureResult>) -> u64 {
        let slot = rpc_signature_result.context.slot;
        slot
    }

    async fn start(&self) -> TxnData {
        info!("[+] Network : {} started", self.name);
        match self.test_transaction() {
            Ok((signature, time, slot)) => {
                info!("[+] rpc : {} : Transaction sent : {:?}", self.name, signature);
                let time_start = Instant::now();
                // info!("[+] Time : {:?}", time);
                let time_at_txn_sent = time;
                // let rpc_ws = self.rpc_ws.clone();
                // let name = self.name.clone();
                // let (send_processed, recv_processed) = std::sync::mpsc::channel();
                // let (send_confirmed, recv_confirmed) = std::sync::mpsc::channel();
                let landed_slot = self.get_txn_status(signature.clone()).await.unwrap_or(6969);
                let duration1 = time_start.elapsed();
                // let pubsub_processed_handle = task::spawn(async move {
                //     SingleTxnProcess::transaction_status(rpc_ws.clone(), name.clone(), signature, CommitmentConfig::processed(), send_processed);
                // });
                // let rpc_ws = self.rpc_ws.clone();
                // let name = self.name.clone();
                // let pubsub_confirmed_handle = task::spawn(async move{
                //     SingleTxnProcess::transaction_status(rpc_ws.clone(), name.clone(), signature, CommitmentConfig::confirmed(), send_confirmed);
                // });
                // let (response_when_txn_processed, time_at_txn_processed) = recv_processed.recv().expect("Error in receiving from sig sub checking processed");
                // info!("[+] rpc : {} : Txn processed",self.name );
                // let duration1 = time_start.elapsed();
                // let (response_when_txn_confirmed, time_at_txn_confirmed) = recv_confirmed.recv().expect("Error in receiving from sig sub checking confirmed");
                // let duration2 = time_start.elapsed();
                //
                // let landed_slot = SingleTxnProcess::get_txn_slot(response_when_txn_processed);
                info!("[+] rpc : {} : Txn confirmed",self.name );
                // info!("Time taken for txn to be processed : {} {} {:#?} {:#?}", time_at_txn_processed , time_at_txn_sent, duration1, duration2);
                let txn_data = TxnData {
                    id: self.id,
                    txn_id: signature.to_string(),
                    status: true,
                    get_slot: slot,
                    landed_slot,
                    slot_delta: landed_slot - slot,
                    time_at_txn_sent,
                    // time_at_txn_processed,
                    // time_at_txn_confirmed,
                    time_taken_processed: duration1.as_millis(),
                    // time_taken_confirmed : duration2.as_millis(),
                };
                info!("[+] rpc : {} : Txn data : {:#?}",self.name, txn_data);
                txn_data
            }
            Err(e) => {
                error!("Error in start : {:?}", e);
                panic!("Error in start")
            }
        }
    }

    async fn init(&self) -> TxnData {
        let txn_data = self.start().await;
        txn_data
        // println!("txn_data : {:#?}", data);
    }
}

#[tokio::main]
async fn main() {
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

    let csv_file_astra = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("txn_data_astralane_rpct.csv")
        .unwrap();
    let csv_writer_astra = Arc::new(Mutex::new(Writer::from_writer(csv_file_astra)));
    let csv_file_main = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("txn_data_lite_rpct.csv")
        .unwrap();
    let csv_writer_quic = Arc::new(Mutex::new(Writer::from_writer(csv_file_main)));
    let csv_file_quic = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("txn_data_quic_rpct.csv")
        .unwrap();
    let csv_writer_main = Arc::new(Mutex::new(Writer::from_writer(csv_file_quic)));
    let csv_file_tri = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open("txn_data_triton_rpct.csv")
        .unwrap();
    let csv_writer_tri = Arc::new(Mutex::new(Writer::from_writer(csv_file_tri)));

    /*
    Todo :
    1. make a tokio task
    2. make a benchmark trait that it to orch the benchmark  abit late
     */

    let rpc_vec = vec![
        RpcUrl {
            rpc_name: "Astralane RPC".to_string(),
            rpc_http: "http://rpc:8899".to_string(),
            rpc_ws: "ws://rpc:8900".to_string(),
            txn_data_vec: vec![],
        },
        RpcUrl {
            rpc_name: "lite RPC".to_string(),
            rpc_http: "http://rpc:8890".to_string(),
            rpc_ws: "ws://rpc:8891".to_string(),
            txn_data_vec: vec![],
        },
        RpcUrl {
            rpc_name: "quic RPC".to_string(),
            rpc_http: "https://young-withered-needle.solana-mainnet.quiknode.pro/ebca017c4a950e92de81dbe69a41ec2e6e4583b6".to_string(),
            rpc_ws: "ws://rpc:8891".to_string(),
            txn_data_vec: vec![],
        },
        RpcUrl {
            rpc_name: "triton RPC".to_string(),
            rpc_http: "https://astralan-solanac-be6d.mainnet.rpcpool.com/71e5497d-a074-481d-9da9-d03278df8ea4".to_string(),
            rpc_ws: "ws://rpc:8891".to_string(),
            txn_data_vec: vec![],
        }
    ];


    let number_of_txn = 10;
    let number_of_times = 5;

    let mut handle_vec = vec![];

    let blockhash_arc = Arc::new(RwLock::new(None));
    let slot_arc = Arc::new(RwLock::new(None));

    let blockhash_arc_clone = blockhash_arc.clone();
    task::spawn(async move {
        let rpc = solana_client::nonblocking::rpc_client::RpcClient::new_with_commitment("http://rpc:8899".to_string(), CommitmentConfig::processed());
        loop {
            let blockhash = rpc.get_latest_blockhash().await.unwrap();
            {
                let mut lock = blockhash_arc_clone.write().unwrap();
                *lock = Some(blockhash);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    let slot_arc_clone = slot_arc.clone();
    task::spawn(async move {
        let rpc = solana_client::nonblocking::rpc_client::RpcClient::new_with_commitment("http://rpc:8899".to_string(), CommitmentConfig::processed());
        loop {
            let slot = rpc.get_slot().await.unwrap();
            {
                let mut lock = slot_arc_clone.write().unwrap();
                *lock = Some(slot);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // sleep for setting the slots
    tokio::time::sleep(Duration::from_secs(1)).await;

    for j in 0..number_of_times {
        for i in 0..number_of_txn {
            for rpc in rpc_vec.clone() {
                let csv_file_astra_clone = Arc::clone(&csv_writer_astra);
                let csv_file_main_clone = Arc::clone(&csv_writer_main);
                let csv_file_quic_clone = Arc::clone(&csv_writer_quic);
                let csv_file_tri_clone = Arc::clone(&csv_writer_tri);


                let slot_arc_clone = slot_arc.clone();
                let block_hash_clone = blockhash_arc.clone();

                let handle = task::spawn(async move {
                    info!("[+] rpc : {}, id : {}  initialized",rpc.rpc_name.clone(), i);
                    let single_txn_process = SingleTxnProcess::new(
                        (j*10) + i,
                        rpc.rpc_name.clone(),
                        rpc.rpc_http.clone(),
                        rpc.rpc_ws.clone(),
                        Keypair::read_from_file("/Users/rxw777/.config/solana/id.json").unwrap(),
                        block_hash_clone,
                        slot_arc_clone,
                        true,
                        100000,
                        1000,
                    );
                    // single_txn_process.start().await;
                    let data = single_txn_process.init().await;
                    match rpc.rpc_name.as_str() {
                        "Astralane RPC" => {
                            let mut writer = csv_file_astra_clone.lock().unwrap();
                            writer.serialize(&data).expect("Failed to write to CSV");
                            writer.flush().expect("Failed to flush CSV writer");
                        }
                        "lite RPC" => {
                            let mut writer = csv_file_main_clone.lock().unwrap();
                            writer.serialize(&data).expect("Failed to write to CSV");
                            writer.flush().expect("Failed to flush CSV writer");
                        }
                        "quic RPC" => {
                            let mut writer = csv_file_quic_clone.lock().unwrap();
                            writer.serialize(&data).expect("Failed to write to CSV");
                            writer.flush().expect("Failed to flush CSV writer");
                        }
                        "triton RPC" => {
                            let mut writer = csv_file_tri_clone.lock().unwrap();
                            writer.serialize(&data).expect("Failed to write to CSV");
                            writer.flush().expect("Failed to flush CSV writer");
                        }
                        _ => {
                            error!("[-] rpc : {} : Error in rpc name", rpc.rpc_name.clone());
                            panic!("Error in rpc name")
                        }
                    }
                });
                handle_vec.push(handle);
            }
        }
        tokio::time::sleep(Duration::from_millis(2000)).await;
    }

    for handle in handle_vec {
        handle.await.unwrap();
    }
}

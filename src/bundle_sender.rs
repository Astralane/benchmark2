use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use anyhow::anyhow;
use jito_sdk_rust::JitoJsonRpcSDK;
use log::{error, info, warn};
use rand::Rng;
use serde_json::json;
use solana_client::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::{bs58, system_instruction};
use solana_sdk::transaction::Transaction;
use tokio::sync::mpsc::Sender;
use tokio::task;
use tokio::time::sleep;
use crate::{utils, TxnData};

#[derive(Debug)]
struct BundleStatus {
    confirmation_status: Option<String>,
    err: Option<serde_json::Value>,
    transactions: Option<Vec<String>>,
}

pub(crate) struct SingleBundleProcess {
    id: u32,
    name: String,
    rpc_http: String,
    client: Arc<JitoJsonRpcSDK>,
    keypair: Keypair,
    recent_blockhashes: Arc<RwLock<Option<Hash>>>,
    current_slot: Arc<RwLock<Option<u64>>>,
    priority_fee_bool: bool,
    compute_unit_price: u64,
    compute_unit_limit: u32,
    jito_tip_bool: bool,
    jito_tip_amount: u64,
    jito_tip_addr: String,
    sig_status_sender : Sender<(Signature, String, tokio::sync::mpsc::Sender<u64>, u64)>,
}

impl SingleBundleProcess {
    pub(crate) fn new(
        id: u32,
        name: String,
        rpc_http: String,
        keypair: Keypair,
        recent_blockhashes: Arc<RwLock<Option<Hash>>>,
        current_slot: Arc<RwLock<Option<u64>>>,
        priority_fee_bool: bool,
        compute_unit_price: u64,
        compute_unit_limit: u32,
        jito_tip_bool: bool,
        jito_tip_amount: u64,
        jito_tip_addr: String,
        sig_status_sender : Sender<(Signature, String, tokio::sync::mpsc::Sender<u64>, u64)>
    ) -> SingleBundleProcess {
        let client =
            JitoJsonRpcSDK::new(rpc_http.as_str(), None);
        // let jito_sdk = JitoJsonRpcSDK::new("https://mainnet.block-engine.jito.wtf/api/v1", None);

        SingleBundleProcess {
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
            jito_tip_bool,
            jito_tip_amount,
            jito_tip_addr,
            sig_status_sender,
        }
    }


    async fn test_transaction(
        &self,
    ) -> Result<(String, Instant, u64), anyhow::Error> {
        // Get recent blockhash
        let message: Message;

        let pay = (5000 + self.id) as u64 + (rand::thread_rng().gen_range(1..1000));

        let memo = utils::generate_random_string(5);
        let recent_blockhash = {
            let guard = self.recent_blockhashes.read().unwrap();
            *guard
        };
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
        let jito_tip_instruction = system_instruction::transfer(
            &self.keypair.pubkey(),
            &Pubkey::from_str(&self.jito_tip_addr.clone()).unwrap(),
            self.jito_tip_amount,
        );
        let mut instructions = vec![];
        if self.jito_tip_bool {
            instructions.push(jito_tip_instruction);
        }
        if self.priority_fee_bool {
            instructions.push(compute_unit_limit);
            instructions.push(compute_unit_price);
        }
        instructions.push(transfer_instruction);
        message = Message::new(&instructions, Some(&self.keypair.pubkey()));

        let transaction = Transaction::new(
            &[&self.keypair],
            message,
            recent_blockhash.unwrap_or_default(),
        );

        let serialized_tx = bs58::encode(bincode::serialize(&transaction)?).into_string();
        let bundle = json!([serialized_tx]);
        let uuid = None;
        let start_time = Instant::now();
        match self.client.send_bundle(Some(bundle), uuid).await {
            Ok(value) => {
                let slot = { self.current_slot.read().unwrap() };
                info!("[+] rpc : {} : Bundle sent : {:?}", self.name.clone(), value);
                let bundle_uuid = value["result"]
                    .as_str()
                    .ok_or_else(|| anyhow!("Failed to get bundle UUID from response"))?.to_string();
                Ok((bundle_uuid, start_time, slot.unwrap_or_default()))
            }
            Err(e) => {
                error!("[-] rpc : {} : Error in bundle : {:?}", self.name.clone(), e);
                Err(e)
            }
        }
    }

    fn get_bundle_status(status_response: &serde_json::Value) -> Result<BundleStatus, anyhow::Error> {
        status_response
            .get("result")
            .and_then(|result| result.get("value"))
            .and_then(|value| value.as_array())
            .and_then(|statuses| statuses.get(0))
            .ok_or_else(|| anyhow!("Failed to parse bundle status"))
            .map(|bundle_status| BundleStatus {
                confirmation_status: bundle_status.get("confirmation_status").and_then(|s| s.as_str()).map(String::from),
                err: bundle_status.get("err").cloned(),
                transactions: bundle_status.get("transactions").and_then(|t| t.as_array()).map(|arr| {
                    arr.iter().filter_map(|v| v.as_str().map(String::from)).collect()
                }),
            })
    }
    pub(crate) async fn send_transaction(&self) -> TxnData {
        // info!("[+] Network : {} started", self.name);
        // let ping_time = SingleTxnProcess::ping_url(&self.rpc_http).await;
        let mut status = true;
        let mut landed_slot = 6969;
        match self.test_transaction().await {
            Ok((signature, time, slot)) => {

                sleep(Duration::from_secs(60)).await;
                match self.client.get_bundle_statuses(vec![signature.clone()]).await {
                    Ok(value) => {
                        let bundle_status = SingleBundleProcess::get_bundle_status(&value).unwrap();

                        match bundle_status.confirmation_status.as_deref() {
                            Some("finalized") => {
                                landed_slot = value["result"]["value"][0]["slot"]
                                    .as_u64()
                                    .unwrap_or(6969);
                                info!("Bundle finalized on-chain successfully! {} ", landed_slot);
                            },
                            Some(status) => {
                                println!("Unexpected final bundle status: {}. Continuing to poll...", status);
                            },
                            None => {
                                println!("Unable to parse final bundle status. Continuing to poll...");
                            }
                        }
                    }
                    Err(e) => {
                        error!("[-] rpc : {} : Error in bundle status : {:?}", self.name.clone(), e);
                    }
                }

                // let mut landed_slot = handle.await.unwrap();

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
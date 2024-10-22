use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use log::{error, info, warn};
use rand::Rng;
use solana_client::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcSendTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;
use tokio::sync::mpsc::Sender;
use tokio::task;
use crate::{utils, TxnData};

pub(crate) struct SingleTxnProcess {
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
    jito_tip_bool: bool,
    jito_tip_amount: u64,
    jito_tip_addr: String,
    sig_status_sender : Sender<(Signature, String, tokio::sync::mpsc::Sender<u64>, u64)>,
}

impl SingleTxnProcess {
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
            jito_tip_bool,
            jito_tip_amount,
            jito_tip_addr,
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
    pub(crate) async fn send_transaction(&self) -> TxnData {
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
use restate_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

#[derive(Clone)]
struct NativeBridge {
    fabric: PathBuf,
    hash: PathBuf,
    scratch: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct RunRequest {
    mission_id: String,
    node_a: String,
    node_b: String,
    worker_id: String,
    worker_run_id: String,
    protocol_instance_id: String,
    #[serde(default = "default_wait_seconds")]
    wait_seconds: u64,
}

fn default_wait_seconds() -> u64 {
    15
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeReceipt {
    node_id: String,
    digest: String,
    output_uri: String,
    receipt_id: String,
    event_id: String,
}

#[derive(Debug, Serialize)]
struct RunResult {
    worker_run_id: String,
    protocol_instance_id: String,
    node_a: NodeReceipt,
    node_b: NodeReceipt,
    recovery_ref: String,
}

fn terminal(message: impl Into<String>) -> TerminalError {
    TerminalError::new(message.into())
}

fn stable_id(value: &str, field: &str) -> Result<(), TerminalError> {
    if value.is_empty()
        || value.len() > 120
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(terminal(format!(
            "{field} must be a stable Bonfyre identity"
        )));
    }
    Ok(())
}

fn parse_kv(output: &[u8]) -> Vec<(String, String)> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn required(values: &[(String, String)], key: &str) -> Result<String, TerminalError> {
    values
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.clone())
        .ok_or_else(|| terminal(format!("native fabric response omitted {key}")))
}

impl NativeBridge {
    fn fabric(&self, args: &[&str]) -> Result<Vec<(String, String)>, TerminalError> {
        let output = Command::new(&self.fabric)
            .args(args)
            .output()
            .map_err(|error| terminal(format!("execute native fabric: {error}")))?;
        if !output.status.success() {
            return Err(terminal(format!(
                "native fabric {}: {}",
                args.join(" "),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let values = parse_kv(&output.stdout);
        if values
            .iter()
            .any(|(key, value)| key == "result" && value == "ok")
        {
            Ok(values)
        } else {
            Err(terminal(format!(
                "native fabric {} did not acknowledge success",
                args.join(" ")
            )))
        }
    }

    fn bind_lifecycle(&self, request: &RunRequest) -> Result<(), TerminalError> {
        self.fabric(&["lifecycle", "register-restate"])?;
        self.fabric(&[
            "worker-run",
            "start",
            &request.worker_run_id,
            &request.worker_id,
            &request.mission_id,
            &request.protocol_instance_id,
        ])?;
        self.fabric(&[
            "protocol-machine",
            "start",
            &request.protocol_instance_id,
            "restate-workgraph-v1",
            &request.worker_run_id,
        ])?;
        self.fabric(&[
            "protocol-machine",
            "step",
            &request.protocol_instance_id,
            "invoke",
            "restate-invoke-v1",
        ])?;
        Ok(())
    }

    fn dispatch(
        &self,
        request: &RunRequest,
        node_id: &str,
        suffix: &str,
    ) -> Result<NodeReceipt, TerminalError> {
        let worker = format!("worker-run.{}", request.worker_run_id);
        let claim = self.fabric(&[
            "work",
            "claim",
            &request.mission_id,
            node_id,
            &worker,
            "--lease-ms",
            "60000",
        ])?;
        let claim_token = required(&claim, "claim_token")?;
        fs::create_dir_all(&self.scratch)
            .map_err(|error| terminal(format!("create native scratch: {error}")))?;
        let fixture = self
            .scratch
            .join(format!("{}-{}-{suffix}.txt", request.mission_id, node_id));
        fs::write(
            &fixture,
            format!(
                "restate durable native fixture {} {node_id}\n",
                request.mission_id
            ),
        )
        .map_err(|error| terminal(format!("write native fixture: {error}")))?;
        let hash = Command::new(&self.hash)
            .arg("file")
            .arg(&fixture)
            .output()
            .map_err(|error| terminal(format!("execute BonfyreHash: {error}")))?;
        if !hash.status.success() {
            return Err(terminal("BonfyreHash failed"));
        }
        let digest = String::from_utf8_lossy(&hash.stdout)
            .split_whitespace()
            .next()
            .ok_or_else(|| terminal("BonfyreHash produced no digest"))?
            .to_owned();
        let output_uri = format!("bonfyre://artifact/{digest}");
        let completion = self.fabric(&[
            "work",
            "complete",
            &request.mission_id,
            node_id,
            &worker,
            &claim_token,
            "--output-uri",
            &output_uri,
        ])?;
        Ok(NodeReceipt {
            node_id: node_id.to_owned(),
            digest,
            output_uri,
            receipt_id: required(&completion, "receipt_id")?,
            event_id: required(&completion, "event_id")?,
        })
    }

    fn enter_wait(&self, request: &RunRequest, receipt: &NodeReceipt) -> Result<(), TerminalError> {
        self.fabric(&[
            "protocol-machine",
            "step",
            &request.protocol_instance_id,
            "node-a-complete",
            "node-a-complete-v1",
            "--receipt",
            &receipt.receipt_id,
        ])?;
        self.fabric(&[
            "worker-run",
            "step",
            &request.worker_run_id,
            "wait",
            "node-a-wait-v1",
            "--receipt",
            &receipt.receipt_id,
        ])?;
        Ok(())
    }

    fn recover(&self, request: &RunRequest) -> Result<String, TerminalError> {
        let reconciled = self.fabric(&["work", "reconcile-effects", &request.mission_id])?;
        let recovery_ref = format!("effect-recovery:{}", required(&reconciled, "reconciled")?);
        self.fabric(&[
            "protocol-machine",
            "step",
            &request.protocol_instance_id,
            "resume",
            "effect-recovery-v1",
            "--receipt",
            &recovery_ref,
        ])?;
        self.fabric(&[
            "worker-run",
            "step",
            &request.worker_run_id,
            "resume",
            "effect-recovery-v1",
            "--receipt",
            &recovery_ref,
        ])?;
        Ok(recovery_ref)
    }

    fn complete(&self, request: &RunRequest, receipt: &NodeReceipt) -> Result<(), TerminalError> {
        self.fabric(&[
            "protocol-machine",
            "step",
            &request.protocol_instance_id,
            "node-b-complete",
            "node-b-complete-v1",
            "--receipt",
            &receipt.receipt_id,
        ])?;
        self.fabric(&[
            "worker-run",
            "step",
            &request.worker_run_id,
            "complete",
            "worker-complete-v1",
            "--receipt",
            &receipt.receipt_id,
        ])?;
        Ok(())
    }
}

#[service(name = "BonfyreWorkGraph")]
impl NativeBridge {
    #[handler]
    async fn run_two_nodes(
        &self,
        context: Context<'_>,
        request: Json<RunRequest>,
    ) -> Result<Json<RunResult>, TerminalError> {
        let request = request.into_inner();
        for (value, field) in [
            (&request.mission_id, "mission_id"),
            (&request.node_a, "node_a"),
            (&request.node_b, "node_b"),
            (&request.worker_id, "worker_id"),
            (&request.worker_run_id, "worker_run_id"),
            (&request.protocol_instance_id, "protocol_instance_id"),
        ] {
            stable_id(value, field)?;
        }
        if request.node_a == request.node_b || request.wait_seconds > 3600 {
            return Err(terminal(
                "nodes must differ and wait_seconds must be <= 3600",
            ));
        }

        let bridge = self.clone();
        let bind_request = request.clone();
        context
            .run(move || async move {
                bridge
                    .bind_lifecycle(&bind_request)
                    .map_err(HandlerError::from)
            })
            .name("bind-stable-lifecycle")
            .await?;
        let bridge = self.clone();
        let node_a_request = request.clone();
        let node_a_json = context
            .run(move || async move {
                let receipt = bridge
                    .dispatch(&node_a_request, &node_a_request.node_a, "a")
                    .map_err(HandlerError::from)?;
                serde_json::to_string(&receipt).map_err(|error| {
                    HandlerError::from(terminal(format!("encode node-a receipt: {error}")))
                })
            })
            .name("dispatch-node-a")
            .await?;
        let node_a: NodeReceipt = serde_json::from_str(&node_a_json)
            .map_err(|error| terminal(format!("decode node-a durable receipt: {error}")))?;
        let bridge = self.clone();
        let wait_request = request.clone();
        let wait_receipt = NodeReceipt {
            node_id: node_a.node_id.clone(),
            digest: node_a.digest.clone(),
            output_uri: node_a.output_uri.clone(),
            receipt_id: node_a.receipt_id.clone(),
            event_id: node_a.event_id.clone(),
        };
        context
            .run(move || async move {
                bridge
                    .enter_wait(&wait_request, &wait_receipt)
                    .map_err(HandlerError::from)
            })
            .name("record-node-a-and-wait")
            .await?;
        context
            .sleep(Duration::from_secs(request.wait_seconds))
            .await?;
        let bridge = self.clone();
        let recovery_request = request.clone();
        let recovery_ref = context
            .run(move || async move {
                bridge
                    .recover(&recovery_request)
                    .map_err(HandlerError::from)
            })
            .name("effect-kernel-recovery-before-node-b")
            .await?;
        let bridge = self.clone();
        let node_b_request = request.clone();
        let node_b_json = context
            .run(move || async move {
                let receipt = bridge
                    .dispatch(&node_b_request, &node_b_request.node_b, "b")
                    .map_err(HandlerError::from)?;
                serde_json::to_string(&receipt).map_err(|error| {
                    HandlerError::from(terminal(format!("encode node-b receipt: {error}")))
                })
            })
            .name("dispatch-node-b")
            .await?;
        let node_b: NodeReceipt = serde_json::from_str(&node_b_json)
            .map_err(|error| terminal(format!("decode node-b durable receipt: {error}")))?;
        let bridge = self.clone();
        let complete_request = request.clone();
        let complete_receipt = NodeReceipt {
            node_id: node_b.node_id.clone(),
            digest: node_b.digest.clone(),
            output_uri: node_b.output_uri.clone(),
            receipt_id: node_b.receipt_id.clone(),
            event_id: node_b.event_id.clone(),
        };
        context
            .run(move || async move {
                bridge
                    .complete(&complete_request, &complete_receipt)
                    .map_err(HandlerError::from)
            })
            .name("complete-stable-lifecycle")
            .await?;
        Ok(Json::from(RunResult {
            worker_run_id: request.worker_run_id,
            protocol_instance_id: request.protocol_instance_id,
            node_a,
            node_b,
            recovery_ref,
        }))
    }
}

#[tokio::main]
async fn main() {
    let root = env::var("BONFYRE_ROOT").unwrap_or_else(|_| ".".to_owned());
    let bridge = NativeBridge {
        fabric: env::var("BONFYRE_FABRIC_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(&root).join("programs/bonfyre/bonfyre")),
        hash: env::var("BONFYRE_HASH_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(&root).join("cmd/BonfyreHash/bonfyre-hash")),
        scratch: env::var("BONFYRE_RESTATE_SCRATCH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| env::temp_dir().join("bonfyre-restate-native")),
    };
    HttpServer::new(Endpoint::builder().bind(bridge).build())
        .listen_and_serve("0.0.0.0:9080".parse().expect("valid Restate bind address"))
        .await;
}

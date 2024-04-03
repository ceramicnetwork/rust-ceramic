use crate::{CommitResult, CommitTransaction, Committer, DetachedTimeEvent, ProofBlock};
use ceramic_event::{Cid, JwkSigner, Signer};
use log::debug;
use serde::{Deserialize, Serialize};
use url::Url;

pub struct CasTransaction {
    car: Vec<u8>,
    proof: Cid,
}

impl CasTransaction {
    pub async fn new(car: Vec<u8>) -> anyhow::Result<Self> {
        let reader = tokio::io::BufReader::new(car.as_slice());
        let reader = iroh_car::CarReader::new(reader).await?;
        let root = reader
            .header()
            .roots()
            .first()
            .ok_or_else(|| anyhow::anyhow!("No roots found"))?
            .clone();
        Ok(Self { car, proof: root })
    }
}

impl CommitTransaction for CasTransaction {
    fn root(&self) -> &Cid {
        &self.proof
    }
}

pub struct CasCommitter {
    url: Url,
    cli: reqwest::Client,
    signer: JwkSigner,
}

const CAS_ENDPOINT: &str = "/api/v0/cas";

impl CasCommitter {
    pub fn new(url: Url, signer: JwkSigner) -> anyhow::Result<Self> {
        let url = url.join(CAS_ENDPOINT)?;
        Ok(Self {
            url,
            cli: reqwest::Client::new(),
            signer,
        })
    }
}

#[derive(Deserialize, Serialize)]
struct CasPayload {
    url: Url,
    nonce: String,
    digest: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum CasResponseStatus {
    Pending,
    Processing,
    Failed,
    Ready,
    Replaced,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CasResponseMetadata {
    id: String,
    status: CasResponseStatus,
    cid: Cid,
    created_at: Option<time::OffsetDateTime>,
    updated_at: Option<time::OffsetDateTime>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum CasResponse {
    Pending {
        #[serde(flatten)]
        metadata: CasResponseMetadata,
    },
    Processing {
        #[serde(flatten)]
        metadata: CasResponseMetadata,
    },
    Failed {
        #[serde(flatten)]
        metadata: CasResponseMetadata,
        message: String,
    },
    Ready {
        #[serde(flatten)]
        metadata: CasResponseMetadata,
        witness_car: Vec<u8>,
    },
    Replaced {
        #[serde(flatten)]
        metadata: CasResponseMetadata,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", untagged)]
enum CasResponseOrError {
    Error { error: String },
    Response(CasResponse),
}

#[async_trait::async_trait]
impl Committer for CasCommitter {
    type Transaction = CasTransaction;

    async fn commit(&self, transaction: Self::Transaction) -> anyhow::Result<CommitResult> {
        let proof = transaction.proof.to_string();
        let mut url = self.url.clone();
        url.set_query(Some(&format!("event={}", proof)));
        let payload = CasPayload {
            url: self.url.clone(),
            nonce: uuid::Uuid::new_v4().to_string(),
            digest: proof.clone(),
        };
        let payload = serde_json::to_vec(&payload)?;
        let signature = self.signer.sign(&payload).await?;
        let auth = format!("Bearer {}", signature);

        let resp = self
            .cli
            .post(url)
            .header(reqwest::header::AUTHORIZATION, auth.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/vnd.ipld.car")
            .body(transaction.car)
            .send()
            .await?;

        match resp.status() {
            reqwest::StatusCode::CREATED | reqwest::StatusCode::ACCEPTED => {
                debug!("CAS accepted request for {}", proof);
            }
            _ => {
                let body = resp.text().await?;
                return Err(anyhow::anyhow!("Failed to anchor for {}: {}", proof, body));
            }
        }

        let url = self.url.join(&format!("{}/{}", CAS_ENDPOINT, proof))?;
        loop {
            let resp = self.cli.get(url.clone()).send().await?;
            if resp.status().is_success() {
                let resp: CasResponseOrError = resp.json().await?;
                let resp = match resp {
                    CasResponseOrError::Response(resp) => resp,
                    CasResponseOrError::Error { error } => {
                        return Err(anyhow::anyhow!("Error from CAS: {}", error))
                    }
                };
                match resp {
                    CasResponse::Ready {
                        metadata,
                        witness_car,
                    } => {
                        let reader = tokio::io::BufReader::new(witness_car.as_slice());
                        let mut car = iroh_car::CarReader::new(reader).await?;
                        let root = car
                            .header()
                            .roots()
                            .first()
                            .ok_or_else(|| anyhow::anyhow!("No roots found"))?
                            .clone();
                        if let Some((cid, _block)) = car.next_block().await? {
                            let time_event = DetachedTimeEvent {
                                path: cid.to_string(),
                                proof: cid.clone(),
                            };
                            let proof_block = ProofBlock {
                                chain_id: "cas".to_string(),
                                root: root.clone(),
                                tx_hash: metadata.cid.clone(),
                                tx_type: "anchor".to_string(),
                            };
                            return Ok(CommitResult {
                                time_event,
                                proof_block,
                                nodes: vec![metadata.cid],
                            });
                        } else {
                            return Err(anyhow::anyhow!("No blocks found in witness CAR"));
                        }
                    }
                    CasResponse::Pending { metadata } => {
                        debug!("CAS is pending for {}", metadata.id);
                    }
                    CasResponse::Processing { metadata } => {
                        debug!("CAS is processing for {}", metadata.id);
                    }
                    CasResponse::Failed { metadata, message } => {
                        return Err(anyhow::anyhow!(
                            "CAS failed for {}: {}",
                            metadata.id,
                            message
                        ));
                    }
                    CasResponse::Replaced { metadata } => {
                        debug!("CAS replaced for {}", metadata.id);
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::r#impl::tests::{mock_server, test_signer, write_car, CID_STR};
    use std::str::FromStr;

    fn car_response(_req: &wiremock::Request) -> wiremock::ResponseTemplate {
        let fut = async move {
            let car = write_car(Cid::from_str(CID_STR).unwrap(), Vec::new()).await;
            serde_json::json!({
                "id": CID_STR,
                "cid": Cid::from_str(CID_STR).unwrap(),
                "status": "READY",
                "witnessCar": car,
            })
        };
        let resp = tokio::runtime::Handle::current().block_on(fut);
        wiremock::ResponseTemplate::new(200).set_body_json(resp)
    }

    #[tokio::test]
    async fn should_commit_transaction() {
        let (_server, url) = mock_server(|mock| async move {
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::path("/api/v0/cas"))
                .respond_with(wiremock::ResponseTemplate::new(201))
                .expect(1)
                .mount(&mock)
                .await;

            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .and(wiremock::matchers::path(&format!(
                    "/api/v0/cas/{}",
                    CID_STR
                )))
                .respond_with(car_response)
                .expect(1)
                .mount(&mock)
                .await;

            mock
        })
        .await;
        let committer = CasCommitter::new(url, test_signer().await).unwrap();
        let car = write_car(Cid::from_str(CID_STR).unwrap(), Vec::new()).await;
        let transaction = CasTransaction {
            proof: Cid::from_str(CID_STR).unwrap(),
            car,
        };
        let _result = committer.commit(transaction).await.unwrap();
    }
}

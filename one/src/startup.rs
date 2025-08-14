//! Startup utilities for the Ceramic One daemon.

use anyhow::{anyhow, Result};
use ceramic_api::InterestService as InterestServiceTrait;
use ceramic_core::{EventId, Interest, NodeId, StreamId};
use ceramic_interest_svc::InterestService;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Process initial interests by registering them with the interest service
pub async fn process_extra_interests(
    extra_interests: &[String],
    interest_svc: &Arc<InterestService>,
    network: &ceramic_core::Network,
    node_id: &NodeId,
) -> Result<()> {
    if extra_interests.is_empty() {
        return Ok(());
    }

    info!("Processing {} extra interests", extra_interests.len());

    for stream_id_str in extra_interests {
        let stream_id_str = stream_id_str.trim();
        if stream_id_str.is_empty() {
            continue;
        }

        // Validate that the model stream ID is parseable
        let _stream_id = StreamId::from_str(stream_id_str)
            .map_err(|e| anyhow!("Invalid model ID '{}': {}", stream_id_str, e))?;

        // Create an interest for the "model" separator key covering the full range for this stream
        // This follows the same pattern as the API endpoint /ceramic/interests/model/{stream_id}
        let stream_id_bytes = multibase::decode(stream_id_str)
            .map_err(|e| anyhow!("Failed to decode stream ID '{}': {}", stream_id_str, e))?
            .1;
        let start = EventId::builder()
            .with_network(network)
            .with_sep("model", &stream_id_bytes)
            .with_min_controller()
            .with_min_init()
            .with_min_event()
            .build_fencepost();
        let stop = EventId::builder()
            .with_network(network)
            .with_sep("model", &stream_id_bytes)
            .with_max_controller()
            .with_max_init()
            .with_max_event()
            .build_fencepost();

        let interest = Interest::builder()
            .with_sep_key("model")
            .with_peer_id(&node_id.peer_id())
            .with_range((start.as_slice(), stop.as_slice()))
            .with_not_after(0)
            .build();

        match interest_svc.insert(interest).await {
            Ok(was_inserted) => {
                if was_inserted {
                    info!(
                        "Successfully registered extra interest for model: {}",
                        stream_id_str
                    );
                } else {
                    debug!(
                        "Interest for model {} was already registered",
                        stream_id_str
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Failed to register initial interest for model {}: {}",
                    stream_id_str, e
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_core::Network;
    use ceramic_sql::sqlite::SqlitePool;
    use recon::Store;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_process_extra_interests_empty() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let interest_svc = Arc::new(InterestService::new(pool));
        let network = Network::InMemory;
        let node_key = ceramic_core::NodeKey::random();
        let node_id = node_key.id();

        // Test with empty interests
        let result = process_extra_interests(&[], &interest_svc, &network, &node_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_extra_interests_invalid_stream_id() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let interest_svc = Arc::new(InterestService::new(pool));
        let network = Network::InMemory;
        let node_key = ceramic_core::NodeKey::random();
        let node_id = node_key.id();

        // Invalid stream ID - tests daemon-specific stream ID parsing and error handling
        let stream_ids = vec!["invalid-stream-id".to_string()];

        let result = process_extra_interests(&stream_ids, &interest_svc, &network, &node_id).await;
        assert!(result.is_err());

        // Verify the error message contains information about the invalid stream ID
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("invalid-stream-id"));
    }

    #[tokio::test]
    async fn test_process_extra_interests_mixed_valid_invalid() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let interest_svc = Arc::new(InterestService::new(pool));
        let network = Network::InMemory;
        let node_key = ceramic_core::NodeKey::random();
        let node_id = node_key.id();

        // Mix of valid and invalid stream IDs - tests early failure behavior
        let stream_ids = vec![
            "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9".to_string(), // Valid
            "invalid-stream-id".to_string(),                                              // Invalid
        ];

        let result = process_extra_interests(&stream_ids, &interest_svc, &network, &node_id).await;
        // Should fail on the first invalid ID
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_extra_interests_whitespace_handling() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let interest_svc = Arc::new(InterestService::new(pool));
        let network = Network::InMemory;
        let node_key = ceramic_core::NodeKey::random();
        let node_id = node_key.id();

        // Test daemon-specific input sanitization logic
        let stream_ids = vec![
            "  k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9  ".to_string(), // Valid with whitespace
            "".to_string(),    // Empty string
            "   ".to_string(), // Only whitespace
        ];

        let result = process_extra_interests(&stream_ids, &interest_svc, &network, &node_id).await;

        // Should succeed and handle whitespace correctly
        assert!(result.is_ok());

        let registered_interests = interest_svc.full_range().await.unwrap();
        assert_eq!(registered_interests.count(), 1);
    }

    #[tokio::test]
    async fn test_process_extra_interests_multibase_decoding() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let interest_svc = Arc::new(InterestService::new(pool));
        let network = Network::InMemory;
        let node_key = ceramic_core::NodeKey::random();
        let node_id = node_key.id();

        // Test daemon-specific multibase decoding logic
        let stream_ids = vec![
            "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9".to_string(), // Valid multibase-encoded stream ID
        ];

        let result = process_extra_interests(&stream_ids, &interest_svc, &network, &node_id).await;

        // Should succeed with proper multibase decoding
        assert!(result.is_ok());

        let registered_interests = interest_svc.full_range().await.unwrap();
        assert_eq!(registered_interests.count(), 1);
    }
}

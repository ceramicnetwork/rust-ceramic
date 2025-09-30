use std::{
    collections::BTreeMap,
    fmt::Display,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, Result};
use futures::{future::BoxFuture, StreamExt, TryStreamExt};
use k8s_openapi::{
    api::{
        apps::v1::{StatefulSet, StatefulSetSpec},
        batch::v1::{Job, JobSpec},
        core::v1::{
            ConfigMap, ConfigMapEnvSource, ConfigMapVolumeSource, Container, ContainerPort,
            EmptyDirVolumeSource, EnvFromSource, EnvVar, EnvVarSource, ExecAction, Namespace,
            ObjectFieldSelector, PersistentVolumeClaim, PersistentVolumeClaimSpec,
            PersistentVolumeClaimVolumeSource, Pod, PodSpec, PodTemplateSpec, Probe,
            ResourceRequirements, Secret, SecretKeySelector, Service, ServiceAccount, ServicePort,
            ServiceSpec, Volume, VolumeMount,
        },
        rbac::v1::{ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, Subject},
    },
    apimachinery::pkg::{
        api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
    },
    ClusterResourceScope, NamespaceResourceScope,
};
use keramik_operator::{
    network::{IpfsSpec, Network},
    simulation::Simulation,
};
use kube::{
    api::{
        Api, DeleteParams, ListParams, LogParams, ObjectMeta, Patch, PatchParams, PostParams,
        WatchParams,
    },
    core::WatchEvent,
    Client, Resource, ResourceExt,
};
use log::{debug, info, trace};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{fs, time::sleep};

const TESTER_NAME: &str = "ceramic-tester";
const CERAMIC_ADMIN_DID_SECRET_NAME: &str = "ceramic-admin";
const DID_PRIVATE_KEY_NAME: &str = "private-key";
const SERVICE_ACCOUNT_NAME: &str = "ceramic-tests-service-account";
const CLUSTER_ROLE_NAME: &str = "ceramic-tests-cluster-role";
const LOCALSTACK_SERVICE_NAME: &str = "ceramic-tests-localstack";
const LOCALSTACK_DATA_NAME: &str = "localstack-data";
const PROCESS_PEERS_NAME: &str = "process-peers";

// See [`WatchParams::timeout`] documentation.
const MAX_KUBE_WATCH_TIMEOUT: u32 = 290;

const DID_PRIVATE_KEY: &str = "c864a33033626b448912a19509992552283fd463c143bdc4adc75f807b7a4dce";

const NODE_INSPECTION_PORT: i32 = 9229;

#[derive(Debug, Clone)]
pub struct TestConfig {
    pub network: PathBuf,

    pub test_image: Option<String>,

    pub ceramic_one_image: Option<String>,

    pub flavor: Flavor,

    pub suffix: Option<String>,

    pub network_ttl: u64,

    pub network_timeout: u32,

    pub job_timeout: u32,

    pub test_selector: String,
}

#[derive(Debug, Clone)]
pub enum Flavor {
    /// Correctness tests
    Correctness,
    /// Migration tests
    Migration { wait_secs: u64, migration: PathBuf },
    /// Performance tests
    Performance(PathBuf),
}

impl Flavor {
    fn name(&self) -> &'static str {
        match self {
            Flavor::Correctness => "correctness",
            Flavor::Migration { .. } => "migration",
            Flavor::Performance(_) => "perf",
        }
    }
}

impl Display for Flavor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

async fn load_network_file(
    file_path: impl AsRef<Path>,
    ttl: u64,
    flavor: &Flavor,
    suffix: &Option<String>,
    ceramic_one_image: &Option<String>,
) -> Result<Network> {
    // Parse network file
    let mut network: Network = serde_yaml::from_str(&fs::read_to_string(file_path).await?)?;
    debug!("input network {:#?}", network);

    // The test driver relies on the Keramik operator network TTL to clean up the network, with an 8 hour default that
    // allows devs to investigate any failures. The TTL can also be extended at any time, if more time is needed.
    network.spec.ttl_seconds = Some(ttl);

    let mut network_name = format!(
        "{}-{}",
        flavor,
        network
            .metadata
            .name
            .as_ref()
            .expect("network should have a defined name in metadata")
    );
    if let Some(suffix) = &suffix {
        if !suffix.is_empty() {
            network_name = format!("{network_name}-{suffix}");
        }
    }
    network.metadata.name = Some(network_name.clone());

    if let Some(ceramic_one_image) = ceramic_one_image {
        network.spec.ceramic = network.spec.ceramic.map(|ceramic_specs| {
            ceramic_specs
                .into_iter()
                .map(|mut ceramic_spec| {
                    ceramic_spec.ipfs = match ceramic_spec.ipfs {
                        Some(IpfsSpec::Rust(mut spec)) => {
                            spec.image = Some(ceramic_one_image.clone());
                            spec.image_pull_policy = Some("IfNotPresent".to_owned());
                            Some(IpfsSpec::Rust(spec))
                        }
                        spec => spec,
                    };
                    ceramic_spec
                })
                .collect()
        });
    }
    Ok(network)
}

pub async fn run(opts: TestConfig) -> Result<()> {
    // Infer the runtime environment and try to create a Kubernetes Client
    let client = Client::try_default().await?;

    // Parse network file
    let network = load_network_file(
        opts.network,
        opts.network_ttl,
        &opts.flavor,
        &opts.suffix,
        &opts.ceramic_one_image,
    )
    .await?;
    debug!("configured network {:#?}", network);
    let network_name = network.name_unchecked();

    let namespace = format!("keramik-{network_name}");

    // Create namespace if it does not already exist
    apply_namespace(client.clone(), &namespace).await?;

    // If it is missing, create the admin secret directly in the network namespace instead of in the "keramik" namespace
    // then having the Keramik operator in turn create one in the network namespace. Creating the secret directly in the
    // network namespace allows a unique secret to be created for each network instead of applying the same secret
    // across all networks.
    if is_secret_missing(
        client.clone(),
        namespace.as_str(),
        CERAMIC_ADMIN_DID_SECRET_NAME,
    )
    .await?
    {
        info!("creating admin secret");
        create_secret(
            client.clone(),
            namespace.as_str(),
            CERAMIC_ADMIN_DID_SECRET_NAME,
            BTreeMap::from_iter([(
                DID_PRIVATE_KEY_NAME.to_string(),
                std::env::var("DID_PRIVATE_KEY").unwrap_or_else(|_| DID_PRIVATE_KEY.to_owned()),
            )]),
        )
        .await?;
    }

    // Apply service account within the test network namespace
    // The test job uses this account.
    apply_resource_namespaced(client.clone(), &namespace, service_account(&namespace)).await?;

    // Apply cluster role and binding.
    apply_resource(client.clone(), cluster_role()).await?;
    apply_resource(client.clone(), cluster_role_binding(&namespace)).await?;

    // Apply the network itself
    apply_resource(client.clone(), network).await?;

    // Apply the process-peers config map
    apply_resource_namespaced(client.clone(), &namespace, process_peers()).await?;

    // Create any dependencies of the job
    match opts.flavor {
        Flavor::Performance(_) | Flavor::Migration { .. } => {}
        Flavor::Correctness => {
            apply_resource_namespaced(
                client.clone(),
                &namespace,
                localstack_stateful_set(&namespace),
            )
            .await?;
            apply_resource_namespaced(client.clone(), &namespace, localstack_service(&namespace))
                .await?;
        }
    }

    // Wait for network to be ready
    wait_for_network(client.clone(), &network_name, opts.network_timeout).await?;

    // Create the job/simulation
    let job_name = match &opts.flavor {
        Flavor::Correctness | Flavor::Migration { .. } => {
            create_resource_namespaced(
                client.clone(),
                &namespace,
                correctness_test(&namespace, opts.test_image, opts.test_selector),
            )
            .await?
        }
        Flavor::Performance(simulation) => {
            create_resource_namespaced(
                client.clone(),
                &namespace,
                performance_simulation(&namespace, simulation).await?,
            )
            .await?
        }
    };

    let results = match opts.flavor {
        Flavor::Correctness => {
            wait_for_job(client.clone(), &namespace, &job_name, opts.job_timeout).await?
        }
        Flavor::Migration {
            wait_secs,
            ref migration,
        } => {
            // Wait designated time before starting the migration
            sleep(Duration::from_secs(wait_secs)).await;
            let migration_network = load_network_file(
                migration,
                opts.network_ttl,
                &opts.flavor,
                &opts.suffix,
                &opts.ceramic_one_image,
            )
            .await?;

            // Apply the migration network
            apply_resource(client.clone(), migration_network).await?;

            // Finally wait for the test job to finish
            wait_for_job(client.clone(), &namespace, &job_name, opts.job_timeout).await?
        }
        Flavor::Performance(_) => {
            wait_for_simulation(client.clone(), &namespace, opts.job_timeout).await?
        }
    };

    match results {
        (JobResult::Pass, logs) => {
            println!("Passed:\n {logs}");
            Ok(())
        }
        (JobResult::Fail, logs) => {
            println!("Failed:\n {logs}");
            Err(anyhow!("Tests Failed"))
        }
    }
}

async fn wait_for_network(client: Client, network_name: &str, timeout_seconds: u32) -> Result<()> {
    // Kube watches have a max timeout of 290s so we retry in a loop until we reach the provided
    // timeout_seconds.
    loop_with_max_timeout(timeout_seconds, MAX_KUBE_WATCH_TIMEOUT, move |timeout| {
        let client = client.clone();
        Box::pin(async move {
            let networks: Api<Network> = Api::all(client.clone());
            let lp = WatchParams::default()
                .fields(&format!("metadata.name={network_name}"))
                .timeout(timeout);
            let mut stream = networks.watch(&lp, "0").await?.boxed();
            while let Some(status) = stream.try_next().await? {
                trace!("network watch event {:?}", status);
                match status {
                    WatchEvent::Added(network)
                    | WatchEvent::Modified(network)
                    | WatchEvent::Deleted(network) => {
                        if let Some(status) = network.status {
                            if status.ready_replicas == status.replicas {
                                return Ok(Some(()));
                            }
                        }
                    }
                    WatchEvent::Bookmark(_) => {}
                    WatchEvent::Error(err) => return Err(err.into()),
                }
            }
            Ok(None)
        })
    })
    .await
}

async fn wait_for_job(
    client: Client,
    namespace: &str,
    job_name: &str,
    timeout_seconds: u32,
) -> Result<(JobResult, String)> {
    // Kube watches have a max timeout of 290s so we retry in a loop until we reach the provided
    // timeout_seconds.
    loop_with_max_timeout(timeout_seconds, MAX_KUBE_WATCH_TIMEOUT, move |timeout| {
        let client = client.clone();
        Box::pin(async move {
            let jobs: Api<Job> = Api::namespaced(client.clone(), namespace);
            let lp = WatchParams::default()
                .fields(&format!("metadata.name={job_name}"))
                .timeout(timeout);
            let mut stream = jobs.watch(&lp, "0").await?.boxed();
            while let Some(status) = stream.try_next().await? {
                trace!("job watch event {:?}", status);
                match status {
                    WatchEvent::Added(job)
                    | WatchEvent::Modified(job)
                    | WatchEvent::Deleted(job) => {
                        if let Some(result) = check_job_result(&job) {
                            let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
                            let lp = ListParams::default().labels(&format!("job-name={job_name}"));
                            let mut logs = String::new();
                            for p in pods.list(&lp).await? {
                                trace!("found job pod: {}", p.name_unchecked());
                                logs += &pods
                                    .logs(&p.name_unchecked(), &LogParams::default())
                                    .await?;
                                logs += "\n";
                            }
                            return Ok(Some((result, logs)));
                        }
                    }
                    WatchEvent::Bookmark(_) => {}
                    WatchEvent::Error(err) => return Err(err.into()),
                }
            }
            Ok(None)
        })
    })
    .await
}

// waits for the simulate manager pod to fail or succeed
async fn wait_for_simulation(
    client: Client,
    namespace: &str,
    timeout_seconds: u32,
) -> Result<(JobResult, String)> {
    loop_with_max_timeout(timeout_seconds, MAX_KUBE_WATCH_TIMEOUT, move |timeout| {
        let client = client.clone();
        Box::pin(async move {
            let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
            let wp = WatchParams::default()
                .labels("job-name=simulate-manager")
                .timeout(timeout);
            let mut stream = pods.watch(&wp, "0").await?.boxed();
            while let Some(status) = stream.try_next().await? {
                debug!("pod watch event {:#?}", status);
                match status {
                    WatchEvent::Added(pod) | WatchEvent::Modified(pod) => {
                        trace!("found pod: {}", pod.name_unchecked());
                        debug!("pod status: {:#?}", pod.status);

                        if let Some(status) = &pod.status {
                            if let Some(phase) = &status.phase {
                                if phase == "Succeeded" {
                                    let logs = pods
                                        .logs(&pod.name_unchecked(), &LogParams::default())
                                        .await?;
                                    return Ok(Some((JobResult::Pass, logs)));
                                } else if phase == "Failed" {
                                    let logs = pods
                                        .logs(&pod.name_unchecked(), &LogParams::default())
                                        .await?;
                                    return Ok(Some((JobResult::Fail, logs)));
                                }
                            }
                        }
                    }
                    WatchEvent::Deleted(_) | WatchEvent::Bookmark(_) => {}
                    WatchEvent::Error(err) => return Err(err.into()),
                }
            }
            Ok(None)
        })
    })
    .await
}

// Loop calling f with a timeout until timeout_seconds is reached where timeout will never be
// greater than max_timeout.
//
// Returning Ok(None) from f indicates f should be called again.
// Returning Ok(Some(_)) from f indicates the value should be returned.
// Returning Err(_) from f indicates the loop should halt and the error be returned.
async fn loop_with_max_timeout<'a, R>(
    timeout_seconds: u32,
    max_timeout: u32,
    f: impl Fn(u32) -> BoxFuture<'a, Result<Option<R>>>,
) -> Result<R> {
    let mut total_time = 0;
    while total_time < timeout_seconds {
        let timeout = std::cmp::min(timeout_seconds - total_time, max_timeout);
        let fut = f(timeout);
        if let Some(result) = fut.await? {
            return Ok(result);
        }
        total_time += timeout;
    }
    Err(anyhow!("watch timeout exceeded"))
}

#[derive(Debug)]
enum JobResult {
    Pass,
    Fail,
}
fn check_job_result(job: &Job) -> Option<JobResult> {
    if let Some(status) = &job.status {
        if let Some(conditions) = &status.conditions {
            for condition in conditions {
                if condition.type_ == "Failed" && condition.status == "True" {
                    return Some(JobResult::Fail);
                } else if condition.type_ == "Complete" && condition.status == "True" {
                    return Some(JobResult::Pass);
                }
            }
        }
    }
    None
}

async fn is_secret_missing(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<bool, kube::error::Error> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
    Ok(secrets.get_opt(name).await?.is_none())
}

async fn create_secret(
    client: Client,
    namespace: &str,
    name: &str,
    string_data: BTreeMap<String, String>,
) -> Result<()> {
    let secret = Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            ..ObjectMeta::default()
        },
        string_data: Some(string_data),
        ..Default::default()
    };
    apply_resource_namespaced(client, namespace, secret).await
}

// Delete a network
pub async fn delete_network(client: Client, name: &str) -> Result<()> {
    let networks: Api<Network> = Api::all(client.clone());
    networks.delete(name, &DeleteParams::default()).await?;
    Ok(())
}

// Applies the namespace
async fn apply_namespace(client: Client, name: &str) -> Result<()> {
    let namespace: Namespace = Namespace {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            ..ObjectMeta::default()
        },
        ..Default::default()
    };
    apply_resource(client, namespace).await
}

pub async fn apply_resource_namespaced<R>(
    client: Client,
    namespace: &str,
    resource: R,
) -> Result<()>
where
    R: Serialize
        + DeserializeOwned
        + Resource<Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug,
    <R as kube::Resource>::DynamicType: Default,
{
    let serverside = PatchParams::apply(TESTER_NAME);
    let resources: Api<R> = Api::namespaced(client.clone(), namespace);
    let name = resource
        .meta()
        .name
        .as_ref()
        .ok_or(anyhow!("resource must have a name"))?
        .to_owned();
    resources
        .patch(&name, &serverside, &Patch::Apply(resource))
        .await?;
    Ok(())
}
pub async fn create_resource_namespaced<R>(
    client: Client,
    namespace: &str,
    resource: R,
) -> Result<String>
where
    R: Serialize
        + DeserializeOwned
        + Resource<Scope = NamespaceResourceScope>
        + Clone
        + std::fmt::Debug,
    <R as kube::Resource>::DynamicType: Default,
{
    let resources: Api<R> = Api::namespaced(client.clone(), namespace);
    let result = resources.create(&PostParams::default(), &resource).await?;
    Ok(result.name_unchecked())
}

pub async fn apply_resource<R>(client: Client, resource: R) -> Result<()>
where
    R: Serialize
        + DeserializeOwned
        + Resource<Scope = ClusterResourceScope>
        + Clone
        + std::fmt::Debug,
    <R as kube::Resource>::DynamicType: Default,
{
    let serverside = PatchParams::apply(TESTER_NAME);
    let resources: Api<R> = Api::all(client.clone());
    let name = resource
        .meta()
        .name
        .as_ref()
        .ok_or(anyhow!("resource must have a name"))?
        .to_owned();
    resources
        .patch(&name, &serverside, &Patch::Apply(resource))
        .await?;
    Ok(())
}

fn service_account(namespace: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: ObjectMeta {
            name: Some(SERVICE_ACCOUNT_NAME.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        automount_service_account_token: Some(true),
        ..Default::default()
    }
}
fn cluster_role() -> ClusterRole {
    ClusterRole {
        metadata: ObjectMeta {
            name: Some(CLUSTER_ROLE_NAME.to_owned()),
            ..Default::default()
        },
        rules: Some(vec![PolicyRule {
            api_groups: Some(vec!["keramik.3box.io".to_owned()]),
            resources: Some(vec!["networks/status".to_owned()]),
            verbs: vec!["get".to_owned()],
            ..Default::default()
        }]),
        ..Default::default()
    }
}

fn cluster_role_binding(namespace: &str) -> ClusterRoleBinding {
    ClusterRoleBinding {
        metadata: ObjectMeta {
            name: Some(format!("ceramic-tests-cluster-binding-role-{namespace}")),
            ..Default::default()
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_owned(),
            name: SERVICE_ACCOUNT_NAME.to_owned(),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        }]),
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_owned(),
            kind: "ClusterRole".to_owned(),
            name: CLUSTER_ROLE_NAME.to_owned(),
        },
    }
}

fn process_peers() -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(PROCESS_PEERS_NAME.to_owned()),
            ..ObjectMeta::default()
        },
        data: Some(BTreeMap::from_iter([(
            "process-peers.sh".to_owned(),
            include_str!("./process-peers.sh").to_owned(),
        )])),
        ..Default::default()
    }
}

fn correctness_test(namespace: &str, image: Option<String>, test_selector: String) -> Job {
    let (image, image_pull_policy) = if let Some(image) = image {
        (Some(image), Some("IfNotPresent".to_owned()))
    } else {
        (
            Some("public.ecr.aws/r5b3e0r5/3box/ceramic-tests-suite".to_owned()),
            Some("Always".to_owned()),
        )
    };

    Job {
        metadata: ObjectMeta {
            generate_name: Some("ceramic-tests-".to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        spec: Some(JobSpec {
            // Clean up finished jobs after 10m
            ttl_seconds_after_finished: Some(600),
            backoff_limit: Some(0),
            template: PodTemplateSpec {
                spec: Some(PodSpec {
                    restart_policy: Some("Never".to_owned()),
                    service_account_name: Some(SERVICE_ACCOUNT_NAME.to_owned()),
                    init_containers: Some(vec![job_init()]),
                    containers: vec![Container {
                        name: "tests".to_owned(),
                        image,
                        image_pull_policy,
                        ports: Some(vec![ContainerPort {
                            container_port: NODE_INSPECTION_PORT,
                            name: Some("inspect".to_owned()),
                            protocol: Some("TCP".to_owned()),
                            ..Default::default()
                        }]),
                        resources: Some(ResourceRequirements {
                            limits: Some(BTreeMap::from_iter([
                                ("cpu".to_owned(), Quantity("500m".to_owned())),
                                ("ephemeral-storage".to_owned(), Quantity("1Gi".to_owned())),
                                ("memory".to_owned(), Quantity("1Gi".to_owned())),
                            ])),
                            requests: Some(BTreeMap::from_iter([
                                ("cpu".to_owned(), Quantity("500m".to_owned())),
                                ("ephemeral-storage".to_owned(), Quantity("1Gi".to_owned())),
                                ("memory".to_owned(), Quantity("1Gi".to_owned())),
                            ])),
                            ..Default::default()
                        }),
                        volume_mounts: Some(vec![VolumeMount {
                            mount_path: "/config".to_owned(),
                            name: "config-volume".to_owned(),
                            ..Default::default()
                        }]),
                        env: Some(vec![
                            EnvVar {
                                name: "DOTENV_CONFIG_PATH".to_owned(),
                                value: Some("/config/.env".to_owned()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "AWS_ACCESS_KEY_ID".to_owned(),
                                value: Some(".".to_owned()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "AWS_SECRET_ACCESS_KEY".to_owned(),
                                value: Some(".".to_owned()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "DB_ENDPOINT".to_owned(),
                                value: Some(format!("http://{LOCALSTACK_SERVICE_NAME}:4566")),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "TEST_SELECTOR".to_owned(),
                                value: Some(test_selector),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "NETWORK".to_owned(),
                                value: Some("local".to_owned()),
                                ..Default::default()
                            },
                            EnvVar {
                                name: "NODE_OPTIONS".to_owned(),
                                value: Some("--inspect".to_owned()),
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    }],
                    volumes: Some(job_volumes()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn performance_simulation(namespace: &str, simulation_path: &PathBuf) -> Result<Simulation> {
    let mut simulation: Simulation =
        serde_yaml::from_str(&fs::read_to_string(&simulation_path).await?)?;
    debug!("input simulation {:#?}", simulation);
    simulation.metadata.namespace = Some(namespace.to_string());
    debug!(
        "simulation.metadata.namespace {:#?}",
        simulation.metadata.namespace
    );

    Ok(simulation)
}

// Create volumes for init container
fn job_volumes() -> Vec<Volume> {
    vec![
        Volume {
            name: PROCESS_PEERS_NAME.to_owned(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(PROCESS_PEERS_NAME.to_owned()),
                default_mode: Some(0o755),
                ..Default::default()
            }),
            ..Default::default()
        },
        Volume {
            name: "peers-volume".to_owned(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some("keramik-peers".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        },
        Volume {
            name: "config-volume".to_owned(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
    ]
}

// Create init container for job
fn job_init() -> Container {
    Container {
        name: "init-tests".to_owned(),
        image: Some("alpine:latest".to_owned()),
        image_pull_policy: Some("IfNotPresent".to_owned()),
        command: Some(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "apk add --no-cache jq curl && /network/process-peers.sh".to_owned(),
        ]),
        env: Some(vec![
            EnvVar {
                name: "NAMESPACE".to_owned(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.namespace".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            EnvVar {
                name: "CERAMIC_ADMIN_DID_SECRET".to_owned(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        key: DID_PRIVATE_KEY_NAME.to_owned(),
                        name: Some(CERAMIC_ADMIN_DID_SECRET_NAME.to_owned()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]),
        env_from: Some(vec![EnvFromSource {
            config_map_ref: Some(ConfigMapEnvSource {
                name: Some("keramik-peers".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        }]),
        volume_mounts: Some(vec![
            VolumeMount {
                mount_path: "/network".to_owned(),
                name: PROCESS_PEERS_NAME.to_owned(),
                ..Default::default()
            },
            VolumeMount {
                mount_path: "/peers".to_owned(),
                name: "peers-volume".to_owned(),
                ..Default::default()
            },
            VolumeMount {
                mount_path: "/config".to_owned(),
                name: "config-volume".to_owned(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

fn localstack_stateful_set(namespace: &str) -> StatefulSet {
    StatefulSet {
        metadata: ObjectMeta {
            name: Some(LOCALSTACK_SERVICE_NAME.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(BTreeMap::from_iter([(
                    "app".to_owned(),
                    LOCALSTACK_SERVICE_NAME.to_owned(),
                )])),
                ..Default::default()
            },
            service_name: LOCALSTACK_SERVICE_NAME.to_owned(),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from_iter([(
                        "app".to_owned(),
                        LOCALSTACK_SERVICE_NAME.to_owned(),
                    )])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: LOCALSTACK_SERVICE_NAME.to_owned(),
                        image: Some("gresau/localstack-persist:3".to_owned()),
                        resources: Some(ResourceRequirements {
                            limits: Some(BTreeMap::from_iter([
                                ("cpu".to_owned(), Quantity("500m".to_owned())),
                                ("ephemeral-storage".to_owned(), Quantity("512Mi".to_owned())),
                                ("memory".to_owned(), Quantity("1Gi".to_owned())),
                            ])),
                            requests: Some(BTreeMap::from_iter([
                                ("cpu".to_owned(), Quantity("500m".to_owned())),
                                ("ephemeral-storage".to_owned(), Quantity("512Mi".to_owned())),
                                ("memory".to_owned(), Quantity("1Gi".to_owned())),
                            ])),
                            ..Default::default()
                        }),
                        ports: Some(vec![ContainerPort {
                            container_port: 4566,
                            ..Default::default()
                        }]),
                        readiness_probe:Some(Probe{
                            initial_delay_seconds: Some(0),
                            period_seconds:Some(30),
                            timeout_seconds:Some(30),
                            exec: Some(ExecAction{
                                command:Some(vec![
                                     "bash".to_owned(),
                                     "-c".to_owned(),
                                     "awslocal dynamodb list-tables && awslocal es list-domain-names && awslocal s3 ls && awslocal sqs list-queues".to_owned(),
                                ]),
                            }),
                        ..Default::default()
                        }),
                        volume_mounts: Some(vec![VolumeMount{
                            name: LOCALSTACK_DATA_NAME.to_owned(),
                            mount_path: "/persisted-data".to_owned(),
                        ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    volumes: Some(vec![
                                 Volume{
                                     name: LOCALSTACK_DATA_NAME.to_owned(),
                                     persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource{
                                         claim_name: LOCALSTACK_DATA_NAME.to_owned(),
                                         ..Default::default()
                                     }),
                                         ..Default::default()
                                 }
                    ]),
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some(LOCALSTACK_DATA_NAME.to_owned()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_owned()]),
                    resources: Some(ResourceRequirements {
                        requests: Some(BTreeMap::from_iter([(
                            "storage".to_owned(),
                            Quantity("1Gi".to_owned()),
                        )])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn localstack_service(namespace: &str) -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(LOCALSTACK_SERVICE_NAME.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: Some(BTreeMap::from_iter([(
                "app".to_owned(),
                LOCALSTACK_SERVICE_NAME.to_owned(),
            )])),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some(LOCALSTACK_SERVICE_NAME.to_string()),
                port: 4566,
                target_port: Some(IntOrString::Int(4566)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            selector: Some(BTreeMap::from_iter([(
                "app".to_owned(),
                LOCALSTACK_SERVICE_NAME.to_owned(),
            )])),
            type_: Some("NodePort".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#![allow(missing_docs, unused_variables, trivial_casts)]

#[allow(unused_imports)]
use ceramic_kubo_rpc_server::{
    models, Api, ApiNoContext, BlockGetPostResponse, BlockPutPostResponse, BlockStatPostResponse,
    Client, ContextWrapperExt, DagGetPostResponse, DagImportPostResponse, DagPutPostResponse,
    DagResolvePostResponse, IdPostResponse, PinAddPostResponse, PinRmPostResponse,
    PubsubLsPostResponse, PubsubPubPostResponse, PubsubSubPostResponse, SwarmConnectPostResponse,
    SwarmPeersPostResponse, VersionPostResponse,
};
use clap::{App, Arg};
#[allow(unused_imports)]
use futures::{future, stream, Stream};

#[allow(unused_imports)]
use log::info;

// swagger::Has may be unused if there are no examples
#[allow(unused_imports)]
use swagger::{AuthData, ContextBuilder, EmptyContext, Has, Push, XSpanIdString};

type ClientContext = swagger::make_context_ty!(
    ContextBuilder,
    EmptyContext,
    Option<AuthData>,
    XSpanIdString
);

// rt may be unused if there are no examples
#[allow(unused_mut)]
fn main() {
    env_logger::init();

    let matches = App::new("client")
        .arg(
            Arg::with_name("operation")
                .help("Sets the operation to run")
                .possible_values(&[
                    "BlockGetPost",
                    "BlockPutPost",
                    "BlockStatPost",
                    "DagGetPost",
                    "DagImportPost",
                    "DagPutPost",
                    "DagResolvePost",
                    "IdPost",
                    "PinAddPost",
                    "PinRmPost",
                    "PubsubLsPost",
                    "PubsubPubPost",
                    "PubsubSubPost",
                    "SwarmConnectPost",
                    "SwarmPeersPost",
                    "VersionPost",
                ])
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("https")
                .long("https")
                .help("Whether to use HTTPS or not"),
        )
        .arg(
            Arg::with_name("host")
                .long("host")
                .takes_value(true)
                .default_value("localhost")
                .help("Hostname to contact"),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .takes_value(true)
                .default_value("8080")
                .help("Port to contact"),
        )
        .get_matches();

    let is_https = matches.is_present("https");
    let base_url = format!(
        "{}://{}:{}",
        if is_https { "https" } else { "http" },
        matches.value_of("host").unwrap(),
        matches.value_of("port").unwrap()
    );

    let context: ClientContext = swagger::make_context!(
        ContextBuilder,
        EmptyContext,
        None as Option<AuthData>,
        XSpanIdString::default()
    );

    let mut client: Box<dyn ApiNoContext<ClientContext>> = if matches.is_present("https") {
        // Using Simple HTTPS
        let client =
            Box::new(Client::try_new_https(&base_url).expect("Failed to create HTTPS client"));
        Box::new(client.with_context(context))
    } else {
        // Using HTTP
        let client =
            Box::new(Client::try_new_http(&base_url).expect("Failed to create HTTP client"));
        Box::new(client.with_context(context))
    };

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    match matches.value_of("operation") {
        Some("BlockGetPost") => {
            let result = rt.block_on(client.block_get_post("arg_example".to_string()));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("BlockPutPost") => {
            let result = rt.block_on(client.block_put_post(
                swagger::ByteArray(Vec::from("BYTE_ARRAY_DATA_HERE")),
                None,
                None,
                None,
            ));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("BlockStatPost") => {
            let result = rt.block_on(client.block_stat_post("arg_example".to_string()));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("DagGetPost") => {
            let result = rt.block_on(client.dag_get_post("arg_example".to_string(), None));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("DagImportPost") => {
            let result = rt.block_on(
                client.dag_import_post(swagger::ByteArray(Vec::from("BYTE_ARRAY_DATA_HERE"))),
            );
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("DagPutPost") => {
            let result = rt.block_on(client.dag_put_post(
                swagger::ByteArray(Vec::from("BYTE_ARRAY_DATA_HERE")),
                None,
                None,
            ));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("DagResolvePost") => {
            let result = rt.block_on(client.dag_resolve_post("arg_example".to_string()));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("IdPost") => {
            let result = rt.block_on(client.id_post(Some("arg_example".to_string())));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("PinAddPost") => {
            let result = rt.block_on(client.pin_add_post("arg_example".to_string(), None, None));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("PinRmPost") => {
            let result = rt.block_on(client.pin_rm_post("arg_example".to_string()));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("PubsubLsPost") => {
            let result = rt.block_on(client.pubsub_ls_post());
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("PubsubPubPost") => {
            let result = rt.block_on(client.pubsub_pub_post(
                "arg_example".to_string(),
                swagger::ByteArray(Vec::from("BYTE_ARRAY_DATA_HERE")),
            ));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("PubsubSubPost") => {
            let result = rt.block_on(client.pubsub_sub_post("arg_example".to_string()));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("SwarmConnectPost") => {
            let result = rt.block_on(client.swarm_connect_post(&Vec::new()));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("SwarmPeersPost") => {
            let result = rt.block_on(client.swarm_peers_post());
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("VersionPost") => {
            let result = rt.block_on(client.version_post());
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        _ => {
            panic!("Invalid operation provided")
        }
    }
}

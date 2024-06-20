#![allow(missing_docs, unused_variables, trivial_casts)]

#[allow(unused_imports)]
use ceramic_api_server::{
    models, Api, ApiNoContext, AuthTokenAccessPostResponse, AuthTokenRefreshPostResponse, Client,
    ContextWrapperExt, DebugHeapGetResponse, EventsEventIdGetResponse, EventsPostResponse,
    ExperimentalEventsSepSepValueGetResponse, ExperimentalInterestsGetResponse,
    FeedEventsGetResponse, FeedResumeTokenGetResponse, InterestsPostResponse,
    InterestsSortKeySortValuePostResponse, LivenessGetResponse, VersionGetResponse,
    VersionPostResponse,
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
                    "DebugHeapGet",
                    "EventsEventIdGet",
                    "ExperimentalEventsSepSepValueGet",
                    "ExperimentalInterestsGet",
                    "FeedEventsGet",
                    "FeedResumeTokenGet",
                    "InterestsSortKeySortValuePost",
                    "LivenessGet",
                    "VersionGet",
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
        /* Disabled because there's no example.
        Some("AuthTokenAccessPost") => {
            let result = rt.block_on(client.auth_token_access_post(
                  "authorization_example".to_string(),
                  "did_example".to_string(),
                  ???
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        */
        /* Disabled because there's no example.
        Some("AuthTokenRefreshPost") => {
            let result = rt.block_on(client.auth_token_refresh_post(
                  "authorization_example".to_string(),
                  "x_did_example".to_string(),
                  ???
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        */
        Some("DebugHeapGet") => {
            let result = rt.block_on(client.debug_heap_get());
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("EventsEventIdGet") => {
            let result = rt.block_on(client.events_event_id_get(
                "event_id_example".to_string(),
                Some("authorization_example".to_string()),
                Some("did_example".to_string()),
            ));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        /* Disabled because there's no example.
        Some("EventsPost") => {
            let result = rt.block_on(client.events_post(
                  ???
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        */
        Some("ExperimentalEventsSepSepValueGet") => {
            let result = rt.block_on(client.experimental_events_sep_sep_value_get(
                "sep_example".to_string(),
                "sep_value_example".to_string(),
                Some("controller_example".to_string()),
                Some("stream_id_example".to_string()),
                Some(56),
                Some(56),
            ));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("ExperimentalInterestsGet") => {
            let result = rt.block_on(client.experimental_interests_get());
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("FeedEventsGet") => {
            let result = rt
                .block_on(client.feed_events_get(Some("resume_at_example".to_string()), Some(56)));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("FeedResumeTokenGet") => {
            let result = rt.block_on(client.feed_resume_token_get());
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        /* Disabled because there's no example.
        Some("InterestsPost") => {
            let result = rt.block_on(client.interests_post(
                  ???
            ));
            info!("{:?} (X-Span-ID: {:?})", result, (client.context() as &dyn Has<XSpanIdString>).get().clone());
        },
        */
        Some("InterestsSortKeySortValuePost") => {
            let result = rt.block_on(client.interests_sort_key_sort_value_post(
                "sort_key_example".to_string(),
                "sort_value_example".to_string(),
                Some("controller_example".to_string()),
                Some("stream_id_example".to_string()),
            ));
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("LivenessGet") => {
            let result = rt.block_on(client.liveness_get());
            info!(
                "{:?} (X-Span-ID: {:?})",
                result,
                (client.context() as &dyn Has<XSpanIdString>).get().clone()
            );
        }
        Some("VersionGet") => {
            let result = rt.block_on(client.version_get());
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

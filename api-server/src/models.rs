#![allow(clippy::useless_vec)]
#![allow(unused_qualifications)]

use validator::Validate;

#[cfg(any(feature = "client", feature = "server"))]
use crate::header;
use crate::models;

/// A Ceramic event as part of a Ceramic Stream
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Event {
    /// Multibase encoding of event id bytes.
    #[serde(rename = "id")]
    pub id: String,

    /// Multibase encoding of event data.
    #[serde(rename = "data")]
    pub data: String,
}

impl Event {
    #[allow(clippy::new_without_default)]
    pub fn new(id: String, data: String) -> Event {
        Event { id, data }
    }
}

/// Converts the Event value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for Event {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("id".to_string()),
            Some(self.id.to_string()),
            Some("data".to_string()),
            Some(self.data.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Event value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Event {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub id: Vec<String>,
            pub data: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Event".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "id" => intermediate_rep.id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "data" => intermediate_rep.data.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Event".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Event {
            id: intermediate_rep
                .id
                .into_iter()
                .next()
                .ok_or_else(|| "id missing in Event".to_string())?,
            data: intermediate_rep
                .data
                .into_iter()
                .next()
                .ok_or_else(|| "data missing in Event".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Event> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<Event>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Event>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for Event - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<Event> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => match <Event as std::str::FromStr>::from_str(value) {
                std::result::Result::Ok(value) => {
                    std::result::Result::Ok(header::IntoHeaderValue(value))
                }
                std::result::Result::Err(err) => std::result::Result::Err(format!(
                    "Unable to convert header value '{}' into Event - {}",
                    value, err
                )),
            },
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// A Ceramic event as part of a Ceramic Stream
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct EventDeprecated {
    /// Multibase encoding of event id bytes.
    #[serde(rename = "eventId")]
    pub event_id: String,

    /// Multibase encoding of event data.
    #[serde(rename = "eventData")]
    pub event_data: String,
}

impl EventDeprecated {
    #[allow(clippy::new_without_default)]
    pub fn new(event_id: String, event_data: String) -> EventDeprecated {
        EventDeprecated {
            event_id,
            event_data,
        }
    }
}

/// Converts the EventDeprecated value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for EventDeprecated {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            Some("eventId".to_string()),
            Some(self.event_id.to_string()),
            Some("eventData".to_string()),
            Some(self.event_data.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a EventDeprecated value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for EventDeprecated {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub event_id: Vec<String>,
            pub event_data: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing EventDeprecated".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "eventId" => intermediate_rep.event_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "eventData" => intermediate_rep.event_data.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing EventDeprecated".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(EventDeprecated {
            event_id: intermediate_rep
                .event_id
                .into_iter()
                .next()
                .ok_or_else(|| "eventId missing in EventDeprecated".to_string())?,
            event_data: intermediate_rep
                .event_data
                .into_iter()
                .next()
                .ok_or_else(|| "eventData missing in EventDeprecated".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<EventDeprecated> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<EventDeprecated>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<EventDeprecated>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for EventDeprecated - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<EventDeprecated>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <EventDeprecated as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into EventDeprecated - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Ceramic event keys as part of a Ceramic Stream
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct EventFeed {
    /// An array of events. For now, the value is empty.
    #[serde(rename = "events")]
    pub events: Vec<models::Event>,

    /// The token/highwater mark to used as resumeAt on a future request
    #[serde(rename = "resumeToken")]
    pub resume_token: String,
}

impl EventFeed {
    #[allow(clippy::new_without_default)]
    pub fn new(events: Vec<models::Event>, resume_token: String) -> EventFeed {
        EventFeed {
            events,
            resume_token,
        }
    }
}

/// Converts the EventFeed value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for EventFeed {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            // Skipping events in query parameter serialization
            Some("resumeToken".to_string()),
            Some(self.resume_token.to_string()),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a EventFeed value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for EventFeed {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub events: Vec<Vec<models::Event>>,
            pub resume_token: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing EventFeed".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    "events" => {
                        return std::result::Result::Err(
                            "Parsing a container in this style is not supported in EventFeed"
                                .to_string(),
                        )
                    }
                    #[allow(clippy::redundant_clone)]
                    "resumeToken" => intermediate_rep.resume_token.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing EventFeed".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(EventFeed {
            events: intermediate_rep
                .events
                .into_iter()
                .next()
                .ok_or_else(|| "events missing in EventFeed".to_string())?,
            resume_token: intermediate_rep
                .resume_token
                .into_iter()
                .next()
                .ok_or_else(|| "resumeToken missing in EventFeed".to_string())?,
        })
    }
}

// Methods for converting between header::IntoHeaderValue<EventFeed> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<EventFeed>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<EventFeed>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for EventFeed - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<EventFeed> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <EventFeed as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into EventFeed - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Event to add to the node. Temp while we transition to id/data style, at which time we can swap this out for the Event schema.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct EventsPostRequest {
    /// Multibase encoding of event id bytes.
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Multibase encoding of event data.
    #[serde(rename = "data")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,

    /// Multibase encoding of event id bytes.
    #[serde(rename = "event_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,

    /// Multibase encoding of event data.
    #[serde(rename = "event_data")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_data: Option<String>,
}

impl EventsPostRequest {
    #[allow(clippy::new_without_default)]
    pub fn new() -> EventsPostRequest {
        EventsPostRequest {
            id: None,
            data: None,
            event_id: None,
            event_data: None,
        }
    }
}

/// Converts the EventsPostRequest value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for EventsPostRequest {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![
            self.id
                .as_ref()
                .map(|id| ["id".to_string(), id.to_string()].join(",")),
            self.data
                .as_ref()
                .map(|data| ["data".to_string(), data.to_string()].join(",")),
            self.event_id
                .as_ref()
                .map(|event_id| ["event_id".to_string(), event_id.to_string()].join(",")),
            self.event_data
                .as_ref()
                .map(|event_data| ["event_data".to_string(), event_data.to_string()].join(",")),
        ];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a EventsPostRequest value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for EventsPostRequest {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub id: Vec<String>,
            pub data: Vec<String>,
            pub event_id: Vec<String>,
            pub event_data: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing EventsPostRequest".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "id" => intermediate_rep.id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "data" => intermediate_rep.data.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "event_id" => intermediate_rep.event_id.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    #[allow(clippy::redundant_clone)]
                    "event_data" => intermediate_rep.event_data.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing EventsPostRequest".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(EventsPostRequest {
            id: intermediate_rep.id.into_iter().next(),
            data: intermediate_rep.data.into_iter().next(),
            event_id: intermediate_rep.event_id.into_iter().next(),
            event_data: intermediate_rep.event_data.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<EventsPostRequest> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<EventsPostRequest>>
    for hyper::header::HeaderValue
{
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<EventsPostRequest>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for EventsPostRequest - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue>
    for header::IntoHeaderValue<EventsPostRequest>
{
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <EventsPostRequest as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into EventsPostRequest - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

/// Version of the Ceramic node in semver format, e.g. 2.1.0
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Version {
    /// Version of the Ceramic node
    #[serde(rename = "version")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl Version {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Version {
        Version { version: None }
    }
}

/// Converts the Version value to the Query Parameters representation (style=form, explode=false)
/// specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde serializer
impl std::string::ToString for Version {
    fn to_string(&self) -> String {
        let params: Vec<Option<String>> = vec![self
            .version
            .as_ref()
            .map(|version| ["version".to_string(), version.to_string()].join(","))];

        params.into_iter().flatten().collect::<Vec<_>>().join(",")
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a Version value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for Version {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        /// An intermediate representation of the struct to use for parsing.
        #[derive(Default)]
        #[allow(dead_code)]
        struct IntermediateRep {
            pub version: Vec<String>,
        }

        let mut intermediate_rep = IntermediateRep::default();

        // Parse into intermediate representation
        let mut string_iter = s.split(',');
        let mut key_result = string_iter.next();

        while key_result.is_some() {
            let val = match string_iter.next() {
                Some(x) => x,
                None => {
                    return std::result::Result::Err(
                        "Missing value while parsing Version".to_string(),
                    )
                }
            };

            if let Some(key) = key_result {
                #[allow(clippy::match_single_binding)]
                match key {
                    #[allow(clippy::redundant_clone)]
                    "version" => intermediate_rep.version.push(
                        <String as std::str::FromStr>::from_str(val).map_err(|x| x.to_string())?,
                    ),
                    _ => {
                        return std::result::Result::Err(
                            "Unexpected key while parsing Version".to_string(),
                        )
                    }
                }
            }

            // Get the next key
            key_result = string_iter.next();
        }

        // Use the intermediate representation to return the struct
        std::result::Result::Ok(Version {
            version: intermediate_rep.version.into_iter().next(),
        })
    }
}

// Methods for converting between header::IntoHeaderValue<Version> and hyper::header::HeaderValue

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<header::IntoHeaderValue<Version>> for hyper::header::HeaderValue {
    type Error = String;

    fn try_from(
        hdr_value: header::IntoHeaderValue<Version>,
    ) -> std::result::Result<Self, Self::Error> {
        let hdr_value = hdr_value.to_string();
        match hyper::header::HeaderValue::from_str(&hdr_value) {
            std::result::Result::Ok(value) => std::result::Result::Ok(value),
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Invalid header value for Version - value: {} is invalid {}",
                hdr_value, e
            )),
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl std::convert::TryFrom<hyper::header::HeaderValue> for header::IntoHeaderValue<Version> {
    type Error = String;

    fn try_from(hdr_value: hyper::header::HeaderValue) -> std::result::Result<Self, Self::Error> {
        match hdr_value.to_str() {
            std::result::Result::Ok(value) => {
                match <Version as std::str::FromStr>::from_str(value) {
                    std::result::Result::Ok(value) => {
                        std::result::Result::Ok(header::IntoHeaderValue(value))
                    }
                    std::result::Result::Err(err) => std::result::Result::Err(format!(
                        "Unable to convert header value '{}' into Version - {}",
                        value, err
                    )),
                }
            }
            std::result::Result::Err(e) => std::result::Result::Err(format!(
                "Unable to convert header: {:?} to string: {}",
                hdr_value, e
            )),
        }
    }
}

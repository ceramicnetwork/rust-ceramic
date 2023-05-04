use ceramic_event::StreamId;
use schemars::schema::RootSchema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ModelAccountRelation {
    List,
    Single,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ModelRelationDefinition {
    Account,
    Document { model: StreamId },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ModelViewDefinition {
    DocumentAccount,
    DocumentVersion,
    RelationDocument { model: StreamId, property: String },
    RelationFrom { model: StreamId, property: String },
    RelationCountFrom { model: StreamId, property: String },
}

#[derive(Debug, Deserialize, Serialize)]
#[repr(transparent)]
pub struct CborSchema(serde_json::Value);

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelDefinition {
    version: &'static str,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    schema: CborSchema,
    account_relation: ModelAccountRelation,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    relations: HashMap<String, ModelRelationDefinition>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    views: HashMap<String, ModelViewDefinition>,
}

impl ModelDefinition {
    pub fn new<T: GetRootSchema>(
        name: &str,
        account_relation: ModelAccountRelation,
    ) -> anyhow::Result<Self> {
        let schema = T::root_schema();
        let schema = serde_json::to_value(&schema)?;
        Ok(Self {
            version: "1.0",
            name: name.to_string(),
            description: None,
            schema: CborSchema(schema),
            account_relation,
            relations: HashMap::default(),
            views: HashMap::default(),
        })
    }

    pub fn schema(&self) -> anyhow::Result<RootSchema> {
        let s = serde_json::from_value(self.schema.0.clone())?;
        Ok(s)
    }

    pub fn with_description(&mut self, description: String) -> &mut Self {
        self.description = Some(description);
        self
    }

    pub fn with_relation(&mut self, key: String, relation: ModelRelationDefinition) -> &mut Self {
        self.relations.insert(key, relation);
        self
    }

    pub fn with_view(&mut self, key: String, view: ModelViewDefinition) -> &mut Self {
        self.views.insert(key, view);
        self
    }
}

pub trait GetRootSchema: JsonSchema {
    fn root_schema() -> RootSchema {
        let settings = schemars::gen::SchemaSettings::default().with(|s| {
            s.meta_schema = Some("https://json-schema.org/draft/2020-12/schema".to_string());
            s.option_nullable = true;
            s.option_add_null_type = false;
        });
        let gen = settings.into_generator();
        gen.into_root_schema_for::<Self>()
    }
}

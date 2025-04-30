use aws_sdk_dynamodb::types as dynamodb_types;
use aws_sdk_dynamodb::{Client, Error as DynamodbError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::import::SeqRepoSeqAlias;

const SEQREPO_TABLE_NAME: &str = "seqrepo";

#[derive(Debug, Clone)]
pub enum ObjectType {
    SeqAlias,
    SeqInfo,
}

const PK_NAME: &str = "type";
const SK_NAME: &str = "name";
const SEQ_ID_ATTR_NAME: &str = "seqid";

impl ObjectType {
    pub fn to_db_value(&self) -> String {
        match self {
            ObjectType::SeqAlias => "seqalias".to_string(),
            ObjectType::SeqInfo => "seqinfo".to_string(),
        }
    }
}

pub async fn create_table_if_not_exists(client: &Client) -> Result<(), DynamodbError> {
    let tables = client.list_tables().send().await?;
    let names = tables.table_names();
    for name in names {
        if name == SEQREPO_TABLE_NAME {
            return Ok(());
        }
    }
    create_table(client).await?;
    Ok(())
}

#[derive(Debug)]
pub enum SeqRepoLookupError {
    InvalidData(String),
    DynamoDbError(DynamodbError),
}

// lookups
// * given sequence ID/alias (incl namespace) -> get all aliases, sequence metadata
//
// * pk: type (metadata vs sequence/fasta ?)
// * sk: id value (sequence id, sequence alias for seqalias)
//
// alias properties:
// * sequence_id: ga4gh seq id (could be redundant to sk) [GSI on pk-sequence_id]
// * added [timestamp of some kind]
// * is_current [bool]
//
// seqinfo properties:
// * uri
// * len
// * alpha
// * added
async fn create_table(client: &Client) -> Result<(), DynamodbError> {
    // primary key: item type
    // either "seqalias" or "fastadir"
    let pk_key = dynamodb_types::KeySchemaElement::builder()
        .attribute_name(String::from(PK_NAME))
        .key_type(dynamodb_types::KeyType::Hash)
        .build()?;
    let pk_attr = dynamodb_types::AttributeDefinition::builder()
        .attribute_name(PK_NAME)
        .attribute_type(dynamodb_types::ScalarAttributeType::S)
        .build()?;

    // sort key: namespaced identifier or alias
    // eg
    // * "refseq:NC_000001.11"
    // * "ga4gh:SQ.Ya6Rs7DHhDeg7YaOSg1EoNi3U_nQ9SvO"
    let sk_key = dynamodb_types::KeySchemaElement::builder()
        .attribute_name(String::from(SK_NAME))
        .key_type(dynamodb_types::KeyType::Range)
        .build()?;

    let sk_attr = dynamodb_types::AttributeDefinition::builder()
        .attribute_name(SK_NAME)
        .attribute_type(dynamodb_types::ScalarAttributeType::S)
        .build()?;

    let seq_id_attr = dynamodb_types::AttributeDefinition::builder().att

    let throughput = dynamodb_types::ProvisionedThroughput::builder()
        .set_read_capacity_units(Some(100))
        .set_write_capacity_units(Some(100))
        .build()?;

    let _ = client
        .create_table()
        .table_name(String::from(SEQREPO_TABLE_NAME))
        .key_schema(pk_key)
        .attribute_definitions(pk_attr)
        .key_schema(sk_key)
        .attribute_definitions(sk_attr)
        .provisioned_throughput(throughput)
        .send()
        .await?;

    // TODO create GSI on type + sequence_id

    Ok(())
}

pub async fn get_aws_client() -> Result<Client, DynamodbError> {
    // TODO make this configurable
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .test_credentials()
        .endpoint_url("http://localhost:8001")
        .load()
        .await;
    let client = Client::new(&config);
    let _ = create_table_if_not_exists(&client);
    Ok(client)
}

pub async fn put_seq_alias(
    client: &Client,
    seq_alias: SeqRepoSeqAlias,
) -> Result<(), DynamodbError> {
    let pk_av = dynamodb_types::AttributeValue::S(ObjectType::SeqAlias.to_db_value());
    let sk_av =
        dynamodb_types::AttributeValue::S(format!("{}:{}", seq_alias.namespace, seq_alias.alias));
    let seq_id_av = dynamodb_types::AttributeValue::S(seq_alias.seq_id);
    let added_av = dynamodb_types::AttributeValue::S(seq_alias.added);
    let current_av = dynamodb_types::AttributeValue::Bool(seq_alias.is_current);
    let request = client
        .put_item()
        .table_name(SEQREPO_TABLE_NAME)
        .item(PK_NAME, pk_av)
        .item(SK_NAME, sk_av)
        .item("seq_id", seq_id_av)
        .item("added", added_av)
        .item("current", current_av);

    let _ = request.send().await?;

    Ok(())
}

async fn get_items(
    client: &Client,
    primary_key: &str,
    sort_key: &str,
    //index_name: Option<&str> // TODO figure out indexes
) -> Result<Option<Vec<HashMap<String, dynamodb_types::AttributeValue>>>, DynamodbError> {
    let results = client
        .query()
        .table_name(SEQREPO_TABLE_NAME)
        .key_condition_expression("#pk = :type and #sk = :name")
        .expression_attribute_names("#pk", PK_NAME)
        .expression_attribute_values(
            ":type",
            dynamodb_types::AttributeValue::S(primary_key.to_string()),
        )
        .expression_attribute_names("#sk", SK_NAME)
        .expression_attribute_values(
            ":name",
            dynamodb_types::AttributeValue::S(sort_key.to_string()),
        )
        .send()
        .await?;
    if let Some(items) = results.items {
        Ok(Some(items))
    } else {
        // TODO not actually sure this should be an ok?
        // figure out error conditions etc
        Ok(None)
    }
}

pub async fn get_sequence_id_from_alias(
    client: &Client,
    alias: &str,
) -> Result<Option<String>, SeqRepoLookupError> {
    if let Some(alias_matches) = get_items(client, &ObjectType::SeqAlias.to_db_value(), alias)
        .await
        .map_err(|e| SeqRepoLookupError::DynamoDbError(e))?
    {
        if alias_matches.len() == 0 {
            return Ok(None);
        }
        if alias_matches.len() != 1 {
            return Err(SeqRepoLookupError::InvalidData(
                "Multiple items matching this alias -- should be impossible".to_string(),
            ));
        }
        let item = alias_matches[0].clone();
        match item.get("seq_id") {
            Some(i) => i.as_s().map(|s| Some(s.to_owned())).map_err(|_| {
                SeqRepoLookupError::InvalidData("seq_id not parseable as string".to_string())
            }),
            None => Err(SeqRepoLookupError::InvalidData(
                "Missing seq_id property".to_string(),
            )),
        }
    } else {
        return Ok(None);
    }
}

pub async fn get_sequence_metadata( client: &Client, seq_id: &str) -> Result<SequenceMetadata, SeqRepoLookupError> {
    Ok(SequenceMetadata {
        added: "added".to_string(),
        aliases: vec!("".to_string()),
        alphabet: "alpha".to_string(),
        len: 20,
    })
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SequenceMetadata {
    pub added: String,
    pub aliases: Vec<String>,
    pub alphabet: String,
    pub len: u32,
}

#[derive(Debug)]
pub enum DynamodbConversionError {
    MissingField(String),
}

impl TryFrom<&HashMap<String, dynamodb_types::AttributeValue>> for SequenceMetadata {
    type Error = DynamodbConversionError;

    fn try_from(
        value: &HashMap<String, dynamodb_types::AttributeValue>,
    ) -> Result<Self, Self::Error> {
        let added = value
            .get("added")
            .ok_or_else(|| DynamodbConversionError::MissingField("added".into()))?
            .as_s()
            .unwrap()
            .clone();
        let name = value
            .get("name")
            .ok_or_else(|| DynamodbConversionError::MissingField("name".into()))?
            .as_s()
            .unwrap()
            .clone();
        let seq_id = value
            .get("seq_id")
            .ok_or_else(|| DynamodbConversionError::MissingField("seq_id".into()))?
            .as_s()
            .unwrap()
            .clone();
        let aliases = vec![name, seq_id];
        //let alphabet = value
        //    .get("alphabet")
        //    .ok_or_else(|| DynamodbConversionError::MissingField("alphabet".into()))?
        //    .as_s()
        //    .unwrap()
        //    .clone();
        //let len = value
        //    .get("len")
        //    .ok_or_else(|| DynamodbConversionError::MissingField("len".into()))?
        //    .as_n()
        //    .unwrap()
        //    .parse::<u32>()
        //    .unwrap();
        Ok(SequenceMetadata {
            added,
            aliases,
            alphabet: "agct".to_string(),
            len: 5,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SequenceAlias {
    pub alias: String,
    pub seq_id: String,
}

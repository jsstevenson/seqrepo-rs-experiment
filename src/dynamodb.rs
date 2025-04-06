use aws_sdk_dynamodb::types as dynamodb_types;
use aws_sdk_dynamodb::{Client, Error as DynamodbError};

use crate::import::SeqAlias;

const TABLE_NAME: &str = "seqrepo";

pub async fn create_table_if_not_exists(client: &Client) -> Result<(), DynamodbError> {
    let tables = client.list_tables().send().await?;
    let names = tables.table_names();
    println!("---{:?}", names);
    for name in names {
        if name == TABLE_NAME {
            return Ok(());
        }
    }

    create_table(client).await?;

    Ok(())
}

#[derive(Debug, Clone)]
enum ObjectType {
    SeqAlias,
    FastaDir
}

const PK_NAME: &str = "type";
const SK_NAME: &str = "name";

impl ObjectType {
    pub fn to_db_value(&self) -> String {
        match self {
            ObjectType::SeqAlias => "seqalias".to_string(),
            ObjectType::FastaDir => "fastadir".to_string()
        }
    }
}


// lookups
// * given sequence ID/alias (incl namespace) -> get all aliases, sequence metadata
//
// * pk: type (metadata vs sequence/fasta ?)
// * sk: id value (sequence id, sequence alias for seqalias)
//
// alias properties:
// * sequence_id: ga4gh seq id (could be redundant to sk) [GSI on pk-sequence_id]
// * len [int]
// * alpha
// * added [timeestamp]
//
// fastadir properties:
// * uri
async fn create_table(client: &Client) -> Result<(), DynamodbError> {
    // primary key: item type
    // either "seqalias" or "fastadir"
    let pk = dynamodb_types::KeySchemaElement::builder()
        .attribute_name(String::from(PK_NAME))
        .key_type(dynamodb_types::KeyType::Hash)
        .build()?;

    // sort key: namespaced identifier or alias
    // eg
    // * "refseq:NC_000001.11"
    // * "ga4gh:SQ.Ya6Rs7DHhDeg7YaOSg1EoNi3U_nQ9SvO"
    let sk = dynamodb_types::KeySchemaElement::builder()
        .attribute_name(String::from(SK_NAME))
        .key_type(dynamodb_types::KeyType::Range)
        .build()?;

    let _ = client
        .create_table()
        .table_name(String::from(TABLE_NAME))
        .key_schema(pk)
        .key_schema(sk)
        .send()
        .await;

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

pub async fn put_seq_alias(client: &Client, seq_alias: SeqAlias) -> Result<(), DynamodbError> {
    let pk_av = dynamodb_types::AttributeValue::S(seq_alias.seq_id);
    let sk_av = dynamodb_types::AttributeValue::S(seq_alias.namespace);
    // TODO might need to combine namaspace and alias
    let alias_av = dynamodb_types::AttributeValue::S(seq_alias.alias);
    let added_av = dynamodb_types::AttributeValue::S(seq_alias.added);
    let current_av = dynamodb_types::AttributeValue::Bool(seq_alias.is_current);
    let request = client
        .put_item()
        .table_name(TABLE_NAME)
        .item(PK_NAME, pk_av)
        .item(SK_NAME, sk_av)
        .item("alias", alias_av)
        .item("added", added_av)
        .item("current", current_av);

    let resp = request.send().await?;
    let attributes = resp.attributes().unwrap();
    println!("{:?}", attributes);

    Ok(())
}

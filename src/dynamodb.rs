use aws_sdk_dynamodb::types as dynamodb_types;
use aws_sdk_dynamodb::{Client, Error as DynamodbError};

use crate::import::SeqAlias;

pub async fn create_table_if_not_exists(client: &Client) -> Result<(), DynamodbError> {
    let tables = client.list_tables().send().await?;
    let names = tables.table_names();
    println!("---{:?}", names);
    for name in names {
        if name == "seqrepo" {
            return Ok(());
        }
    }

    create_table(client).await?;

    Ok(())
}

// primary key: type + seq ID
// sort key: namespace?
async fn create_table(client: &Client) -> Result<(), DynamodbError> {
    let pk = dynamodb_types::KeySchemaElement::builder()
        .attribute_name(String::from("pk"))
        .key_type(dynamodb_types::KeyType::Hash)
        .build()?;

    let sk = dynamodb_types::KeySchemaElement::builder()
        .attribute_name(String::from("sk"))
        .key_type(dynamodb_types::KeyType::Range)
        .build()?;

    let _ = client
        .create_table()
        .table_name(String::from("seqrepo"))
        .key_schema(pk)
        .key_schema(sk)
        .send()
        .await;

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
        .table_name("seqrepo")
        .item("pk", pk_av)
        .item("sk", sk_av)
        .item("alias", alias_av)
        .item("added", added_av)
        .item("current", current_av);

    let resp = request.send().await?;
    let attributes = resp.attributes().unwrap();
    println!("{:?}", attributes);

    Ok(())
}

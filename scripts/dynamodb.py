import boto3

table_name = "seqrepo"
dynamodb = boto3.resource("dynamodb", endpoint_url="http://localhost:8001")


def drop_table():
    table = dynamodb.Table(table_name)

    response = table.delete()
    print("Delete initiated:", response)

    waiter = table.meta.client.get_waiter("table_not_exists")
    waiter.wait(TableName=table_name)
    print(f"Table '{table_name}' has been deleted.")


def get_all_items():
    table = dynamodb.Table(table_name)

    response = table.scan()
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    return items

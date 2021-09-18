import json
import boto3
import datetime
from finnhub_utils import get_finnhub_client

finnhub_client = get_finnhub_client()


def get_finnhub_quote_data(entity_name: str):
    data = finnhub_client.quote(entity_name)
    return json.dumps(data)


def get_finnhub_indicators_data(entity_name: str):
    data = finnhub_client.aggregate_indicator(entity_name, 'D')
    return json.dumps(data)


def get_finnhub_social_media(entity_name: str):
    data = finnhub_client.stock_social_sentiment(entity_name)
    return json.dumps(data)


def get_dynamo_db_connection():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('LambdaAPIPullingTest')
    return table


def pull_data_from_api(entity_name: str) -> dict:
    return {
        "finnhub_quote_data": get_finnhub_quote_data(entity_name=entity_name),
        "finnhub_technical_indicators_data": get_finnhub_indicators_data(entity_name=entity_name),
        "finnhub_social_sentiment_data": get_finnhub_social_media(entity_name=entity_name)
    }


def create_item_to_write(entity_name: str) -> dict:
    data = pull_data_from_api(entity_name=entity_name)
    pull_datetime = datetime.datetime.utcnow()
    item = {
        "entity_name": entity_name,
        "datetime": str(pull_datetime),
        "timestamp": int(pull_datetime.timestamp()),
        'data_from_finnhub': data
    }
    return item


def lambda_handler(event, context):
    # TODO implement
    entity_name = event.get('entity_name', 'AAPL')
    item_to_write = create_item_to_write(entity_name=entity_name)
    statusCode = 200
    message = 'success'
    try:
        table = get_dynamo_db_connection()
        table.put_item(Item=item_to_write)
    except Exception as e:
        statusCode = 403
        message = f'error: {e}'

    return {
        'entity_name': entity_name,
        'statusCode': statusCode,
        'body': message
    }

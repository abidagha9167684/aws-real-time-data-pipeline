import base64
import json
import boto3


def lambda_handler(event, context):

    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])
        res = json.loads(payload)

        write_to_db(res)
        print("Object successfully stored in DB.")

    return "Success"


def write_to_db(data):
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
    table = dynamodb.Table("Project-DB")

    table.put_item(Item=data)

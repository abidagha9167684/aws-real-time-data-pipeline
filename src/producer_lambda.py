import json
import boto3
import uuid


def lambda_handler(event, context):
    client = boto3.client('kinesis')

    response = client.put_record(
        StreamName='KinesisDS-Project',
        Data=json.dumps(event),
        PartitionKey=str(uuid.uuid4())
    )

    print(response)
    return response

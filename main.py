import os

import boto3
import json
from faker import Faker
import random
from random import randint
from dotenv import load_dotenv
import uuid

load_dotenv()
fake = Faker('en_US')
sqs_client = boto3.client('sqs', region_name="ap-south-1",
                          aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                          aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
SQS_QUEUE_URL = ""


def generate_json(index) -> dict:
    return {
        "index": index,
        "foo": randint(0, 100),
        "bar": fake.name(),
        "poo": float(random.randrange(155, 389)) / 100
    }


def send_messages(random_json: str):
    print(type(random_json))
    response = sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=random_json,
        MessageGroupId=str(uuid.uuid4()),
        MessageDeduplicationId=str(uuid.uuid4())
    )
    return response


def receive_messages(index):
    messages = sqs_client.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=10,
        VisibilityTimeout=300,
        WaitTimeSeconds=0,
    )
    json_messages = []
    for message in messages['Messages']:
        json_messages.append(json.loads(message["Body"]))
    with open(f"response/messages_{index}.json", 'w') as file:
        json.dump({"Messages": json_messages}, file)


def delete_messages():
    messages = sqs_client.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=10,
        VisibilityTimeout=300,
        WaitTimeSeconds=0,
    )
    print(f"{messages=}")
    for message in messages['Messages']:
        response = sqs_client.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )
        print(f"Deleted Message: {response}")


def main():
    # generate 100 messages and send to AWS SQS
    responses = {}
    for i in range(100):
        random_json = json.dumps(generate_json(i))
        print(f"Sending {i} message, {random_json}")
        response = send_messages(random_json)
        responses[i] = response
    with open("response/sqs.json", 'w') as file:
        json.dump(responses, file, indent=4)


if __name__ == '__main__':
    receive_messages(0)

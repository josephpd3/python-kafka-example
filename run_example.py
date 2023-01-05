import argparse
import logging
import os
import random
import sys
import time

from dotenv import load_dotenv

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic

# For this example, modify PYTHONPATH to include inner module
sys.path.append(os.path.dirname(__file__))

from kafka_example.kafka import (
    get_deserialize_helper,
    serialize_helper,
    TransactionKey,
    TransactionValue
)

ACCOUNT_IDS = [
    "account_1",
    "account_2",
    "account_3",
]

ZIP_CODES = [
    "10001",
    "10002",
    "10003",
    "10004",
    "10005",
    "10006",
]

TEST_ENTITIES = [
    "market_basket",
    "starbucks",
    "walmart",
    "target",
    "whole_foods",
    "cvs",
    "walgreens",
    "rite_aid",
    "kroger",
]


def create_transaction_topic(bootstrap_servers: str, args: argparse.Namespace):
    """Creates 'transactions' topic for example"""
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    topic = NewTopic(
        "transactions",
        num_partitions=1,
        replication_factor=3
    )
    topics_to_futures = admin_client.create_topics([topic])
    for topic, future in topics_to_futures.items():
        try:
            future.result()
            logging.info(f"Topic {topic} created")
        except Exception as e:
            logging.error(f"Failed to create topic {topic}: {e}")


def run_consumer(bootstrap_servers: str, args: argparse.Namespace):
    key_deserializer = get_deserialize_helper(TransactionKey)
    value_deserializer = get_deserialize_helper(TransactionValue)

    consumer = DeserializingConsumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": args.consumer_group,
        "group.instance.id": args.consumer_id,
        "key.deserializer": key_deserializer,
        "value.deserializer": value_deserializer
    })

    # Consume from 'transactions' topic, awaiting keyboard interrupt
    consumer.subscribe(["transactions"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue
            print(f"Consumed record with key {msg.key()} and value {msg.value()}")
    except KeyboardInterrupt:
        logging.info("Shutting down consumer")
    finally:
        consumer.close()


def run_producer(bootstrap_servers: str, args: argparse.Namespace):
    producer = SerializingProducer({
        "bootstrap.servers": bootstrap_servers,
        "key.serializer": serialize_helper,
        "value.serializer": serialize_helper
    })

    # Produce 10 messages using random data
    for _ in range(10):
        key = TransactionKey(
            account_id=random.choice(ACCOUNT_IDS),
            transaction_zip_code=random.choice(ZIP_CODES),
            receiving_entity=random.choice(TEST_ENTITIES)
        )
        value = TransactionValue(
            account_id=key.account_id,
            transaction_zip_code=key.transaction_zip_code,
            receiving_entity=key.receiving_entity,
            transaction_dollars=random.randint(1, 1000),
            transaction_cents=random.randint(0, 99),
            transaction_epoch_seconds=time.time()
        )
        producer.produce(
            topic="transactions",
            key=key,
            value=value
        )

    producer.flush()


def main(args: argparse.Namespace):
    """Entrypoint for program"""
    load_dotenv()

    try:
        bootstrap_servers = os.environ["KAFKA_BROKERS"]
    except KeyError as ke:
        raise ValueError(f"Could not read bootstrap servers from ENV: {str(ke)}")

    if args.action == "consume":
        run_consumer(bootstrap_servers, args)
    elif args.action == "produce":
        run_producer(bootstrap_servers, args)
    elif args.action == "create_topic":
        create_transaction_topic(bootstrap_servers, args)
    else:
        raise ValueError(f"Invalid action: {args.action}")


def get_args() -> argparse.Namespace:
    """Retrieves runtime args"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action",
        type=str,
        choices=["consume", "produce", "create_topic"],
        required=True
    )
    parser.add_argument(
        "--consumer_id",
        type=str,
        default="test_consumer_1"
    )
    parser.add_argument(
        "--consumer_group",
        type=str,
        default="test_consumers"
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    run_args = get_args()
    main(run_args)

"""Kafka utilities module

"""
from dataclasses import dataclass, asdict
import json
from typing import Any, Callable, ClassVar


def serialize_helper(obj: Any, _ctx: Any) -> str:
    """Useful for serializing any dataclass into a seris of bytes"""
    return json.dumps(asdict(obj))


def get_deserialize_helper(cls: Any) -> Callable[[Any, Any], Any]:
    """Can be used to get a callable for deserializing a given dataclass"""
    def deserialize_helper(obj_bytes: str, _ctx: Any) -> Any:
        obj = json.loads(obj_bytes)
        return cls(**obj)

    return deserialize_helper


@dataclass(slots=True)
class TransactionKey:
    """Wraps data for the key of a Transaction"""
    schema_str: ClassVar[str] = """
    {
        "type": "record",
        "namespace": "kafka-example.transaction",
        "name": "TransactionKey",
        "fields": [
            {
                "name": "account_id",
                "type": "string"
            },
            {
                "name": "transaction_zip_code",
                "type": "string"
            },
            {
                "name" "receiving_entity",
                "type": "string"
            }
        ]
    }
    """

    account_id: str
    transaction_zip_code: str
    receiving_entity: str


@dataclass(slots=True)
class TransactionValue:
    """Wraps data for the value/body of a Transaction"""
    schema_str: ClassVar[str] = """
    {
        "type": "record",
        "namespace": "kafka-example.transaction",
        "name": "TransactionValue",
        "fields": [
            {
                "name": "account_id",
                "type": "string"
            },
            {
                "name": "transaction_zip_code",
                "type": "string"
            },
            {
                "name" "receiving_entity",
                "type": "string"
            },
            {
                "name": "transaction_dollars",
                "type": "int"
            },
            {
                "name": "transaction_cents",
                "type": "int"
            },
            {
                "name": "transaction_epoch_seconds",
                "type": "float"
            }
        ]
    }
    """

    account_id: str
    transaction_zip_code: str
    receiving_entity: str
    transaction_dollars: int
    transaction_cents: int
    transaction_epoch_seconds: float

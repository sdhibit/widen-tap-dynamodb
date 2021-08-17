"""Stream type classes for tap-dynamodb."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th

from tap_dynamodb.client import DynamoDBStream


class DynamicStream(DynamoDBStream):
    """Define dynamic stream."""

    def __init__(self,
                 tap,
                 name,
                 primary_keys=None,
                 replication_key=None,
                 except_keys=None,
                 records_path=None,
                 schema=None,
                 client=None,
                 ):
        super().__init__(tap=tap, name=tap.name, schema=schema)
        if primary_keys is None:
            primary_keys = []

        self.name = name
        self.primary_keys = primary_keys
        self.replication_key = replication_key
        self.except_keys = except_keys
        self.records_path = records_path
        self.client = client

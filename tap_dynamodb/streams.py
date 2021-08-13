"""Stream type classes for tap-dynamodb."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_dynamodb.client import DynamoDBStream

# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class DynamicStream(DynamoDBStream):
    """Define custom stream."""
    name = "users"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("age", th.IntegerType),
        th.Property("email", th.StringType),
        th.Property("street", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip", th.StringType),
    ).to_dict()



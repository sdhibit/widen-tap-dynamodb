"""DynamoDB tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_dynamodb.streams import DynamicStream


class TapDynamoDB(Tap):
    """DynamoDB tap class."""
    name = "tap-dynamodb"

    config_jsonschema = th.PropertiesList(
        th.Property("region_name", th.StringType, required=True),
        th.Property("account_id", th.StringType, required=True),
        th.Property("external_id", th.StringType, required=True),
        th.Property("role_name", th.StringType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams (i.e., DynamoDB tables for the given account and region)."""
        return []

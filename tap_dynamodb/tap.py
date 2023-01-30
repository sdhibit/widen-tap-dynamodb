"""DynamoDB tap class."""

from typing import List

from botocore.exceptions import ClientError
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_dynamodb.streams import DynamicStream
from tap_dynamodb import dynamodb
from tap_dynamodb.schema import flatten_json, infer_schema, merge_schemas
from tap_dynamodb.sync_strategies.full_table import scan_table
from tap_dynamodb.deserialize import Deserializer

def _merge_dicts(*dict_args):
    """
    Given any number of dictionaries, shallow copy and merge into a new dict,
    precedence goes to key-value pairs in latter dictionaries.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

class TapDynamoDB(Tap):
    """DynamoDB tap class."""
    jsonschema_additional_dict = {
        "dependencies": {
            "account_id": ["external_id", "role_arn"],
            "external_id": ["account_id", "role_arn"],
            "role_arn": ["account_id", "external_id"]
        }
    }

    name = "tap-dynamodb"

    config_jsonschema = _merge_dicts(jsonschema_additional_dict, th.PropertiesList(
        th.Property("region_name", th.StringType, required=True),
        th.Property("account_id", th.StringType, required=False),
        th.Property("external_id", th.StringType, required=False),
        th.Property("role_name", th.StringType, required=False),
        th.Property("use_local_dynamo", th.BooleanType, default=False, required=False),
        th.Property('num_inference_records', th.NumberType, default=50, required=False),
        th.Property('tables_to_discover', th.ArrayType(th.StringType), default=[], required=False),
    ).to_dict())

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams (i.e., DynamoDB tables for the given account and region)."""
        if not self.config.get('use_local_dynamo'):
            dynamodb.setup_aws_client(self.config)
        client = dynamodb.get_client(self.config)

        try:
            response = client.list_tables()
        except ClientError:
            raise Exception("Authorization to AWS failed. Please ensure the role and "
                            "policy are configured correctly on your AWS account.")

        config_table_list = self.config.get('tables_to_discover')
        table_list = config_table_list if config_table_list else response.get('TableNames')

        streams = [x for x in
                   (self.discover_table_schema(client, table) for table in table_list)
                   if x is not None]

        return streams

    def discover_table_schema(self, client, table_name):
        try:
            table_info = client.describe_table(TableName=table_name).get('Table', {})
        except ClientError:
            self.logger.info(f'Access to table {table_name} was denied, skipping')
            return None

        # write stream metadata
        key_props = [key_schema.get('AttributeName') for key_schema in table_info.get('KeySchema', [])]
        results = scan_table(table_name, None, None, self.config, True)

        orig_projection = ''
        schema = th.PropertiesList().to_dict()
        for result in results:
            i = 0
            for item in result.get('Items', []):
                orig_projection = ",".join(item.keys())
                record = Deserializer().deserialize_item(item)

                if type(record) is not dict:
                    raise ValueError("Input must be a dict object.")

                flat_record = flatten_json(record, self.config.get('except_keys', []))
                new_schema = infer_schema(flat_record)
                schema = merge_schemas(schema, new_schema.to_dict())

                i += 1
                if i > self.config.get('num_inference_records', 50):
                    break
            break

        return DynamicStream(
            tap=self,
            name=table_name,
            primary_keys=key_props,
            replication_key=None,
            schema=schema,
            client=client,
            orig_projection=orig_projection,
        )

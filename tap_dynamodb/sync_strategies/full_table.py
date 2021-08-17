"""
Taken heavily from https://github.com/singer-io/tap-dynamodb/blob/master/tap_dynamodb/sync_strategies/full_table.py
"""

import time

import singer
from singer import metadata
from tap_dynamodb import dynamodb
from tap_dynamodb.deserialize import Deserializer

LOGGER = singer.get_logger()


def scan_table(table_name, projection, last_evaluated_key, config, schema_inf=False):
    scan_params = {
        'TableName': table_name,
        'Limit': config['num_inference_records'] if schema_inf else 1000
    }

    if projection is not None and projection != '':
        scan_params['ProjectionExpression'] = projection
    if last_evaluated_key is not None:
        scan_params['ExclusiveStartKey'] = last_evaluated_key

    client = dynamodb.get_client(config)
    has_more = True

    while has_more:
        LOGGER.info(f'Scanning table {table_name} with params:')
        for key, value in scan_params.items():
            LOGGER.info(f'\t{key} = {value}')

        result = client.scan(**scan_params)
        yield result

        if result.get('LastEvaluatedKey'):
            scan_params['ExclusiveStartKey'] = result['LastEvaluatedKey']

        has_more = result.get('LastEvaluatedKey', False)

"""
DynamoDB access utilities. Taken heavily from
https://github.com/singer-io/tap-dynamodb/blob/master/tap_dynamodb/dynamodb.py
"""

import backoff
import boto3
import singer

from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    CredentialResolver,
    DeferredRefreshableCredentials,
    JSONFileCache
)
from botocore.exceptions import ClientError
from botocore.session import Session

LOGGER = singer.get_logger()


def retry_pattern():
    return backoff.on_exception(backoff.expo,
                                ClientError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=10)


def log_backoff_attempt(details):
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


class AssumeRoleProvider:
    METHOD = 'assume-role'

    def __init__(self, fetcher):
        self._fetcher = fetcher

    def load(self):
        return DeferredRefreshableCredentials(
            self._fetcher.fetch_credentials,
            self.METHOD
        )


@retry_pattern()
def setup_aws_client(config):
    assume_role_keys = ['account_id', 'external_id', 'role_name']

    if set(assume_role_keys).issubset(set(config.keys())):
        role_arn = f"arn:aws:iam::{config['account_id'].replace('-', '')}:role/{config['role_name']}"

        session = Session()
        fetcher = AssumeRoleCredentialFetcher(
            session.create_client,
            session.get_credentials(),
            role_arn,
            extra_args={
                'DurationSeconds': 3600,
                'RoleSessionName': 'TapDynamodDB',
                'ExternalId': config['external_id']
            },
            cache=JSONFileCache()
        )

        refreshable_session = Session()
        refreshable_session.register_component(
            'credential_provider',
            CredentialResolver([AssumeRoleProvider(fetcher)])
        )

        LOGGER.info(f"Attempting to assume_role on RoleArn: {role_arn}")
        boto3.setup_default_session(botocore_session=refreshable_session)
    else:
        session = Session()

        LOGGER.info("Using default AWS session")
        boto3.setup_default_session(botocore_session=session)


def get_client(config):
    if config.get('use_local_dynamo'):
        return boto3.client('dynamodb',
                            endpoint_url='http://localhost:8000',
                            region_name=config['region_name'])
    return boto3.client('dynamodb', config['region_name'])


def get_stream_client(config):
    if config.get('use_local_dynamo'):
        return boto3.client('dynamodbstreams',
                            endpoint_url='http://localhost:8000',
                            region_name=config['region_name'])
    return boto3.client('dynamodbstreams', region_name=config['region_name'])

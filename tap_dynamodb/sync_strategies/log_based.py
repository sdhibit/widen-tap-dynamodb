"""
Taken heavily from https://github.com/singer-io/tap-dynamodb/blob/master/tap_dynamodb/sync_strategies/log_based.py
"""

import datetime
import singer

from tap_dynamodb import dynamodb

WRITE_STATE_PERIOD = 1000


def get_shards(streams_client, stream_arn):
    """
    Yields closed shards.
    We only yield closed shards because it is
    impossible to tell if an open shard has any more records or if you
    will infinitely loop over the lastEvaluatedShardId
    """

    params = {
        'StreamArn': stream_arn
    }

    has_more = True

    while has_more:
        stream_info = streams_client.describe_stream(**params)['StreamDescription']

        for shard in stream_info['Shards']:
            # See
            # https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_DescribeStream.html
            # for documentation on how to identify closed shards
            # Closed shards all have an EndingSequenceNumber
            if shard['SequenceNumberRange'].get('EndingSequenceNumber'):
                yield shard

        last_evaluated_shard_id = stream_info.get('LastEvaluatedShardId')
        has_more = last_evaluated_shard_id is not None

        if has_more:
            params['ExclusiveStartShardId'] = last_evaluated_shard_id


def get_shard_records(streams_client, stream_arn, shard, sequence_number):
    """
    This should only be called on closed shards. Calling this on an open
    shard will lead to an infinite loop
    Yields the records on a shard.
    """
    if sequence_number:
        iterator_type = 'AFTER_SEQUENCE_NUMBER'
    else:
        iterator_type = 'TRIM_HORIZON'

    params = {
        'StreamArn': stream_arn,
        'ShardId': shard['ShardId'],
        'ShardIteratorType': iterator_type
    }

    if sequence_number:
        params['SequenceNumber'] = sequence_number

    shard_iterator = streams_client.get_shard_iterator(**params)['ShardIterator']

    # This will loop indefinitely if called on open shards
    while shard_iterator:
        records = streams_client.get_records(ShardIterator=shard_iterator, Limit=1000)

        for record in records['Records']:
            yield record

        shard_iterator = records.get('NextShardIterator')


def has_stream_aged_out(state, table_name):
    """
    Uses the success_timestamp on the stream to determine if we have
    successfully synced the stream in the last 19 hours 30 minutes.
    This uses 19 hours and 30 minutes because records are guaranteed to be
    available on a shard for 24 hours, but we only process closed shards
    and shards close after 4 hours. 24-4 = 20 and we gave 30 minutes of
    wiggle room because I don't trust AWS.
    See https://aws.amazon.com/blogs/database/dynamodb-streams-use-cases-and-design-patterns/
    """
    current_time = singer.utils.now()

    success_timestamp = singer.get_bookmark(state, table_name, 'success_timestamp')

    # If we have no success_timestamp then we have aged out
    if not success_timestamp:
        return True

    time_span = current_time - singer.utils.strptime_to_utc(success_timestamp)

    # If it has been > than 19h30m since the last successful sync of this
    # stream then we consider the stream to be aged out
    return time_span > datetime.timedelta(hours=19, minutes=30)


def get_initial_bookmarks(config, state, table_name):
    """
    Returns the state including all bookmarks necessary for the initial
    full table sync
    """
    client = dynamodb.get_client(config)
    streams_client = dynamodb.get_stream_client(config)

    table = client.describe_table(TableName=table_name)['Table']
    stream_arn = table['LatestStreamArn']

    finished_shard_bookmarks = [shard['ShardId'] for shard in get_shards(streams_client, stream_arn)]
    state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)

    return state

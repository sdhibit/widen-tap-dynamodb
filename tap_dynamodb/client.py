"""
Custom client handling, including DynamoDBStream base class.+
Taken heavily from https://github.com/singer-io/tap-dynamodb/blob/master/tap_dynamodb/sync_strategies/log_based.py
"""

from typing import Optional, Iterable

import singer
from singer_sdk.streams import Stream

from tap_dynamodb.sync_strategies import full_table, log_based
from tap_dynamodb.deserialize import Deserializer
from tap_dynamodb import dynamodb
from tap_dynamodb.schema import flatten_json


class DynamoDBStream(Stream):
    """Stream class for DynamoDB streams."""

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """

        if self.replication_method == "FULL_TABLE":
            for record in self.full_table_get_records():
                yield record

        elif self.replication_method == "LOG_BASED":
            if log_based.has_stream_aged_out(self.tap_state, self.name):
                self.logger.info("Clearing state because stream has aged out")
                self.tap_state.get('bookmarks', {}).pop(self.name)

            if not singer.get_bookmark(self.tap_state, self.name, 'initial_full_table_complete'):
                self.logger.info(f'Must complete full table sync before replicating '
                                 f'from dynamodb streams for {self.name}')
                for record in self.full_table_get_records():
                    yield record

            # TODO: test log-based replication
            for record in self.log_based_get_records():
                yield record

        else:
            self.logger.info(f'Unknown replication method: {self.replication_method} for stream: {self.name}')

    def full_table_get_records(self):
        results = full_table.scan_table(self.name, self.orig_projection, None, self.config, False)
        for result in results:
            for item in result.get('Items', []):
                record = Deserializer().deserialize_item(item)
                flat_record = flatten_json(record, self.config.get('except_keys', []))
                yield flat_record

    def log_based_get_records(self):
        table_name = self.name

        client = dynamodb.get_client(self.config)
        streams_client = dynamodb.get_stream_client(self.config)
        state = self.tap_state

        # Write activate version message
        # stream_version = singer.get_bookmark(state, table_name, 'version')
        # singer.write_version(table_name, stream_version)

        table = client.describe_table(TableName=table_name)['Table']
        try:
            stream_arn = table['LatestStreamArn']
        except KeyError:
            self.logger.error("Streams are not enabled for this table. Please use FULL_TABLE replication")
            raise RuntimeError("Streams are not enabled for this table. Please use FULL_TABLE replication")

        # Stores a dictionary of shardId : sequence_number for a shard. Should
        # only store sequence numbers for closed shards that have not been
        # fully synced
        seq_number_bookmarks = singer.get_bookmark(state, table_name, 'shard_seq_numbers')
        if not seq_number_bookmarks:
            seq_number_bookmarks = dict()

        # Get the list of closed shards which we have fully synced. These
        # are removed after performing a sync and not seeing the shardId
        # returned by get_shards() because at that point the shard has been
        # killed by DynamoDB and will not be returned anymore
        finished_shard_bookmarks = singer.get_bookmark(state, table_name, 'finished_shards')
        if not finished_shard_bookmarks:
            finished_shard_bookmarks = list()

        # The list of shardIds we found this sync. Is used to determine which
        # finished_shard_bookmarks to kill
        found_shards = []

        deserializer = Deserializer()

        for shard in log_based.get_shards(streams_client, stream_arn):
            found_shards.append(shard['ShardId'])
            # Only sync shards which we have not fully synced already
            if shard['ShardId'] not in finished_shard_bookmarks:
                records = self.process_shard(shard, seq_number_bookmarks, streams_client, stream_arn,
                                             self.orig_projection, deserializer,
                                             table_name, state)
                for record in records:
                    yield record

            # Now that we have fully synced the shard, move it from the
            # shard_seq_numbers to finished_shards.
            finished_shard_bookmarks.append(shard['ShardId'])
            state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)

            if seq_number_bookmarks.get(shard['ShardId']):
                seq_number_bookmarks.pop(shard['ShardId'])
                state = singer.write_bookmark(state, table_name, 'shard_seq_numbers', seq_number_bookmarks)

        for shardId in finished_shard_bookmarks:
            if shardId not in found_shards:
                # Remove this shard because its no longer appearing when we query for get_shards
                finished_shard_bookmarks.remove(shardId)
                state = singer.write_bookmark(state, table_name, 'finished_shards', finished_shard_bookmarks)

    def process_shard(
            self, shard, seq_number_bookmarks, streams_client, stream_arn, projection, deserializer, table_name, state
    ):
        seq_number = seq_number_bookmarks.get(shard['ShardId'])

        for record in log_based.get_shard_records(streams_client, stream_arn, shard, seq_number):
            if record['eventName'] == 'REMOVE':
                record_message = deserializer.deserialize_item(record['dynamodb']['Keys'])
                deleted_at = record['dynamodb']['ApproximateCreationDateTime']
                record_message["_sdc_deleted_at"] = singer.utils.strftime(deleted_at)
            else:
                record_message = deserializer.deserialize_item(record['dynamodb'].get('NewImage'))
                if record_message is None:
                    self.logger.fatal('Dynamo stream view type must be either "NEW_IMAGE" "NEW_AND_OLD_IMAGES"')
                    raise RuntimeError('Dynamo stream view type must be either "NEW_IMAGE" "NEW_AND_OLD_IMAGES"')
                if projection is not None and projection != '':
                    try:
                        record_message = deserializer.apply_projection(record_message, projection)
                    except:
                        self.logger.fatal("Projection failed to apply: %s", projection)
                        raise RuntimeError('Projection failed to apply: {}'.format(projection))

            yield record_message

            seq_number_bookmarks[shard['ShardId']] = record['dynamodb']['SequenceNumber']
            singer.write_bookmark(state, table_name, 'shard_seq_numbers', seq_number_bookmarks)

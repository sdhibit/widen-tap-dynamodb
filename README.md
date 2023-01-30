# tap-dynamodb

`tap-dynamodb` is a Singer tap for DynamoDB.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

Install by cloning this git repo. Or install through `meltano install extractor tap-dynamodb`
once your `meltano.yml` is properly configured.

## Configuration

### Accepted Config Options

The following Settings are required
- `region_name`: str: The name of the AWS region that the target DynamoDB table resides in.

The following Settings are optional
- `account_id`: str: The AWS account id (number) that the target DynamoDB table resides in.
- `external_id`: str: The external id required to assume the target role. See the [AWS docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)
  for instructions on how to set this up.
- `role_name`: str: the name of the IAM role in the target AWS account that will be used for 
  querying DynamoDB.
- `use_local_dynamo`: bool: Whether or not to use `https` protocol for accessing the DynamoDB table.
- `num_inference_records`: number: The number of records used to infer a DynamoDB table's schema
- `tables_to_discover`: list of strings: The DynamoDB tables that are to be used to create streams for.
  This is particularly useful if you have many DynamoDB tables within the given region because
  it will cut down the amount of time required to infer stream schemas.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-dynamodb --about
```

Two replication methods are available for this tap `FULL_TABLE` and `LOG_BASED`. 
At this time the `LOG_BASED` has not been tested, but it does require that a stream be
enabled for the desired DynamoDB table and that a `replication-key` be provided in the 
`metadata` settings.

### Source Authentication and Authorization

See the [AWS docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)
for instructions on how to set up IAM permissions to access DynamoDB.

## Usage

You can easily run `tap-dynamodb` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-dynamodb --version
tap-dynamodb --help
tap-dynamodb --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_dynamodb/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-dynamodb` CLI interface directly using `poetry run`:

```bash
poetry run tap-dynamodb --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

This project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-dynamodb
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-dynamodb --version
# OR run a test `elt` pipeline:
meltano elt tap-dynamodb target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.

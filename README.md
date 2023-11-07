# Milena

A CLI for consuming kafka messages.

### Features :sparkles:

- Deals with pesky magic bytes
- Will dynamically decode any protobuf message typ
- Outputs messages as newline delimited JSON with an envelope containing common metadata


### Usage

```sh
milena help [command]
```

#### Consume

```sh
❯ milena help consume
Usage: milena consume [OPTIONS] --topic <TOPIC> <--message-name <MESSAGE_NAME>|--message-name-from-header <MESSAGE_NAME_FROM_HEADER>>

Options:
  -t, --topic <TOPIC>
          Name of the topic to consume

  -N, --message-name <MESSAGE_NAME>
          The protobuf message name itself. Useful when there's only one schema per topic

  -F, --config <FILE>
          Sets a custom config file

  -H, --message-name-from-header <MESSAGE_NAME_FROM_HEADER>
          The message name header key that contains the message type as the value to enable dynamic decoding. Useful when there's more than one message type/schema per topic, but requires that the protobuf message name is present in the specified header

  -f, --file-descriptors <FILE_DESCRIPTORS>
          The path to the protobuf file descriptors

          [default: ./descriptors.binpb]

  -g, --group-id <GROUP_ID>
          The consumer group id to use, defaults to a v4 uuid

          [default: e8642122-b0c9-405b-9a28-2919365668dc]

  -o, --offset=<OFFSET>
          The offset for the topic

          [default: end]

          Possible values:
          - beginning: Start consuming from the beginning of the partition
          - end:       Start consuming from the end of the partition
          - stored:    Start consuming from the stored offset

  -X, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options

  -T, --trim-leading-bytes <TRIM_LEADING_BYTES>
          Trim a number of bytes from the start of the payload before attempting to deserialize

  -h, --help
          Print help (see a summary with '-h')
```

#### Produce

```sh
❯ milena help produce
Usage: milena produce [OPTIONS] --topic <TOPIC> --data <DATA> --key <KEY> --message-name <MESSAGE_NAME>

Options:
  -t, --topic <TOPIC>
          Name of the topic to produce to
  -d, --data <DATA>
          Data to send to the topic, curl style
  -F, --config <FILE>
          Sets a custom config file
  -H, --header <HEADERS>
          Message headers <header=value>. Can be supplied more than once
  -f, --file-descriptors <FILE_DESCRIPTORS>
          The path to the protobuf file descriptors [default: ./descriptors.binpb]
  -k, --key <KEY>
          Key for the message
  -m, --message-name <MESSAGE_NAME>
          The fully qualified name of the protobuf message
  -X, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options
  -h, --help
          Print help
```


#### Example:

Produce protobuf messages from JSON to a topic

```sh
milena produce \
  -X bootstrap.servers=localhost:9092 \
  --topic sometopic \
  --file-descriptors descriptors.binpb \
  --message-name example.v1.UserUpdated \
  --key 123 \
  --header 'message-name=example.v1.UserUpdated' \
  --data '{"name": "John Smith", "age": 40}'
```

Consume those messages as JSON

```sh
milena consume \
  -X bootstrap.servers=localhost:9092 \
  -t sometopic \
  -f descriptors.binpb \
  -H "message-name" \
  -o=beginning
```

## Installation

### Prerequisites

- A compiled protobuf file descriptor set

1. Clone this repo
2. `cargo install --path .`


### Troubleshooting

#### Extended logging

Set `RUST_LOG="info,librdkafka=trace,rdkafka::client=debug"`

#### CMake can't find openssl/libsasl2

Ensure that `openssl@1.1` installed with brew & set the following environment variables.

```
export OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1)
export OPENSSL_LIBRARIES="${OPENSSL_ROOT_DIR}/lib"
```

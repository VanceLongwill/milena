# Milena

A CLI for consuming protobuf messages over Kakfa.

Wrangling `kcat` and `protoc` can be difficult or impossible in some cases. Milena aims to provide an easy interface
for producing & consuming protobuf encoded messages over Kafka.

### Features :sparkles:

- Dynamically decodes any protobuf message type in the provided file descriptors
- Outputs messages as newline delimited JSON with an envelope containing common metadata
- Produces protobuf messages from JSON
- Deals with pesky magic bytes `-T`

### Screencast

![milena screencast](./milena-screencast.gif)

### Usage

```sh
Usage: milena [OPTIONS] <COMMAND>

Commands:
  consume  Consume & decode protobuf messages to JSON
  produce  Encode JSON messages as protobuf and produce them to a topic
  decode   Decode an arbitrary protobuf message
  encode   Encode an arbitrary protobuf message
  help     Print this message or the help of the given subcommand(s)

Options:
  -F, --config <FILE>
          Sets a custom config file
  -f, --file-descriptors <FILE_DESCRIPTORS>
          The path to the protobuf file descriptors [default: ./descriptors.binpb]
  -X, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options
  -h, --help
          Print help
  -V, --version
          Print version
```

#### Consume

```sh
❯ milena consume -h
Usage: milena consume [OPTIONS] --topic <TOPIC> <--message-name <MESSAGE_NAME>|--message-name-from-header <MESSAGE_NAME_FROM_HEADER>> [KEY_MESSAGE_NAME] [HEADER_MESSAGE_NAME]...

Arguments:
  [KEY_MESSAGE_NAME]        Decode the message key using a protobuf message
  [HEADER_MESSAGE_NAME]...  Decode a header value with the given protobuf message in the format `<header-name>=<message-name>`

Options:
  -t, --topic <TOPIC>
          Name of the topic to consume
  -N, --message-name <MESSAGE_NAME>
          The protobuf message name itself. Useful when there's only one schema per topic
  -H, --message-name-from-header <MESSAGE_NAME_FROM_HEADER>
          The message name header key that contains the message type as the value to enable dynamic decoding. Useful when there's more than one message type/schema per topic, but requires that the protobuf message name is present in the specified header
  -g, --group-id <GROUP_ID>
          The consumer group id to use, defaults to a v4 uuid [default: 4ead12ea-bc48-4e2e-b75a-e8c97f4aee6f]
  -F, --config <FILE>
          Sets a custom config file
  -o, --offset=<OFFSET>
          The offset for the topic [default: end] [possible values: beginning, end, stored]
  -f, --file-descriptors <FILE_DESCRIPTORS>
          The path to the protobuf file descriptors [default: ./descriptors.binpb]
  -T, --trim-leading-bytes <TRIM_LEADING_BYTES>
          Trim a number of bytes from the start of the payload before attempting to deserialize
  -X, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options
  -h, --help
          Print help (see more with '--help')
```

#### Produce

```sh
❯ milena produce -h
Usage: milena produce [OPTIONS] --topic <TOPIC> --data <DATA> --key <KEY> --message-name <MESSAGE_NAME>

Options:
  -t, --topic <TOPIC>
          Name of the topic to produce to
  -d, --data <DATA>
          Data to send to the topic, curl style
  -H, --header <HEADERS>
          Message headers <header=value>. Can be supplied more than once
  -k, --key <KEY>
          Key for the message
  -F, --config <FILE>
          Sets a custom config file
  -N, --message-name <MESSAGE_NAME>
          The fully qualified name of the protobuf message
  -f, --file-descriptors <FILE_DESCRIPTORS>
          The path to the protobuf file descriptors [default: ./descriptors.binpb]
  -X, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options
  -h, --help
          Print help
```


### Examples

#### Produce protobuf messages from JSON to a topic

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

#### Encode from JSON / decode from protobuf

See `milena encode -h` or `milena decode -h` for more options.

```sh
milena encode -N example.v1.UserUpdated '{"name": "alice", "age": 90}' |\
  milena decode -N example.v1.UserUpdated
{"name":"alice","age":90}%
```

## Installation

### Prerequisites

- [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- openssl@1.1
- A compiled [protobuf file descriptor set](#getting-file-descriptors)

1. Clone this repo
2. `cargo install --path .`


### Troubleshooting

#### Extended logging

Set `RUST_LOG="info,librdkafka=trace,rdkafka::client=debug"`

#### CMake can't find openssl/libsasl2

Ensure that `openssl@1.1` installed with brew & set the following environment variables.

```
export OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1)
export OPENSSL_LIBRARIES="${OPENSSL_ROOT_DIR}/lib"
```

## Getting file descriptors

Protobuf file descriptors can be generated using the [buf](https://buf.build/docs/build/explanation) CLI or with [protoc](protoc).

#### With buf:

```sh
buf build ./ -o descriptors.binpb
```

#### With protoc:

```sh
protoc example.proto -o protocdescriptors.binpb
```

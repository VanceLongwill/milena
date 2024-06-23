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

## Installation

### Prebuilt binaries

- Download the latest release archive for your platform from [the latest release](https://github.com/VanceLongwill/milena/releases/latest)
- Extract the archive
- If you're on MacOS, remove the binary from quarantine

### Compile from source with cargo

```sh
cargo install --git https://github.com/VanceLongwill/milena
```

### Docker image

Based on the [prebuilt binaries](#prebuild-binaries) on top of the Debian Bookworm slim image (linux/amd64).

```sh
docker pull vancelongwill/milena:latest
```

## Usage

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

#### Serve

```sh
Usage: milena serve [OPTIONS]

Options:
  -p, --port <PORT>
          Port to run the server on
  -F, --config <FILE>
          Sets a custom config file
  -f, --file-descriptors <FILE_DESCRIPTORS>
          The path to the protobuf file descriptors [default: ./descriptors.binpb]
  -X, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options
  -v, --verbose...
          Enable verbose logging, can be repeated for more verbosity up to 5 times
  -h, --help
          Print help
```

`milena` can also proxy kafka/protobuf to HTTP/json using the `serve`
command. Each request starts a new consumer stream.

This provides a number of advantages:
- Developers can read protobuf encoded kakfa messages without having to install another tool (just curl it)
- Consume only access to Kafka
- Credentials need only reside in a single server (this could live
  within your network for devs to access).
- Develops don't have to compile protobuf file descriptors. It's all
  just done once and can be automated via CI/CD.

```sh
milena serve -p 29999 \
	-X bootstrap.servers=localhost:9092 \
	-f descriptors.binpb
```

##### With curl

> The JSON request body can contain any option provided to the `consume`
> command. See `milena consume -h` for more details.

###### Stream all messages from the earliest available to the end of the topic.

```sh
curl localhost:29999 -H 'Content-type: application/json' -d '{
        "topic": "sometopic",
        "message_name_from_header": "message-name",
        "exit_on_last": true
}'
```

###### Stream from the end of the topic onward without exiting

(while also simultaneously piping to `jq` for pretty printing)

```sh
curl localhost:29999 -H 'Content-type: application/json' -d '{
        "topic": "sometopic",
        "message_name_from_header": "message-name",
        "offsets": ["end"]
}' -N -s | jq -rc
```

> -  `-N` prevents curl buffering the output so we can can pipe it to jq as
soon as we receive something.   
> - `-s` hides other output from curl 


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

### Troubleshooting

#### Extended logging

Set the verbose flag up to 5 times `-vvvvv` to get increasingly more
detailed logging.

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

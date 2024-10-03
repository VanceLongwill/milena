# Milena

A CLI for consuming protobuf messages over Kakfa.

### Motivation

Serializing Kafka messages to protobuf has many benefits, but
it can make debugging and peeking at messages can be difficult. Milena
aims to make it easy to produce and consume protobuf messages over Kafka
using JSON.

### Features :sparkles:

- Dynamically decodes any protobuf message type in the provided file descriptors
- Outputs messages as newline delimited JSON with an envelope containing common metadata
- Produces protobuf messages from JSON
- Deals with pesky magic bytes `-T`
- Server mode that proxies Kafka/protobuf messages to HTTP/JSON

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
  serve    Start a HTTP server that can proxy kafka/protobuf to http/json
  help     Print this message or the help of the given subcommand(s)

Options:
  -F, --config <FILE>
          Sets a custom config file
  -f, --file-descriptors <FILE_DESCRIPTORS>
          The path to the protobuf file descriptors [default: ./descriptors.binpb]
  -X, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options
  -b, --brokers <BROKERS>
          Bootstrap brokers, comma separated (i.e. equivalent to `-X bootstrap.servers=serv1:port1,serv2:port2`)
  -v, --verbose...
          Enable verbose logging, can be repeated for more verbosity up to 5 times
  -h, --help
          Print help
  -V, --version
          Print version

```

## Examples

Based on the [docker example](./examples/docker).

### Produce protobuf messages from JSON to a topic

```sh
milena produce \
  -b localhost:9092 \
  --topic sometopic \
  --file-descriptors descriptors.binpb \
  --message-name example.v1.UserUpdated \
  --key 123 \
  --header 'message-name=example.v1.UserUpdated' \
  --data '{"name": "John Smith", "age": 40}'
```

### Consume protobuf messages as line delimited JSON

```sh
milena consume \
  -b localhost:9092 \
  -t sometopic \
  -f descriptors.binpb \
  -H "message-name" \
  -o=beginning
```

### Serve

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
	-b localhost:9092 \
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


### Encode from JSON / decode from protobuf

See `milena encode -h` or `milena decode -h` for more options.

```sh
milena encode -N example.v1.UserUpdated '{"name": "alice", "age": 90}' |\
  milena decode -N example.v1.UserUpdated
{"name":"alice","age":90}%
```

## Troubleshooting

### Extended logging

Set the verbose flag up to 5 times `-vvvvv` to get increasingly more
detailed logging.

### CMake can't find openssl/libsasl2

Ensure that `openssl@1.1` installed with brew & set the following environment variables.

```
export OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1)
export OPENSSL_LIBRARIES="${OPENSSL_ROOT_DIR}/lib"
```

### Getting file descriptors

Protobuf file descriptors can be generated using the [buf](https://buf.build/docs/build/explanation) CLI or with [protoc](protoc).

#### With buf:

```sh
buf build ./ -o descriptors.binpb
```

#### With protoc:

```sh
protoc example.proto -o protocdescriptors.binpb
```

# Milena

A CLI for consuming kafka messages.

### Features :sparkles:

- Deals with pesky magic bytes
- Will dynamically decode any protobuf message typ
- Outputs messages as newline delimited JSON with an envelope containing common metadata


### Usage

```sh
❯ milena --help
Usage: milena [OPTIONS] --topic <TOPIC>

Options:
  -t, --topic <TOPIC>
          Name of the topic to consume

  -c, --config <FILE>
          Sets a custom config file

          [default: ~/.config/kafka.config]

  -d, --descriptors <DESCRIPTORS>
          The path to the protobuf file descriptors

          [default: ./descriptors.bin]

      --message-name-header <MESSAGE_NAME_HEADER>
          The message header key that contains the message type as the value to enable dynamic decoding

          [default: message-name]

  -g, --group-id <GROUP_ID>
          The consumer group id to use, defaults to a v4 uuid

          [default: 951c948e-21f7-41a3-9257-8cc05bd2149e]

  -o, --offset=<OFFSET>
          The offset for the topic

          [default: end]

          Possible values:
          - beginning: Start consuming from the beginning of the partition
          - end:       Start consuming from the end of the partition
          - stored:    Start consuming from the stored offset

  -r, --rdkafka-options <RDKAFKA_OPTIONS>
          A catchall for specifying additional librdkafka options in the format `<k1>=<v1>,<k2>=<v2>,...` (takes precedence over config file)

  -t, --trim-leading-bytes <TRIM_LEADING_BYTES>
          Trim a number of bytes from the start of the payload

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

#### Example:

```sh
milena --topic my-topic -o=beginning
```

#### Output 

> with `RUST_LOG="info,librdkafka=trace,rdkafka::client=debug"`

```sh
... // @TODO
```

## Installation

### Prerequisites

- An INI formatted kakfa config file as used by `kcat`. This will be used to connect to the kafka cluster. By default the CLI looks for the config file at `$HOME/.config/kafka.config` although you can specify a different location with the `--config` flag.
- A compiled protobuf file descriptor set // @TODO: add instructions

1. Clone this repo
2. `cargo install --path .`


### Troubleshooting

#### CMake can't find openssl/libsasl2

Ensure that `openssl@1.1` installed with brew & set the following environment variables.

```
export OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1)
export OPENSSL_LIBRARIES="${OPENSSL_ROOT_DIR}/lib"
```

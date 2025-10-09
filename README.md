# Honey Badger

[![Go Report Card](https://goreportcard.com/badge/github.com/honey-badger-io/honey-badger)](https://goreportcard.com/report/github.com/honey-badger-io/honey-badger)
[![Build honey-badger](https://github.com/honey-badger-io/honey-badger/actions/workflows/ci.yml/badge.svg)](https://github.com/honey-badger-io/honey-badger/actions/workflows/ci.yml)

Honey Badger is simple and fast key/value db server build on top of [BadgerDB](https://github.com/dgraph-io/badger). It uses [RESP3](https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md) as transport protocol so you can use any Redis client.

## Getting Started
### Build server
To build Honey Badger server you need [Go 1.25](https://go.dev/dl/) or above.

Windows users may need to install GNU Make. The best way is to use some package manager like [scoop](https://scoop.sh/#/apps?q=make)

To start, clone the repo

```sh
git clone git@github.com:honey-badger-io/honey-badger.git
```

Go to `honey-badger` directory and build server binaries
```sh
$ make build
```

This will produce server binary. Run it with default configuration
```sh
$ ./bin/hb
```

or on Windows
```
bin\hb.exe
```

### Docker

Run Docker image using
```sh
docker run --name honey-badger -p 18950:18950 -d meeron/honey-badger:latest
```

### RESP
Not all RESP commands has been implemented. Currently working commands:
* [CLIENT SETNAME](https://redis.io/docs/latest/commands/client-setname/)
* [CLIENT GETNAME](https://redis.io/docs/latest/commands/client-getname/)
* [SET](https://redis.io/docs/latest/commands/set/)
* [GET](https://redis.io/docs/latest/commands/get/)
* [DEL](https://redis.io/docs/latest/commands/del/)
* [HELLO](https://redis.io/docs/latest/commands/hello/)
* [PING](https://redis.io/docs/latest/commands/ping/)
* [SELECT](https://redis.io/docs/latest/commands/select/)

### Benchmark
You can run benchmark using `redis-benchmark` cli tool
```sh
$ redis-benchmark -t set,get -n 1000000 -q -p 18950
WARNING: Could not fetch server CONFIG
SET: 257997.94 requests per second, p50=0.119 msec
GET: 265041.09 requests per second, p50=0.103 msec
```

## Hardware requirements
Honey Badger server should run on anything. CPU and RAM depends on your needs, but absolute minium is SSD disk (if persistance storage will be in use). Use benchmark command to check how Honey Badger is working on your instance.

## System requirements
### Linux and Mac
Honey Badger should build and run on any Linux distro. [BadgerDB recommends](https://dgraph.io/docs/badger/faq/#are-there-any-linux-specific-settings-that-i-should-use) `max file descriptors` set to a high number depending upon the expected size of your data.

### Windows
It should build and run just fine.

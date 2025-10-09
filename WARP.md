# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## About Honey Badger

Honey Badger is a simple and fast key/value database server built on top of [BadgerDB](https://github.com/dgraph-io/badger). It uses RESP (REdis Serialization Protocol) over TCP as its transport protocol.

## Common Commands

### Building and Running

```bash
# Build the server (creates ./bin/hb)
make build

# Build with version
make build ver=1.0.0

# Run in development mode (no build)
make dev

# Build and run with config
make run

# Run manually with default configuration
./bin/hb

# Run with custom configuration
./bin/hb -config config.json

# Check version
./bin/hb -version
```

### Testing

```bash
# Run all tests with race detection
make test

# Run tests for specific package
go test ./db -v -race

# Run single test
go test ./server -run TestSpecificTest -v
```

### Benchmarking

```bash
# Build and run benchmarks against local server
make bench

# Or specify target manually
./bin/hb -bench 127.0.0.1:18950
```

### Protocol Buffers (Future)

```bash
# Generate protobuf code (when .proto files exist)
make proto
```

### Docker

```bash
# Build Docker image with version tag
make docker ver=1.0.0

# Run Docker container
docker run --name honey-badger -p 18950:18950 -d meeron/honey-badger:latest
```

## Architecture Overview

### Core Components

The codebase is structured into several key packages:

**`main.go`** - Entry point that:
- Parses flags (`-config`, `-bench`, `-version`)
- Initializes configuration, logger, and database context
- Starts the TCP server

**`config/`** - Configuration management
- Loads settings from JSON file or uses defaults
- Manages BadgerDB, Server, and Logger configurations
- Default port: 18950, data directory: `./data`

**`db/`** - Database abstraction layer
- `DbContext`: Manages multiple named databases
- `Database`: Wraps BadgerDB operations (Get, Set, Delete, Stats)
- `Writer`: Batch write operations
- Automatic garbage collection at configurable intervals (default: 60 min)
- Supports both persistent (on-disk) and in-memory databases

**`server/`** - TCP server and request handlers
- Main `Server`: Listens on TCP, handles connections, parses RESP protocol
- `DataServer`: Key/value operations (Set, Get, Delete, DeleteByPrefix)
- `DbServer`: Database management (Create, Drop, Exists, EnsureDb)
- `SysServer`: System commands (Ping)
- Each connection handled in a separate goroutine

**`resp/`** - RESP protocol implementation
- `ParseCmd()`: Parses RESP-formatted commands from TCP stream
- `commands/`: Command implementations (Ping, Hello)
- `common/`: Error handling and result types

**`logger/`** - Logging infrastructure
- Supports multiple sinks: console, file
- Named loggers for different components (server, badger, dbcontext)
- Configured via JSON config

**`bench/`** - Benchmarking utilities
- Tests Set/Get operations with concurrent goroutines
- Tests batch operations with streams
- Currently commented out (migration in progress)

### Key Design Patterns

1. **Database Isolation**: Each database is a separate BadgerDB instance in its own directory under `./data/`

2. **Default Database**: `db0` is the default database used by `DataServer` operations

3. **Connection Handling**: Each TCP connection spawns a goroutine that continuously reads and processes RESP commands

4. **Configuration Defaults**: All config sections have sensible defaults that are applied if not specified in config file

5. **Graceful Shutdown**: Server responds to SIGTERM/SIGINT signals and cleanly closes databases

6. **BadgerDB Integration**: 
   - Uses BadgerDB v4
   - Automatic value log garbage collection
   - Configurable GC period
   - Supports TTL on keys

### RESP Protocol Flow

1. Client connects via TCP to port 18950
2. Client sends RESP-formatted commands (e.g., `*1\r\n$4\r\nPING\r\n`)
3. Server parses command and arguments
4. Command is dispatched to appropriate handler
5. Result sent back in RESP format

### Testing Approach

- Use `testify` for assertions
- Race detection enabled by default in tests
- Tests in `*_test.go` files alongside implementation

## Development Notes

### BadgerDB Recommendations

From BadgerDB documentation:
- Set `max file descriptors` to high number on Linux/Mac (depends on data size)
- For production: Use SSD storage for persistent databases
- Set `GOMAXPROCS=128` for best performance (already done in `server.go`)

### Current State

- RESP protocol implementation is in progress (only PING and HELLO commands fully implemented)
- Benchmark code is commented out (appears to be migrating from gRPC to RESP protocol)
- Protocol buffer generation is available but not currently used

### Configuration

The `config.json` structure:

```json
{
  "Badger": {
    "DataDirPath": "./data",
    "GCPeriodMin": 60
  },
  "Server": {
    "Port": 18950
  },
  "Logger": {
    "Sinks": {
      "console": true,
      "file": {"dir": "logs"}
    }
  }
}
```

### Adding New RESP Commands

1. Create command file in `resp/commands/` (e.g., `cmd_set.go`)
2. Implement `RespCmd` interface with `Invoke()` method
3. Register command in `commands.NewCmd()` function
4. Return `common.RespResult` from command execution

## Official Clients

- .NET: [HoneyBadger.Client](https://www.nuget.org/packages/HoneyBadger.Client)
- Go: [go-client](https://pkg.go.dev/github.com/honey-badger-io/go-client)

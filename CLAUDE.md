# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Honey Badger is a key/value database server built on BadgerDB v4. It speaks RESP3 over TCP (default port 18950), so any Redis client can talk to it. Requires Go 1.27 (the code uses newer stdlib APIs such as `errors.AsType`).

## Commands

```sh
make build            # builds ./bin/hb; version is injected via `make build ver=1.2.3` (-X main.version)
make dev              # go run .
make run              # build + run ./bin/hb
make test             # go test ./... -v -race  (what CI runs, along with make build)
make fuzz             # fuzzes the RESP parser and commands; length set via `make fuzz fuzztime=5m`
make docker ver=1.2.3 # builds meeron/honey-badger:<ver> and :latest

go test ./server -run TestServer -v -race              # single test
go test ./server -run 'TestServer/should_call_get' -v  # single subtest
./bin/hb -version

redis-benchmark -t set,get -n 1000000 -q -p 18950      # benchmark a running server
```

`server/server_test.go` starts real servers on ports 18950 (`TestServer`) and 18951 (`TestStop`) and talks to them with `go-redis`, so tests fail if another `hb` instance is already listening on either port.

## Configuration

Configuration comes **only from environment variables** (`config/config.go`); there is no config file or `-config` flag:

- `HB_PORT`: listen port. Values ≤ 1023 or unset fall back to 18950.
- `HB_DATA_DIR`: data directory (default `data`, created on startup).
- `HB_DB_IN_MEM`: databases are **in-memory by default**. Only the literal value `false` enables on-disk persistence.

`MaxDbs` (16) and `GCPeriodMin` (60) are hard-coded defaults. Tests call `config.SetDefault()` instead of `config.Init()`.

## Architecture

Request flow: `main.go` → `server.Server` accepts TCP connections → each connection gets a `resp.Session` running in its own goroutine → `commands.Parse` reads one RESP array from the stream and builds a `RespCmd` → `cmd.Invoke(session)` returns a `common.RespResult`, which is written to the session's `bufio.Writer`. The writer is flushed only once the reader has no buffered input left, so replies to pipelined commands go out in a single write. Don't write to `conn` directly.

On SIGINT/SIGTERM, `Server.Stop` closes the listener and sets a read deadline on every session's connection. Sessions finish the commands they have already received and then exit. `Start` returns only after every session has ended, so the deferred `CloseAllDbs` in `main` never closes a database that is still in use. A second signal kills the process immediately.

- **`db/`**: package-level registry (`dbs` map, guarded by a mutex) of named BadgerDB instances. `OpenDb(name, inMemory)` is idempotent and returns the cached instance. On-disk databases live at `<DataDirPath>/<name>`. `StartGCRoutine` runs value-log GC on a ticker for non-in-memory databases only. `CloseAllDbs` is deferred in `main`.
- **`resp/session.go`**: per-connection state (id, current DB, client name). New connections always start on `db0`, and `SELECT n` switches the session to database `db<n>` (0 ≤ n < MaxDbs, opened lazily).
- **`resp/common/`**: holds the `Session` interface that commands depend on (it exists to avoid an import cycle with `resp`), the RESP serializers (`ResultOk`, `ResultNull`, `NewResultString`/`BulkString`/`Integer`/`Map`) and `RespError`.
- **`resp/commands/`**: one `cmd_<name>.go` file per command, each with a `const cmd<Name> = "<NAME>"` and a struct `{numOfArgs int; args []string}` implementing `Invoke`.
- **`logger/`**: named loggers (`logger.Server()`, `logger.Badger()`) that write to sinks set up in `logger.Init()`. A logger copies the sink list the first time `Get` is called, so loggers obtained before `Init` never log anything.

### Adding a RESP command

1. Add `resp/commands/cmd_<name>.go` following the existing pattern.
2. Register it in the `if` chain in `newCmd` in `resp/commands/commands.go`. Command names are upper-cased before dispatch.
3. Add a subtest to `TestServer` in `server/server_test.go` that uses the `go-redis` client.
4. Add the command to the supported-commands list in `README.md`.

### Error handling convention

- Return a `common.RespError` (`NewRespError`/`NewRespErrorf`) from `Invoke` or `Parse` to send that message to the client as `-ERR ...`. The session keeps going.
- `Parse` returns a `common.ProtocolError` (`NewProtocolErrorf`) for malformed input. The client gets `-ERR Protocol error: ...` and the session ends, because the stream can't be resynced.
- Any other error from `Invoke` is logged, and the client receives a generic `-ERR server error`. Any other error from `Parse` is an I/O error and ends the session.
- `Handle` recovers panics, so a buggy command only kills its own session.

### Parser

`commands.Parse` only accepts RESP arrays of bulk strings (no inline commands). It reads exactly `$<len>` bytes for each bulk string, so keys and values may hold any binary data, including `\r\n`, and they are never trimmed. The limits match Redis' defaults: at most `maxArrayLen` (1M) elements and `maxBulkLen` (512MB) per bulk string. Declared lengths never preallocate more than `maxPrealloc` before the data arrives. `FuzzParse` (no panics in `Parse` + `Invoke`) and `FuzzReadArrayRoundTrip` (encoded args parse back unchanged) in `resp/commands/commands_test.go` cover it; run `make fuzz` after you change parsing or add a command.

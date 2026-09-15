# faas-supervisor (Go implementation)

Go reimplementation of [faas-supervisor](https://github.com/grycap/faas-supervisor) from the
OSCAR project, using the Python code in `faassupervisor/` and `SPEC.md` as the faithful reference.

The binary runs **inside the function container** in OSCAR: it reads the event from **stdin**,
downloads the inputs from the corresponding storage provider, executes the user script, and
returns the **response on stdout** (or uploads the outputs to storage).

> Development priority: **MinIO/S3** (binary/OSCAR mode). Lambda mode is minimal; the Python
> package is used when that platform is detected.

## How it works

### Main flow (`cmd/faassupervisor/main.go` → `internal/supervisor`)

```
input (stdin) ──► ParseEvent (event type)
                    ├─ APIGateway │ dCache │ Rucio │ delegated │ storage │ Unknown
                    ▼
            GenericSupervisor.Run()
                    ├─ parseInput():   downloads input from storage → $INPUT_FILE_PATH
                    ├─ ExecuteFunction():  runs /oscar/config/script.sh (or $SCRIPT)
                    ├─ parseOutput(): uploads $TMP_OUTPUT_DIR to storage
                    └─ CreateResponse():   stdout → HTTP response (MINIO) / base64 (UNKNOWN)
```

- **Config**: read from `/oscar/config/function_config.yaml` or from the `FUNCTION_CONFIG`
  environment variable (base64 YAML).
- **Events**: detection order `APIGateway → dCache → Rucio → delegated → storage → Unknown`.
  The raw event is saved (JSON or base64) and the type decides how the response is built.
- **Storage**: providers `S3`/`MinIO` (AWS SDK v2), `Local`, `HTTP`, `Rucio/WebDAV`.
  MinIO `default` obtains credentials from
  `/var/run/secrets/providers/minio.default/{accessKey,secretKey}`; if the directory does not
  exist it is skipped (log `No MinIO user configuration found`).
- **Script execution** (`internal/supervisor/supervisor.go`, `ExecuteFunction`):
  - Uses `/bin/sh`.
  - Script **stdout** and **stderr** are captured **separately** (`io.MultiWriter`) while a
    `merged` buffer preserves the real ordering → the synchronous response is identical to the
    Python reference (`stderr=subprocess.STDOUT`).
  - **stdout** lines are emitted through `logger.Stdout()` (plain message, no prefix).
  - **stderr** lines are emitted through `logger.Stderr()` (prefix `... - supervisor - ERROR - ...`).
  - Both are *always visible*, independent of `log_level`.

### Logging (`internal/logger`)

- Levels: `DEBUG < INFO < WARNING < ERROR < CRITICAL` (same as Python's `logging`).
- `log_level` is read from the config (`ReadCfgString("log_level")`); unknown/empty values ⇒ `INFO`
  (never `DEBUG`).
- **`CRITICAL` filters out all internal noise** (INFO/DEBUG/WARNING/ERROR): in the pod only the
  clean function output remains (script stdout, `Stdout` type) plus the script errors (script
  stderr, `ERROR` type). To see "only the cow" in OSCAR, use `log_level: CRITICAL`.
- Function-output methods: `Stdout()` (plain message), `Stderr()` (`ERROR` type),
  `Output()` (`OUTPUT` type, full prefix) — the first two are not filtered by level.

### Error semantics

- A script that exits with code ≠ 0 propagates the exit code to the process (`os.Exit`):
  OSCAR/Kubernetes handles it as a job error. Internal failures (`parseInput`/`parseOutput`)
  exit with code 1; *warnings* do not abort.

## Requirements

- Go 1.24+ (the module declares `go 1.24.0`).
- Dependencies: `aws-sdk-go-v2` (S3/STS) and `gopkg.in/yaml.v3`. They are **pure Go**, so with
  `CGO_ENABLED=0` the binaries are **statically linked** and work on both glibc and musl without
  `-alpine` variants.

## Building

### Local (current host)

```bash
go build -o build/faassupervisor ./cmd/faassupervisor
# static (no libc dependencies), recommended for any container:
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o build/faassupervisor ./cmd/faassupervisor
```

### Verify

```bash
go build ./... && go vet ./... && go test ./...
file build/faassupervisor        # should say "statically linked" if you used CGO_ENABLED=0
```

### Cross-compile for all architectures

`./build.sh` builds **25 targets** into `dist/binaries/<name>` (and its `.zip`): `CGO_ENABLED=0`,
`-trimpath`, `-ldflags="-s -w"`, `GOMIPS/GOMIPS64=softfloat`:

| Platform | Architectures | Output names |
|---|---|---|
| Linux | amd64, arm64, 386, arm(v7), arm(v6), riscv64, ppc64le, ppc64, s390x, loong64, mips64le, mips64, mipsle, mips | `supervisor`, `supervisor-arm64`, `supervisor-386`, `supervisor-arm`, `supervisor-arm6`, `supervisor-riscv64`, `supervisor-ppc64le`, `supervisor-ppc64`, `supervisor-s390x`, `supervisor-loong64`, `supervisor-mips64le`, `supervisor-mips64`, `supervisor-mipsle`, `supervisor-mips` |
| macOS | amd64 (Intel), arm64 (Apple Silicon) | `supervisor-darwin-amd64`, `supervisor-darwin-arm64` |
| Windows | amd64, arm64, 386 | `supervisor-windows-amd64.exe`, `supervisor-windows-arm64.exe`, `supervisor-windows-386.exe` |
| FreeBSD | amd64, arm64, 386, arm, riscv64 | `supervisor-freebsd-{amd64,arm64,386,arm,riscv64}` |

#### Single target

```bash
# Linux ARM64 (e.g. aarch64 OSCAR nodes)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" \
  -o dist/binaries/supervisor-arm64 ./cmd/faassupervisor

# macOS Intel
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags="-s -w" \
  -o dist/binaries/supervisor-darwin-amd64 ./cmd/faassupervisor

# Windows
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -ldflags="-s -w" \
  -o dist/binaries/supervisor-windows-amd64.exe ./cmd/faassupervisor
```

> **Watch out for architectures**: always use the **Linux** binary for OSCAR pods
> (e.g. `supervisor-arm64` = ELF aarch64). `supervisor-darwin-arm64` is a **macOS Mach-O**
> executable and raises `Exec format error` on a Linux node, even though both are called "arm64".

## Deployment in OSCAR

The supervisor is mounted into the pod from the **supervisor PV** (typically
`/pv/pvc-<id>/supervisor`). To update it:

```bash
scp dist/binaries/supervisor-arm64 my_cluster:/pv/pvc-<id>/supervisor
ssh my_cluster 'file /pv/pvc-<id>/supervisor'   # verify "ELF ... ARM aarch64, statically linked"
```

The function config lives in `/oscar/config/function_config.yaml` (or `FUNCTION_CONFIG` in base64).
The user script can be `/oscar/config/script.sh` or defined with the `SCRIPT` environment variable
(base64, written to `$TMP_INPUT_DIR/script.sh`).

### Quick local test

```bash
# event + script via environment variables (implicit Local provider)
EVENT='{"Records":[{"eventTime":"...","EventName":"s3:ObjectCreated:*","requestParameters":{"sourceIPAddress":"..."},"s3":{"bucket":{"name":"mybucket"},"object":{"key":"input.txt"}},"responseElements":{"x-amz-request-id":"...","x-amz-id-2":"..."}}]}'
SCRIPT=$(printf 'cat "$INPUT_FILE_PATH"' | base64 -w0)
printf '%s' "$EVENT" | SCRIPT="$SCRIPT" ./build/faassupervisor
```

## Repository layout

```
cmd/faassupervisor/        entrypoint (read stdin → run → print response)
internal/
  config/                  config parsing (yaml / custom env vars)
  errors/                  faas-supervisor error types (warning, fatal error)
  events/                  event parsing and type detection
  logger/                  leveled logging (DEBUG..CRITICAL) and function output
  storage/                 storage providers (S3/MinIO, Local, HTTP, Rucio/WebDAV)
  supervisor/              supervisor logic (Generic/Binary/Lambda)
  utils/                   OS/file helpers
build.sh                   multi-architecture cross-compilation → dist/binaries/
dist/binaries/             generated binaries (+ .zip)
```

References: `SPEC.md`, the Python code in `../faassupervisor/`, and session/working notes in
`SESSION.md`.
# Prometheus Remote Storage Adapter for S3

A Go application that implements the Prometheus remote storage API using Amazon S3 as the backend storage.

## Features

- **Remote Write**: Store Prometheus metrics in S3
- **Remote Read**: Query metrics from S3
- **Time-based organization**: Metrics are organized by name and timestamp for efficient querying
- **Snappy compression**: Uses Snappy compression for protocol buffer messages
- **Retention cleanup**: Hourly scan deletes objects older than configured retention period
- **Per-metric manifest indexing**: Maintains `manifest.ndjson` with min/max timestamps to narrow read scans
- **Concurrent writes with locking**: Per-metric in-process mutex prevents manifest race conditions

## Prerequisites

- Go 1.25.4 or later
- AWS credentials configured (via environment variables, AWS credentials file, or IAM role)
- An S3 bucket

## Installation

```bash
go mod download
go build -o prom-store-s3
```

## Usage

### Running the server

```bash
# Using command-line flags
./prom-store-s3 -s3-bucket=my-prometheus-bucket -s3-region=us-east-1 -listen-address=:9201 -retention-days=7

# Using environment variables
export S3_BUCKET=my-prometheus-bucket
export AWS_REGION=us-east-1
export RETENTION_DAYS=7
./prom-store-s3
```

### Configuration Options

- `-listen-address`: HTTP listen address (default: `:9201`)
- `-s3-bucket`: S3 bucket name (required, can also use `S3_BUCKET` env var)
- `-s3-region`: AWS region (default: `us-east-1`, can also use `AWS_REGION` env var)
- `-retention-days`: Data retention period in days (default: `7`, can also use `RETENTION_DAYS` env var)
- `-log-level`: Structured log level (`debug`, `info`, `warn`, `error`). Defaults to `info`. Can also use `LOG_LEVEL` env var.

### Logging

The application uses Go's standard library `slog` for structured JSON logging to stdout.

Set log level via flag or environment variable:

```bash
./prom-store-s3 -log-level=debug ...

export LOG_LEVEL=warn
./prom-store-s3
```

Example log entry:
```json
{"time":"2025-11-18T12:00:00Z","level":"INFO","msg":"starting prometheus remote storage adapter","listen_address":":9201","s3_bucket":"my-prometheus-bucket","s3_region":"us-east-1","retention_days":7,"log_level":"INFO"}
```

All operations (writes, reads, retention cleanup, errors) emit structured fields for easier ingestion by log processors.

### AWS Credentials

The application uses the AWS SDK's default credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (if running on EC2, ECS, or Lambda)

## Prometheus Configuration

Add the following to your `prometheus.yml`:

```yaml
remote_write:
  - url: "http://localhost:9201/write"

remote_read:
  - url: "http://localhost:9201/read"
```

## Storage Structure

Metrics are stored in S3 as Parquet files with the following key structure (note second-level granularity plus original first sample timestamp suffix):
```
metrics/{metric_name}/{YYYY}/{MM}/{DD}/{HH}/{mm}/{ss}_{firstSampleTsMs}.parquet
```

Example:
```
metrics/http_requests_total/2025/11/18/13/45/07_1700315100000.parquet
```

Each Parquet file contains an Arrow table with columns:
- `timestamp`: int64 (Unix timestamp in milliseconds)
- `value`: float64 (metric value)
- `labels`: string (JSON-encoded map of label names to values)

### Manifest Index

For each metric a `manifest.ndjson` file is maintained at:
```
metrics/{metric_name}/manifest.ndjson
```
Each line is a JSON object:
```json
{"key":"metrics/http_requests_total/2025/11/18/13/45/07_1700315100000.parquet","min_ts":1700315100000,"max_ts":1700315160000,"samples":120}
```
Reads first attempt to filter candidate parquet objects by intersecting requested time range with `min_ts` / `max_ts`. If manifest is missing or partially unreadable, the code falls back to listing all objects under the metric prefix.

## Retention & Lifecycle Management

Two modes are available:

1. Internal deletion (default): An hourly goroutine lists objects and deletes those older than the configured `-retention-days` using LastModified timestamps.
2. S3 Lifecycle mode (`-use-s3-lifecycle`): Internal deletion is disabled. Each written object is tagged with:
   ```
   prom-retention-expiry=YYYY-MM-DD
   ```
   The date is computed as (max sample timestamp + retention period) in UTC. Configure an S3 Lifecycle rule to expire objects matching this tag on or after that date.

### Example Lifecycle Rule (JSON snippet)
```json
{
  "Rules": [
    {
      "ID": "PromMetricsExpiry",
      "Status": "Enabled",
      "Filter": {"Tag": {"Key": "prom-retention-expiry", "Value": "*"}},
      "Expiration": {"ExpiredObjectDeleteMarker": false},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
    }
  ]
}
```
Note: AWS Lifecycle does not support wildcard tag values directly in JSON; you typically scope by tag key regardless of value. You can alternatively filter by prefix and use the tag for auditing.

When running multiple replicas, prefer lifecycle mode to avoid race conditions or redundant delete operations.

## API Endpoints

- `POST /write`: Prometheus remote write endpoint
- `POST /read`: Prometheus remote read endpoint
- `GET /health`: Health check endpoint

## Development

### Build
```bash
go build
```

### Run
```bash
go run .
```

## Limitations & Next Steps

This implementation is suitable for experimentation and small-scale use. For production consider:

- Distributed manifest consistency: Current locking prevents races only within a single process. Multiple replica deployments could still race on manifest updates (use S3 object versioning, DynamoDB, or atomic append pattern).
- Bounding parallel write goroutines (add a worker pool / semaphore)
- Exposing Prometheus metrics (write/read latency, S3 ops, retention deletions)
- Adding authentication / authorization (e.g. mTLS or reverse proxy integration)
- Improved regex matcher support (full RE / NRE semantics)
- Configurable Parquet row group / compression tuning
- Pluggable storage class selection (STANDARD vs GLACIER tiers)
- Retention currently uses S3 object LastModified; consider tagging + lifecycle rules for offloading work to S3
- Cold storage tier transitions / lifecycle policies for large historical volumes

## License

MIT

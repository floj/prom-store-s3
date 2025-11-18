# Prometheus Remote Storage Adapter for S3

A Go application that implements the Prometheus remote storage API using Amazon S3 as the backend storage.

## Features

- **Remote Write**: Store Prometheus metrics in S3
- **Remote Read**: Query metrics from S3
- **Time-based organization**: Metrics are organized by name and timestamp for efficient querying
- **Snappy compression**: Uses Snappy compression for protocol buffer messages

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

Metrics are stored in S3 as Parquet files with the following key structure:
```
metrics/{metric_name}/{year}/{month}/{day}/{hour}/{minute}/{timestamp}.parquet
```

Example:
```
metrics/http_requests_total/2025/11/18/13/45/1700315100000.parquet
```

Each Parquet file contains an Arrow table with columns:
- `timestamp`: int64 (Unix timestamp in milliseconds)
- `value`: float64 (metric value)
- `labels`: string (JSON-encoded map of label names to values)

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

- Indexing objects (e.g. manifest per metric) for faster remote read scans
- Bounding parallel write goroutines (add a worker pool / semaphore)
- Exposing Prometheus metrics (write/read latency, S3 ops, retention deletions)
- Adding authentication / authorization (e.g. mTLS or reverse proxy integration)
- Improved regex matcher support (full RE / NRE semantics)
- Configurable Parquet row group / compression tuning
- Pluggable storage class selection (STANDARD vs GLACIER tiers)
- Retention currently uses S3 object LastModified; consider tagging + lifecycle rules for offloading work to S3

## License

MIT

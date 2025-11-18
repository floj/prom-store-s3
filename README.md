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
./prom-store-s3 -s3-bucket=my-prometheus-bucket -s3-region=us-east-1 -listen-address=:9201

# Using environment variables
export S3_BUCKET=my-prometheus-bucket
export AWS_REGION=us-east-1
./prom-store-s3
```

### Configuration Options

- `-listen-address`: HTTP listen address (default: `:9201`)
- `-s3-bucket`: S3 bucket name (required, can also use `S3_BUCKET` env var)
- `-s3-region`: AWS region (default: `us-east-1`, can also use `AWS_REGION` env var)

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

Metrics are stored in S3 with the following key structure:
```
metrics/{metric_name}/{year}/{month}/{day}/{hour}/{minute}/{timestamp}.json
```

Example:
```
metrics/http_requests_total/2025/11/18/13/45/1700315100000.json
```

Each JSON file contains:
```json
{
  "labels": {
    "__name__": "http_requests_total",
    "method": "GET",
    "status": "200"
  },
  "timestamp": 1700315100000,
  "value": 42.0
}
```

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

## Limitations

This is a basic implementation suitable for learning and small-scale deployments. For production use, consider:

- Adding indexing for faster queries
- Implementing batch writes
- Adding caching
- Implementing retention policies
- Adding authentication/authorization
- Using compression for stored data
- Implementing more sophisticated query optimization

## License

MIT

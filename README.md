# Prometheus Remote Storage Adapter for S3 🚀

---

> ### ✨ **This project is vibe-coded!**
> 
> Expect creative solutions, experimental features, and a focus on vibes over rigorous production standards. Built with AI assistance and good energy! 🎨
>
> ### ⚠️ **Untested & Experimental**
>
> This project is **not tested** and is **not ready for production use**. Use at your own risk for experimentation and learning purposes only.

---

A Go application that implements the Prometheus remote storage API using Amazon S3 as the backend storage.

## Features ⭐

- **📤 Remote Write**: Store Prometheus metrics in S3
- **📥 Remote Read**: Query metrics from S3
- **🕒 Time-based organization**: Metrics are organized by name and timestamp for efficient querying
- **🗜️ Snappy compression**: Uses Snappy compression for protocol buffer messages
- **📄 Manifest indexing**: Maintains `manifest.ndjson` with min/max timestamps to narrow read scans
- **🔐 Concurrent writes with locking**: Per-metric in-process mutex prevents manifest race conditions

## Prerequisites ⚙️

- Go 1.25.4 or later
- AWS credentials configured (via environment variables, AWS credentials file, or IAM role)
- An S3 bucket

## Installation 🛠️

```bash
go mod download
go build -o prom-store-s3
```

## Usage ▶️

### Running the server 🏃

```bash
# Using command-line flags
./prom-store-s3 -s3-bucket=my-prometheus-bucket -s3-region=us-east-1 -listen-address=:9201

# Using environment variables
export S3_BUCKET=my-prometheus-bucket
export AWS_REGION=us-east-1
./prom-store-s3
```

### Configuration Options ⚙️

- `-listen-address`: HTTP listen address (default: `:9201`)
- `-s3-bucket`: S3 bucket name (required, can also use `S3_BUCKET` env var)
- `-s3-region`: AWS region (default: `us-east-1`, can also use `AWS_REGION` env var)
- `-log-level`: Structured log level (`debug`, `info`, `warn`, `error`). Defaults to `info`. Can also use `LOG_LEVEL` env var.

### Logging 🧾

The application uses Go's standard library `slog` for structured JSON logging to stdout.

Set log level via flag or environment variable:

```bash
./prom-store-s3 -log-level=debug ...

export LOG_LEVEL=warn
./prom-store-s3
```

Example log entry (fields may vary):
```json
{"time":"2025-11-18T12:00:00Z","level":"INFO","msg":"starting prometheus remote storage adapter","listen_address":":9201","s3_bucket":"my-prometheus-bucket","s3_region":"us-east-1","log_level":"INFO"}
```

All operations (writes, reads, retention cleanup, errors) emit structured fields for easier ingestion by log processors.

### AWS Credentials

The application uses the AWS SDK's default credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (if running on EC2, ECS, or Lambda)

## Prometheus Configuration 📡

Add the following to your `prometheus.yml`:

```yaml
remote_write:
  - url: "http://localhost:9201/write"

remote_read:
  - url: "http://localhost:9201/read"
```

## Storage Structure 🗂️

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

### Manifest Index 🗃️

For each metric a `manifest.ndjson` file is maintained at:
```
metrics/{metric_name}/manifest.ndjson
```
Each line is a JSON object:
```json
{"key":"metrics/http_requests_total/2025/11/18/13/45/07_1700315100000.parquet","min_ts":1700315100000,"max_ts":1700315160000,"samples":120}
```
Reads first attempt to filter candidate parquet objects by intersecting requested time range with `min_ts` / `max_ts`. If manifest is missing or partially unreadable, the code falls back to listing all objects under the metric prefix.

## Retention & Lifecycle Management ♻️

Retention deletion is not performed by this application. Configure S3 Lifecycle rules externally if you need automatic expiry.

## API Endpoints 🔌

- `POST /write`: Prometheus remote write endpoint
- `POST /read`: Prometheus remote read endpoint
- `GET /health`: Health check endpoint

## Development 👨‍💻

### Build
```bash
go build
```

### Run
```bash
go run .
```

## Comparison with Other Solutions 🔍

### vs. Thanos

**[Thanos](https://github.com/thanos-io/thanos)** is a production-grade CNCF project designed for highly available Prometheus with long-term storage. Key differences:

| Feature | This Project | Thanos |
|---------|-------------|---------|
| **Maturity** | 🧪 Experimental, vibe-coded | ✅ Production-ready, CNCF Incubating |
| **Architecture** | Simple remote write/read adapter | Multi-component system (Sidecar, Store, Query, Compactor, etc.) |
| **Storage** | Direct S3 parquet files | Prometheus TSDB blocks + object storage |
| **HA Setup** | Single process (no HA) | Full HA with query deduplication |
| **Global View** | Single Prometheus instance | Unified view across multiple Prometheus servers |
| **Downsampling** | None | Automatic downsampling for historical data |
| **Query Performance** | Basic manifest-based filtering | Optimized with Store Gateway, caching, sharding |
| **Operational Complexity** | Very low (single binary) | Higher (multiple components to deploy) |
| **Use Case** | Learning, experimentation | Large-scale production deployments |

### vs. Cortex

**[Cortex](https://github.com/cortexproject/cortex)** is a horizontally scalable, multi-tenant, long-term Prometheus storage system. Key differences:

| Feature | This Project | Cortex |
|---------|-------------|--------|
| **Maturity** | 🧪 Experimental, vibe-coded | ✅ Production-ready, CNCF project |
| **Multi-tenancy** | None | Built-in with tenant isolation |
| **Scalability** | Single process | Horizontally scalable (distributors, ingesters, queriers) |
| **Storage** | Direct S3 parquet | Blocks storage (S3, GCS, Azure, Swift) |
| **Write Path** | Simple HTTP endpoint | Distributed ingestion with replication |
| **Query Path** | Single process | Distributed query execution with caching |
| **High Availability** | None | Built-in replication and redundancy |
| **Data Format** | Parquet | Prometheus TSDB format |
| **Operational Complexity** | Very low | High (many microservices) |
| **Managed Service** | None | AWS Managed Prometheus (AMP) available |
| **Use Case** | Toy projects, learning | Enterprise multi-tenant environments |

### When to Use This Project

Choose **prom-store-s3** if you want to:
- 🎓 Learn about Prometheus remote storage protocols
- 🧪 Experiment with Parquet and S3-based metric storage
- 🚀 Build a simple proof-of-concept quickly
- 💡 Understand the basics before diving into production systems

Choose **Thanos** or **Cortex** if you need:
- 📈 Production-grade reliability and support
- 🏢 Multi-tenant or multi-cluster environments
- ⚡ High availability and horizontal scaling
- 🔧 Enterprise features and operational tooling

## Limitations & Next Steps 🚧

This implementation is suitable for experimentation and small-scale use. For production consider:

- Distributed manifest consistency: Current locking prevents races only within a single process. Multiple replica deployments could still race on manifest updates (use S3 object versioning, DynamoDB, or atomic append pattern).
- Bounding parallel write goroutines (add a worker pool / semaphore)
- Exposing Prometheus metrics (write/read latency, S3 ops)
- Adding authentication / authorization (e.g. mTLS or reverse proxy integration)
- Improved regex matcher support (full RE / NRE semantics)
- Configurable Parquet row group / compression tuning
- Pluggable storage class selection (STANDARD vs GLACIER tiers)
- External retention only: rely on S3 lifecycle rules configured outside the application
- Cold storage tier transitions / lifecycle policies for large historical volumes

## License 📄

MIT

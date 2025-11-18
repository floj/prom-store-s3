#!/bin/bash

# Example startup script for the Prometheus S3 adapter

# Set AWS credentials (alternatively use IAM role or ~/.aws/credentials)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"

# Set S3 bucket
export S3_BUCKET="my-prometheus-metrics-bucket"

# Start the adapter
./prom-store-s3 -listen-address=:9201

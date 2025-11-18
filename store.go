package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/prompb"
)

type S3Store struct {
	client *s3.Client
	bucket string
}

func NewS3Store(bucket, region string) (*S3Store, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Store{
		client: client,
		bucket: bucket,
	}, nil
}

// Write stores timeseries data in S3, batching all samples for a given metric
// name from a single WriteRequest into one S3 object.
func (s *S3Store) Write(ctx context.Context, req *prompb.WriteRequest) error {
	// Group samples by metric name
	metrics := make(map[string][]map[string]interface{})

	for _, ts := range req.Timeseries {
		metricName := getMetricName(ts.Labels)
		if metricName == "" {
			metricName = "unknown"
		}

		for _, sample := range ts.Samples {
			data := map[string]interface{}{
				"labels":    labelsToMap(ts.Labels),
				"timestamp": sample.Timestamp,
				"value":     sample.Value,
			}
			metrics[metricName] = append(metrics[metricName], data)
		}
	}

	for metricName, samples := range metrics {
		if len(samples) == 0 {
			continue
		}

		jsonData, err := json.Marshal(samples)
		if err != nil {
			return fmt.Errorf("failed to marshal data for metric %s: %w", metricName, err)
		}

		// Use the timestamp of the first sample for the key.
		// In a real-world scenario, you might want a more sophisticated naming scheme
		// to avoid collisions and allow for efficient querying.
		firstTimestamp := samples[0]["timestamp"].(int64)
		timestamp := time.UnixMilli(firstTimestamp)
		key := fmt.Sprintf("metrics/%s/%s_%d.json",
			sanitizeMetricName(metricName),
			timestamp.Format("2006/01/02/15/04/05"), // Added seconds for more granular files
			firstTimestamp,
		)

		_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(string(jsonData)),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			return fmt.Errorf("failed to write batch for metric %s to S3: %w", metricName, err)
		}
	}

	return nil
}

// Read retrieves timeseries data from S3 based on query matchers
func (s *S3Store) Read(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	resp := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}

	for i, query := range req.Queries {
		result, err := s.executeQuery(ctx, query)
		if err != nil {
			log.Printf("Error executing query: %v", err)
			// Return empty result on error rather than failing completely
			result = &prompb.QueryResult{
				Timeseries: []*prompb.TimeSeries{},
			}
		}
		resp.Results[i] = result
	}

	return resp, nil
}

func (s *S3Store) executeQuery(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	// Extract metric name from matchers
	metricName := ""
	for _, matcher := range query.Matchers {
		if matcher.Name == "__name__" && matcher.Type == prompb.LabelMatcher_EQ {
			metricName = matcher.Value
			break
		}
	}

	if metricName == "" {
		// If no metric name specified, we'd need to list all metrics
		// For simplicity, return empty result
		return &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{},
		}, nil
	}

	// List objects with the metric prefix
	prefix := fmt.Sprintf("metrics/%s/", sanitizeMetricName(metricName))

	// Use time range to narrow down the search
	startTime := time.UnixMilli(query.StartTimestampMs)
	endTime := time.UnixMilli(query.EndTimestampMs)

	var timeseries []*prompb.TimeSeries
	timeseriesMap := make(map[string]*prompb.TimeSeries)

	// List objects in time range subdirectories
	// This is a simplified approach - in production, you'd want more sophisticated indexing
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			// Get the object
			result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(s.bucket),
				Key:    obj.Key,
			})
			if err != nil {
				log.Printf("Error getting object %s: %v", *obj.Key, err)
				continue
			}

			var data map[string]interface{}
			if err := json.NewDecoder(result.Body).Decode(&data); err != nil {
				log.Printf("Error decoding object %s: %v", *obj.Key, err)
				result.Body.Close()
				continue
			}
			result.Body.Close()

			timestamp := int64(data["timestamp"].(float64))
			value := data["value"].(float64)

			// Filter by time range
			if timestamp < query.StartTimestampMs || timestamp > query.EndTimestampMs {
				continue
			}

			// Convert labels back
			labelsMap := data["labels"].(map[string]interface{})
			labels := make([]prompb.Label, 0, len(labelsMap))
			labelKey := ""
			for name, val := range labelsMap {
				labels = append(labels, prompb.Label{
					Name:  name,
					Value: val.(string),
				})
				labelKey += name + "=" + val.(string) + ","
			}

			// Group samples by label set
			ts, exists := timeseriesMap[labelKey]
			if !exists {
				ts = &prompb.TimeSeries{
					Labels:  labels,
					Samples: []prompb.Sample{},
				}
				timeseriesMap[labelKey] = ts
				timeseries = append(timeseries, ts)
			}

			ts.Samples = append(ts.Samples, prompb.Sample{
				Timestamp: timestamp,
				Value:     value,
			})
		}
	}

	log.Printf("Query for %s from %v to %v returned %d timeseries",
		metricName, startTime, endTime, len(timeseries))

	return &prompb.QueryResult{
		Timeseries: timeseries,
	}, nil
}

func getMetricName(labels []prompb.Label) string {
	for _, label := range labels {
		if label.Name == "__name__" {
			return label.Value
		}
	}
	return ""
}

func labelsToMap(labels []prompb.Label) map[string]string {
	result := make(map[string]string, len(labels))
	for _, label := range labels {
		result[label.Name] = label.Value
	}
	return result
}

func sanitizeMetricName(name string) string {
	// Replace characters that might cause issues in S3 keys
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		" ", "_",
	)
	return replacer.Replace(name)
}

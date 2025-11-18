package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/prometheus/prompb"
)

type S3Store struct {
	client          *s3.Client
	bucket          string
	retentionPeriod time.Duration
	retentionCancel context.CancelFunc
	retentionWG     sync.WaitGroup
	logger          *slog.Logger
	mapMutex        sync.Mutex
	manifestLocks   map[string]*sync.Mutex
}

type Sample struct {
	labels    map[string]string
	timestamp int64
	value     float64
}

// ManifestEntry stores min/max timestamp metadata for a parquet object
type ManifestEntry struct {
	Key     string `json:"key"`
	MinTs   int64  `json:"min_ts"`
	MaxTs   int64  `json:"max_ts"`
	Samples int    `json:"samples"`
}

func NewS3Store(bucket, region string, retentionPeriod time.Duration, logger *slog.Logger) (*S3Store, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Store{
		client:          client,
		bucket:          bucket,
		retentionPeriod: retentionPeriod,
		logger:          logger,
		manifestLocks:   make(map[string]*sync.Mutex),
	}, nil
}

// Write stores timeseries data in S3, batching all samples for a given metric
// name from a single WriteRequest into one S3 object.
func (s *S3Store) Write(ctx context.Context, req *prompb.WriteRequest) error {
	// Group samples by metric name
	metrics := make(map[string][]Sample)

	for _, ts := range req.Timeseries {
		metricName := getMetricName(ts.Labels)
		if metricName == "" {
			metricName = "unknown"
		}

		for _, sample := range ts.Samples {
			data := Sample{
				labels:    labelsToMap(ts.Labels),
				timestamp: sample.Timestamp,
				value:     sample.Value,
			}
			metrics[metricName] = append(metrics[metricName], data)
		}
	}

	// Write metrics concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, len(metrics))

	for metricName, samples := range metrics {
		if len(samples) == 0 {
			continue
		}

		wg.Add(1)
		go func(name string, data []Sample) {
			defer wg.Done()
			if err := s.writeMetricBatch(ctx, name, data); err != nil {
				errChan <- err
			}
		}(metricName, samples)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	if err := <-errChan; err != nil {
		return err
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
			s.logger.Error("query execution failed", "error", err)
			// Return empty result on error rather than failing completely
			result = &prompb.QueryResult{Timeseries: []*prompb.TimeSeries{}}
		}
		resp.Results[i] = result
	}

	return resp, nil
}

func (s *S3Store) executeQuery(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
	// Extract metric name from matchers
	metricName := ""
	labelMatchers := make(map[string]*prompb.LabelMatcher)
	for _, matcher := range query.Matchers {
		if matcher.Name == "__name__" && matcher.Type == prompb.LabelMatcher_EQ {
			metricName = matcher.Value
		} else {
			labelMatchers[matcher.Name] = matcher
		}
	}

	if metricName == "" {
		// If no metric name specified, we'd need to list all metrics
		// For simplicity, return empty result
		return &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{},
		}, nil
	}

	// Metric prefix and manifest key
	prefix := fmt.Sprintf("metrics/%s/", sanitizeMetricName(metricName))
	manifestKey := fmt.Sprintf("metrics/%s/manifest.ndjson", sanitizeMetricName(metricName))

	// Use time range to narrow down the search
	startTime := time.UnixMilli(query.StartTimestampMs)
	endTime := time.UnixMilli(query.EndTimestampMs)
	var timeseries []*prompb.TimeSeries
	timeseriesMap := make(map[string]*prompb.TimeSeries)
	// Attempt to use manifest to select candidate object keys
	candidateKeys, usedManifest := s.loadManifestKeys(ctx, manifestKey, query.StartTimestampMs, query.EndTimestampMs)

	if !usedManifest {
		// Fallback: list objects with prefix (legacy path)
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
				candidateKeys = append(candidateKeys, *obj.Key)
			}
		}
	}

	for _, objectKey := range candidateKeys {
		result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			s.logger.Error("get object failed", "key", objectKey, "error", err)
			continue
		}

		data, err := io.ReadAll(result.Body)
		result.Body.Close()
		if err != nil {
			s.logger.Error("read object body failed", "key", objectKey, "error", err)
			continue
		}

		parquetReader, err := file.NewParquetReader(bytes.NewReader(data), file.WithReadProps(parquet.NewReaderProperties(memory.DefaultAllocator)))
		if err != nil {
			s.logger.Error("parquet reader create failed", "key", objectKey, "error", err)
			continue
		}

		fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		if err != nil {
			s.logger.Error("arrow file reader create failed", "key", objectKey, "error", err)
			continue
		}

		table, err := fileReader.ReadTable(ctx)
		if err != nil {
			s.logger.Error("read parquet table failed", "key", objectKey, "error", err)
			continue
		}
		defer table.Release()

		tr := array.NewTableReader(table, 0)
		defer tr.Release()

		for tr.Next() {
			rec := tr.Record()
			timestampCol := rec.Column(0).(*array.Int64)
			valueCol := rec.Column(1).(*array.Float64)
			labelsCol := rec.Column(2).(*array.String)
			for i := 0; i < int(rec.NumRows()); i++ {
				tsVal := timestampCol.Value(i)
				if tsVal < query.StartTimestampMs || tsVal > query.EndTimestampMs {
					continue
				}
				val := valueCol.Value(i)
				labelsStr := labelsCol.Value(i)
				labels := stringToLabelsMap(labelsStr)
				if !matchesLabels(labels, labelMatchers) {
					continue
				}
				pbLabels := make([]prompb.Label, 0, len(labels))
				labelKey := ""
				for name, v := range labels {
					pbLabels = append(pbLabels, prompb.Label{Name: name, Value: v})
					labelKey += name + "=" + v + ","
				}
				series, ok := timeseriesMap[labelKey]
				if !ok {
					series = &prompb.TimeSeries{Labels: pbLabels, Samples: []prompb.Sample{}}
					timeseriesMap[labelKey] = series
					timeseries = append(timeseries, series)
				}
				series.Samples = append(series.Samples, prompb.Sample{Timestamp: tsVal, Value: val})
			}
		}
	}

	s.logger.Info("query result summary", "metric", metricName, "start", startTime, "end", endTime, "timeseries", len(timeseries))

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

// labelsMapToString converts labels map to JSON string for storage
func labelsMapToString(labels map[string]string) string {
	data, err := json.Marshal(labels)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// stringToLabelsMap converts JSON string back to labels map
func stringToLabelsMap(s string) map[string]string {
	var labels map[string]string
	if err := json.Unmarshal([]byte(s), &labels); err != nil {
		return make(map[string]string)
	}
	return labels
}

// matchesLabels checks if the given labels match all the provided matchers
func matchesLabels(labels map[string]string, matchers map[string]*prompb.LabelMatcher) bool {
	for name, matcher := range matchers {
		value, exists := labels[name]
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			if !exists || value != matcher.Value {
				return false
			}
		case prompb.LabelMatcher_NEQ:
			if exists && value == matcher.Value {
				return false
			}
		case prompb.LabelMatcher_RE:
			// For simplicity, treat RE as EQ (full regex support would require regexp package)
			if !exists || value != matcher.Value {
				return false
			}
		case prompb.LabelMatcher_NRE:
			if exists && value == matcher.Value {
				return false
			}
		}
	}
	return true
}

// StartRetentionCleanup starts a background goroutine that periodically cleans up old data
func (s *S3Store) StartRetentionCleanup() {
	if s.retentionPeriod <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.retentionCancel = cancel
	s.retentionWG.Add(1)
	go func() {
		defer s.retentionWG.Done()
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		s.logger.Info("starting retention cleanup", "period", s.retentionPeriod)
		for {
			select {
			case <-ctx.Done():
				s.logger.Info("stopping retention cleanup")
				return
			case <-ticker.C:
				if err := s.cleanupOldData(ctx); err != nil {
					s.logger.Error("retention cleanup error", "error", err)
				}
			}
		}
	}()
}

func (s *S3Store) Stop() {
	if s.retentionCancel != nil {
		s.retentionCancel()
	}
	s.retentionWG.Wait()
}

// cleanupOldData removes data older than the retention period
func (s *S3Store) cleanupOldData(ctx context.Context) error {
	cutoffTime := time.Now().Add(-s.retentionPeriod)
	s.logger.Info("retention cleanup scanning", "cutoff", cutoffTime)

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String("metrics/"),
	})

	var objectsToDelete []string

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list S3 objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.LastModified == nil {
				continue
			}
			if obj.LastModified.Before(cutoffTime) {
				objectsToDelete = append(objectsToDelete, *obj.Key)
			}
		}
	}

	if len(objectsToDelete) == 0 {
		return nil
	}

	const batchSize = 1000
	for i := 0; i < len(objectsToDelete); i += batchSize {
		end := i + batchSize
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}
		if err := s.deleteObjectsBatch(ctx, objectsToDelete[i:end]); err != nil {
			return fmt.Errorf("failed to delete batch: %w", err)
		}
	}

	s.logger.Info("retention cleanup deleted objects", "count", len(objectsToDelete))
	return nil
}

// deleteObjectsBatch deletes a batch of objects from S3
func (s *S3Store) deleteObjectsBatch(ctx context.Context, keys []string) error {
	objects := make([]types.ObjectIdentifier, len(keys))
	for i, key := range keys {
		objects[i] = types.ObjectIdentifier{Key: aws.String(key)}
	}

	_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{
			Objects: objects,
		},
	})

	return err
}

// writeMetricBatch writes a batch of samples for a single metric to S3
func (s *S3Store) writeMetricBatch(ctx context.Context, metricName string, samples []Sample) error {
	// Create Arrow schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
			{Name: "labels", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// Build Arrow record
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	for _, sample := range samples {
		builder.Field(0).(*array.Int64Builder).Append(sample.timestamp)
		builder.Field(1).(*array.Float64Builder).Append(sample.value)

		// Serialize labels as JSON string
		labelsStr := labelsMapToString(sample.labels)
		builder.Field(2).(*array.StringBuilder).Append(labelsStr)
	}

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet
	buf := new(bytes.Buffer)
	writer, err := pqarrow.NewFileWriter(schema, buf, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("failed to create parquet writer for metric %s: %w", metricName, err)
	}

	if err := writer.Write(record); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write record to parquet for metric %s: %w", metricName, err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer for metric %s: %w", metricName, err)
	}

	// Use the timestamp of the first sample for the key.
	firstTimestamp := samples[0].timestamp
	timestamp := time.UnixMilli(firstTimestamp)
	key := fmt.Sprintf("metrics/%s/%s_%d.parquet",
		sanitizeMetricName(metricName),
		timestamp.Format("2006/01/02/15/04/05"),
		firstTimestamp,
	)

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("failed to write batch for metric %s to S3: %w", metricName, err)
	}

	// Update manifest with min/max timestamps
	minTs, maxTs := samples[0].timestamp, samples[0].timestamp
	for _, s := range samples[1:] {
		if s.timestamp < minTs {
			minTs = s.timestamp
		}
		if s.timestamp > maxTs {
			maxTs = s.timestamp
		}
	}
	entry := ManifestEntry{Key: key, MinTs: minTs, MaxTs: maxTs, Samples: len(samples)}
	if err := s.appendManifestEntry(ctx, metricName, entry); err != nil {
		s.logger.Warn("manifest update failed", "metric", metricName, "error", err)
	}

	return nil
}

// appendManifestEntry appends a manifest entry, protecting against concurrent writes
func (s *S3Store) appendManifestEntry(ctx context.Context, metricName string, entry ManifestEntry) error {
	s.mapMutex.Lock()
	mu, ok := s.manifestLocks[metricName]
	if !ok {
		mu = &sync.Mutex{}
		s.manifestLocks[metricName] = mu
	}
	s.mapMutex.Unlock()

	mu.Lock()
	defer mu.Unlock()

	manifestKey := fmt.Sprintf("metrics/%s/manifest.ndjson", sanitizeMetricName(metricName))
	// Get existing manifest (if any)
	existing, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(manifestKey)})
	var buf bytes.Buffer
	if err == nil {
		io.Copy(&buf, existing.Body)
		existing.Body.Close()
	}
	line, _ := json.Marshal(entry)
	buf.Write(line)
	buf.WriteByte('\n')
	_, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(manifestKey),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	return putErr
}

// loadManifestKeys returns candidate object keys filtered by time range using manifest; bool indicates if manifest used
func (s *S3Store) loadManifestKeys(ctx context.Context, manifestKey string, startMs, endMs int64) ([]string, bool) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(manifestKey)})
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	var keys []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var e ManifestEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		// Time window overlap check
		if e.MaxTs < startMs || e.MinTs > endMs {
			continue
		}
		keys = append(keys, e.Key)
	}
	if err := scanner.Err(); err != nil {
		return keys, true // partial
	}
	return keys, true
}

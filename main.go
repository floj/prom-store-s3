package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func main() {
	var (
		listenAddr    = flag.String("listen-address", ":9201", "Address to listen on for HTTP requests")
		s3Bucket      = flag.String("s3-bucket", os.Getenv("S3_BUCKET"), "S3 bucket name")
		s3Region      = flag.String("s3-region", os.Getenv("AWS_REGION"), "AWS region")
		retentionDays = flag.Int("retention-days", getEnvInt("RETENTION_DAYS", 7), "Data retention period in days (default 7)")
		logLevelFlag  = flag.String("log-level", os.Getenv("LOG_LEVEL"), "Log level (debug, info, warn, error). Can also use LOG_LEVEL env var")
	)
	flag.Parse()

	// Determine log level
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(*logLevelFlag)) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if *s3Bucket == "" {
		logger.Error("missing required S3 bucket")
		os.Exit(1)
	}
	if *s3Region == "" {
		*s3Region = "us-east-1"
	}

	store, err := NewS3Store(*s3Bucket, *s3Region, time.Duration(*retentionDays*24)*time.Hour, logger)
	if err != nil {
		logger.Error("failed to create S3 store", "error", err)
		os.Exit(1)
	}

	// Start retention cleanup goroutine (managed internally)
	store.StartRetentionCleanup()

	handler := NewHandler(store, logger)

	http.HandleFunc("/write", handler.Write)
	http.HandleFunc("/read", handler.Read)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	logger.Info("starting prometheus remote storage adapter", "listen_address", *listenAddr, "s3_bucket", *s3Bucket, "s3_region", *s3Region, "retention_days", *retentionDays, "log_level", level.String())
	server := &http.Server{
		Addr:    *listenAddr,
		Handler: nil, // Uses default mux
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		logger.Info("received shutdown signal, initiating graceful shutdown")
		// Stop background processes
		store.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("server shutdown error", "error", err)
		}
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server listen failed", "error", err)
		os.Exit(1)
	}
}

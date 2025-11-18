package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	)
	flag.Parse()

	if *s3Bucket == "" {
		log.Fatal("S3 bucket is required (use -s3-bucket or S3_BUCKET env var)")
	}
	if *s3Region == "" {
		*s3Region = "us-east-1"
	}

	store, err := NewS3Store(*s3Bucket, *s3Region, time.Duration(*retentionDays*24)*time.Hour)
	if err != nil {
		log.Fatalf("Failed to create S3 store: %v", err)
	}

	// Start retention cleanup goroutine
	go store.StartRetentionCleanup(context.Background())

	handler := NewHandler(store)

	http.HandleFunc("/write", handler.Write)
	http.HandleFunc("/read", handler.Read)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Printf("Starting Prometheus remote storage adapter on %s", *listenAddr)
	log.Printf("Using S3 bucket: %s in region: %s", *s3Bucket, *s3Region)
	server := &http.Server{
		Addr:    *listenAddr,
		Handler: nil, // Uses default mux
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutting down server...")
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("Server shutdown error: %v", err)
		}
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start server: %v", err)
	}
}

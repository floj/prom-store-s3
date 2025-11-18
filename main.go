package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		listenAddr = flag.String("listen-address", ":9201", "Address to listen on for HTTP requests")
		s3Bucket   = flag.String("s3-bucket", os.Getenv("S3_BUCKET"), "S3 bucket name")
		s3Region   = flag.String("s3-region", os.Getenv("AWS_REGION"), "AWS region")
	)
	flag.Parse()

	if *s3Bucket == "" {
		log.Fatal("S3 bucket is required (use -s3-bucket or S3_BUCKET env var)")
	}
	if *s3Region == "" {
		*s3Region = "us-east-1"
	}

	store, err := NewS3Store(*s3Bucket, *s3Region)
	if err != nil {
		log.Fatalf("Failed to create S3 store: %v", err)
	}

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

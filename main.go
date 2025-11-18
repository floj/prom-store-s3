package main

import (
	"flag"
	"log"
	"net/http"
	"os"
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
	if err := http.ListenAndServe(*listenAddr, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

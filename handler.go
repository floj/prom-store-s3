package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type Handler struct {
	store *S3Store
}

func NewHandler(store *S3Store) *Handler {
	return &Handler{store: store}
}

func (h *Handler) Write(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Printf("Error decompressing request: %v", err)
		http.Error(w, "Failed to decompress request", http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Printf("Error unmarshaling request: %v", err)
		http.Error(w, "Failed to unmarshal request", http.StatusBadRequest)
		return
	}

	if err := h.store.Write(r.Context(), &req); err != nil {
		log.Printf("Error writing to S3: %v", err)
		http.Error(w, "Failed to write to storage", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Read(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Printf("Error decompressing request: %v", err)
		http.Error(w, "Failed to decompress request", http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Printf("Error unmarshaling request: %v", err)
		http.Error(w, "Failed to unmarshal request", http.StatusBadRequest)
		return
	}

	resp, err := h.store.Read(r.Context(), &req)
	if err != nil {
		log.Printf("Error reading from S3: %v", err)
		http.Error(w, fmt.Sprintf("Failed to read from storage: %v", err), http.StatusInternalServerError)
		return
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	compressed = snappy.Encode(nil, data)

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(compressed); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type Handler struct {
	store  *S3Store
	logger *slog.Logger
}

func NewHandler(store *S3Store, logger *slog.Logger) *Handler {
	return &Handler{store: store, logger: logger}
}

func (h *Handler) Write(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("read request body failed", "error", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	if len(compressed) == 0 {
		http.Error(w, "Empty request body", http.StatusBadRequest)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		h.logger.Error("snappy decompress failed", "error", err)
		http.Error(w, "Failed to decompress request", http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		h.logger.Error("unmarshal write request failed", "error", err)
		http.Error(w, "Failed to unmarshal request", http.StatusBadRequest)
		return
	}

	if len(req.Timeseries) == 0 {
		h.logger.Warn("empty write request")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	totalSamples := 0
	for _, ts := range req.Timeseries {
		totalSamples += len(ts.Samples)
	}
	h.logger.Info("write request", "timeseries", len(req.Timeseries), "samples", totalSamples)

	if err := h.store.Write(r.Context(), &req); err != nil {
		h.logger.Error("write to storage failed", "error", err)
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
		h.logger.Error("read request body failed", "error", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		h.logger.Error("snappy decompress failed", "error", err)
		http.Error(w, "Failed to decompress request", http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		h.logger.Error("unmarshal read request failed", "error", err)
		http.Error(w, "Failed to unmarshal request", http.StatusBadRequest)
		return
	}

	resp, err := h.store.Read(r.Context(), &req)
	if err != nil {
		h.logger.Error("read from storage failed", "error", err)
		http.Error(w, fmt.Sprintf("Failed to read from storage: %v", err), http.StatusInternalServerError)
		return
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		h.logger.Error("marshal response failed", "error", err)
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	compressed = snappy.Encode(nil, data)

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(compressed); err != nil {
		h.logger.Error("write response failed", "error", err)
	}
}

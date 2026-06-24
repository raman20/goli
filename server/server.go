package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/raman20/storage"
)

type Server struct {
	db        *storage.DB
	objectDir string
}

type ObjectMetadata struct {
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	ETag        string    `json:"etag"`
	LastMod     time.Time `json:"last_modified"`
	DataPath    string    `json:"data_path"`
}

func NewServer(db *storage.DB, objectDir string) (*Server, error) {
	if err := os.MkdirAll(objectDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create object store directory: %w", err)
	}
	return &Server{
		db:        db,
		objectDir: objectDir,
	}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	// Route based on path segments
	if path == "" {
		if r.Method == http.MethodGet {
			s.handleListBuckets(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) == 1 {
		// Bucket operations: /<bucket>
		bucketName := parts[0]
		switch r.Method {
		case http.MethodPut:
			s.handleCreateBucket(w, bucketName)
		case http.MethodGet:
			s.handleListObjects(w, bucketName, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// Object operations: /<bucket>/<key...>
	bucketName := parts[0]
	objectKey := strings.Join(parts[1:], "/")

	switch r.Method {
	case http.MethodPut:
		s.handlePutObject(w, r, bucketName, objectKey)
	case http.MethodGet:
		s.handleGetObject(w, r, bucketName, objectKey)
	case http.MethodDelete:
		s.handleDeleteObject(w, r, bucketName, objectKey)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleCreateBucket(w http.ResponseWriter, bucket string) {
	bucketKey := "bucket:" + bucket
	creationTime := time.Now().Format(time.RFC3339)
	if err := s.db.Set(bucketKey, creationTime); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create bucket: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   Owner    `xml:"Owner"`
	Buckets []Bucket `xml:"Buckets>Bucket"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	scanResults, err := s.db.Scan("bucket:")
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to scan buckets: %v", err), http.StatusInternalServerError)
		return
	}

	var buckets []Bucket
	for k, v := range scanResults {
		bucketName := strings.TrimPrefix(k, "bucket:")
		buckets = append(buckets, Bucket{
			Name:         bucketName,
			CreationDate: v,
		})
	}

	// Sort buckets alphabetically
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	result := ListAllMyBucketsResult{
		Owner: Owner{
			ID:          "goli-owner",
			DisplayName: "goli",
		},
		Buckets: buckets,
	}

	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	// Auto-create bucket if not exists
	bucketKey := "bucket:" + bucket
	if _, found := s.db.Get(bucketKey); !found {
		s.db.Set(bucketKey, time.Now().Format(time.RFC3339))
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	hash := md5.Sum(payload)
	etag := hex.EncodeToString(hash[:])

	filename := uuid.NewString() + ".data"
	dataPath := filepath.Join(s.objectDir, filename)
	if err := os.WriteFile(dataPath, payload, 0644); err != nil {
		http.Error(w, fmt.Sprintf("Failed to save payload: %v", err), http.StatusInternalServerError)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	meta := ObjectMetadata{
		Bucket:      bucket,
		Key:         key,
		Size:        int64(len(payload)),
		ContentType: contentType,
		ETag:        `"` + etag + `"`,
		LastMod:     time.Now(),
		DataPath:    dataPath,
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		os.Remove(dataPath)
		http.Error(w, "Failed to serialize metadata", http.StatusInternalServerError)
		return
	}

	metaKey := fmt.Sprintf("obj:%s:%s", bucket, key)
	if err := s.db.Set(metaKey, string(metaBytes)); err != nil {
		os.Remove(dataPath)
		http.Error(w, fmt.Sprintf("Failed to store metadata: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	metaKey := fmt.Sprintf("obj:%s:%s", bucket, key)
	metaStr, found := s.db.Get(metaKey)
	if !found {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}

	var meta ObjectMetadata
	if err := json.Unmarshal([]byte(metaStr), &meta); err != nil {
		http.Error(w, "Failed to parse metadata", http.StatusInternalServerError)
		return
	}

	file, err := os.Open(meta.DataPath)
	if err != nil {
		http.Error(w, "Failed to read object content", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.LastMod.Format(time.RFC1123))

	io.Copy(w, file)
}

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	metaKey := fmt.Sprintf("obj:%s:%s", bucket, key)
	metaStr, found := s.db.Get(metaKey)
	if !found {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	var meta ObjectMetadata
	if err := json.Unmarshal([]byte(metaStr), &meta); err == nil {
		os.Remove(meta.DataPath)
	}

	if err := s.db.Delete(metaKey); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete metadata: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type ListBucketResult struct {
	XMLName     xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	Marker      string   `xml:"Marker"`
	MaxKeys     int      `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`
	Contents    []Object `xml:"Contents"`
}

type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func (s *Server) handleListObjects(w http.ResponseWriter, bucket string, r *http.Request) {
	prefixFilter := r.URL.Query().Get("prefix")

	scanPrefix := fmt.Sprintf("obj:%s:%s", bucket, prefixFilter)
	scanResults, err := s.db.Scan(scanPrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to scan objects: %v", err), http.StatusInternalServerError)
		return
	}

	var contents []Object
	for _, v := range scanResults {
		var meta ObjectMetadata
		if err := json.Unmarshal([]byte(v), &meta); err != nil {
			continue
		}

		contents = append(contents, Object{
			Key:          meta.Key,
			LastModified: meta.LastMod.Format(time.RFC3339),
			ETag:         meta.ETag,
			Size:         meta.Size,
			StorageClass: "STANDARD",
		})
	}

	sort.Slice(contents, func(i, j int) bool {
		return contents[i].Key < contents[j].Key
	})

	result := ListBucketResult{
		Name:        bucket,
		Prefix:      prefixFilter,
		MaxKeys:     1000,
		IsTruncated: false,
		Contents:    contents,
	}

	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

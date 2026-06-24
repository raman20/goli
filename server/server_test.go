package server

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/raman20/storage"
)

func TestObjectStoreServer(t *testing.T) {
	// 1. Setup DB
	tmpDir, err := os.MkdirTemp("", "server_db_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := storage.DefaultOptions()
	opts.DataDir = tmpDir

	db, err := storage.Open("server_test_db", opts)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	// 2. Setup Server
	objDir := os.ExpandEnv(tmpDir + "/objects")
	srv, err := NewServer(db, objDir)
	if err != nil {
		t.Fatalf("failed to init server: %v", err)
	}

	testServer := httptest.NewServer(srv)
	defer testServer.Close()

	client := testServer.Client()

	// 3. Test Create Bucket
	req, _ := http.NewRequest(http.MethodPut, testServer.URL+"/mybucket", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT /mybucket failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 4. Test List Buckets
	req, _ = http.NewRequest(http.MethodGet, testServer.URL+"/", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var listBuckets ListAllMyBucketsResult
	if err := xml.Unmarshal(body, &listBuckets); err != nil {
		t.Fatalf("failed to parse ListBuckets XML response: %v", err)
	}
	if len(listBuckets.Buckets) != 1 || listBuckets.Buckets[0].Name != "mybucket" {
		t.Errorf("expected bucket 'mybucket', got %v", listBuckets.Buckets)
	}

	// 5. Test Put Object
	objectData := []byte("Hello, Google Antigravity!")
	req, _ = http.NewRequest(http.MethodPut, testServer.URL+"/mybucket/docs/hello.txt", bytes.NewReader(objectData))
	req.Header.Set("Content-Type", "text/plain")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("PUT /mybucket/docs/hello.txt failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Errorf("expected ETag header in response")
	}
	resp.Body.Close()

	// 6. Test Get Object
	req, _ = http.NewRequest(http.MethodGet, testServer.URL+"/mybucket/docs/hello.txt", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET /mybucket/docs/hello.txt failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("expected content type text/plain, got %s", resp.Header.Get("Content-Type"))
	}
	gotData, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(gotData, objectData) {
		t.Errorf("expected data %q, got %q", objectData, gotData)
	}

	// 7. Test List Objects (with prefix)
	req, _ = http.NewRequest(http.MethodGet, testServer.URL+"/mybucket?prefix=docs/", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET /mybucket?prefix=docs/ failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var listObjects ListBucketResult
	if err := xml.Unmarshal(body, &listObjects); err != nil {
		t.Fatalf("failed to parse ListObjects XML response: %v", err)
	}
	if len(listObjects.Contents) != 1 || listObjects.Contents[0].Key != "docs/hello.txt" {
		t.Errorf("expected object key 'docs/hello.txt', got %v", listObjects.Contents)
	}

	// 8. Test Delete Object
	req, _ = http.NewRequest(http.MethodDelete, testServer.URL+"/mybucket/docs/hello.txt", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204 No Content, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 9. Verify deletion (Get should return 404)
	req, _ = http.NewRequest(http.MethodGet, testServer.URL+"/mybucket/docs/hello.txt", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET after DELETE failed: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

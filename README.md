# 🚀 Goli: A Log-Structured Merge Tree Database & Object Store in Go

Goli is a high-performance database implemented in Go, based on the Log-Structured Merge (LSM) tree architecture, combined with a built-in S3-compatible Object Storage facade. It is optimized for write-heavy metadata indexing while serving object payloads from disk.

---

## 🌟 Key Features

### 🛠️ LSM Engine (Core)
- **Skip List-based Memtable**: Thread-safe in-memory buffer with lock-free reads.
- **Robust Binary WAL**: Length-prefixed Write-Ahead Log for crash recovery. Safe for binary payloads, colons, and newlines.
- **SSTable Storage**: Disk persistence with block-based indexes and footer verification.
- **Background Compaction**: K-way merge compaction that merges SSTables, discards tombstones (deletions), and reclaims disk space asynchronously.
- **Prefix Range Queries**: Quick scanning of key spaces via `DB.Scan(prefix)`.

### ☁️ S3-Compatible Object Store API
- **Metadata Indexing**: Goli LSM stores object metadata (ETags, Content-Types, Sizes, timestamps) for lightning-fast listings.
- **Data Deduplication**: Payload files are saved in a flat, content-addressable storage structure (`data/objects/`).
- **REST Endpoints**:
  - `PUT /<bucket>`: Create bucket.
  - `GET /`: List all buckets.
  - `PUT /<bucket>/<key>`: Upload object payload + auto-calculate MD5 ETag.
  - `GET /<bucket>/<key>`: Retrieve object payload + original metadata headers.
  - `DELETE /<bucket>/<key>`: Delete object and its payload file.
  - `GET /<bucket>?prefix=<prefix>`: List objects matching a prefix (S3-compatible XML output).

---

## 🏗️ Architecture

```
                 ┌────────────────────────────────┐
                 │    Client Request (HTTP/S3)    │
                 └───────────────┬────────────────┘
                                 │
                 ┌───────────────▼────────────────┐
                 │       S3 API Controller        │
                 └────────┬───────────────┬───────┘
                          │ (Metadata)    │ (Payloads)
                          ▼               ▼
      ┌───────────────────────────────┐ ┌───────────────────┐
      │         Goli LSM Engine       │ │  Flat File Store  │
      │ ┌───────────────────────────┐ │ │ (data/objects/)   │
      │ │ Active Memtable           │ │ └───────────────────┘
      │ └─────────────┬─────────────┘ │
      │               │ (Flush)       │
      │               ▼               │
      │ ┌───────────────────────────┐ │
      │ │ Sorted String Tables (sst)│ │
      │ └───────────────────────────┘ │
      └───────────────────────────────┘
```

---

## 🚀 Getting Started

### 📋 Prerequisites
- Go 1.23 or higher

### 🔨 Compilation & Testing
Run all tests to verify database, WAL, SSTable, and Server layers:
```bash
make test
```

Build the binary:
```bash
make build
```

Run the server (defaults to port `8080` and directory `data/`):
```bash
make run
```

---

## 💻 API Usage Examples

You can interact with Goli using standard S3 clients or simple `curl` commands.

### 1. Create a Bucket
```bash
curl -X PUT http://localhost:8080/images
```

### 2. List Buckets
```bash
curl -X GET http://localhost:8080/
```
**Response:**
```xml
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>goli-owner</ID>
    <DisplayName>goli</DisplayName>
  </Owner>
  <Buckets>
    <Bucket>
      <Name>images</Name>
      <CreationDate>2026-06-24T12:00:00Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>
```

### 3. Upload an Object (File)
```bash
curl -X PUT -H "Content-Type: text/plain" -d "Hello, Goli LSM!" http://localhost:8080/images/hello.txt
```

### 4. Retrieve an Object
```bash
curl -i http://localhost:8080/images/hello.txt
```
**Response:**
```http
HTTP/1.1 200 OK
Content-Length: 16
Content-Type: text/plain
Etag: "80b2a75d506d396781216d7a229a008c"
Last-Modified: Wed, 24 Jun 2026 12:05:00 UTC

Hello, Goli LSM!
```

### 5. List Objects in Bucket
```bash
curl http://localhost:8080/images?prefix=hello
```
**Response:**
```xml
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>images</Name>
  <Prefix>hello</Prefix>
  <Marker></Marker>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>hello.txt</Key>
    <LastModified>2026-06-24T12:05:00Z</LastModified>
    <ETag>"80b2a75d506d396781216d7a229a008c"</ETag>
    <Size>16</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>
```

### 6. Delete an Object
```bash
curl -X DELETE http://localhost:8080/images/hello.txt
```
build:
	go build -o bin/goli cmd/main.go

run: build
	./bin/goli

test:
	go test -v ./...
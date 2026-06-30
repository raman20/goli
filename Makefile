build:
	go build -o bin/goli cmd/goli/main.go


run: build
	./bin/goli

test:
	go test -v ./...
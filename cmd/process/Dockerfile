FROM golang:1.22 AS builder

# Build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
# duckdb requires CGO
RUN CGO_ENABLED=1 go build -o process ./cmd/process
CMD ["/src/process"]

FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /bin/clickhouse-dump .

FROM alpine:latest
COPY --from=builder /bin/clickhouse-dump /bin/clickhouse-dump
ENTRYPOINT ["./clickhouse-dump"]

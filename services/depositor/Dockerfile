FROM golang:1.26-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git ca-certificates

COPY . .

WORKDIR /workspace/services/depositor
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/depositor ./cmd/depositor

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app/depositor /app/depositor
COPY --from=builder /workspace/services/depositor/migrations /app/migrations

EXPOSE 8080
ENTRYPOINT ["/app/depositor"]

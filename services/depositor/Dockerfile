FROM golang:1.26-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git ca-certificates

COPY go.work ./
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/depositor/go.mod services/depositor/go.sum ./services/depositor/
COPY services/publisher/go.mod services/publisher/go.sum ./services/publisher/
COPY services/dashboard/go.mod services/dashboard/go.sum ./services/dashboard/

WORKDIR /workspace/services/depositor
RUN go mod download

WORKDIR /workspace
COPY pkg/ ./pkg/
COPY services/depositor/ ./services/depositor/

WORKDIR /workspace/services/depositor
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/depositor ./cmd/depositor

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app/depositor /app/depositor
COPY --from=builder /workspace/services/depositor/migrations /app/migrations

EXPOSE 8080
ENTRYPOINT ["/app/depositor"]
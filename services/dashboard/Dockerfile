FROM golang:1.26-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git ca-certificates

COPY go.work ./
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/depositor/go.mod services/depositor/go.sum ./services/depositor/
COPY services/publisher/go.mod services/publisher/go.sum ./services/publisher/
COPY services/dashboard/go.mod services/dashboard/go.sum ./services/dashboard/
COPY services/gateway/go.mod services/gateway/go.sum ./services/gateway/

COPY vendor/ ./vendor/

COPY pkg/ ./pkg/
COPY services/dashboard/ ./services/dashboard/

WORKDIR /workspace/services/dashboard
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o /app/dashboard ./cmd/dashboard

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/dashboard /app/dashboard

EXPOSE 8080
ENTRYPOINT ["/app/dashboard"]
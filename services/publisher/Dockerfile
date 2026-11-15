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
COPY services/publisher/ ./services/publisher/

WORKDIR /workspace/services/publisher
ARG GIT_REVISION=dev
ARG SERVICE_VERSION=1.0.0
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -buildvcs=false \
    -ldflags="-s -w -X github.com/alihojaty/reliable-deposit-processor/pkg/metricsx.Revision=${GIT_REVISION} -X github.com/alihojaty/reliable-deposit-processor/pkg/metricsx.Version=${SERVICE_VERSION}" \
    -o /app/publisher ./cmd/publisher

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget
COPY --from=builder /app/publisher /app/publisher

EXPOSE 50051 8090 9090

HEALTHCHECK --interval=2s --timeout=1s --retries=2 --start-period=10s \
  CMD wget -qO- http://127.0.0.1:8090/livez >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/app/publisher"]
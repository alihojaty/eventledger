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
COPY services/depositor/ ./services/depositor/

WORKDIR /workspace/services/depositor
ARG GIT_REVISION=dev
ARG SERVICE_VERSION=1.0.0
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -buildvcs=false \
    -ldflags="-s -w -X github.com/alihojaty/reliable-deposit-processor/pkg/metricsx.Revision=${GIT_REVISION} -X github.com/alihojaty/reliable-deposit-processor/pkg/metricsx.Version=${SERVICE_VERSION}" \
    -o /app/depositor ./cmd/depositor

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget
COPY --from=builder /app/depositor /app/depositor
COPY --from=builder /workspace/services/depositor/migrations /app/migrations

EXPOSE 9090
ENTRYPOINT ["/app/depositor"]
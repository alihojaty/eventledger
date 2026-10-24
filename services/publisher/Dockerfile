FROM golang:1.27.3-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache ca-certificates tzdata


COPY go.work go.work.sum ./
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/depositor/go.mod services/depositor/go.sum ./services/depositor/
COPY services/publisher/go.mod services/publisher/go.sum ./services/publisher/

RUN cd pkg && go mod download && \
    cd ../services/publisher && go mod download


COPY pkg/ ./pkg/
COPY services/publisher/ ./services/publisher/

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/publisher ./services/publisher/cmd/publisher

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -g '' appuser

COPY --from=builder /out/publisher /app/publisher

USER appuser

ENTRYPOINT ["/app/publisher"]
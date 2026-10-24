FROM golang:1.27.3-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache ca-certificates tzdata


COPY go.work go.work.sum ./
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/depositor/go.mod services/depositor/go.sum ./services/depositor/
COPY services/publisher/go.mod services/publisher/go.sum ./services/publisher/

RUN cd pkg && go mod download && \
    cd ../services/depositor && go mod download


COPY pkg/ ./pkg/
COPY services/depositor/ ./services/depositor/

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/depositor ./services/depositor/cmd/depositor

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -g '' appuser

COPY --from=builder /out/depositor /app/depositor
COPY --from=builder /workspace/services/depositor/migrations /app/migrations

USER appuser

EXPOSE 8080

ENTRYPOINT ["/app/depositor"]
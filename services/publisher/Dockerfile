FROM golang:1.26-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git ca-certificates

COPY . .

WORKDIR /workspace/services/publisher
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/publisher ./cmd/publisher

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /app/publisher /app/publisher

EXPOSE 8080
ENTRYPOINT ["/app/publisher"]
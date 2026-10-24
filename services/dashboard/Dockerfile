FROM golang:1.26-alpine AS builder

WORKDIR /workspace

RUN apk add --no-cache git ca-certificates

COPY . .

WORKDIR /workspace/services/dashboard
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/dashboard ./cmd/dashboard

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/dashboard /app/dashboard

EXPOSE 8080
ENTRYPOINT ["/app/dashboard"]

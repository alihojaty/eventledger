module github.com/alihojaty/reliable-deposit-processor/services/depositor

go 1.26.0

require (
	github.com/alihojaty/reliable-deposit-processor/pkg v0.0.0
	github.com/google/uuid v1.6.0
	github.com/nats-io/nats.go v1.54.0
	github.com/shopspring/decimal v1.5.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/caarlos0/env/v11 v11.4.1 // indirect
	github.com/golang-migrate/migrate/v4 v4.20.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

replace github.com/alihojaty/reliable-deposit-processor/pkg => ../../pkg

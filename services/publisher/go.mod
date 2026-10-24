module github.com/alihojaty/reliable-deposit-processor/services/publisher

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
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

replace github.com/alihojaty/reliable-deposit-processor/pkg => ../../pkg

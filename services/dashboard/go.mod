module github.com/alihojaty/reliable-deposit-processor/services/dashboard

go 1.26.0

require (
	github.com/alihojaty/reliable-deposit-processor/pkg v0.0.0
	github.com/lib/pq v1.10.9
	github.com/nats-io/nats.go v1.54.0
)

replace github.com/alihojaty/reliable-deposit-processor/pkg => ../../pkg

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/shopspring/decimal v1.5.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

package metrics

import (
	"sync/atomic"
)

type Collector struct {
	received          atomic.Uint64
	processed         atomic.Uint64
	duplicate         atomic.Uint64
	invalid           atomic.Uint64
	dlq               atomic.Uint64
	retried           atomic.Uint64
	restoredAtStartup atomic.Uint64
}

type Snapshot struct {
	MessagesReceived  uint64 `json:"messages_received"`
	MessagesProcessed uint64 `json:"messages_processed"`
	MessagesDuplicate uint64 `json:"messages_duplicate"`
	MessagesInvalid   uint64 `json:"messages_invalid"`
	MessagesDLQ       uint64 `json:"messages_dlq"`
	MessagesRetried   uint64 `json:"messages_retried"`
	RestoredFromDB    uint64 `json:"restored_from_db"`
}

func NewCollector() *Collector {
	return &Collector{}
}

func (c *Collector) RestoreState(processedInDB, dlqInBroker uint64) {
	c.restoredAtStartup.Store(processedInDB)
	c.processed.Store(processedInDB)
	c.dlq.Store(dlqInBroker)
	c.invalid.Store(dlqInBroker)
	c.received.Store(processedInDB + dlqInBroker)
}

func (c *Collector) IncReceived()  { c.received.Add(1) }
func (c *Collector) IncProcessed() { c.processed.Add(1) }
func (c *Collector) IncDuplicate() { c.duplicate.Add(1) }
func (c *Collector) IncInvalid()   { c.invalid.Add(1) }
func (c *Collector) IncDLQ()       { c.dlq.Add(1) }
func (c *Collector) IncRetried()   { c.retried.Add(1) }

func (c *Collector) GetSnapshot() Snapshot {
	return Snapshot{
		MessagesReceived:  c.received.Load(),
		MessagesProcessed: c.processed.Load(),
		MessagesDuplicate: c.duplicate.Load(),
		MessagesInvalid:   c.invalid.Load(),
		MessagesDLQ:       c.dlq.Load(),
		MessagesRetried:   c.retried.Load(),
		RestoredFromDB:    c.restoredAtStartup.Load(),
	}
}

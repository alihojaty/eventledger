package main

import (
	"log"

	"github.com/alihojaty/eventledger/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("config: %+v", cfg)
}

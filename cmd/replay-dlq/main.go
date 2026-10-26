// Command replay-dlq republishes dead-lettered records to the Bronze topic.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Monoji77/Run-Safe/internal/replay"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()
	n, err := replay.Replay(context.Background(), cl, "replay-dlq")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("replayed %d record(s) from weather.dlq to weather.bronze\n", n)
}

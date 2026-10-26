package fetcher

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher publishes records with an idempotent producer and waits for the broker's acknowledgement.
type KafkaPublisher struct{ cl *kgo.Client }

func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{cl: cl}, nil
}

func (k *KafkaPublisher) Publish(ctx context.Context, r Record) error {
	keys := make([]string, 0, len(r.Headers))
	for key := range r.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rec := &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	for _, key := range keys {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(r.Headers[key])})
	}
	return k.cl.ProduceSync(ctx, rec).FirstErr()
}

func (k *KafkaPublisher) Close() { k.cl.Close() }

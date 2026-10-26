// Package replay moves dead-lettered records back onto the Bronze topic once whatever rejected them is fixed.
package replay

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	dlqTopic    = "weather.dlq"
	bronzeTopic = "weather.bronze"
)

// sourceHeaders are the headers the Connect mapping needs; everything else on a DLQ record is diagnostic.
var sourceHeaders = map[string]bool{"source": true, "fetched_at": true, "payload_sha256": true}

// Replay republishes every record in weather.dlq that group has not yet replayed to weather.bronze,
// preserving key, value and the source headers and adding replayed_from_dlq_offset ("partition:offset").
// It commits group's offsets only after every produce is acknowledged, so a crash can replay a record
// twice (the gold upsert absorbs that) but never lose one. It returns how many records were replayed.
func Replay(ctx context.Context, cl *kgo.Client, group string) (int, error) {
	adm := kadm.NewClient(cl)
	starts, err := adm.ListStartOffsets(ctx, dlqTopic)
	if err != nil {
		return 0, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, dlqTopic)
	if err != nil {
		return 0, fmt.Errorf("list end offsets: %w", err)
	}
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return 0, fmt.Errorf("fetch committed offsets: %w", err)
	}

	from := map[int32]kgo.Offset{}
	end := map[int32]int64{}
	ends.Each(func(e kadm.ListedOffset) {
		start := int64(0)
		if s, ok := starts.Lookup(e.Topic, e.Partition); ok {
			start = s.Offset
		}
		if c, ok := committed.Lookup(e.Topic, e.Partition); ok && c.Err == nil && c.At > start {
			start = c.At
		}
		if start < e.Offset {
			from[e.Partition] = kgo.NewOffset().At(start)
			end[e.Partition] = e.Offset
		}
	})
	if len(from) == 0 {
		return 0, nil
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cl.OptValue(kgo.SeedBrokers).([]string)...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{dlqTopic: from}),
	)
	if err != nil {
		return 0, fmt.Errorf("dlq consumer: %w", err)
	}
	defer consumer.Close()

	var out []*kgo.Record
	remaining := len(from)
	next := map[int32]int64{}
	for remaining > 0 {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return 0, fmt.Errorf("read dlq: %w", errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			limit, ok := end[r.Partition]
			if !ok || r.Offset >= limit {
				return
			}
			out = append(out, republish(r))
			next[r.Partition] = r.Offset + 1
			if r.Offset+1 == limit {
				remaining--
			}
		})
	}

	if err := cl.ProduceSync(ctx, out...).FirstErr(); err != nil {
		return 0, fmt.Errorf("republish to bronze: %w", err)
	}
	offs := kadm.Offsets{}
	for p, at := range next {
		offs.Add(kadm.Offset{Topic: dlqTopic, Partition: p, At: at, LeaderEpoch: -1})
	}
	if _, err := adm.CommitOffsets(ctx, group, offs); err != nil {
		return 0, fmt.Errorf("commit offsets: %w", err)
	}
	return len(out), nil
}

func republish(r *kgo.Record) *kgo.Record {
	rec := &kgo.Record{Topic: bronzeTopic, Key: r.Key, Value: r.Value}
	for _, h := range r.Headers {
		if sourceHeaders[h.Key] {
			rec.Headers = append(rec.Headers, h)
		}
	}
	rec.Headers = append(rec.Headers, kgo.RecordHeader{
		Key:   "replayed_from_dlq_offset",
		Value: []byte(strconv.Itoa(int(r.Partition)) + ":" + strconv.FormatInt(r.Offset, 10)),
	})
	return rec
}

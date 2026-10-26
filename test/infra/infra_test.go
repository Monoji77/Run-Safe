//go:build integration

package infra_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"gopkg.in/yaml.v3"

	"github.com/Monoji77/Run-Safe/test/harness"
)

func topicConfig(t *testing.T, topic, key string) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	adm := kadm.NewClient(harness.Kafka(t))
	cfgs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatalf("describe %s: %v", topic, err)
	}
	for _, rc := range cfgs {
		if rc.Err != nil {
			t.Fatalf("describe %s: %v", topic, rc.Err)
		}
		for _, c := range rc.Configs {
			if c.Key == key && c.Value != nil {
				return *c.Value, true
			}
		}
	}
	return "", false
}

func TestBronzeTopicHasInfiniteRetention(t *testing.T) {
	if v, ok := topicConfig(t, "weather.bronze", "retention.ms"); !ok || v != "-1" {
		t.Fatalf("weather.bronze retention.ms = %q (found=%v), want -1", v, ok)
	}
	if v, ok := topicConfig(t, "weather.bronze", "write.caching"); !ok || v != "false" {
		t.Fatalf("weather.bronze write.caching = %q (found=%v), want false", v, ok)
	}
}

func TestDLQTopicExists(t *testing.T) {
	if v, ok := topicConfig(t, "weather.dlq", "retention.ms"); !ok || v != "-1" {
		t.Fatalf("weather.dlq retention.ms = %q (found=%v), want -1", v, ok)
	}
}

func TestPostGISAvailable(t *testing.T) {
	var v string
	if err := harness.DB(t).QueryRow(context.Background(), "SELECT postgis_version()").Scan(&v); err != nil {
		t.Fatalf("postgis_version: %v", err)
	}
	if v == "" {
		t.Fatal("empty postgis_version")
	}
}

func adminAddr() string {
	if v := os.Getenv("RUNSAFE_ADMIN"); v != "" {
		return v
	}
	return "127.0.0.1:19644"
}

// Global constraint: Community Edition only. Redpanda's trial licence silently enables continuous
// balancing, an enterprise feature that is restricted once the trial ends; pin the Community values.
func TestRedpandaEnterpriseBalancingDisabled(t *testing.T) {
	resp, err := http.Get("http://" + adminAddr() + "/v1/cluster_config")
	if err != nil {
		t.Fatalf("admin api: %v", err)
	}
	defer resp.Body.Close()
	var cfg map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		t.Fatalf("decode cluster_config: %v", err)
	}
	if got := cfg["partition_autobalancing_mode"]; got != "node_add" {
		t.Errorf("partition_autobalancing_mode = %v, want node_add", got)
	}
	if got := cfg["core_balancing_continuous"]; got != false {
		t.Errorf("core_balancing_continuous = %v, want false", got)
	}
}

// Only the api service (added in a later task) may publish a host port in the main compose file.
func TestBrokerAndDatabaseNotPublishedByMainCompose(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Services map[string]struct {
			Ports []any `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Services) == 0 {
		t.Fatal("no services parsed from deploy/compose.yaml")
	}
	for name, svc := range cfg.Services {
		if len(svc.Ports) > 0 && name != "api" {
			t.Errorf("service %q publishes ports in the main compose file: %v", name, svc.Ports)
		}
	}
}

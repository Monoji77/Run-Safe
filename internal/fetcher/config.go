package fetcher

import (
	"fmt"
	"strings"
	"time"
)

// DefaultInterval is the polling interval used when FETCH_INTERVAL is unset.
const DefaultInterval = 60 * time.Second

type Config struct {
	Interval time.Duration
	APIKey   string
	Brokers  []string
	Topic    string
}

// LoadConfig reads FETCH_INTERVAL, DATA_GOV_SG_API_KEY (optional) and KAFKA_BROKERS (comma-separated, required).
func LoadConfig(getenv func(string) string) (Config, error) {
	c := Config{Interval: DefaultInterval, APIKey: getenv("DATA_GOV_SG_API_KEY"), Topic: "weather.bronze"}
	if v := getenv("FETCH_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("FETCH_INTERVAL %q must be a positive duration", v)
		}
		c.Interval = d
	}
	if v := getenv("KAFKA_BROKERS"); v != "" {
		c.Brokers = strings.Split(v, ",")
	} else {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
	}
	return c, nil
}

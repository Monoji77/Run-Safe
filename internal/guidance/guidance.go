// Package guidance turns readings into the official activity advice. It holds no scoring of its own:
// every verdict is a lookup in the versioned config.
package guidance

import (
	"fmt"
	"math"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Status is a headline. "unknown" means no reliable reading; it must never be shown as safe.
type Status string

const (
	StatusOK      Status = "ok"
	StatusCaution Status = "caution"
	StatusAvoid   Status = "avoid"
	StatusUnknown Status = "unknown"
)

type Advice struct {
	Headline Status `yaml:"headline"`
	Text     string `yaml:"text"`
}

// PM25Verdict is the answer for one reading. Status is the headline for a healthy person.
// Reason is set only when Status is unknown.
type PM25Verdict struct {
	Status     Status
	Band       string
	Healthy    Advice
	Vulnerable Advice
	Reason     string
}

type UVVerdict struct {
	Band                 string
	SunscreenRecommended bool
}

type Meta struct{ Version, Source, Retrieved string }

type pm25Band struct {
	Name       string   `yaml:"name"`
	Max        *float64 `yaml:"max"`
	Healthy    Advice   `yaml:"healthy"`
	Vulnerable Advice   `yaml:"vulnerable"`
}

type uvBand struct {
	Name string   `yaml:"name"`
	Max  *float64 `yaml:"max"`
}

type Config struct {
	meta       Meta
	staleAfter time.Duration
	pm25       []pm25Band
	uv         []uvBand
	sunscreen  float64
}

type fileConfig struct {
	Version    string `yaml:"version"`
	Source     string `yaml:"source"`
	Retrieved  string `yaml:"retrieved"`
	StaleAfter string `yaml:"stale_after"`
	PM25       struct {
		Bands []pm25Band `yaml:"bands"`
	} `yaml:"pm25"`
	UV struct {
		SunscreenFrom float64  `yaml:"sunscreen_from"`
		Bands         []uvBand `yaml:"bands"`
	} `yaml:"uv"`
}

// LoadConfig reads and validates a guidance file.
func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read guidance config: %w", err)
	}
	var f fileConfig
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return Config{}, fmt.Errorf("parse guidance config: %w", err)
	}
	stale, err := time.ParseDuration(f.StaleAfter)
	if err != nil || stale <= 0 {
		return Config{}, fmt.Errorf("stale_after %q must be a positive duration", f.StaleAfter)
	}
	if f.Version == "" || f.Source == "" || f.Retrieved == "" {
		return Config{}, fmt.Errorf("version, source and retrieved are required")
	}
	pmMax := make([]*float64, len(f.PM25.Bands))
	for i, b := range f.PM25.Bands {
		pmMax[i] = b.Max
	}
	if err := checkOrdered("pm25", pmMax); err != nil {
		return Config{}, err
	}
	uvMax := make([]*float64, len(f.UV.Bands))
	for i, b := range f.UV.Bands {
		uvMax[i] = b.Max
	}
	if err := checkOrdered("uv", uvMax); err != nil {
		return Config{}, err
	}
	return Config{
		meta:       Meta{Version: f.Version, Source: f.Source, Retrieved: f.Retrieved},
		staleAfter: stale,
		pm25:       f.PM25.Bands,
		uv:         f.UV.Bands,
		sunscreen:  f.UV.SunscreenFrom,
	}, nil
}

// checkOrdered requires ascending maxes with only the last band open-ended.
func checkOrdered(name string, maxes []*float64) error {
	if len(maxes) == 0 {
		return fmt.Errorf("%s: no bands", name)
	}
	prev := math.Inf(-1)
	for i, m := range maxes {
		last := i == len(maxes)-1
		switch {
		case last && m != nil:
			return fmt.Errorf("%s: last band must be open-ended", name)
		case !last && m == nil:
			return fmt.Errorf("%s: only the last band may omit max", name)
		case m != nil && *m <= prev:
			return fmt.Errorf("%s: band maxes must be ascending", name)
		case m != nil:
			prev = *m
		}
	}
	return nil
}

func (c Config) Meta() Meta                { return c.meta }
func (c Config) StaleAfter() time.Duration { return c.staleAfter }

// Unknown is the verdict when no reliable reading exists.
func Unknown(reason string) PM25Verdict {
	a := Advice{Headline: StatusUnknown, Text: "No reliable reading; do not assume it is safe."}
	return PM25Verdict{Status: StatusUnknown, Healthy: a, Vulnerable: a, Reason: reason}
}

// EvaluatePM25 maps a 1-hour PM2.5 reading (µg/m³) to the official band and advice.
// A band holds values <= its max, so 55.5 falls in the next band.
func (c Config) EvaluatePM25(value float64, observedAt, now time.Time) PM25Verdict {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return Unknown("invalid_value")
	}
	if now.Sub(observedAt) > c.staleAfter {
		return Unknown("stale")
	}
	for _, b := range c.pm25 {
		if b.Max == nil || value <= *b.Max {
			return PM25Verdict{Status: b.Healthy.Headline, Band: b.Name, Healthy: b.Healthy, Vulnerable: b.Vulnerable}
		}
	}
	return Unknown("invalid_value") // unreachable: the last band is open-ended
}

// EvaluateUV maps a UV index to its band and whether sunscreen is recommended.
func (c Config) EvaluateUV(index float64) UVVerdict {
	if math.IsNaN(index) || index < 0 {
		return UVVerdict{Band: "unknown"}
	}
	for _, b := range c.uv {
		if b.Max == nil || index <= *b.Max {
			return UVVerdict{Band: b.Name, SunscreenRecommended: index >= c.sunscreen}
		}
	}
	return UVVerdict{Band: "unknown"}
}

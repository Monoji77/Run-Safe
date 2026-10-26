package fetcher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type stamp struct {
	Timestamp        string `json:"timestamp"`
	UpdatedTimestamp string `json:"updatedTimestamp"`
}

type payload struct {
	Data struct {
		Items   []stamp `json:"items"`
		Records []stamp `json:"records"`
	} `json:"data"`
}

// stampOf extracts the record timestamps: data.items[0] for pm25 and psi, data.records[0] for uv.
func stampOf(endpoint string, body []byte) (stamp, bool) {
	var p payload
	if json.Unmarshal(body, &p) != nil {
		return stamp{}, false
	}
	list := p.Data.Items
	if endpoint == "uv" {
		list = p.Data.Records
	}
	if len(list) == 0 || list[0].Timestamp == "" || list[0].UpdatedTimestamp == "" {
		return stamp{}, false
	}
	return list[0], true
}

// Fingerprint identifies a response for dedupe. A record that is revised keeps its timestamp but gets a
// newer updatedTimestamp, so both are part of it. A body without them falls back to its SHA-256, so a
// persistently malformed response is published once and then deduped.
func Fingerprint(endpoint string, body []byte) string {
	if s, ok := stampOf(endpoint, body); ok {
		return s.Timestamp + "|" + s.UpdatedTimestamp
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// dataTimestamp is the observation timestamp used in the message key, or "unknown".
func dataTimestamp(endpoint string, body []byte) string {
	if s, ok := stampOf(endpoint, body); ok {
		return s.Timestamp
	}
	return "unknown"
}
